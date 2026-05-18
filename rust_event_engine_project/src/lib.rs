/*!
`rust_event_engine` 提供基于共享内存的多进程事件总线实现，并通过 PyO3
向 Python 暴露与 vn.py 对齐的事件接口。

# Python 对应关系

- [`Event`] ↔ `event/engine.py` 中的 `Event`
- [`MmapPublisher`] ↔ `event/engine.py` 中的 `MmapPublisher`
- [`MmapSubscriber`] ↔ `event/engine.py` 中的 `MmapSubscriber`
- [`EventEngine`] ↔ `event/engine.py` 中的 `EventEngine`

# 设计要点

1. 本地事件处理改为 `std::sync::mpsc` + 独立处理线程，减少 `asyncio` 调度开销。
2. 共享内存轮询阶段全程零 GIL，仅在 Python 反序列化和 handler 回调时进入 GIL。
3. 运行状态使用 `AtomicBool` 协调线程生命周期，降低线程间同步开销。
4. 共享内存读写保持与原实现一致的 seqlock 双重校验与 `EVENT_TICK` 广播语义。

*/

use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use log::{LevelFilter, error};
use pyo3::{
    exceptions::{PyIOError, PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBytes, PyModule},
};
use shared_memory::{Shmem, ShmemConf};

/// 定时器事件类型。
const EVENT_TIMER: &str = "eTimer.";
/// Tick 广播事件类型，优先通过共享内存进行跨进程传播。
const EVENT_TICK: &str = "eTick.";

const WORD_ALIGN: usize = 8;
const CACHE_LINE_SIZE: usize = 64;

const fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// 环形共享内存缓冲区的槽位数量。
///
/// 16,384 = 2^14，可在订阅端短暂落后时保留最新 16,384 条事件。
const NUM_SLOTS: usize = 16384;
const NUM_SLOTS_U64: u64 = NUM_SLOTS as u64;
/// 单个事件序列化后的最大负载大小。
const MAX_PAYLOAD: usize = 4 * 1024;

/// 共享内存布局标识：用于识别当前 Rust 版本创建的事件总线。
const SHM_MAGIC: u64 = 0x5255535445564255; // "RUSTEVBU"
/// 共享内存布局版本。修改 header/slot 布局时递增。
///
/// v5 将原来的全局 publisher/subscriber refcount + heartbeat 改为
/// per-client 槽位表，用于准确登记每个发布端/订阅端实例。
const SHM_LAYOUT_VERSION: u32 = 1;

/// 发布端/订阅端 heartbeat 更新周期。
const SHM_HEARTBEAT_INTERVAL_MS: u64 = 1_000;
/// 客户端 heartbeat 超时阈值。超过该时间未刷新时，新发布端/订阅端会把槽位视为崩溃遗留并回收。
const SHM_CLIENT_STALE_TIMEOUT_MS: u64 = SHM_HEARTBEAT_INTERVAL_MS * 30;
/// 写锁最大等待时间，避免上一个进程崩溃时永久卡死。
const WRITE_LOCK_TIMEOUT_MS: u64 = 2_000;

const CLIENT_ROLE_EMPTY: u32 = 0;
const CLIENT_ROLE_PUBLISHER: u32 = 1;
const CLIENT_ROLE_SUBSCRIBER: u32 = 2;
const CLIENT_SLOTS: usize = 128;
const CLIENT_SLOTS_U32: u32 = CLIENT_SLOTS as u32;

const C_ROLE: usize = 0;
const C_PID: usize = C_ROLE + std::mem::size_of::<u32>();
const C_REFCOUNT: usize = C_PID + std::mem::size_of::<u32>();
const C_RESERVED: usize = C_REFCOUNT + std::mem::size_of::<u32>();
const C_INSTANCE_ID: usize = align_up(C_RESERVED + std::mem::size_of::<u32>(), WORD_ALIGN);
const C_HEARTBEAT_MS: usize = C_INSTANCE_ID + std::mem::size_of::<u64>();
const CLIENT_SLOT_USED: usize = C_HEARTBEAT_MS + std::mem::size_of::<u64>();
const CLIENT_SLOT_SIZE: usize = align_up(CLIENT_SLOT_USED, CACHE_LINE_SIZE);
const CLIENT_SLOT_SIZE_U32: u32 = CLIENT_SLOT_SIZE as u32;

const H_MAGIC: usize = 0;
const H_LAYOUT_VERSION: usize = H_MAGIC + std::mem::size_of::<u64>();
const H_HEADER_SIZE_FIELD: usize = H_LAYOUT_VERSION + std::mem::size_of::<u32>();
const H_SLOT_SIZE_FIELD: usize = H_HEADER_SIZE_FIELD + std::mem::size_of::<u32>();
const H_NUM_SLOTS_FIELD: usize = H_SLOT_SIZE_FIELD + std::mem::size_of::<u32>();
const H_MAX_PAYLOAD_FIELD: usize = H_NUM_SLOTS_FIELD + std::mem::size_of::<u32>();

const H_WRITE_LOCK: usize = H_MAX_PAYLOAD_FIELD + std::mem::size_of::<u32>();
const H_WRITE_SEQ: usize = align_up(
    H_WRITE_LOCK + std::mem::size_of::<AtomicU32>(),
    WORD_ALIGN,
);
const H_EPOCH: usize = H_WRITE_SEQ + std::mem::size_of::<u64>();

// 发布端与订阅端分别登记到 client table，避免全局单 heartbeat 无法识别崩溃实例。
const H_OWNER_PID: usize = H_EPOCH + std::mem::size_of::<u64>();
const H_CLIENT_SLOTS_FIELD: usize = H_OWNER_PID + std::mem::size_of::<u32>();
const H_CLIENT_SLOT_SIZE_FIELD: usize = H_CLIENT_SLOTS_FIELD + std::mem::size_of::<u32>();
const H_CLIENT_TABLE: usize = align_up(
    H_CLIENT_SLOT_SIZE_FIELD + std::mem::size_of::<u32>(),
    CACHE_LINE_SIZE,
);

const HEADER_USED: usize = H_CLIENT_TABLE + CLIENT_SLOTS * CLIENT_SLOT_SIZE;
const HEADER_SIZE: usize = align_up(HEADER_USED, CACHE_LINE_SIZE);

const _: () = assert!(H_WRITE_LOCK % std::mem::align_of::<AtomicU32>() == 0);
const _: () = assert!(H_WRITE_SEQ % std::mem::align_of::<AtomicU64>() == 0);
const _: () = assert!(H_EPOCH % std::mem::align_of::<AtomicU64>() == 0);
const _: () = assert!(H_CLIENT_TABLE % CACHE_LINE_SIZE == 0);
const _: () = assert!(C_ROLE % std::mem::align_of::<AtomicU32>() == 0);
const _: () = assert!(C_PID % std::mem::align_of::<AtomicU32>() == 0);
const _: () = assert!(C_REFCOUNT % std::mem::align_of::<AtomicU32>() == 0);
const _: () = assert!(C_INSTANCE_ID % std::mem::align_of::<AtomicU64>() == 0);
const _: () = assert!(C_HEARTBEAT_MS % std::mem::align_of::<AtomicU64>() == 0);

const SLOT_SEQ_INVALID: u64 = 0;
const S_SEQ: usize = 0;
const S_SIZE: usize = S_SEQ + std::mem::size_of::<u64>();
const S_DATA: usize = S_SIZE + std::mem::size_of::<u32>();
const SLOT_OVERHEAD: usize = S_DATA;
/// 槽位大小按 8 字节对齐，保证每个槽位起点和 slot_seq 都是 8 字节对齐。
const SLOT_SIZE: usize = align_up(SLOT_OVERHEAD + MAX_PAYLOAD, WORD_ALIGN);
const SHM_TOTAL: usize = HEADER_SIZE + NUM_SLOTS * SLOT_SIZE;

const _: () = assert!(HEADER_SIZE >= HEADER_USED);
const _: () = assert!(S_DATA + MAX_PAYLOAD <= SLOT_SIZE);

const POLL_FAST_US: u64 = 100;
const POLL_MED_US: u64 = 500;
const POLL_IDLE_MS: u64 = 1;
const FAST_IDLE_WINDOW_US: u64 = 5_000;
const MED_IDLE_WINDOW_US: u64 = 20_000;
const IDLE_THRESH_FAST: u32 = (FAST_IDLE_WINDOW_US / POLL_FAST_US) as u32;
const IDLE_THRESH_MED: u32 = (MED_IDLE_WINDOW_US / POLL_MED_US) as u32;

/// 按 100,000 条/秒峰值估算：16,384 个槽位约覆盖 163 ms。
/// 重连/重开检查使用半个缓冲窗口，避免发布端启动或重建后订阅端睡太久。
const EXPECTED_PEAK_EVENTS_PER_SEC: u64 = 100_000;
const RAW_RECONNECT_SLEEP_MS: u64 =
    (NUM_SLOTS as u64 * 1_000) / EXPECTED_PEAK_EVENTS_PER_SEC / 2;
const RECONNECT_SLEEP_MS: u64 = if RAW_RECONNECT_SLEEP_MS < 10 {
    10
} else if RAW_RECONNECT_SLEEP_MS > 100 {
    100
} else {
    RAW_RECONNECT_SLEEP_MS
};

const STOP_CHECK_MS: u64 = 100;
const IDLE_REOPEN_CHECKS: u32 =
    ((RECONNECT_SLEEP_MS + POLL_IDLE_MS - 1) / POLL_IDLE_MS) as u32;

const WRITE_LOCK_BACKOFF_US: u64 = POLL_FAST_US / 2;
const SHM_MAX_NAME: usize = 31;

type HandlerVec = Vec<Py<PyAny>>;
type HandlerMap = HashMap<String, HandlerVec>;
type SharedHandlerMap = Arc<Mutex<HandlerMap>>;
type SharedHandlerVec = Arc<Mutex<HandlerVec>>;
type EventSender = Sender<Vec<u8>>;
type SharedSenderSlot = Arc<Mutex<Option<EventSender>>>;

static LOGGER_INIT: Once = Once::new();

/// 初始化一次性的错误日志记录器，确保后台线程中的异常可见。
fn init_error_logger() {
    LOGGER_INIT.call_once(|| {
        let _ = env_logger::Builder::from_default_env()
            .filter_level(LevelFilter::Error)
            .try_init();
    });
}

/// 对 [`Mutex`] 加锁，并将 poison 错误转换为 [`PyRuntimeError`]。
fn lock_mutex<'a, T>(mutex: &'a Mutex<T>, context: &str) -> PyResult<std::sync::MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|e| PyRuntimeError::new_err(format!("{context} 失败: {e}")))
}

/// 根据全局写序号计算共享内存槽位的起始偏移。
fn slot_base(seq: u64) -> Option<usize> {
    let slots = u64::try_from(NUM_SLOTS).ok()?;
    let index = usize::try_from(seq % slots).ok()?;
    HEADER_SIZE.checked_add(index.checked_mul(SLOT_SIZE)?)
}

fn read_u32_le(buf: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let slice = buf.get(offset..end)?;
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(slice);
    Some(u32::from_le_bytes(bytes))
}

fn write_u32_le(buf: &mut [u8], offset: usize, value: u32) -> Result<(), String> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| "u32 写入偏移溢出".to_string())?;
    let buf_len = buf.len();
    let dst = buf
        .get_mut(offset..end)
        .ok_or_else(|| format!("u32 写入越界: offset={offset}, end={end}, len={buf_len}"))?;
    dst.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn atomic_u64_ptr(buf: &[u8], offset: usize) -> Result<*const AtomicU64, String> {
    let end = offset
        .checked_add(std::mem::size_of::<AtomicU64>())
        .ok_or_else(|| "AtomicU64 偏移溢出".to_string())?;
    if end > buf.len() {
        return Err(format!(
            "AtomicU64 越界: offset={offset}, end={end}, len={}",
            buf.len()
        ));
    }

    let ptr = unsafe { buf.as_ptr().add(offset) };
    if (ptr as usize) % std::mem::align_of::<AtomicU64>() != 0 {
        return Err(format!("AtomicU64 未按 {} 字节对齐", std::mem::align_of::<AtomicU64>()));
    }

    Ok(ptr.cast::<AtomicU64>())
}

fn load_u64_atomic(buf: &[u8], offset: usize, ordering: Ordering) -> Result<u64, String> {
    let ptr = atomic_u64_ptr(buf, offset)?;
    // SAFETY: 指针来自已校验边界和对齐的共享内存映射，AtomicU64 只做原子读。
    Ok(unsafe { (*ptr).load(ordering) })
}

fn store_u64_atomic(
    buf: &mut [u8],
    offset: usize,
    value: u64,
    ordering: Ordering,
) -> Result<(), String> {
    let ptr = atomic_u64_ptr(buf, offset)?;
    // SAFETY: 指针来自已校验边界和对齐的共享内存映射，AtomicU64 只做原子写。
    unsafe {
        (*ptr).store(value, ordering);
    }
    Ok(())
}

fn store_u32_atomic(
    buf: &mut [u8],
    offset: usize,
    value: u32,
    ordering: Ordering,
) -> Result<(), String> {
    let ptr = atomic_u32_ptr(buf, offset)?;
    // SAFETY: 指针来自已校验边界和对齐的共享内存映射，AtomicU32 只做原子写。
    unsafe {
        (*ptr).store(value, ordering);
    }
    Ok(())
}


fn new_bus_epoch() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let pid = u64::from(std::process::id());
    nanos ^ pid.rotate_left(17) ^ 0x9E37_79B9_7F4A_7C15
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn atomic_u32_ptr(buf: &[u8], offset: usize) -> Result<*const AtomicU32, String> {
    let end = offset
        .checked_add(std::mem::size_of::<AtomicU32>())
        .ok_or_else(|| "AtomicU32 偏移溢出".to_string())?;
    if end > buf.len() {
        return Err(format!(
            "AtomicU32 越界: offset={offset}, end={end}, len={}",
            buf.len()
        ));
    }

    let ptr = unsafe { buf.as_ptr().add(offset) };
    if (ptr as usize) % std::mem::align_of::<AtomicU32>() != 0 {
        return Err(format!("AtomicU32 未按 {} 字节对齐", std::mem::align_of::<AtomicU32>()));
    }

    Ok(ptr.cast::<AtomicU32>())
}

fn load_u32_atomic(
    buf: &[u8],
    offset: usize,
    ordering: Ordering,
) -> Result<u32, String> {
    let ptr = atomic_u32_ptr(buf, offset)?;
    // SAFETY: 指针来自已校验边界和对齐的共享内存映射，AtomicU32 只做原子读。
    Ok(unsafe { (*ptr).load(ordering) })
}

fn validate_shmem_header(buf: &[u8], shm_name: &str) -> Result<(), String> {
    let magic = load_u64_atomic(buf, H_MAGIC, Ordering::Acquire)
        .map_err(|e| format!("读取共享内存 magic 失败: {e}"))?;

        if magic != SHM_MAGIC {
        return Err(format!(
            "共享内存 '{shm_name}' magic 不匹配: actual=0x{magic:016X}, expected=0x{SHM_MAGIC:016X}。             可能是旧版本遗留对象，或其它程序占用了同名共享内存。请关闭相关进程后手动清理，或更换 shm_name。"
        ));
    }

    let version = read_u32_le(buf, H_LAYOUT_VERSION)
        .ok_or_else(|| "读取共享内存 layout_version 失败".to_string())?;
    let header_size = read_u32_le(buf, H_HEADER_SIZE_FIELD)
        .ok_or_else(|| "读取共享内存 header_size 失败".to_string())?;
    let slot_size = read_u32_le(buf, H_SLOT_SIZE_FIELD)
        .ok_or_else(|| "读取共享内存 slot_size 失败".to_string())?;
    let num_slots = read_u32_le(buf, H_NUM_SLOTS_FIELD)
        .ok_or_else(|| "读取共享内存 num_slots 失败".to_string())?;
    let max_payload = read_u32_le(buf, H_MAX_PAYLOAD_FIELD)
        .ok_or_else(|| "读取共享内存 max_payload 失败".to_string())?;
    let client_slots = read_u32_le(buf, H_CLIENT_SLOTS_FIELD)
        .ok_or_else(|| "读取共享内存 client_slots 失败".to_string())?;
    let client_slot_size = read_u32_le(buf, H_CLIENT_SLOT_SIZE_FIELD)
        .ok_or_else(|| "读取共享内存 client_slot_size 失败".to_string())?;

    if version != SHM_LAYOUT_VERSION
        || header_size as usize != HEADER_SIZE
        || slot_size as usize != SLOT_SIZE
        || num_slots as usize != NUM_SLOTS
        || max_payload as usize != MAX_PAYLOAD
        || client_slots as usize != CLIENT_SLOTS
        || client_slot_size as usize != CLIENT_SLOT_SIZE
    {
        return Err(format!(
            "共享内存 '{shm_name}' 布局不兼容:              version={version}/{SHM_LAYOUT_VERSION},              header_size={header_size}/{HEADER_SIZE},              slot_size={slot_size}/{SLOT_SIZE},              num_slots={num_slots}/{NUM_SLOTS},              max_payload={max_payload}/{MAX_PAYLOAD},              client_slots={client_slots}/{CLIENT_SLOTS},              client_slot_size={client_slot_size}/{CLIENT_SLOT_SIZE}"
        ));
    }

    Ok(())
}

fn initialize_header_buf(buf: &mut [u8], keep_write_lock: bool) -> Result<(), String> {
    store_u64_atomic(buf, H_MAGIC, SHM_MAGIC, Ordering::Release)?;
    write_u32_le(buf, H_LAYOUT_VERSION, SHM_LAYOUT_VERSION)?;
    write_u32_le(buf, H_HEADER_SIZE_FIELD, HEADER_SIZE as u32)?;
    write_u32_le(buf, H_SLOT_SIZE_FIELD, SLOT_SIZE as u32)?;
    write_u32_le(buf, H_NUM_SLOTS_FIELD, NUM_SLOTS as u32)?;
    write_u32_le(buf, H_MAX_PAYLOAD_FIELD, MAX_PAYLOAD as u32)?;
    write_u32_le(buf, H_CLIENT_SLOTS_FIELD, CLIENT_SLOTS_U32)?;
    write_u32_le(buf, H_CLIENT_SLOT_SIZE_FIELD, CLIENT_SLOT_SIZE_U32)?;

    if !keep_write_lock {
        store_u32_atomic(buf, H_WRITE_LOCK, 0, Ordering::Release)?;
    }

    store_u64_atomic(buf, H_WRITE_SEQ, 0, Ordering::Release)?;
    store_u64_atomic(buf, H_EPOCH, new_bus_epoch(), Ordering::Release)?;
    write_u32_le(buf, H_OWNER_PID, std::process::id())?;

    // 新 header 初始化时清空 client table。每个 publisher/subscriber 后续会单独登记。
    for index in 0..CLIENT_SLOTS {
        clear_client_slot(buf, index)?;
    }

    Ok(())
}

struct ShmemWriteGuard {
    lock: *const AtomicU32,
}

impl Drop for ShmemWriteGuard {
    fn drop(&mut self) {
        // SAFETY: lock 指向共享内存头部 H_WRITE_LOCK；guard 生命周期内映射保持有效。
        unsafe {
            (*self.lock).store(0, Ordering::Release);
        }
    }
}

fn acquire_shmem_write_lock(buf: &mut [u8]) -> Result<ShmemWriteGuard, String> {
    let lock = atomic_u32_ptr(buf, H_WRITE_LOCK)?;
    let start = Instant::now();

    while unsafe {
        (*lock)
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
    } {
        if start.elapsed() >= Duration::from_millis(WRITE_LOCK_TIMEOUT_MS) {
            return Err(format!(
                "获取共享内存写锁超时，可能有进程崩溃时持有锁未释放，timeout={}ms",
                WRITE_LOCK_TIMEOUT_MS
            ));
        }

        thread::sleep(Duration::from_micros(WRITE_LOCK_BACKOFF_US));
    }

    Ok(ShmemWriteGuard { lock })
}

fn read_header(buf: &[u8]) -> Result<(u64, u64), String> {
    let write_seq = load_u64_atomic(buf, H_WRITE_SEQ, Ordering::Acquire)
        .map_err(|e| format!("读取写指针失败: {e}"))?;
    let epoch = load_u64_atomic(buf, H_EPOCH, Ordering::Acquire)
        .map_err(|e| format!("读取共享内存 epoch 失败: {e}"))?;
    Ok((write_seq, epoch))
}

fn is_not_found_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("not found")
        || lower.contains("no such file")
        || lower.contains("cannot find")
        || lower.contains("os error 2")
}

fn is_publisher_closed_error(message: &str) -> bool {
    message.contains("共享内存发布器已关闭")
        || message.contains("共享内存发布通道已关闭")
}

/// 使用 Python `pickle.dumps` 将对象序列化为字节流。
fn py_serialize(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    py.import("pickle")
        .map_err(|e| PyRuntimeError::new_err(format!("导入 pickle 失败: {e}")))?
        .call_method1("dumps", (obj, 5_u8))
        .map_err(|e| PyRuntimeError::new_err(format!("pickle.dumps 失败: {e}")))?
        .extract::<Vec<u8>>()
        .map_err(|e| PyRuntimeError::new_err(format!("提取 pickle 字节失败: {e}")))
}

/// 使用 Python `pickle.loads` 将字节流恢复为 Python 对象。
fn py_deserialize<'py>(py: Python<'py>, raw: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    py.import("pickle")
        .map_err(|e| PyRuntimeError::new_err(format!("导入 pickle 失败: {e}")))?
        .call_method1("loads", (PyBytes::new(py, raw),))
        .map_err(|e| PyRuntimeError::new_err(format!("pickle.loads 失败: {e}")))
}

/// 校验给定 Python 对象是否可调用。
fn ensure_callable(value: &Bound<'_, PyAny>, name: &str) -> PyResult<()> {
    if value.is_callable() {
        Ok(())
    } else {
        Err(PyTypeError::new_err(format!("{name} 必须是可调用对象")))
    }
}

fn is_same_handler(a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> bool {
    a.as_ptr() == b.as_ptr() || a.eq(b).unwrap_or(false)
}

fn is_handler_lifecycle_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("cannot schedule new futures after shutdown")
        || lower.contains("cannot schedule new futures after interpreter shutdown")
        || lower.contains("event loop is closed")
}

/// 从事件对象的 `type_` 属性或映射键中提取事件类型。
fn extract_type(event: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(value) = event.getattr("type_") {
        return value
            .extract::<String>()
            .map_err(|e| PyTypeError::new_err(format!("事件属性 type_ 类型错误: {e}")));
    }

    if let Ok(value) = event.get_item("type_") {
        return value
            .extract::<String>()
            .map_err(|e| PyTypeError::new_err(format!("事件键 type_ 类型错误: {e}")));
    }

    Err(PyTypeError::new_err(
        "事件对象必须提供 type_ 属性或 type_ 键",
    ))
}

fn clone_handler_list(py: Python<'_>, handlers: &SharedHandlerMap, type_: &str) -> Vec<Py<PyAny>> {
    match handlers.lock() {
        Ok(map) => map
            .get(type_)
            .map(|items| items.iter().map(|item| item.clone_ref(py)).collect())
            .unwrap_or_default(),
        Err(e) => {
            error!("[EventEngine] handler 锁失败: {e}");
            Vec::new()
        }
    }
}

fn clone_general_handlers(py: Python<'_>, handlers: &SharedHandlerVec) -> Vec<Py<PyAny>> {
    match handlers.lock() {
        Ok(items) => items.iter().map(|item| item.clone_ref(py)).collect(),
        Err(e) => {
            error!("[EventEngine] general handler 锁失败: {e}");
            Vec::new()
        }
    }
}

fn remove_specific_handler_by_identity(
    py: Python<'_>,
    handlers: &SharedHandlerMap,
    type_: &str,
    target: &Py<PyAny>,
) {
    let target_ptr = target.bind(py).as_ptr();
    match handlers.lock() {
        Ok(mut map) => {
            let mut should_remove_type = false;
            if let Some(items) = map.get_mut(type_) {
                items.retain(|item| item.bind(py).as_ptr() != target_ptr);
                should_remove_type = items.is_empty();
            }
            if should_remove_type {
                map.remove(type_);
            }
        }
        Err(e) => error!("[EventEngine] 注销失效 handler 锁失败: {e}"),
    }
}

fn remove_general_handler_by_identity(
    py: Python<'_>,
    handlers: &SharedHandlerVec,
    target: &Py<PyAny>,
) {
    let target_ptr = target.bind(py).as_ptr();
    match handlers.lock() {
        Ok(mut items) => items.retain(|item| item.bind(py).as_ptr() != target_ptr),
        Err(e) => error!("[EventEngine] 注销失效 general handler 锁失败: {e}"),
    }
}

/// 按"类型处理器 → 通用处理器"的顺序分发事件。
///
/// 单个处理器抛出的异常会被记录，但不会影响后续处理器执行。
fn dispatch_event_isolated(
    py: Python<'_>,
    event: &Bound<'_, PyAny>,
    handlers: &SharedHandlerMap,
    general_handlers: &SharedHandlerVec,
) {
    let type_ = match extract_type(event) {
        Ok(type_) if !type_.is_empty() => type_,
        Ok(_) => {
            error!("[EventEngine] handler 跳过空事件类型");
            return;
        }
        Err(e) => {
            error!("[EventEngine] 提取事件类型失败: {e}");
            return;
        }
    };

    let specific_handlers = clone_handler_list(py, handlers, &type_);
    for handler in specific_handlers {
        if let Err(e) = handler.call1(py, (event,)) {
            let error_text = e.to_string();
            if is_handler_lifecycle_error(&error_text) {
                remove_specific_handler_by_identity(py, handlers, &type_, &handler);
                // error!(
                //     "[EventEngine] handler 生命周期已结束，已自动注销 type={type_}: {error_text}"
                // );
            } else {
                error!("[EventEngine] handler 异常 type={type_}: {error_text}");
            }
        }
    }

    let general = clone_general_handlers(py, general_handlers);
    for handler in general {
        if let Err(e) = handler.call1(py, (event,)) {
            let error_text = e.to_string();
            if is_handler_lifecycle_error(&error_text) {
                remove_general_handler_by_identity(py, general_handlers, &handler);
                // error!("[EventEngine] general_handler 生命周期已结束，已自动注销: {error_text}");
            } else {
                error!("[EventEngine] general_handler 异常: {error_text}");
            }
        }
    }
}

/// 等待线程退出，并将 panic 转换为 Python 运行时异常。
fn join_handle(handle: JoinHandle<()>, name: &str) -> PyResult<()> {
    if handle.thread().id() == thread::current().id() {
        error!("[{name}] 跳过当前线程 join，避免自 join 死锁");
        return Ok(());
    }

    handle
        .join()
        .map_err(|_| PyRuntimeError::new_err(format!("{name} 线程异常退出")))
}

/// 返回共享内存的只读视图，并校验映射大小满足总线布局要求。
fn shmem_slice(shmem: &Shmem) -> Result<&[u8], String> {
    // SAFETY: `shared_memory::Shmem` 保证映射区域在 `shmem` 生命周期内有效，
    // 此处只创建共享借用且不会越过 `shmem` 的生命周期使用。
    let slice = unsafe { shmem.as_slice() };
    if slice.len() < SHM_TOTAL {
        return Err(format!(
            "共享内存只读大小不足: actual={}, expected={SHM_TOTAL}",
            slice.len()
        ));
    }
    Ok(&slice[..SHM_TOTAL])
}

/// 返回共享内存的可写视图，并校验映射大小满足总线布局要求。
fn shmem_slice_mut(shmem: &mut Shmem) -> Result<&mut [u8], String> {
    // SAFETY: 这里持有 `&mut Shmem`，Rust 保证该 handle 没有其它活动的可变借用；
    // 返回切片仅在当前映射生命周期内使用，并且被截断到固定的 `SHM_TOTAL` 范围。
    let slice = unsafe { shmem.as_slice_mut() };
    if slice.len() < SHM_TOTAL {
        return Err(format!(
            "共享内存可写大小不足: actual={}, expected={SHM_TOTAL}",
            slice.len()
        ));
    }
    Ok(&mut slice[..SHM_TOTAL])
}

/// 初始化共享内存头部。
fn initialize_header(shmem: &mut Shmem) -> Result<(), String> {
    let buf = shmem_slice_mut(shmem)?;
    initialize_header_buf(buf, false)
}


fn is_already_exists_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("already exists")
        || lower.contains("os error 17")   // Unix EEXIST
        || lower.contains("os error 183")  // Windows ERROR_ALREADY_EXISTS
        || lower.contains("error 183")
        || lower.contains("已存在")
}

fn overwrite_shmem_contents(shmem: &mut Shmem) -> Result<(), String> {
    {
        let buf = shmem_slice_mut(shmem)?;

        // 初始化新建共享内存时先整体清零，使所有槽位的 slot_seq 均为 SLOT_SEQ_INVALID，
        // 再写入 header，避免订阅端读到未初始化数据。
        buf.fill(0);
    }

    initialize_header(shmem)
}



fn create_initialized_publisher_shmem(
    shm_name: &str,
    publisher_instance_id: u64,
) -> Result<(Shmem, ShmemClientRegistration), String> {
    let mut shmem = ShmemConf::new()
        .size(SHM_TOTAL)
        .os_id(shm_name)
        .create()
        .map_err(|e| format!("创建共享内存 '{shm_name}' 失败: {e}"))?;

    overwrite_shmem_contents(&mut shmem)?;
    // 共享内存对象的内容生命周期由 client table + heartbeat 管理；
    // 这里保持非 owner，避免某个 handle drop 时误 unlink 其它进程仍在使用的命名对象。
    shmem.set_owner(false);

    let client = register_client(
        &mut shmem,
        shm_name,
        CLIENT_ROLE_PUBLISHER,
        publisher_instance_id,
    )?;
    Ok((shmem, client))
}

fn normalize_plain_shmem_name(shm_name: &str) -> Option<&str> {
    // shared_memory-rs 在 Windows/Linux 都会把 os_id 当成命名对象名或缓存文件名使用。
    // 这里只接受普通名称，避免 shm_name 中带路径分隔符时误删任意文件。
    let normalized = shm_name
        .trim_start_matches('/')
        .trim_start_matches('\\');

    if normalized.is_empty()
        || normalized.contains('/')
        || normalized.contains('\\')
        || normalized.contains(':')
    {
        return None;
    }

    Some(normalized)
}

#[cfg(target_os = "linux")]
fn local_shmem_cache_path(shm_name: &str) -> Option<PathBuf> {
    // shared_memory 在 Linux 下最终走 POSIX shm_open，命名对象通常表现为 /dev/shm/<name>。
    normalize_plain_shmem_name(shm_name)
        .map(|normalized| PathBuf::from("/dev/shm").join(normalized))
}

#[cfg(target_os = "windows")]
fn local_shmem_cache_path(shm_name: &str) -> Option<PathBuf> {
    // shared_memory-rs 当前 Windows 持久化实现会在系统临时目录下创建 shared_memory-rs 子目录，
    // 例如 C:\Users\<user>\AppData\Local\Temp\shared_memory-rs\<os_id>。
    normalize_plain_shmem_name(shm_name)
        .map(|normalized| std::env::temp_dir().join("shared_memory-rs").join(normalized))
}

#[cfg(all(not(target_os = "linux"), not(target_os = "windows")))]
fn local_shmem_cache_path(_shm_name: &str) -> Option<PathBuf> {
    None
}

fn remove_local_shmem_cache_file(shm_name: &str) -> Result<bool, String> {
    let Some(path) = local_shmem_cache_path(shm_name) else {
        return Ok(false);
    };

    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(format!(
            "删除本地共享内存缓存文件 '{}' 失败: {err}",
            path.display()
        )),
    }
}

fn cleanup_cached_shmem_mapping(shm_name: &str) -> Result<String, String> {
    let mut actions = Vec::new();
    let mut errors = Vec::new();

    // 优先通过 shared_memory 自己的 owner drop 路径释放命名对象。
    // Linux 下这会触发 shm_unlink；如果对象内容损坏但仍能打开，这比直接删文件更干净。
    match ShmemConf::new().os_id(shm_name).open() {
        Ok(mut stale) => {
            stale.set_owner(true);
            drop(stale);
            actions.push("已通过 shared_memory owner drop 释放残留映射".to_string());
        }
        Err(err) => {
            errors.push(format!("通过 os_id 打开残留映射失败: {err}"));
        }
    }

    match remove_local_shmem_cache_file(shm_name) {
        Ok(true) => actions.push("已删除本地共享内存缓存文件".to_string()),
        Ok(false) => {}
        Err(err) => errors.push(err),
    }

    if !actions.is_empty() {
        Ok(actions.join("；"))
    } else {
        Err(format!(
            "未能移除共享内存 '{shm_name}' 的残留对象/缓存文件: {}",
            errors.join("；")
        ))
    }
}

fn recreate_shmem_after_cleanup(
    shm_name: &str,
    publisher_instance_id: u64,
    create_err: &str,
    reuse_err: &str,
) -> Result<(Shmem, ShmemClientRegistration), String> {
    let cleanup_action = cleanup_cached_shmem_mapping(shm_name).map_err(|cleanup_err| {
        format!(
            "共享内存 '{shm_name}' 创建失败且复用失败；清理残留对象/缓存文件也失败。\
             create_err={create_err}; reuse_err={reuse_err}; cleanup_err={cleanup_err}"
        )
    })?;

    match create_initialized_publisher_shmem(shm_name, publisher_instance_id) {
        Ok(pair) => Ok(pair),
        Err(recreate_err) => Err(format!(
            "共享内存 '{shm_name}' 创建失败且复用失败；{cleanup_action}，但重新创建仍失败。\
             create_err={create_err}; reuse_err={reuse_err}; recreate_err={recreate_err}"
        )),
    }
}

fn open_existing_shmem_for_publisher(
    shm_name: &str,
    publisher_instance_id: u64,
) -> Result<(Shmem, ShmemClientRegistration), String> {
    let mut shmem = ShmemConf::new()
        .os_id(shm_name)
        .open()
        .map_err(|e| format!("共享内存 '{shm_name}' 已存在，但打开失败: {e}"))?;

    shmem.set_owner(false);

    let actual = shmem.len();
    if actual < SHM_TOTAL {
        return Err(format!(
            "共享内存 '{shm_name}' 已存在，但大小不足: actual={actual}, expected={SHM_TOTAL}。\
             这通常表示旧 layout 版本的残留对象，将尝试清理本地缓存后重建。"
        ));
    }

    let client = {
        let buf = shmem_slice_mut(&mut shmem)?;
        validate_shmem_header(buf, shm_name).map_err(|e| {
            format!(
                "共享内存 '{shm_name}' 已存在，但布局不兼容或无法复用: {e}。\
                 将尝试清理本地缓存后重建。"
            )
        })?;
        let _guard = acquire_shmem_write_lock(buf)?;
        let active_clients = cleanup_stale_clients_locked(buf, now_millis())?;
        if active_clients == 0 {
            reset_bus_contents_under_lock(buf)?;
        }
        register_client_locked(buf, CLIENT_ROLE_PUBLISHER, publisher_instance_id)?
    };

    Ok((shmem, client))
}

#[derive(Clone, Copy, Debug)]
struct ShmemClientRegistration {
    role: u32,
    slot_index: u32,
    instance_id: u64,
}

struct SubscriberMapping {
    shmem: Shmem,
    client: ShmemClientRegistration,
}

fn new_client_instance_id(role: u32) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let pid = u64::from(std::process::id());
    nanos ^ pid.rotate_left(17) ^ (u64::from(role) << 56) ^ 0xD6E8_FD9A_7A4C_15F3
}

fn client_slot_base(index: usize) -> Result<usize, String> {
    if index >= CLIENT_SLOTS {
        return Err(format!("client 槽位越界: index={index}, slots={CLIENT_SLOTS}"));
    }
    H_CLIENT_TABLE
        .checked_add(index.checked_mul(CLIENT_SLOT_SIZE).ok_or_else(|| "client 槽位偏移溢出".to_string())?)
        .ok_or_else(|| "client 槽位偏移溢出".to_string())
}

fn client_slot_field(index: usize, field: usize) -> Result<usize, String> {
    client_slot_base(index)?
        .checked_add(field)
        .ok_or_else(|| "client 字段偏移溢出".to_string())
}

fn client_role_name(role: u32) -> &'static str {
    match role {
        CLIENT_ROLE_PUBLISHER => "publisher",
        CLIENT_ROLE_SUBSCRIBER => "subscriber",
        CLIENT_ROLE_EMPTY => "empty",
        _ => "unknown",
    }
}

#[cfg(target_os = "linux")]
fn process_may_be_alive(pid: u32) -> bool {
    pid != 0 && std::path::Path::new(&format!("/proc/{pid}")).exists()
}

#[cfg(not(target_os = "linux"))]
fn process_may_be_alive(_pid: u32) -> bool {
    // 非 Linux 平台不依赖 pid 探测，统一用 heartbeat 超时回收。
    true
}

fn clear_client_slot(buf: &mut [u8], index: usize) -> Result<(), String> {
    store_u32_atomic(buf, client_slot_field(index, C_REFCOUNT)?, 0, Ordering::Release)?;
    store_u64_atomic(buf, client_slot_field(index, C_HEARTBEAT_MS)?, 0, Ordering::Release)?;
    store_u64_atomic(buf, client_slot_field(index, C_INSTANCE_ID)?, 0, Ordering::Release)?;
    store_u32_atomic(buf, client_slot_field(index, C_PID)?, 0, Ordering::Release)?;
    store_u32_atomic(buf, client_slot_field(index, C_ROLE)?, CLIENT_ROLE_EMPTY, Ordering::Release)?;
    Ok(())
}

fn client_slot_matches(
    buf: &[u8],
    index: usize,
    client: &ShmemClientRegistration,
) -> Result<bool, String> {
    let role = load_u32_atomic(buf, client_slot_field(index, C_ROLE)?, Ordering::Acquire)?;
    let instance_id = load_u64_atomic(buf, client_slot_field(index, C_INSTANCE_ID)?, Ordering::Acquire)?;
    Ok(role == client.role && instance_id == client.instance_id)
}

fn cleanup_stale_clients_locked(buf: &mut [u8], now_ms: u64) -> Result<usize, String> {
    let mut active_clients = 0_usize;
    let current_pid = std::process::id();

    for index in 0..CLIENT_SLOTS {
        let role = load_u32_atomic(buf, client_slot_field(index, C_ROLE)?, Ordering::Acquire)?;
        if role == CLIENT_ROLE_EMPTY {
            continue;
        }

        let refcount = load_u32_atomic(buf, client_slot_field(index, C_REFCOUNT)?, Ordering::Acquire)?;
        let pid = load_u32_atomic(buf, client_slot_field(index, C_PID)?, Ordering::Acquire)?;
        let heartbeat = load_u64_atomic(buf, client_slot_field(index, C_HEARTBEAT_MS)?, Ordering::Acquire)?;
        let heartbeat_stale = heartbeat == 0
            || now_ms.saturating_sub(heartbeat) > SHM_CLIENT_STALE_TIMEOUT_MS;
        let pid_dead = pid != current_pid && !process_may_be_alive(pid);

        if refcount == 0 || heartbeat_stale || pid_dead {
            clear_client_slot(buf, index)?;
        } else {
            active_clients = active_clients.saturating_add(1);
        }
    }

    Ok(active_clients)
}

fn reset_bus_contents_under_lock(buf: &mut [u8]) -> Result<(), String> {
    let lock_start = H_WRITE_LOCK;
    let lock_end = H_WRITE_LOCK + std::mem::size_of::<AtomicU32>();

    // 保留当前写锁值，避免清零期间其它进程抢入看到半初始化 header。
    for index in 0..buf.len() {
        if index < lock_start || index >= lock_end {
            buf[index] = 0;
        }
    }

    initialize_header_buf(buf, true)
}

fn register_client_locked(
    buf: &mut [u8],
    role: u32,
    instance_id: u64,
) -> Result<ShmemClientRegistration, String> {
    let now_ms = now_millis();
    let _ = cleanup_stale_clients_locked(buf, now_ms)?;

    // 同一实例重复打开同一个命名对象时复用槽位并增加 refcount。
    for index in 0..CLIENT_SLOTS {
        let slot_role = load_u32_atomic(buf, client_slot_field(index, C_ROLE)?, Ordering::Acquire)?;
        if slot_role != role {
            continue;
        }

        let slot_instance_id = load_u64_atomic(buf, client_slot_field(index, C_INSTANCE_ID)?, Ordering::Acquire)?;
        if slot_instance_id != instance_id {
            continue;
        }

        let refcount = load_u32_atomic(buf, client_slot_field(index, C_REFCOUNT)?, Ordering::Acquire)?;
        let next = refcount
            .checked_add(1)
            .ok_or_else(|| format!("共享内存 {} 引用计数溢出", client_role_name(role)))?;
        store_u32_atomic(buf, client_slot_field(index, C_REFCOUNT)?, next, Ordering::Release)?;
        store_u64_atomic(buf, client_slot_field(index, C_HEARTBEAT_MS)?, now_ms, Ordering::Release)?;
        return Ok(ShmemClientRegistration {
            role,
            slot_index: index as u32,
            instance_id,
        });
    }

    for index in 0..CLIENT_SLOTS {
        let slot_role = load_u32_atomic(buf, client_slot_field(index, C_ROLE)?, Ordering::Acquire)?;
        if slot_role != CLIENT_ROLE_EMPTY {
            continue;
        }

        // 先写 role=EMPTY 以外的字段，最后发布 role，避免其它进程看到半初始化槽位。
        store_u32_atomic(buf, client_slot_field(index, C_ROLE)?, CLIENT_ROLE_EMPTY, Ordering::Release)?;
        store_u32_atomic(buf, client_slot_field(index, C_PID)?, std::process::id(), Ordering::Release)?;
        store_u64_atomic(buf, client_slot_field(index, C_INSTANCE_ID)?, instance_id, Ordering::Release)?;
        store_u64_atomic(buf, client_slot_field(index, C_HEARTBEAT_MS)?, now_ms, Ordering::Release)?;
        store_u32_atomic(buf, client_slot_field(index, C_REFCOUNT)?, 1, Ordering::Release)?;
        store_u32_atomic(buf, client_slot_field(index, C_ROLE)?, role, Ordering::Release)?;
        return Ok(ShmemClientRegistration {
            role,
            slot_index: index as u32,
            instance_id,
        });
    }

    Err(format!(
        "共享内存 client table 已满: role={}, slots={CLIENT_SLOTS}",
        client_role_name(role)
    ))
}

fn register_client(
    shmem: &mut Shmem,
    shm_name: &str,
    role: u32,
    instance_id: u64,
) -> Result<ShmemClientRegistration, String> {
    let buf = shmem_slice_mut(shmem)?;
    validate_shmem_header(buf, shm_name)?;
    let _guard = acquire_shmem_write_lock(buf)?;
    register_client_locked(buf, role, instance_id)
}

fn release_client_ref(
    shmem: &mut Shmem,
    client: &ShmemClientRegistration,
) -> Result<(), String> {
    let buf = shmem_slice_mut(shmem)?;
    let _guard = acquire_shmem_write_lock(buf)?;
    let index = client.slot_index as usize;

    if !client_slot_matches(buf, index, client)? {
        // 可能已经被其它进程按 heartbeat 超时回收。释放路径保持幂等。
        return Ok(());
    }

    let refcount = load_u32_atomic(buf, client_slot_field(index, C_REFCOUNT)?, Ordering::Acquire)?;
    if refcount <= 1 {
        clear_client_slot(buf, index)?;
    } else {
        store_u32_atomic(buf, client_slot_field(index, C_REFCOUNT)?, refcount - 1, Ordering::Release)?;
        store_u64_atomic(buf, client_slot_field(index, C_HEARTBEAT_MS)?, now_millis(), Ordering::Release)?;
    }

    Ok(())
}

fn update_client_heartbeat(
    shmem: &mut Shmem,
    client: &ShmemClientRegistration,
) -> Result<(), String> {
    let buf = shmem_slice_mut(shmem)?;
    let index = client.slot_index as usize;

    if !client_slot_matches(buf, index, client)? {
        return Err(format!(
            "共享内存 {} 心跳槽位失效: slot={}, instance_id={}",
            client_role_name(client.role),
            client.slot_index,
            client.instance_id
        ));
    }

    store_u64_atomic(buf, client_slot_field(index, C_HEARTBEAT_MS)?, now_millis(), Ordering::Release)
}

fn update_client_heartbeat_locked(
    buf: &mut [u8],
    client: &ShmemClientRegistration,
) -> Result<(), String> {
    let index = client.slot_index as usize;

    if !client_slot_matches(buf, index, client)? {
        return Err(format!(
            "共享内存 {} 心跳槽位失效: slot={}, instance_id={}",
            client_role_name(client.role),
            client.slot_index,
            client.instance_id
        ));
    }

    store_u64_atomic(buf, client_slot_field(index, C_HEARTBEAT_MS)?, now_millis(), Ordering::Release)
}

/// 创建或复用共享内存段。
///
/// 语义：
/// - 首次创建成功：初始化 header，并在 client table 中登记当前发布端。
/// - 名称已存在且布局兼容：清理超时/死亡 client 槽位后登记当前发布端。
/// - 若清理后没有任何活跃 client，说明该命名对象只是历史残留，直接在写锁内重置 header 和环形缓冲区。
/// - 名称已存在但布局不兼容：返回明确错误，交由运维清理或更换 shm_name，避免误删其它程序对象。
fn create_shmem(
    shm_name: &str,
    publisher_instance_id: u64,
) -> Result<(Shmem, ShmemClientRegistration), String> {
    let create_result = ShmemConf::new()
        .size(SHM_TOTAL)
        .os_id(shm_name)
        .create();

    match create_result {
        Ok(mut shmem) => {
            overwrite_shmem_contents(&mut shmem)?;
            // 共享内存对象的内容生命周期由 client table + heartbeat 管理；
            // 这里保持非 owner，避免某个 handle drop 时误 unlink 其它进程仍在使用的命名对象。
            shmem.set_owner(false);
            let client = register_client(
                &mut shmem,
                shm_name,
                CLIENT_ROLE_PUBLISHER,
                publisher_instance_id,
            )?;
            Ok((shmem, client))
        }

        Err(err) => {
            let create_err = err.to_string();

            if !is_already_exists_error(&create_err) {
                return Err(format!("创建共享内存 '{shm_name}' 失败: {create_err}"));
            }

            match open_existing_shmem_for_publisher(shm_name, publisher_instance_id) {
                Ok(pair) => Ok(pair),
                Err(reuse_err) => recreate_shmem_after_cleanup(
                    shm_name,
                    publisher_instance_id,
                    &create_err,
                    &reuse_err,
                ),
            }
        }
    }
}

/// 以 seqlock 双重校验方式读取指定槽位中的事件负载。
fn read_slot(buf: &[u8], seq: u64) -> Option<Vec<u8>> {
    let base = slot_base(seq)?;

    // 使用真正的原子 Acquire 读取 slot_seq，不能只用 fence + 普通字节读取；
    // 否则跨进程读写时可能读到撕裂序号或乱序 payload。
    let seq_offset = base.checked_add(S_SEQ)?;
    let slot_seq = load_u64_atomic(buf, seq_offset, Ordering::Acquire).ok()?;
    if slot_seq != seq {
        return None;
    }

    let size = usize::try_from(read_u32_le(buf, base.checked_add(S_SIZE)?)?).ok()?;
    if size == 0 || size > MAX_PAYLOAD {
        return None;
    }

    let start = base.checked_add(S_DATA)?;
    let end = start.checked_add(size)?;
    let payload = buf.get(start..end)?.to_vec();

    // 二次校验。如果写端复用了同一槽位，会先把 slot_seq 原子置为 INVALID，
    // 再写 payload，最后原子发布新 seq；这里能检测到复用/覆盖。
    let slot_seq2 = load_u64_atomic(buf, seq_offset, Ordering::Acquire).ok()?;
    if slot_seq2 != seq {
        return None;
    }

    Some(payload)
}

fn call_log_callback(callback: &Option<Py<PyAny>>, message: &str) {
    if let Some(callback) = callback {
        let _ = Python::try_attach(|py| {
            if let Err(e) = callback.call1(py, (message.to_string(),)) {
                error!("[MmapSubscriber] 调用 log_exception 失败: {e}");
            }
        });
    }
}

fn call_disconnect_callback(callback: &Option<Py<PyAny>>) {
    if let Some(callback) = callback {
        let _ = Python::try_attach(|py| {
            if let Err(e) = callback.call0(py) {
                error!("[MmapSubscriber] 调用 on_disconnect 失败: {e}");
            }
        });
    }
}

/// 将原始事件推送到本地处理队列；若通道失效则自动清理发送端缓存。
fn forward_raw_event(sender_slot: &SharedSenderSlot, raw: Vec<u8>) -> bool {
    let sender = match sender_slot.lock() {
        Ok(guard) => guard.clone(),
        Err(e) => {
            error!("[MmapSubscriber] 事件发送槽加锁失败: {e}");
            return false;
        }
    };

    let Some(sender) = sender else {
        return false;
    };

    if sender.send(raw).is_err() {
        if let Ok(mut guard) = sender_slot.lock() {
            guard.take();
        }
        return false;
    }

    true
}

fn open_shmem_and_header(shm_name: &str) -> Result<(SubscriberMapping, u64, u64), String> {
    let subscriber_instance_id = new_client_instance_id(CLIENT_ROLE_SUBSCRIBER);
    let mut opened = ShmemConf::new()
        .os_id(shm_name)
        .open()
        .map_err(|e| e.to_string())?;

    // 订阅端打开共享内存时在 client table 登记。poll_loop 会把该 Shmem 保存在
    // SubscriberMapping 中持续保活，并周期性刷新本实例自己的 heartbeat 槽位。
    let client = register_client(
        &mut opened,
        shm_name,
        CLIENT_ROLE_SUBSCRIBER,
        subscriber_instance_id,
    )?;

    match shmem_slice(&opened).and_then(read_header) {
        Ok((write_seq, epoch)) => Ok((SubscriberMapping { shmem: opened, client }, write_seq, epoch)),
        Err(err) => {
            let _ = release_client_ref(&mut opened, &client);
            Err(err)
        }
    }
}

fn log_subscriber_message(callback: &Option<Py<PyAny>>, message: String) {
    error!("{message}");
    call_log_callback(callback, &message);
}

fn close_subscriber_mapping(
    shmem: &mut Option<SubscriberMapping>,
    log_exception: &Option<Py<PyAny>>,
) {
    if let Some(mut opened) = shmem.take() {
        if let Err(err) = release_client_ref(&mut opened.shmem, &opened.client) {
            log_subscriber_message(
                log_exception,
                format!("[MmapSubscriber] 释放订阅端 client 槽位失败: {err}"),
            );
        }
    }
}

fn maybe_update_subscriber_heartbeat(
    shmem: &mut Option<SubscriberMapping>,
    last_heartbeat: &mut Instant,
    log_exception: &Option<Py<PyAny>>,
    on_disconnect: &Option<Py<PyAny>>,
) -> bool {
    if last_heartbeat.elapsed() < Duration::from_millis(SHM_HEARTBEAT_INTERVAL_MS) {
        return true;
    }

    let Some(opened) = shmem.as_mut() else {
        *last_heartbeat = Instant::now();
        return true;
    };

    match update_client_heartbeat(&mut opened.shmem, &opened.client) {
        Ok(()) => {
            *last_heartbeat = Instant::now();
            true
        }
        Err(err) => {
            let message = format!("[MmapSubscriber] 更新订阅端 heartbeat 失败，尝试重连: {err}");
            call_log_callback(log_exception, &message);
            call_disconnect_callback(on_disconnect);
            close_subscriber_mapping(shmem, log_exception);
            false
        }
    }
}

/// 共享内存订阅轮询主循环。
///
/// 该循环持续跟踪全局写序号，将新增事件转发到本地队列，并在映射失效或发布端重启时自动恢复。
///
/// # 修复说明
///
/// - 槽位溢出时从当前写指针可保留的最新窗口读取，而不是继续读取已被覆盖的旧窗口。
/// - 检测写序号回退与 epoch 变化，避免发布端重建共享内存后订阅端永久停在旧 read_seq。
/// - 空闲阶段周期性重新打开命名共享内存，用于发布端重启、Windows 映射句柄变化、或 Unix unlink/recreate 后切换到新映射。
/// - 订阅端打开共享内存时登记 client table 槽位，并周期性更新本实例 heartbeat。
fn poll_loop(
    shm_name: String,
    active: Arc<AtomicBool>,
    sender_slot: SharedSenderSlot,
    log_exception: Arc<Option<Py<PyAny>>>,
    on_disconnect: Arc<Option<Py<PyAny>>>,
) {
    let mut shmem: Option<SubscriberMapping> = None;
    let mut read_seq = 0_u64;
    let mut current_epoch = 0_u64;
    let mut idle_count = 0_u32;
    let mut idle_reopen_count = 0_u32;
    let mut warned_no_sender = false;
    let mut last_subscriber_heartbeat = Instant::now();

    while active.load(Ordering::Acquire) {
        if shmem.is_none() {
            match open_shmem_and_header(&shm_name) {
                Ok((opened, write_seq, epoch)) => {
                    read_seq = write_seq;
                    current_epoch = epoch;
                    idle_count = 0;
                    idle_reopen_count = 0;
                    last_subscriber_heartbeat = Instant::now();
                    shmem = Some(opened);
                }
                Err(err) => {
                    if !is_not_found_error(&err) {
                        call_log_callback(
                            &log_exception,
                            &format!("连接共享内存 '{shm_name}' 异常: {err}"),
                        );
                    }
                    sleep_interruptible(active.as_ref(), Duration::from_millis(RECONNECT_SLEEP_MS));
                    continue;
                }
            }
        }

        let header_result = shmem
            .as_ref()
            .ok_or_else(|| "共享内存未连接".to_string())
            .and_then(|mapping| shmem_slice(&mapping.shmem))
            .and_then(read_header);

        let (write_seq, epoch) = match header_result {
            Ok(header) => header,
            Err(err) => {
                if active.load(Ordering::Acquire) {
                    let message = format!("[MmapSubscriber] 读取异常，尝试重连: {err}");
                    call_log_callback(&log_exception, &message);
                    call_disconnect_callback(&on_disconnect);
                }
                close_subscriber_mapping(&mut shmem, &log_exception);
                sleep_interruptible(active.as_ref(), Duration::from_millis(RECONNECT_SLEEP_MS));
                continue;
            }
        };

        if current_epoch != 0 && epoch != current_epoch {
            let message = format!(
                "[MmapSubscriber] 检测到共享内存 epoch 变化，重置读指针 \
                 (old_epoch={current_epoch}, new_epoch={epoch}, write_seq={write_seq})"
            );
            log_subscriber_message(&log_exception, message);
            current_epoch = epoch;
            read_seq = write_seq;
            continue;
        }

        if write_seq < read_seq {
            let message = format!(
                "[MmapSubscriber] 检测到写指针回退，按发布端重启处理 \
                 (read_seq={read_seq}, write_seq={write_seq})"
            );
            log_subscriber_message(&log_exception, message);
            read_seq = write_seq;
            continue;
        }

        if write_seq > read_seq {
            idle_count = 0;
            idle_reopen_count = 0;

            let available = write_seq.saturating_sub(read_seq);
            let start_seq = if available > NUM_SLOTS_U64 {
                let dropped = available.saturating_sub(NUM_SLOTS_U64);
                let message = format!(
                    "[MmapSubscriber] 环形缓冲区溢出，丢失约 {dropped} 条事件 \
                     (read_seq={read_seq}, write_seq={write_seq})"
                );
                log_subscriber_message(&log_exception, message);
                write_seq.saturating_sub(NUM_SLOTS_U64).saturating_add(1)
            } else {
                read_seq.saturating_add(1)
            };

            let buffer_result = shmem
                .as_ref()
                .ok_or_else(|| "共享内存未连接".to_string())
                .and_then(|mapping| shmem_slice(&mapping.shmem));

            let buffer = match buffer_result {
                Ok(buffer) => buffer,
                Err(err) => {
                    if active.load(Ordering::Acquire) {
                        let message = format!("[MmapSubscriber] 读取异常，尝试重连: {err}");
                        call_log_callback(&log_exception, &message);
                        call_disconnect_callback(&on_disconnect);
                    }
                    close_subscriber_mapping(&mut shmem, &log_exception);
                    sleep_interruptible(active.as_ref(), Duration::from_millis(RECONNECT_SLEEP_MS));
                    continue;
                }
            };

            let mut missed_slots = 0_u64;
            let mut forward_failed = 0_u64;
            let mut next_seq = start_seq;
            while next_seq <= write_seq {
                match read_slot(buffer, next_seq) {
                    Some(raw) => {
                        if !forward_raw_event(&sender_slot, raw) {
                            forward_failed = forward_failed.saturating_add(1);
                        }
                    }
                    None => {
                        missed_slots = missed_slots.saturating_add(1);
                    }
                }

                if next_seq == write_seq {
                    break;
                }
                next_seq = next_seq.saturating_add(1);
            }

            if missed_slots > 0 {
                let message = format!(
                    "[MmapSubscriber] {missed_slots} 个槽位读取失败，可能已被发布端覆盖或正在写入 \
                     (start_seq={start_seq}, write_seq={write_seq})"
                );
                log_subscriber_message(&log_exception, message);
            }

            if forward_failed > 0 && !warned_no_sender {
                warned_no_sender = true;
                let message = format!(
                    "[MmapSubscriber] {forward_failed} 条事件未能转发到本地队列；\
                     请确认订阅器已 attach 到 EventEngine 且事件引擎仍在运行"
                );
                log_subscriber_message(&log_exception, message);
            }

            read_seq = write_seq;
            thread::sleep(Duration::from_micros(POLL_FAST_US));
        } else {
            idle_count = idle_count.saturating_add(1);
            if idle_count >= IDLE_THRESH_FAST + IDLE_THRESH_MED {
                thread::sleep(Duration::from_millis(POLL_IDLE_MS));
                idle_reopen_count = idle_reopen_count.saturating_add(1);
            } else if idle_count >= IDLE_THRESH_FAST {
                thread::sleep(Duration::from_micros(POLL_MED_US));
            } else {
                thread::sleep(Duration::from_micros(POLL_FAST_US));
            }

            if idle_reopen_count >= IDLE_REOPEN_CHECKS {
                idle_reopen_count = 0;
                if let Ok((opened, reopened_write_seq, reopened_epoch)) = open_shmem_and_header(&shm_name) {
                    if reopened_epoch != current_epoch || reopened_write_seq < read_seq {
                        let message = format!(
                            "[MmapSubscriber] 重新打开共享内存并切换映射 \
                             (old_epoch={current_epoch}, new_epoch={reopened_epoch}, \
                              old_read_seq={read_seq}, new_write_seq={reopened_write_seq})"
                        );
                        log_subscriber_message(&log_exception, message);
                        close_subscriber_mapping(&mut shmem, &log_exception);
                        last_subscriber_heartbeat = Instant::now();
                        shmem = Some(opened);
                        current_epoch = reopened_epoch;
                        read_seq = reopened_write_seq;
                    } else {
                        // 空闲重开仅用于探测，未切换映射时要释放本次 open 增加的订阅端引用计数。
                        let mut probe = Some(opened);
                        close_subscriber_mapping(&mut probe, &log_exception);
                    }
                }
            }
        }

        if !maybe_update_subscriber_heartbeat(
            &mut shmem,
            &mut last_subscriber_heartbeat,
            &log_exception,
            &on_disconnect,
        ) {
            sleep_interruptible(active.as_ref(), Duration::from_millis(RECONNECT_SLEEP_MS));
        }
    }

    close_subscriber_mapping(&mut shmem, &log_exception);
}

/// 分块睡眠，以便在线程收到停止信号时尽快退出。
fn sleep_interruptible(active: &AtomicBool, total: Duration) {
    let mut slept = Duration::ZERO;
    let chunk = Duration::from_millis(STOP_CHECK_MS);

    while active.load(Ordering::Acquire) && slept < total {
        let remain = total.saturating_sub(slept);
        let step = remain.min(chunk);
        thread::sleep(step);
        slept += step;
    }
}

/// 可被 Python 直接构造与序列化的事件对象。
///
/// `type_` 表示事件主题，`data` 保存任意 Python 负载。
#[pyclass(name = "Event", module = "rust_event_engine")]
pub struct Event {
    type_: String,
    data: Py<PyAny>,
}

impl Event {
    fn new_owned(type_: String, data: Py<PyAny>) -> Self {
        Self { type_, data }
    }
}

#[pymethods]
impl Event {
    /// 创建一个新的事件实例。
    #[new]
    #[pyo3(signature = (type_ = "", data = None))]
    fn new(py: Python<'_>, type_: &str, data: Option<Py<PyAny>>) -> Self {
        Self::new_owned(type_.to_string(), data.unwrap_or_else(|| py.None()))
    }

    /// 返回事件类型。
    #[getter]
    fn type_(&self) -> &str {
        &self.type_
    }

    /// 更新事件类型。
    #[setter]
    fn set_type_(&mut self, value: String) {
        self.type_ = value;
    }

    /// 返回事件负载的 Python 对象副本引用。
    #[getter]
    fn data(&self, py: Python<'_>) -> Py<PyAny> {
        self.data.clone_ref(py)
    }

    /// 更新事件负载。
    #[setter]
    fn set_data(&mut self, value: Py<PyAny>) {
        self.data = value;
    }

    /// 导出 pickle 所需的序列化状态。
    fn __getstate__(&self, py: Python<'_>) -> (String, Py<PyAny>) {
        (self.type_.clone(), self.data.clone_ref(py))
    }

    /// 从 pickle 反序列化状态中恢复事件对象。
    fn __setstate__(&mut self, state: (String, Py<PyAny>)) {
        self.type_ = state.0;
        self.data = state.1;
    }

    /// 返回便于调试的字符串表示。
    fn __repr__(&self) -> String {
        format!("Event(type_='{}')", self.type_)
    }
}

/// 根据类型与负载构造 [`Event`]，并序列化为可跨线程传输的字节流。
fn serialize_event_from_parts(py: Python<'_>, type_: &str, data: Py<PyAny>) -> PyResult<Vec<u8>> {
    let event = Py::new(py, Event::new_owned(type_.to_string(), data))
        .map_err(|e| PyRuntimeError::new_err(format!("构造 Event 失败: {e}")))?;
    py_serialize(py, event.bind(py).as_any())
}

enum PublisherCommand {
    Publish {
        payload: Vec<u8>,
        ack: Sender<Result<(), String>>,
    },
    Close {
        ack: Sender<()>,
    },
}

/// 将序列化后的事件负载写入共享内存环形槽位，并推进全局写指针。
fn write_payload_to_shmem(
    shmem: &mut Shmem,
    publisher_client: &ShmemClientRegistration,
    write_seq: &mut u64,
    payload: &[u8],
) -> Result<(), String> {
    if payload.is_empty() {
        return Err("payload 不能为空".to_string());
    }
    if payload.len() > MAX_PAYLOAD {
        return Err(format!(
            "payload 长度 {}B 超过 MAX_PAYLOAD={}B",
            payload.len(),
            MAX_PAYLOAD
        ));
    }

    let buf = shmem_slice_mut(shmem)?;
    let _guard = acquire_shmem_write_lock(buf)?;

    // 发布前先确认当前 publisher client 槽位仍然有效，避免槽位已被 stale 回收后仍继续写入。
    update_client_heartbeat_locked(buf, publisher_client)?;

    let current = load_u64_atomic(buf, H_WRITE_SEQ, Ordering::Acquire)
        .map_err(|e| format!("读取写指针失败: {e}"))?;
    let seq = current
        .checked_add(1)
        .ok_or_else(|| "写序号溢出".to_string())?;
    let base = slot_base(seq).ok_or_else(|| "计算共享内存槽位偏移失败".to_string())?;
    let seq_offset = base
        .checked_add(S_SEQ)
        .ok_or_else(|| "计算 slot_seq 偏移溢出".to_string())?;

    // 先将槽位标记为无效，避免槽位复用时订阅端读到“旧 seq + 新 payload”的撕裂数据。
    // SeqCst 防止后续 payload 写入被重排到无效标记之前。
    store_u64_atomic(buf, seq_offset, SLOT_SEQ_INVALID, Ordering::SeqCst)?;

    write_u32_le(
        buf,
        base.checked_add(S_SIZE)
            .ok_or_else(|| "写入 data_size 偏移溢出".to_string())?,
        u32::try_from(payload.len()).map_err(|e| format!("payload 长度转换失败: {e}"))?,
    )?;

    let data_start = base
        .checked_add(S_DATA)
        .ok_or_else(|| "payload 起始偏移溢出".to_string())?;
    let data_end = data_start
        .checked_add(payload.len())
        .ok_or_else(|| "payload 结束偏移溢出".to_string())?;
    let dst = buf
        .get_mut(data_start..data_end)
        .ok_or_else(|| "写入 payload 失败: 共享内存越界".to_string())?;
    dst.copy_from_slice(payload);

    // Release 发布完整槽位内容；订阅端 Acquire 读取到该 seq 后即可看到 size/payload。
    store_u64_atomic(buf, seq_offset, seq, Ordering::Release)?;

    // 最后推进全局写指针，保证订阅端不会在槽位发布前看到新 write_seq。
    store_u64_atomic(buf, H_WRITE_SEQ, seq, Ordering::Release)?;

    *write_seq = seq;
    Ok(())
}

/// 发布线程主循环，串行处理写入请求与关闭请求。
fn publisher_loop(
    shm_name: String,
    rx: Receiver<PublisherCommand>,
    init_ack: Sender<Result<(), String>>,
) {
    let publisher_instance_id = new_client_instance_id(CLIENT_ROLE_PUBLISHER);
    let (mut shmem, publisher_client) = match create_shmem(&shm_name, publisher_instance_id) {
        Ok(pair) => pair,
        Err(err) => {
            let _ = init_ack.send(Err(err));
            return;
        }
    };

    let mut write_seq = match shmem_slice(&shmem).and_then(|buf| {
        load_u64_atomic(buf, H_WRITE_SEQ, Ordering::Acquire)
            .map_err(|e| format!("读取初始写指针失败: {e}"))
    }) {
        Ok(seq) => seq,
        Err(err) => {
            let _ = init_ack.send(Err(err));
            return;
        }
    };

    let _ = init_ack.send(Ok(()));

    loop {
        match rx.recv_timeout(Duration::from_millis(SHM_HEARTBEAT_INTERVAL_MS)) {
            Ok(PublisherCommand::Publish { payload, ack }) => {
                let result = write_payload_to_shmem(&mut shmem, &publisher_client, &mut write_seq, &payload);
                let _ = ack.send(result);
            }

            Ok(PublisherCommand::Close { ack }) => {
                let _ = ack.send(());
                break;
            }

            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Err(err) = update_client_heartbeat(&mut shmem, &publisher_client) {
                    error!("[MmapPublisher] 更新 heartbeat 失败: {err}");
                }
            }

            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }

    if let Err(err) = release_client_ref(&mut shmem, &publisher_client) {
        error!("[MmapPublisher] 释放共享内存发布端 client 槽位失败: {err}");
    }

    // 不在正常关闭时 unlink。
    //
    // 原因：
    // 1. 可能还有其它发布端/订阅端正在使用同名共享内存。
    // 2. drop 后再 unlink 存在误删后来者的竞态。
    // 3. 新发布端启动时会通过 client table + heartbeat 清理崩溃遗留并在必要时重置 header。
    drop(shmem);
}

/// 共享内存发布器的线程安全核心实现。
struct PublisherInner {
    shm_name: String,
    tx: Mutex<Option<Sender<PublisherCommand>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
}

impl PublisherInner {
    fn new(shm_name: &str) -> PyResult<Self> {
        let (cmd_tx, cmd_rx) = mpsc::channel::<PublisherCommand>();
        let (init_tx, init_rx) = mpsc::channel::<Result<(), String>>();
        let shm_name_string = shm_name.to_string();
        let thread_name = format!("MmapPublisher-{}", shm_name_string);

        let worker = thread::Builder::new()
            .name(thread_name)
            .spawn({
                let shm_name = shm_name_string.clone();
                move || publisher_loop(shm_name, cmd_rx, init_tx)
            })
            .map_err(|e| PyRuntimeError::new_err(format!("启动共享内存发布线程失败: {e}")))?;

        match init_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                shm_name: shm_name_string,
                tx: Mutex::new(Some(cmd_tx)),
                worker: Mutex::new(Some(worker)),
                closed: AtomicBool::new(false),
            }),
            Ok(Err(err)) => {
                let _ = join_handle(worker, "MmapPublisherInit");
                Err(PyIOError::new_err(format!(
                    "创建共享内存 '{shm_name}' 失败: {err}"
                )))
            }
            Err(err) => {
                let _ = join_handle(worker, "MmapPublisherInit");
                Err(PyRuntimeError::new_err(format!(
                    "等待共享内存发布线程初始化失败: {err}"
                )))
            }
        }
    }

    fn publish(&self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("共享内存发布器已关闭"));
        }

        let payload = py_serialize(py, event)?;
        if payload.len() > MAX_PAYLOAD {
            return Err(PyValueError::new_err(format!(
                "事件序列化后 {}B 超过 MAX_PAYLOAD={}B",
                payload.len(),
                MAX_PAYLOAD
            )));
        }

        let sender = {
            let sender_guard = lock_mutex(&self.tx, "读取共享内存发布通道")?;
            sender_guard.clone()
        };

        let sender = match sender {
            Some(sender) => sender,
            None => return Err(PyRuntimeError::new_err("共享内存发布通道已关闭")),
        };

        let (ack_tx, ack_rx) = mpsc::channel::<Result<(), String>>();
        sender
            .send(PublisherCommand::Publish {
                payload,
                ack: ack_tx,
            })
            .map_err(|e| PyRuntimeError::new_err(format!("发送发布命令失败: {e}")))?;

        ack_rx
            .recv()
            .map_err(|e| PyRuntimeError::new_err(format!("等待发布确认失败: {e}")))?
            .map_err(|e| PyRuntimeError::new_err(format!("发布事件失败: {e}")))
    }

    fn close(&self) -> PyResult<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let mut first_error: Option<PyErr> = None;

        let sender = {
            let mut sender_guard = lock_mutex(&self.tx, "关闭共享内存发布通道")?;
            sender_guard.take()
        };

        if let Some(sender) = sender {
            let (ack_tx, ack_rx) = mpsc::channel::<()>();
            if let Err(e) = sender.send(PublisherCommand::Close { ack: ack_tx }) {
                first_error = Some(PyRuntimeError::new_err(format!("发送关闭命令失败: {e}")));
            } else if let Err(e) = ack_rx.recv() {
                first_error = Some(PyRuntimeError::new_err(format!("等待关闭确认失败: {e}")));
            }
        }

        let handle = {
            let mut worker_guard = lock_mutex(&self.worker, "获取共享内存发布线程")?;
            worker_guard.take()
        };

        if let Some(handle) = handle {
            if let Err(err) = join_handle(handle, "MmapPublisher") {
                if first_error.is_none() {
                    first_error = Some(err);
                } else {
                    error!("[MmapPublisher] 关闭期间额外 join 错误: {err}");
                }
            }
        }

        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }
}

/// 基于共享内存的跨进程事件发布器。
///
/// 该对象会将 Python 事件序列化后写入共享内存环形缓冲区，适合广播高频市场事件。
#[pyclass(name = "MmapPublisher", module = "rust_event_engine")]
pub struct MmapPublisher {
    inner: Arc<PublisherInner>,
}

#[pymethods]
impl MmapPublisher {
    /// 创建发布器并启动后台写线程。
    #[new]
    #[pyo3(signature = (shm_name = "vnpy_evbus"))]
    fn new(shm_name: &str) -> PyResult<Self> {
        init_error_logger();
        Ok(Self {
            inner: Arc::new(PublisherInner::new(shm_name)?),
        })
    }

    /// 发布一个可被 `pickle` 序列化的 Python 事件对象。
    fn publish(&self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        self.inner.publish(py, event)
    }

    /// 关闭发布器并等待后台线程安全退出。
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.detach(move || inner.close())
    }

    /// 返回当前绑定的共享内存名称。
    #[getter]
    fn shm_name(&self) -> String {
        self.inner.shm_name.clone()
    }
}

/// 共享内存订阅器的运行时状态。
struct SubscriberState {
    poll_thread: Option<JoinHandle<()>>,
}

/// 共享内存订阅器的线程安全核心实现。
struct SubscriberInner {
    shm_name: String,
    active: Arc<AtomicBool>,
    sender_slot: SharedSenderSlot,
    log_exception: Arc<Option<Py<PyAny>>>,
    on_disconnect: Arc<Option<Py<PyAny>>>,
    state: Mutex<SubscriberState>,
}

impl SubscriberInner {
    fn new(
        py: Python<'_>,
        shm_name: &str,
        log_exception: Option<Py<PyAny>>,
        on_disconnect: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        if let Some(callback) = log_exception.as_ref() {
            ensure_callable(callback.bind(py), "log_exception")?;
        }
        if let Some(callback) = on_disconnect.as_ref() {
            ensure_callable(callback.bind(py), "on_disconnect")?;
        }

        Ok(Self {
            shm_name: shm_name.to_string(),
            active: Arc::new(AtomicBool::new(false)),
            sender_slot: Arc::new(Mutex::new(None)),
            log_exception: Arc::new(log_exception),
            on_disconnect: Arc::new(on_disconnect),
            state: Mutex::new(SubscriberState { poll_thread: None }),
        })
    }

    fn clear_sender(&self) -> PyResult<()> {
        let mut slot = lock_mutex(&self.sender_slot, "清理订阅者发送通道")?;
        slot.take();
        Ok(())
    }

    fn set_sender(&self, sender: Option<EventSender>) -> PyResult<()> {
        let mut slot = lock_mutex(&self.sender_slot, "设置订阅者发送通道")?;
        *slot = sender;
        Ok(())
    }

    fn start(&self) -> PyResult<()> {
        {
            let slot = lock_mutex(&self.sender_slot, "检查订阅者发送通道")?;
            if slot.is_none() {
                return Err(PyRuntimeError::new_err(
                    "MmapSubscriber 尚未绑定 EventEngine 队列；请先调用 EventEngine.attach_subscriber() 再启动",
                ));
            }
        }

        if self.active.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let handle = match self.spawn_poll_thread() {
            Ok(handle) => handle,
            Err(err) => {
                self.active.store(false, Ordering::Release);
                return Err(err);
            }
        };

        let mut state = lock_mutex(&self.state, "记录订阅轮询线程")?;
        state.poll_thread = Some(handle);
        Ok(())
    }

    /// 停止共享内存轮询线程并等待其安全退出。
    ///
    /// # 修复说明（问题 4）
    ///
    /// 原实现先调用 `clear_sender()`，再 join 轮询线程：轮询线程在真正退出前仍可
    /// 能从共享内存读到新槽位，但此时 `sender_slot` 已为 `None`，
    /// `forward_raw_event` 会静默丢弃这些事件。
    ///
    /// 修复后改为先 join（等待线程完全退出），再 `clear_sender()`，
    /// 确保线程生命周期内的所有事件都能成功送达队列。
    fn stop(&self) -> PyResult<()> {
        self.active.store(false, Ordering::Release);

        // [修复问题 4] 先等待轮询线程完全退出，确保其不再产生新的转发请求，
        // 然后再清理 sender，避免最后几条事件因通道提前关闭而静默丢失。
        let handle = {
            let mut state = lock_mutex(&self.state, "获取订阅轮询线程")?;
            state.poll_thread.take()
        };

        let mut first_error: Option<PyErr> = None;
        if let Some(handle) = handle {
            if let Err(err) = join_handle(handle, "MmapSubscriber") {
                first_error = Some(err);
            }
        }

        if let Err(err) = self.clear_sender() {
            if first_error.is_none() {
                first_error = Some(err);
            } else {
                error!("[MmapSubscriber] 停止期间清理 sender 失败: {err}");
            }
        }

        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    fn start_with_sender(&self, sender: EventSender) -> PyResult<()> {
        self.set_sender(Some(sender))?;
        self.start()
    }

    fn spawn_poll_thread(&self) -> PyResult<JoinHandle<()>> {
        let shm_name = self.shm_name.clone();
        let active = Arc::clone(&self.active);
        let sender_slot = Arc::clone(&self.sender_slot);
        let log_exception = Arc::clone(&self.log_exception);
        let on_disconnect = Arc::clone(&self.on_disconnect);

        thread::Builder::new()
            .name("MmapSubscriberPoll".to_string())
            .spawn(move || poll_loop(shm_name, active, sender_slot, log_exception, on_disconnect))
            .map_err(|e| PyRuntimeError::new_err(format!("启动订阅轮询线程失败: {e}")))
    }
}

/// 共享内存事件订阅器。
///
/// 该对象通过后台轮询线程跟踪共享内存中的新增事件，并将其转发到本地事件队列。
#[pyclass(name = "MmapSubscriber", module = "rust_event_engine")]
pub struct MmapSubscriber {
    inner: Arc<SubscriberInner>,
}

#[pymethods]
impl MmapSubscriber {
    /// 创建订阅器，并可选注册断线通知与异常日志回调。
    #[new]
    #[pyo3(signature = (event_engine = None, shm_name = "vnpy_evbus", log_exception = None, on_disconnect = None))]
    fn new(
        py: Python<'_>,
        event_engine: Option<Py<PyAny>>,
        shm_name: &str,
        log_exception: Option<Py<PyAny>>,
        on_disconnect: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        init_error_logger();
        let inner = Arc::new(SubscriberInner::new(
            py,
            shm_name,
            log_exception,
            on_disconnect,
        )?);

        // 兼容 Python 侧 MmapSubscriber(event_engine=...) 的语义：传入 EventEngine 时立即挂载，
        // 避免构造参数被静默忽略，随后 start() 又因未绑定队列而失败。
        if let Some(event_engine) = event_engine {
            let event_engine_bound = event_engine.bind(py);
            let engine = event_engine_bound
                .extract::<PyRef<'_, EventEngine>>()
                .map_err(|e| PyTypeError::new_err(format!("event_engine 必须是 EventEngine 实例: {e}")))?;
            let sender = {
                let mut state = lock_mutex(&engine.inner.state, "通过构造参数挂载共享内存订阅器")?;
                let sender = state.event_tx.clone();
                state.subscriber = Some(inner.clone());
                sender
            };

            if let Some(sender) = sender {
                if engine.inner.active.load(Ordering::Acquire) {
                    inner.start_with_sender(sender)?;
                } else {
                    inner.set_sender(Some(sender))?;
                }
            }
        }

        Ok(Self { inner })
    }

    /// 返回订阅轮询线程的目标运行状态。
    #[getter]
    fn active(&self) -> bool {
        self.inner.active.load(Ordering::Acquire)
    }

    /// `active` 是只读运行状态；请使用 start()/stop() 改变生命周期。
    #[setter]
    fn set_active(&self, _value: bool) -> PyResult<()> {
        Err(PyRuntimeError::new_err(
            "active 为只读运行状态，请使用 start()/stop() 改变 MmapSubscriber 生命周期",
        ))
    }

    /// 启动共享内存轮询线程。
    fn start(&self) -> PyResult<()> {
        self.inner.start()
    }

    /// 停止共享内存轮询线程并等待其退出。
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.detach(move || inner.stop())
    }

    /// 返回当前绑定的共享内存名称。
    #[getter]
    fn shm_name(&self) -> String {
        self.inner.shm_name.clone()
    }
}

/// 事件引擎在运行期持有的线程与外部组件句柄。
struct EngineState {
    event_tx: Option<EventSender>,
    publisher: Option<Arc<PublisherInner>>,
    subscriber: Option<Arc<SubscriberInner>>,
    process_thread: Option<JoinHandle<()>>,
    timer_thread: Option<JoinHandle<()>>,
}

/// 事件引擎的线程安全核心实现。
struct EventEngineInner {
    interval: u64,
    channel: Mutex<String>,
    active: Arc<AtomicBool>,
    handlers: SharedHandlerMap,
    general_handlers: SharedHandlerVec,
    state: Mutex<EngineState>,
}

impl EventEngineInner {
    fn new(interval: u64) -> Self {
        Self {
            interval,
            channel: Mutex::new(String::new()),
            active: Arc::new(AtomicBool::new(false)),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            general_handlers: Arc::new(Mutex::new(Vec::new())),
            state: Mutex::new(EngineState {
                event_tx: None,
                publisher: None,
                subscriber: None,
                process_thread: None,
                timer_thread: None,
            }),
        }
    }

    fn clone_sender(&self) -> PyResult<Option<EventSender>> {
        let state = lock_mutex(&self.state, "读取事件发送通道")?;
        Ok(state.event_tx.clone())
    }

    fn is_loop_running(&self) -> PyResult<bool> {
        if !self.active.load(Ordering::Acquire) {
            return Ok(false);
        }

        let state = lock_mutex(&self.state, "读取事件引擎状态")?;
        Ok(state
            .process_thread
            .as_ref()
            .is_some_and(|handle| !handle.is_finished()))
    }

    fn event_to_queue_raw(&self, raw: Vec<u8>) -> PyResult<()> {
        if !self.active.load(Ordering::Acquire) {
            return Ok(());
        }

        if let Some(tx) = self.clone_sender()? {
            tx.send(raw)
                .map_err(|e| PyRuntimeError::new_err(format!("发送事件到本地队列失败: {e}")))?;
        }
        Ok(())
    }

    fn take_runtime_parts(
        &self,
    ) -> PyResult<(
        Option<EventSender>,
        Option<Arc<PublisherInner>>,
        Option<Arc<SubscriberInner>>,
        Option<JoinHandle<()>>,
        Option<JoinHandle<()>>,
    )> {
        let mut state = lock_mutex(&self.state, "提取事件引擎运行时状态")?;
        Ok((
            state.event_tx.take(),
            state.publisher.take(),
            state.subscriber.clone(),
            state.process_thread.take(),
            state.timer_thread.take(),
        ))
    }

    fn shutdown_runtime(&self) -> PyResult<()> {
        let (event_sender, publisher, subscriber, process_thread, timer_thread) =
            self.take_runtime_parts()?;

        drop(event_sender);

        let mut first_error: Option<PyErr> = None;

        if let Some(subscriber) = subscriber {
            if let Err(err) = subscriber.stop() {
                first_error = Some(err);
            }
        }

        if let Some(publisher) = publisher {
            if let Err(err) = publisher.close() {
                if first_error.is_none() {
                    first_error = Some(err);
                } else {
                    error!("[EventEngine] 关闭发布器期间额外错误: {err}");
                }
            }
        }

        if let Some(timer_thread) = timer_thread {
            if let Err(err) = join_handle(timer_thread, "EventTimer") {
                if first_error.is_none() {
                    first_error = Some(err);
                } else {
                    error!("[EventEngine] 关闭定时器线程期间额外错误: {err}");
                }
            }
        }

        if let Some(process_thread) = process_thread {
            if let Err(err) = join_handle(process_thread, "EventProcess") {
                if first_error.is_none() {
                    first_error = Some(err);
                } else {
                    error!("[EventEngine] 关闭事件处理线程期间额外错误: {err}");
                }
            }
        }

        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }
}

/// 与 vn.py 兼容的事件引擎实现。
///
/// 该类型负责管理本地事件队列、定时器线程、处理器注册以及可选的共享内存发布/订阅组件。
#[pyclass(name = "EventEngine", module = "rust_event_engine")]
pub struct EventEngine {
    inner: Arc<EventEngineInner>,
}

#[pymethods]
impl EventEngine {
    /// 创建事件引擎。
    ///
    /// `interval` 表示定时器事件的触发周期，单位为秒。
    #[new]
    #[pyo3(signature = (interval = 1))]
    fn new(interval: u64) -> PyResult<Self> {
        if interval == 0 {
            return Err(PyValueError::new_err("interval 必须大于 0 秒"));
        }

        init_error_logger();
        Ok(Self {
            inner: Arc::new(EventEngineInner::new(interval)),
        })
    }

    /// 返回事件引擎的目标运行状态。
    #[getter]
    fn active(&self) -> bool {
        self.inner.active.load(Ordering::Acquire)
    }

    /// `active` 是只读运行状态；请使用 start()/stop() 改变生命周期。
    #[setter]
    fn set_active(&self, _value: bool) -> PyResult<()> {
        Err(PyRuntimeError::new_err(
            "active 为只读运行状态，请使用 start()/stop() 改变 EventEngine 生命周期",
        ))
    }

    /// 启动事件处理线程、定时器线程，以及已挂载的共享内存订阅器。
    fn start(&self) -> PyResult<()> {
        if self.inner.active.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        let process_thread = match spawn_process_thread(
            rx,
            Arc::clone(&self.inner.active),
            Arc::clone(&self.inner.handlers),
            Arc::clone(&self.inner.general_handlers),
        ) {
            Ok(handle) => handle,
            Err(err) => {
                self.inner.active.store(false, Ordering::Release);
                return Err(err);
            }
        };

        let timer_thread = match spawn_timer_thread(
            self.inner.interval,
            Arc::clone(&self.inner.active),
            tx.clone(),
        ) {
            Ok(handle) => handle,
            Err(err) => {
                self.inner.active.store(false, Ordering::Release);
                drop(tx);
                let _ = join_handle(process_thread, "EventProcess");
                return Err(err);
            }
        };

        let subscriber = {
            let mut state = lock_mutex(&self.inner.state, "更新事件引擎运行时状态")?;
            state.event_tx = Some(tx.clone());
            state.process_thread = Some(process_thread);
            state.timer_thread = Some(timer_thread);
            state.subscriber.clone()
        };

        if let Some(subscriber) = subscriber {
            if let Err(err) = subscriber.start_with_sender(tx) {
                self.inner.active.store(false, Ordering::Release);
                if let Err(cleanup_err) = self.inner.shutdown_runtime() {
                    error!("[EventEngine] 启动失败后的清理异常: {cleanup_err}");
                }
                return Err(PyRuntimeError::new_err(format!(
                    "启动共享内存订阅者失败: {err}"
                )));
            }
        }

        Ok(())
    }

    /// 停止事件引擎并回收所有后台线程与共享内存组件。
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        if !self.inner.active.swap(false, Ordering::AcqRel) {
            return Ok(());
        }

        let inner = Arc::clone(&self.inner);
        py.detach(move || inner.shutdown_runtime())
    }

    /// 挂载共享内存发布器，用于将 `EVENT_TICK` 优先广播到跨进程总线。
    fn attach_publisher(&self, py: Python<'_>, publisher: Py<MmapPublisher>) -> PyResult<()> {
        let publisher_inner = publisher.bind(py).borrow().inner.clone();
        let mut state = lock_mutex(&self.inner.state, "挂载共享内存发布器")?;
        state.publisher = Some(publisher_inner);
        Ok(())
    }

    /// 挂载共享内存订阅器，使当前引擎能够消费其他进程广播的事件。
    fn attach_subscriber(&self, py: Python<'_>, subscriber: Py<MmapSubscriber>) -> PyResult<()> {
        let subscriber_inner = subscriber.bind(py).borrow().inner.clone();
        let sender = {
            let mut state = lock_mutex(&self.inner.state, "挂载共享内存订阅器")?;
            let sender = state.event_tx.clone();
            state.subscriber = Some(subscriber_inner.clone());
            sender
        };

        if let Some(sender) = sender {
            if self.inner.active.load(Ordering::Acquire) {
                subscriber_inner.start_with_sender(sender)?;
            } else {
                subscriber_inner.set_sender(Some(sender))?;
            }
        }
        Ok(())
    }

    /// 将事件直接写入本地处理队列，而不经过共享内存广播。
    fn event_to_queue(&self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let raw = py_serialize(py, event)?;
        self.inner.event_to_queue_raw(raw)
    }

    /// 投递事件。
    ///
    /// 当事件类型为 `EVENT_TICK` 且已挂载发布器时，会优先尝试共享内存广播。
    /// 若检测到发布器已正常关闭，则直接调用 EventEngine::stop()，
    /// 其它发布异常仍记录日志、停止引擎并返回错误。
    fn put(&self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        if extract_type(event)? == EVENT_TICK {
            let publisher = {
                let state = lock_mutex(&self.inner.state, "读取共享内存发布器")?;
                state.publisher.clone()
            };

            if let Some(publisher) = publisher {
                match publisher.publish(py, event) {
                    Ok(()) => return Ok(()),

                    Err(err) => {
                        let error_text = err.to_string();

                        // 发布器正常关闭时停止事件引擎，避免后续 tick 被静默丢弃。
                        if is_publisher_closed_error(&error_text) {
                            self.stop(py)?;
                            return Ok(());
                        }

                        // 其它异常：不降级本地队列，停止引擎并返回错误。
                        error!(
                            "[EventEngine] mmap publish 失败，不降级本地队列，停止事件引擎: {error_text}"
                        );

                        self.stop(py)?;

                        return Err(PyRuntimeError::new_err(format!(
                            "共享内存发布失败，事件引擎已停止: {error_text}"
                        )));
                    }
                }
            }
        }

        self.event_to_queue(py, event)
    }

    /// 为指定事件类型注册处理器；重复注册同一处理器会被忽略。
    fn register(&self, py: Python<'_>, type_: String, handler: Py<PyAny>) -> PyResult<()> {
        ensure_callable(handler.bind(py), "handler")?;
        let mut map = lock_mutex(&self.inner.handlers, "注册事件处理器")?;
        let handlers = map.entry(type_).or_default();
        if !handlers
            .iter()
            .any(|existing| is_same_handler(existing.bind(py), handler.bind(py)))
        {
            handlers.push(handler);
        }
        Ok(())
    }

    /// 注销指定事件类型上的处理器。
    fn unregister(&self, py: Python<'_>, type_: String, handler: Py<PyAny>) -> PyResult<()> {
        let mut map = lock_mutex(&self.inner.handlers, "注销事件处理器")?;
        if let Some(handlers) = map.get_mut(&type_) {
            handlers.retain(|existing| !is_same_handler(existing.bind(py), handler.bind(py)));
            if handlers.is_empty() {
                map.remove(&type_);
            }
        }
        Ok(())
    }

    /// 注册通用处理器，使其对每一个事件都执行一次。
    fn register_general(&self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<()> {
        ensure_callable(handler.bind(py), "general_handler")?;
        let mut handlers = lock_mutex(&self.inner.general_handlers, "注册通用处理器")?;
        if !handlers
            .iter()
            .any(|existing| is_same_handler(existing.bind(py), handler.bind(py)))
        {
            handlers.push(handler);
        }
        Ok(())
    }

    /// 注销通用处理器。
    fn unregister_general(&self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<()> {
        let mut handlers = lock_mutex(&self.inner.general_handlers, "注销通用处理器")?;
        handlers.retain(|existing| !is_same_handler(existing.bind(py), handler.bind(py)));
        Ok(())
    }

    /// 立即在当前线程内分发事件，不经过本地队列。
    fn process(&self, py: Python<'_>, event: &Bound<'_, PyAny>) {
        dispatch_event_isolated(
            py,
            event,
            &self.inner.handlers,
            &self.inner.general_handlers,
        );
    }

    /// 返回事件处理线程是否仍处于运行状态。
    fn is_loop_running(&self) -> bool {
        match self.inner.is_loop_running() {
            Ok(running) => running,
            Err(err) => {
                error!("[EventEngine] 读取事件循环状态失败: {err}");
                false
            }
        }
    }

    /// 返回由上层业务维护的逻辑频道名。
    #[getter]
    fn channel(&self) -> PyResult<String> {
        Ok(lock_mutex(&self.inner.channel, "读取 channel")?.clone())
    }

    /// 设置由上层业务维护的逻辑频道名。
    #[setter]
    fn set_channel(&self, value: String) -> PyResult<()> {
        *lock_mutex(&self.inner.channel, "写入 channel")? = value;
        Ok(())
    }
}

/// 启动本地事件处理线程，负责反序列化事件并调用注册处理器。
fn spawn_process_thread(
    rx: Receiver<Vec<u8>>,
    active: Arc<AtomicBool>,
    handlers: SharedHandlerMap,
    general_handlers: SharedHandlerVec,
) -> PyResult<JoinHandle<()>> {
    thread::Builder::new()
        .name("EventProcess".to_string())
        .spawn(move || {
            for raw in rx {
                if !active.load(Ordering::Acquire) {
                    break;
                }

                if Python::try_attach(|py| match py_deserialize(py, &raw) {
                    Ok(event) => {
                        if active.load(Ordering::Acquire) {
                            dispatch_event_isolated(py, &event, &handlers, &general_handlers);
                        }
                    }
                    Err(err) => error!("[EventEngine] 反序列化事件失败: {err}"),
                })
                .is_none()
                {
                    break;
                }
            }
        })
        .map_err(|e| PyRuntimeError::new_err(format!("启动事件处理线程失败: {e}")))
}

/// 启动定时器线程，按固定周期注入 `EVENT_TIMER` 事件。
fn spawn_timer_thread(
    interval: u64,
    active: Arc<AtomicBool>,
    tx: EventSender,
) -> PyResult<JoinHandle<()>> {
    thread::Builder::new()
        .name("EventTimer".to_string())
        .spawn(move || {
            while active.load(Ordering::Acquire) {
                sleep_interruptible(&active, Duration::from_secs(interval));
                if !active.load(Ordering::Acquire) {
                    break;
                }

                let Some(result) = Python::try_attach(|py| {
                    let now = py
                        .import("datetime")
                        .and_then(|module| module.getattr("datetime"))
                        .and_then(|cls| cls.call_method0("now"))
                        .map_err(|e| {
                            PyRuntimeError::new_err(format!("获取 datetime.now() 失败: {e}"))
                        })?;
                    serialize_event_from_parts(py, EVENT_TIMER, now.unbind())
                }) else {
                    break;
                };

                match result {
                    Ok(raw) => {
                        if tx.send(raw).is_err() {
                            break;
                        }
                    }
                    Err(err) => error!("[EventEngine] 定时器事件构造失败: {err}"),
                }
            }
        })
        .map_err(|e| PyRuntimeError::new_err(format!("启动定时器线程失败: {e}")))
}

/// 将任意字符串规范化为适合共享内存标识的名称。
///
/// 结果仅保留 ASCII 字母、数字与下划线，并截断到平台兼容长度。
#[pyfunction]
fn normalize_shm_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(SHM_MAX_NAME)
        .collect()
}

#[pymodule]
fn rust_event_engine(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    init_error_logger();
    module.add_class::<Event>()?;
    module.add_class::<MmapPublisher>()?;
    module.add_class::<MmapSubscriber>()?;
    module.add_class::<EventEngine>()?;
    module.add_function(wrap_pyfunction!(normalize_shm_name, module)?)?;
    module.add("EVENT_TIMER", EVENT_TIMER)?;
    module.add("EVENT_TICK", EVENT_TICK)?;
    module.add("NUM_SLOTS", NUM_SLOTS)?;
    module.add("MAX_PAYLOAD", MAX_PAYLOAD)?;
    Ok(())
}
