````markdown
# 多账户交易与共享内存行情订阅改造说明

## 改造目标

通过共享内存行情订阅机制，实现发布进程与订阅进程之间的行情共享，并支持多账户交易。

发布进程和订阅进程需要同时运行：

- 发布进程：`portfolio_trade_hyperliquid_main.py`
- 订阅进程：`portfolio_trade_hyperliquid_tradfi.py`

其中：

- 发布进程文件名后缀必须为 `_main`
- 订阅进程文件名后缀不能为 `_main`

这样订阅进程才能正确接收共享行情，并实现多账户交易。

---
## 1.修改 vnpy\event\__init__.py
```python
from rust_event_engine import EVENT_TIMER, Event, EventEngine,MmapPublisher,MmapSubscriber
```

## 2. 修改 `utility.py`

在 `utility.py` 中新增以下两个函数：

```python
def mmap_event_subscriber(
    event_engine: "EventEngine",
    log_exception: Callable,
    gateway_names: List[str],
    channel: str,
) -> None:
    """
    共享内存缓存目录：
        Windows：AppData\\Local\\Temp\\shared_memory-rs
        Ubuntu：/dev/shm

    如有残留可手动删除。

    订阅共享内存事件总线并将 eTick. 事件注入本地 EventEngine。

    参数
    ----
    event_engine  : 当前进程的 EventEngine 实例
    log_exception : 异常日志回调（与原 receive_redis_stream 签名兼容）
    gateway_names : 交易接口名列表（断线时重置连接状态）
    channel       : 运行文件名（"stream_" + file_name）
    """
    global EVENT_ENGINE
    EVENT_ENGINE = event_engine

    # 推导共享内存名称
    resolved_name = derive_shm_name(channel)

    # 防止重复订阅
    if event_engine.channel:
        return

    event_engine.channel = resolved_name

    def _on_disconnect():
        msg = f"[MmapSubscriber] shm='{resolved_name}' 读取异常，已重置接口连接状态"
        write_log(msg)

        for gw in gateway_names:
            save_connection_status(gw, False)

    write_log(f"初始化 mmap 行情订阅，shm='{resolved_name}'")

    subscriber = MmapSubscriber(
        event_engine=event_engine,
        shm_name=resolved_name,
        log_exception=log_exception,
        on_disconnect=_on_disconnect,
    )

    event_engine.attach_subscriber(subscriber)

    # 若引擎已在运行则立即启动轮询线程；
    # 否则由 engine.start() 统一拉起
    if event_engine.is_loop_running():
        subscriber.start()

    # 保持当前线程存活
    # 轻量等待，与原 receive_redis_stream 行为一致
    import time as _time

    while event_engine.active:
        _time.sleep(3)


# ----------------------------------------------------------------------------------------------------


def derive_shm_name(channel: str) -> str:
    """
    从 channel 推导共享内存名称，与 MmapPublisher 使用相同规则。

    channel 示例：
        "stream_portfolio_trade_binancef_main"  → publisher 自身
        "stream_portfolio_trade_binancef_sub"   → 订阅 _main 发布的 shm
    """

    if channel.endswith("_sub"):
        core = channel.removesuffix("_sub") + "_main"

        # arbitrage <-> portfolio 互订
        if "arbitrage" in core:
            core = core.replace("arbitrage", "portfolio")
        elif "portfolio" in core:
            core = core.replace("portfolio", "arbitrage")
    else:
        # _main 或其他：直接用 channel 作为 core
        core = channel if channel.endswith("_main") else (
            channel.rsplit("_", 1)[0] + "_main"
        )

    return core
````

如果当前文件中尚未导入以下类型，需要补充：

```python
from typing import Callable, List
```

---

## 3. 修改 `vnpy/trader/engine.py`

修改 `connect` 方法，使其支持传入账户参数 `log_account`。

```python
def connect(self, gateway_name: str, log_account: dict = {}):
    """
    连接交易接口。

    不传入 log_account（账户参数）时，
    交易接口会读取默认账户字典。
    """
    gateway = self.get_gateway(gateway_name)

    if gateway:
        gateway.connect(log_account)  # 账户字典连接交易所
```

---

## 4. 修改 `hyperliquid_gateway.py`

修改交易所接口 `connect` 方法，使其可以使用外部传入的账户参数。

```python
from vnpy.trader.setting import hyperliquid_account_main  # 导入账户字典

def connect(self, log_account: dict = {}) -> None:
    """
    连接交易接口。
    """
    if not log_account:
        log_account = hyperliquid_account_main

    account_address: str = log_account["account_address"]
    private_address: str = log_account["private_address"]
```

这样在调用 `connect` 时，就可以传入不同的账户参数，实现多账户连接。

---

## 4. 发布进程与订阅进程运行规则

为了让订阅进程接收到共享行情，需要同时运行以下两个进程：

```text
portfolio_trade_hyperliquid_main.py
portfolio_trade_hyperliquid_tradfi.py
```

其中：

| 进程类型 | 文件命名规则          | 示例                                      |
| ---- | --------------- | --------------------------------------- |
| 发布进程 | 文件名后缀为 `_main`  | `portfolio_trade_hyperliquid_main.py`   |
| 订阅进程 | 文件名后缀不为 `_main` | `portfolio_trade_hyperliquid_tradfi.py` |

发布进程负责写入共享行情，订阅进程负责读取共享行情。

---

## 5. 共享内存说明

共享内存缓存目录如下：

| 系统      | 共享内存缓存目录                              |
| ------- | ------------------------------------- |
| Windows | `AppData\Local\Temp\shared_memory-rs` |
| Ubuntu  | `/dev/shm`                            |

如果共享内存有残留，可以手动删除对应目录下的残留文件。

---

## 6. 实现效果

完成以上改造后：

1. 发布进程负责发布共享行情。
2. 订阅进程通过共享内存接收行情。
3. `EventEngine` 可以接收共享内存中的 `eTick.` 事件。
4. `connect` 方法支持传入不同账户参数。
5. 可以实现 Hyperliquid 多账户交易。
