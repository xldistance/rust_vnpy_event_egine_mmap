import multiprocessing
from datetime import datetime, timedelta
from pathlib import Path
from threading import Thread
from time import sleep
from typing import List, Optional

from vnpy.app.data_recorder import DataRecorderApp, RecorderEngine
from vnpy.app.portfolio_strategy import PortfolioStrategyApp, StrategyEngine
from vnpy.app.risk_manager import RiskManagerApp
from vnpy.event import EventEngine, MmapPublisher
from vnpy.trader.engine import MainEngine
from vnpy.trader.setting import PROXY_HOST, PROXY_PORT,hyperliquid_account_main
from vnpy.trader.utility import (
    load_json,
    load_redis_data,
    mmap_event_subscriber,
    save_connection_status,
    save_redis_data,
)
from vnpy.gateway.hyperliquid import HyperliquidGateway

file_name = Path(__file__).stem  # 当前运行程序文件名不含后缀
is_subscription_client = file_name.endswith("_sub") or not file_name.endswith("_main")  # 识别订阅进程状态
# 交易接口
GATEWAYS = [
    HyperliquidGateway,
]
# 运行策略列表
STRATEGIES = [
    "HyperliquidExitTrend",
]

# gateway_name和登录账户映射
# hyperliquid金库账户带单交易加密货币
LOG_ACCOUNT = {"HYPERLIQUID": hyperliquid_account_main}
# ---------------------------------------------------------------------------
def run_child_process() -> None:
    """CLI 交易子进程"""
    print("-" * 80)

    history_status   = not is_subscription_client
    event_engine = EventEngine()
    
    # ── main 进程：创建跨进程共享内存并挂载发布器，不创建则使用python异步队列传递数据 ───────────────────────────
    mmap_pub: Optional[MmapPublisher] = None
    if not is_subscription_client:
        mmap_pub = MmapPublisher(shm_name=file_name)
        event_engine.attach_publisher(mmap_pub)

    main_engine = MainEngine(event_engine)
    write_log   = main_engine.info

    write_log(f"启动 CLI 交易子进程，file_name='{file_name}'，is_subscription_client={is_subscription_client}")

    # 只有发布进程下载分钟bar数据，订阅行情数据，发布进程和订阅进程都必须获取合约数据，有的交易所发送委托单需要合约数据
    for gateway in GATEWAYS:
        main_engine.add_gateway(gateway, history_status=history_status,publish_status=history_status, book_trade_status=False)

    GATEWAY_NAMES = list(main_engine.gateways)
    save_redis_data(f"{file_name}", GATEWAY_NAMES)
    write_log("主引擎添加交易接口完成")

    if is_subscription_client:
        # 订阅进程必须等发布进程创建MmapPublisher后才能订阅行情，否则无法收到行情
        # 订阅端等待3秒后再启动订阅线程
        sleep(3)
    # ── 发布端，订阅端都启动 mmap 订阅线程 ─────────────────────────────────
    mmap_thread = Thread(
        target=mmap_event_subscriber,
        args=(
            event_engine,
            main_engine.log_exception,
            GATEWAY_NAMES,
            file_name,
        ),
        daemon=True,
        name="MmapStreamThread",
    )
    mmap_thread.start()

    # 连接交易所
    for gateway_name in GATEWAY_NAMES:
        main_engine.connect(gateway_name, LOG_ACCOUNT.get(gateway_name, {}))
        sleep(10)
        write_log(f"交易接口：{gateway_name} 连接成功")

    # 发布进程和订阅进程都必须调用subscribe_contract订阅合约(委托成交订阅也在subscribe函数里面，订阅进程不调用subscribe_contract则无法获取委托成交数据)，
    # 订阅行情过滤由交易接口执行
    main_engine.subscribe_contract()
    # 发布进程写入行情到本地（仅 _main 进程）
    if not is_subscription_client:
        data_recorder: RecorderEngine = main_engine.add_app(DataRecorderApp)
        data_recorder.load_setting(GATEWAY_NAMES)
        write_log("添加行情记录 App")

    main_engine.add_app(RiskManagerApp)
    write_log("添加风控 App")

    portfolio: StrategyEngine = main_engine.add_app(PortfolioStrategyApp)
    portfolio.init_engine(GATEWAY_NAMES)
    for strategy_name in STRATEGIES:
        portfolio.init_strategy(strategy_name)
        portfolio.start_strategy(strategy_name)
    write_log("portfolio 策略启动成功")

    # 等待策略启动（最多 120 秒）
    load_count = 0
    while (
        not portfolio.strategies
        or not all(s.trading for s in portfolio.strategies.values())
    ) and load_count < 40:
        sleep(3)
        load_count += 1

    if not portfolio.strategies or not all(s.trading for s in portfolio.strategies.values()):
        write_log(f"策略列表：{STRATEGIES} 启动失败，重启交易子进程")
        for gw in GATEWAY_NAMES:
            save_connection_status(gw, False)

    print("-" * 80)

    last_dt = datetime.now()
    while datetime.now() - last_dt < timedelta(hours=1):
        sleep(3)

    main_engine.save_contracts()
    main_engine.save_costs()
# ---------------------------------------------------------------------------
def run_parent_process() -> None:
    """CLI 交易父进程（逻辑不变）"""
    process: Optional[multiprocessing.Process] = None
    GATEWAY_NAMES: List[str] = []
    gateway_connect_count = len(GATEWAY_NAMES)

    while True:
        if gateway_connect_count == len(GATEWAY_NAMES) and process is None:
            process = multiprocessing.Process(target=run_child_process)
            process.start()
            print(f"{datetime.now()} | 交易接口：{list(LOG_ACCOUNT)}，启动子进程")

        if gateway_connect_count < len(GATEWAY_NAMES):
            if process:
                process.terminate()
                process.join()
                process = None
                print(f"{datetime.now()} | 交易接口：{GATEWAY_NAMES}，关闭子进程")
            for gw in GATEWAY_NAMES:
                save_connection_status(gw, True)

        if not GATEWAY_NAMES:
            GATEWAY_NAMES = load_redis_data(file_name) or []

        gateway_connect_count = sum(
            load_json("connection_status.json").get(gw, False)
            for gw in GATEWAY_NAMES
        )
        sleep(3)

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_parent_process()
