"""
predict_service.py - 实盘波动率预测服务

流程：
    接收线程: ZeroMQ PULL → 维护 latest_tick / tick_size_cache
    采样线程: 每 20ms → 特征计算 → 模型推理 → ZeroMQ PUSH
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import orjson
import zmq
from loguru import logger

from model_loader import ModelLoader
from realtime_feature import RealtimeFeatureComputer
from utils.utils import load_yaml_config


# ============================================================
# 全局缓存（dict 单次赋值原子安全，无需加锁）
# ============================================================
latest_tick: dict[str, dict] = {}      # symbol -> 最新 bookTicker
tick_size_cache: dict[str, float] = {} # symbol -> tick_size


# ============================================================
# 接收线程
# ============================================================

def recv_loop(receiver: zmq.Socket) -> None:
    logger.info("接收线程启动")
    while True:
        try:
            # 收到就用 orjson 解析成 Python dict
            msg = orjson.loads(receiver.recv())

            if msg.get("Event") == "symbol":
                sym = msg.get("Symbol")
                if sym:
                    tick_size_cache[sym] = float(msg["TickSize"])
                    logger.debug(f"[{sym}] tick_size 更新: {msg['TickSize']}")

            elif msg.get("event") == "bookTicker":
                sym = msg.get("symbol")
                if sym:
                    cached = latest_tick.get(sym)
                    if cached is None or msg["updateID"] > cached["updateID"]:
                        latest_tick[sym] = msg

        except Exception as e:
            logger.error(f"接收线程异常: {e}")


# ============================================================
# 采样线程
# ============================================================

def sample_loop(
    sender: zmq.Socket,
    symbols: list[str],
    computers: dict[str, RealtimeFeatureComputer],
    loaders: dict[str, ModelLoader],
    interval_ns: int,
) -> None:
    logger.info("采样线程启动，Numba JIT 预热中...")
    # Numba 的规则是：第一次调用时编译成机器码，之后就很快。所以在服务启动时用假数据提前编译，之后真实采样时就不会卡了。
    for computer in computers.values():
        computer.update(1.0, 1.0, 1)
        computer._buffer[:] = 0.0       # 清空缓冲区内容
        computer._count = 0
        computer._head = 0
        computer._prev_mid_tick = None
    logger.info("Numba JIT 预热完成")

    interval_ns = int(interval_ns) # 整除时必须为整数

    # 对齐到第一个规整时间点
    now_ns = time.time_ns()
    next_tick_ns = (now_ns // interval_ns + 1) * interval_ns

    while True:
        # 等到规整时间点
        wait_ns = next_tick_ns - time.time_ns()
        if wait_ns > 0:
            time.sleep(wait_ns / 1e9)

        # 本轮的规整时间戳（用于 msg 的 timestamp 字段）
        tick_ns = next_tick_ns

        for symbol in symbols:
            try:
                tick_size = tick_size_cache.get(symbol)
                if tick_size is None:
                    continue  # 等待 symbol 消息到达

                tick = latest_tick.get(symbol)
                if tick is None:
                    continue  # 等待 bookTicker 到达

                computers[symbol].tick_size = tick_size
                features = computers[symbol].update(
                    float(tick["bidPrice"]),
                    float(tick["askPrice"]),
                    tick_ns,  # 使用采样的规整时间（纳秒）
                )

                # 统一格式
                if features is None:
                    msg = {"timestamp": tick_ns, "symbol": symbol, "status": "warmup"}
                else:
                    loaders[symbol].check_and_reload()
                    pred_vol = loaders[symbol].predict(features)
                    if pred_vol is None:
                        msg = {"timestamp": tick_ns, "symbol": symbol, "status": "no_model"}
                    else:
                        msg = {"timestamp": tick_ns, "symbol": symbol, "status": "ok", "volatility": pred_vol}
                logger.info(f"[PUSH] {msg}")

                sender.send(orjson.dumps(msg), flags=zmq.NOBLOCK)

            except Exception as e:
                logger.error(f"[{symbol}] 采样异常: {e}")

        # 计算下一个规整时间点（跳过已经过去的点）
        now_ns = time.time_ns()
        next_tick_ns = (now_ns // interval_ns + 1) * interval_ns



# ============================================================
# 主入口
# ============================================================

if __name__ == "__main__":
    config_path = Path(__file__).parent / "config" / "config.yaml"
    config = load_yaml_config(config_path)

    rt_cfg = config["realtime_predict"]
    sampling_cfg = config["sampling"]
    feat_cfg = config["features"]
    symbols: list[str] = config["execution"]["symbols"]
    results_dir = Path(rt_cfg["output_root"]) / rt_cfg["results_output_dir"]
    interval_ns = sampling_cfg["interval_ns"]

    # 日志
    log_dir = Path(rt_cfg["log_dir"]) / "predict_service"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_dir / "{time:YYYYMMDD_HHmmss}.log"),
        rotation="100 MB",
        retention="7 days",
        level=config["logging"]["level"],
        encoding="utf-8",
    )

    logger.info(f"启动预测服务 | symbols: {symbols}")
    logger.info(f"采样间隔: {interval_ns // 1_000_000}ms | 预热: {rt_cfg['warmup_minutes']}min")

    # 初始化 RealtimeFeatureComputer（每个 symbol 独立）
    computers = {
        symbol: RealtimeFeatureComputer(
            tick_size=1.0,  # 占位，收到 symbol 消息后实时更新
            vol_windows=feat_cfg["vol_windows"],
            interval_ns=interval_ns,
            warmup_minutes=rt_cfg["warmup_minutes"],
        )
        for symbol in symbols
    }

    # 初始化 ModelLoader（每个 symbol 独立）
    loaders = {
        symbol: ModelLoader(
            symbol=symbol,
            results_dir=results_dir,
            lookback_days=rt_cfg["model_lookback_days"],
        )
        for symbol in symbols
    }

    # ZeroMQ
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    # bind = 主动监听，等别人来连接。同事的程序 connect 到 5555，把数据推过来。
    receiver.bind(rt_cfg["zmq_recv_addr"])
    # connect = 主动连接别人。Go 程序在 6666 上 bind 等待，我们把预测结果推过去。
    sender = context.socket(zmq.PUSH)
    sender.connect(rt_cfg["zmq_send_addr"])
    logger.info(f"ZeroMQ PULL bind: {rt_cfg['zmq_recv_addr']}")
    logger.info(f"ZeroMQ PUSH connect: {rt_cfg['zmq_send_addr']}")

    # 接收线程（daemon）
    threading.Thread(target=recv_loop, args=(receiver,), daemon=True).start()

    # 主线程跑采样循环
    sample_loop(sender, symbols, computers, loaders, interval_ns)
