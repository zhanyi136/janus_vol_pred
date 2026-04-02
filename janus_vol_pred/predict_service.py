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
    interval_s: float,
) -> None:
    logger.info("采样线程启动，Numba JIT 预热中...")
    for computer in computers.values():
        computer.update(1.0, 1.0, 1)
    logger.info("Numba JIT 预热完成")

    while True:
        t0 = time.perf_counter()
        now_ns = time.time_ns()

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
                    tick["tradeTime"] * 1000,  # 微秒 → 纳秒
                )

                if features is None:
                    msg = {"timestamp": now_ns, "symbol": symbol, "status": "warmup"}
                    logger.debug(f"[{symbol}] 预热中 ({computers[symbol].buffer_count}/{computers[symbol]._warmup_ticks})")
                else:
                    loaders[symbol].check_and_reload()
                    pred_vol = loaders[symbol].predict(features)
                    if pred_vol is None:
                        msg = {"timestamp": now_ns, "symbol": symbol, "status": "no_model"}
                    else:
                        msg = {
                            "timestamp": now_ns,
                            "symbol": symbol,
                            "prediction": {"volatility": pred_vol},
                        }

                sender.send(orjson.dumps(msg), flags=zmq.NOBLOCK)
                if "prediction" in msg:
                    logger.info(f"[{symbol}] pred_vol={msg['prediction']['volatility']:.6f}")

            except Exception as e:
                logger.error(f"[{symbol}] 采样异常: {e}")

        elapsed = time.perf_counter() - t0
        sleep_time = interval_s - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)


# ============================================================
# 主入口
# ============================================================

if __name__ == "__main__":
    config_path = Path(__file__).parent / "config" / "config.yaml"
    config = load_yaml_config(config_path)

    rt_cfg = config["realtime"]
    sampling_cfg = config["sampling"]
    feat_cfg = config["features"]
    symbols: list[str] = config["realtime"]["symbols"]
    results_dir = Path(__file__).parent.parent / "results"
    interval_s = sampling_cfg["interval_ns"] / 1e9

    # 日志
    log_dir = Path(config["paths"]["log_root"]) / "predict_service"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_dir / "{time:YYYYMMDD_HHmmss}.log"),
        rotation="100 MB",
        retention="7 days",
        level=config["logging"]["level"],
        encoding="utf-8",
    )

    logger.info(f"启动预测服务 | symbols: {symbols}")
    logger.info(f"采样间隔: {sampling_cfg['interval_ns'] // 1_000_000}ms | 预热: {rt_cfg['warmup_minutes']}min")

    # 初始化 RealtimeFeatureComputer（每个 symbol 独立）
    computers = {
        symbol: RealtimeFeatureComputer(
            tick_size=1.0,  # 占位，收到 symbol 消息后实时更新
            vol_windows=feat_cfg["vol_windows"],
            interval_ns=sampling_cfg["interval_ns"],
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
    receiver.bind(rt_cfg["zmq_recv_addr"])
    sender = context.socket(zmq.PUSH)
    sender.connect(rt_cfg["zmq_send_addr"])
    logger.info(f"ZeroMQ PULL bind: {rt_cfg['zmq_recv_addr']}")
    logger.info(f"ZeroMQ PUSH connect: {rt_cfg['zmq_send_addr']}")

    # 接收线程（daemon）
    threading.Thread(target=recv_loop, args=(receiver,), daemon=True).start()

    # 主线程跑采样循环
    sample_loop(sender, symbols, computers, loaders, interval_s)