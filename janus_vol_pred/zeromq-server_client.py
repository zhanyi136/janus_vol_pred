import zmq
import orjson
from datetime import datetime, UTC
import os
from loguru import logger

# ========== 日志文件夹和文件名 ==========
log_dir = os.path.join(os.path.dirname(__file__), "logs", "server")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "{time:YYYYMMDD_HHmmss}.log")

# ========== 日志设置 ==========
logger.add(
    log_path,
    rotation="10 MB",    # 单文件超过 10MB 自动轮转
    retention="7 days",  # 保留最近 7 天，超期自动删除
    encoding="utf-8",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
)

# ========== ZeroMQ 设置 ==========
context = zmq.Context()
receiver = context.socket(zmq.PULL)
receiver.bind("tcp://127.0.0.1:5555")  # 接收

sender = context.socket(zmq.PUSH)
sender.connect("tcp://127.0.0.1:6666")  # 推送到下游

logger.info("ZeroMQ 服务器已启动，等待数据...")

while True:
    raw = receiver.recv()
    recv_time = datetime.now(UTC)
    try:
        msg = orjson.loads(raw)
    except Exception as e:
        logger.error(f"解析失败: {e}")
        continue

    # 只处理 type=bookTicker 的消息
    if msg.get("type") == "bookTicker":
        bid = msg.get("bidPrice")
        ask = msg.get("askPrice")
        if bid is not None and ask is not None:
            mid_price = (float(bid) + float(ask)) / 2
        else:
            mid_price = None
        # 构造新消息
        new_msg = dict(msg)
        new_msg["mid_price"] = mid_price

        # 推送到下游
        sender.send(orjson.dumps(new_msg))
        logger.info(f"[{recv_time.isoformat()}] 已推送: {new_msg}")
    else:
        logger.info(f"[{recv_time.isoformat()}] 非bookTicker消息: {msg}")