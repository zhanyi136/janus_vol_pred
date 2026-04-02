import zmq
import orjson
import logging
from datetime import datetime, UTC
import os

# ========== 日志文件夹和文件名 ==========
log_dir = os.path.join(os.path.dirname(__file__), "logs", "server")
os.makedirs(log_dir, exist_ok=True)
log_filename = datetime.now(UTC).strftime("%Y%m%d_%H%M%S") + ".log"
log_path = os.path.join(log_dir, log_filename)

# ========== 日志设置 ==========
logger = logging.getLogger("zmq_server")
logger.setLevel(logging.INFO)

# 控制台日志
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

# 文件日志
fh = logging.FileHandler(log_path, encoding="utf-8")
fh.setFormatter(formatter)
logger.addHandler(fh)

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