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

# ------------------ 日志 ------------------
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

# ------------------ ZeroMQ 设置 ------------------
context = zmq.Context()
socket = context.socket(zmq.PULL)
socket.bind("tcp://127.0.0.1:5555")

logger.info("ZeroMQ 服务器已启动，等待数据...")

while True:
    raw = socket.recv()
    recv_time = datetime.now(UTC)
    try:
        msg = orjson.loads(raw)
    except Exception as e:
        logger.error(f"解析失败: {e}")
        continue

    # 直接打印收到的全部内容
    logger.info(f"[{recv_time.isoformat()}] 收到消息: {msg}")