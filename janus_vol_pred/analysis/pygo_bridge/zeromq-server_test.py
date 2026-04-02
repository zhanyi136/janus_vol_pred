# server.py
import zmq
import orjson
import logging
from datetime import datetime, UTC

# ------------------ 日志 ------------------
logger = logging.getLogger("zmq_server")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

# ------------------ ZeroMQ 设置 ------------------
context = zmq.Context()
socket = context.socket(zmq.PULL)  # PULL 接收数据
socket.bind("tcp://127.0.0.1:5555")  # 本机通信，可改为 tcp://127.0.0.1:5555

logger.info("ZeroMQ 服务器已启动，等待行情数据...")

while True:
    raw = socket.recv()  # 接收字节
    recv_time = datetime.now(UTC)
    
    # 解析 JSON
    try:
        order_book = orjson.loads(raw)
    except Exception as e:
        logger.error(f"解析失败: {e}")
        continue

    # 打印日志并统计微秒耗时
    timestamp = order_book.get("timestamp")
    traceid = order_book.get("traceid")
    symbol = order_book.get("symbol")
    ask1_price = order_book.get("ask1_price")
    ask1_qty = order_book.get("ask1_qty")
    bid1_price = order_book.get("bid1_price")
    bid1_qty = order_book.get("bid1_qty")
    mid_price = order_book.get("mid_price")

    logger.info(f"[{recv_time.isoformat()}] {symbol}@{timestamp} | ask1: {ask1_price}@{ask1_qty} | "
                f"bid1: {bid1_price}@{bid1_qty} | mid_price: {mid_price} | traceid: {traceid}")

