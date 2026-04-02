# client.py
import zmq
import orjson
from datetime import datetime, UTC
import uuid
import time

context = zmq.Context()
socket = context.socket(zmq.PUSH)  # PUSH 发送数据
socket.connect("tcp://127.0.0.1:5555")  # 与 server 对应

# 测试发送 100 条行情
for i in range(100):
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "traceid": str(uuid.uuid4()),
        "symbol": "BTCUSDT",
        "ask1_price": 29350.5 + i*0.1,
        "ask1_qty": 0.12,
        "bid1_price": 29348.0 + i*0.1,
        "bid1_qty": 0.5
    }
    
    send_time = datetime.now(UTC)
    socket.send(orjson.dumps(payload))
    end_time = datetime.now(UTC)
    duration_us = int((end_time - send_time).total_seconds() * 1_000_000)
    print(f"发送耗时: {duration_us} μs")
    time.sleep(0.001)  # 可模拟行情间隔