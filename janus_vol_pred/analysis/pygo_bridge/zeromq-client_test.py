import websocket
import json
import zmq
import orjson
from datetime import datetime, UTC
import uuid
import threading
import time

# ZeroMQ 设置
context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect("tcp://127.0.0.1:5555")

def on_message(ws, message):
    data = json.loads(message)
    ask1_price = float(data["a"])
    bid1_price = float(data["b"])
    mid_price = (ask1_price + bid1_price) / 2

    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "traceid": str(uuid.uuid4()),
        "symbol": data.get("s", "BTCUSDT"),
        "ask1_price": ask1_price,
        "ask1_qty": float(data["A"]),
        "bid1_price": bid1_price,
        "bid1_qty": float(data["B"]),
        "mid_price": mid_price
    }
    socket.send(orjson.dumps(payload))
    print("推送:", payload)
    time.sleep(10)  # 控制推送频率为10秒一次

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket opened")

if __name__ == "__main__":
    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(
        http_proxy_host="127.0.0.1",
        http_proxy_port=7890,
        proxy_type="http"
    )