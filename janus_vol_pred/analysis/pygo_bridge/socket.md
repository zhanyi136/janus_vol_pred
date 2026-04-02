# PY与GO通信

PY 监听 tcp://127.0.0.1:5555
GO 监听 tcp://127.0.0.1:6666

# GO 行情推送

最优挂单如下：
```json
{
    "type": "bookTicker",
    "time": 1627794467000000, // us
    "symbol": "BTCUSDT",
    "updateID": 1111111111,
    "bidPrice": 1.23,
    "bidQty": 5.56,
    "askPrice": 1.22,
    "askQty": 5.56
}
```

逐笔成交如下：
```json
{
    "type": "trade",
    "time": 1627794467000000,
    "symbol": "BTCUSDT",
    "tradeID": 1111111111,
    "side": "BUY",
    "price": 1.23,
    "qty": 5.56,
}
```

深度如下:
```json
{
    "type": "depth",
    "time": 1627794467000000,
    "symbol": "BTCUSDT",
    "prevLastUpdateID": 1111111111,
    "firstUpdateID": 1111111111,
    "lastUpdateID": 1111111111,
    "bids": [
        [1.23, 100]
    ],
    "asks": [
        [1.22, 100]
    ]
}
```

账户信息如下：
```json
{
    "type": "account",
    "time": 1627794467000000,
    "symbol": "BTCUSDT",
    "entryPrice": 1.23,
    "qty": 100,
    "latestPrice": 1.22,
    "value": "100.00", // 持仓价值
    "maxValue": "1000.00" // 最大持仓价值
}
```
