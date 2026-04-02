package app

type (
	OrderUpdate struct {
		Event          string  `json:"event"`          // 事件类型
		Account        string  `json:"account"`        // 账户ID
		ClOrdID        string  `json:"clOrdID"`        // 客户端订单ID
		OrderID        string  `json:"orderID"`        // 订单ID
		Symbol         string  `json:"symbol"`         // 交易对
		Side           string  `json:"side"`           // 买卖方向 "BUY" 或 "SELL"
		PositionSide   string  `json:"positionSide"`   // 仓位方向 "LONG" 或 "SHORT"
		Price          float64 `json:"price"`          // 挂单价格
		Quantity       float64 `json:"quantity"`       // 挂单数量
		AvgPrice       float64 `json:"avgPrice"`       // 平均成交价格
		FilledQuantity float64 `json:"filledQuantity"` // 已成交数量
		Status         string  `json:"status"`         // 订单状态
		TradeTime      int64   `json:"tradeTime"`      // 成交时间
		UpdateTime     int64   `json:"updateTime"`     // 更新时间
	}

	PositionUpdate struct {
		Event        string  `json:"event"`        // 事件类型
		Account      string  `json:"account"`      // 账户ID
		Symbol       string  `json:"symbol"`       // 交易对
		PositionSide string  `json:"positionSide"` // 持仓方向
		EntryPrice   float64 `json:"entryPrice"`   // 开仓价格
		Quantity     float64 `json:"quantity"`     // 持仓数量
		UpdateTime   int64   `json:"updateTime"`   // 更新时间
	}

	BalanceUpdate struct {
		Event      string  `json:"event"`      // 事件类型
		Account    string  `json:"account"`    // 账户ID
		Balance    float64 `json:"balance"`    // 账户余额
		UpdateTime int64   `json:"updateTime"` // 更新时间
	}

	TradeUpdate struct {
		Event      string  `json:"event"`      // 事件类型
		Symbol     string  `json:"symbol"`     // 交易对
		TradeID    int64   `json:"tradeID"`    // 成交ID
		Side       string  `json:"side"`       // 买卖方向 "BUY" 或 "SELL"
		Price      float64 `json:"price"`      // 成交价格
		Qty        float64 `json:"qty"`        // 成交数量
		TradeTime  int64   `json:"tradeTime"`  // 成交时间
		UpdateTime int64   `json:"updateTime"` // 更新时间
	}

	BookTickerUpdate struct {
		Event      string  `json:"event"`      // 事件类型
		Symbol     string  `json:"symbol"`     // 交易对
		UpdateID   int64   `json:"updateID"`   // 更新ID
		BidPrice   float64 `json:"bidPrice"`   // 买一价
		BidQty     float64 `json:"bidQty"`     // 买一量
		AskPrice   float64 `json:"askPrice"`   // 卖一价
		AskQty     float64 `json:"askQty"`     // 卖一量
		TradeTime  int64   `json:"tradeTime"`  // 撮合时间
		UpdateTime int64   `json:"updateTime"` // 更新时间
	}

	DepthUpdate struct {
		Event            string      `json:"event"`            // 事件类型
		Symbol           string      `json:"symbol"`           // 交易对
		PrevLastUpdateID int64       `json:"prevLastUpdateID"` // 上一次更新ID
		FirstUpdateID    int64       `json:"firstUpdateID"`    // 本次更新的第一条ID
		LastUpdateID     int64       `json:"lastUpdateID"`     // 本次更新的最后一条ID
		Bids             [][]float64 `json:"bids"`             // [[price, qty], [price, qty], ...]
		Asks             [][]float64 `json:"asks"`             // [[price, qty], [price, qty], ...]
		TradeTime        int64       `json:"tradeTime"`        // 撮合时间
		UpdateTime       int64       `json:"updateTime"`       // 更新时间
	}

	EventType = string
)

var (
	TradeEvent      EventType = "trade"
	BookTickerEvent EventType = "bookTicker"
	DepthEvent      EventType = "depth"
	AccountEvent    EventType = "account"
	OrderEvent      EventType = "order"
	PositionEvent   EventType = "position"
	BalanceEvent    EventType = "balance"
)
