// Package types defines the wire-format primitives shared across the bot:
// ingestion, storage, fair-price models, and execution. Kept dependency-free
// so any package can import it without cycles.
package types

import "time"

// Source identifies the venue a tick came from. Fits in a uint8 so the
// on-disk binary log stays compact.
type Source uint8

const (
	SourceBinance    Source = 0
	SourcePolymarket Source = 1
)

func (s Source) String() string {
	switch s {
	case SourceBinance:
		return "binance"
	case SourcePolymarket:
		return "polymarket"
	default:
		return "unknown"
	}
}

// TickType distinguishes trades from L2 book updates.
type TickType uint8

const (
	TickTrade  TickType = 0
	TickL2     TickType = 1
	TickQuote  TickType = 2 // top-of-book snapshot, used for FP recompute triggers
	TickStrike TickType = 3 // 5m strike marker (Polymarket market id rollover)
)

// Side: 1 = bid/buy, -1 = ask/sell, 0 = N/A.
type Side int8

const (
	SideBuy  Side = 1
	SideSell Side = -1
	SideNone Side = 0
)

// TickData is the canonical compact tick. Encoded little-endian:
//
//	int64  ts_ns
//	uint8  source
//	uint8  type
//	float64 price
//	float64 amount
//	int8   side
//
// Total: 8+1+1+8+8+1 = 27 bytes/tick.
type TickData struct {
	Timestamp int64    // unix nanoseconds
	Source    Source   //
	Type      TickType //
	Price     float64
	Amount    float64
	Side      Side
}

// TickSize is the on-disk size of one TickData record.
const TickSize = 27

// BookLevel is one price level of an L2 book.
type BookLevel struct {
	Price  float64
	Amount float64
}

// BookSnapshot is a synchronized snapshot of one venue's L2 book.
type BookSnapshot struct {
	Source    Source
	Timestamp int64
	Bids      []BookLevel // sorted descending by price
	Asks      []BookLevel // sorted ascending by price
}

// MarketContext is what feeds the FP committee on every tick.
type MarketContext struct {
	Now          time.Time
	BinanceMid   float64       // Binance spot mid-price
	BinanceBook  *BookSnapshot // Binance L2 (top N levels)
	PolyMid      float64       // Polymarket binary option mid (0..1)
	PolyBook     *BookSnapshot // Polymarket CLOB L2 (prices in 0..1 probability)
	Strike       float64       // 5m strike (USD)
	StrikeStart  time.Time     // start of the current 5m window
	StrikeEnd    time.Time     // end of the current 5m window (settlement)
	RecentPrices []float64     // rolling Binance trade prices for Z-score / KDE
	HistITM      uint32        // ITM count last 24h (for EV)
	HistTotal    uint32        // total 5m intervals last 24h
}

// TimeLeft returns seconds until the 5m strike settles. Clamped to [0, 300].
func (m MarketContext) TimeLeft() float64 {
	d := m.StrikeEnd.Sub(m.Now).Seconds()
	if d < 0 {
		return 0
	}
	if d > 300 {
		return 300
	}
	return d
}
