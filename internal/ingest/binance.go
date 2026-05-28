package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/cicn/polytr/internal/book"
	"github.com/cicn/polytr/internal/types"
	"github.com/gorilla/websocket"
)

const (
	binanceWSBase = "wss://stream.binance.com:9443/stream"
	binanceREST   = "https://api.binance.com"
	binanceSymbol = "btcusdt"
)

// TickHandler receives every parsed tick (trade or L2). Implementations are
// expected to be non-blocking — drop, don't stall.
type TickHandler func(types.TickData)

// BinanceIngestor subscribes to combined streams:
//
//	<symbol>@depth20@100ms  → top-20 L2 snapshot every 100ms
//	<symbol>@aggTrade       → trade prints
//
// Choosing the snapshot stream (depth20) instead of the diff stream means we
// don't need to fetch a REST snapshot and reconcile lastUpdateId — the venue
// sends a fully-formed top-of-book every 100ms. Loses depth beyond 20 levels,
// which is irrelevant for FP imbalance calc anyway.
type BinanceIngestor struct {
	Symbol  string
	Book    *book.Book
	OnTick  TickHandler
	OnTrade func(price, qty float64, isBuyerMaker bool, ts int64)
	Logf    func(string, ...any)
}

func NewBinance(b *book.Book, onTick TickHandler) *BinanceIngestor {
	return &BinanceIngestor{
		Symbol: binanceSymbol,
		Book:   b,
		OnTick: onTick,
		Logf:   log.Printf,
	}
}

func (bi *BinanceIngestor) Run(ctx context.Context) {
	loopUntilDone(ctx, "binance", bi.session, bi.Logf)
}

func (bi *BinanceIngestor) session(ctx context.Context) error {
	url := fmt.Sprintf("%s?streams=%s@depth20@100ms/%s@aggTrade", binanceWSBase, bi.Symbol, bi.Symbol)
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	c, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	// Binance closes idle conns after ~3min of no pong; we keep pinging.
	c.SetPongHandler(func(string) error {
		_ = c.SetReadDeadline(time.Now().Add(75 * time.Second))
		return nil
	})
	_ = c.SetReadDeadline(time.Now().Add(75 * time.Second))

	pingT := time.NewTicker(30 * time.Second)
	defer pingT.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pingT.C:
				_ = c.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, msg, err := c.ReadMessage()
		if err != nil {
			return err
		}
		bi.dispatch(msg)
	}
}

type combinedFrame struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type bnDepth struct {
	Bids [][2]string `json:"bids"`
	Asks [][2]string `json:"asks"`
}

type bnAggTrade struct {
	EventTime int64  `json:"E"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	IsBuyerMM bool   `json:"m"` // true => buyer is maker => trade is sell-side
}

func (bi *BinanceIngestor) dispatch(msg []byte) {
	var f combinedFrame
	if err := json.Unmarshal(msg, &f); err != nil {
		return
	}
	switch {
	case len(f.Stream) >= 6 && f.Stream[len(f.Stream)-6:] == "@depth", contains(f.Stream, "@depth"):
		var d bnDepth
		if err := json.Unmarshal(f.Data, &d); err != nil {
			return
		}
		bi.applyDepth(d)
	case contains(f.Stream, "@aggTrade"):
		var t bnAggTrade
		if err := json.Unmarshal(f.Data, &t); err != nil {
			return
		}
		bi.applyTrade(t)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func (bi *BinanceIngestor) applyDepth(d bnDepth) {
	ts := time.Now().UnixNano()
	bids := make([]types.BookLevel, 0, len(d.Bids))
	asks := make([]types.BookLevel, 0, len(d.Asks))
	for _, lv := range d.Bids {
		p, _ := strconv.ParseFloat(lv[0], 64)
		q, _ := strconv.ParseFloat(lv[1], 64)
		bids = append(bids, types.BookLevel{Price: p, Amount: q})
	}
	for _, lv := range d.Asks {
		p, _ := strconv.ParseFloat(lv[0], 64)
		q, _ := strconv.ParseFloat(lv[1], 64)
		asks = append(asks, types.BookLevel{Price: p, Amount: q})
	}
	bi.Book.Reset(ts, bids, asks)

	// Emit a single composite L2 tick (price = mid, amount = depth-weighted volume).
	if mid, ok := bi.Book.Mid(); ok && bi.OnTick != nil {
		var totalAmt float64
		for _, b := range bids {
			totalAmt += b.Amount
		}
		for _, a := range asks {
			totalAmt += a.Amount
		}
		bi.OnTick(types.TickData{
			Timestamp: ts,
			Source:    types.SourceBinance,
			Type:      types.TickL2,
			Price:     mid,
			Amount:    totalAmt,
			Side:      types.SideNone,
		})
	}
}

func (bi *BinanceIngestor) applyTrade(t bnAggTrade) {
	ts := t.EventTime * int64(time.Millisecond)
	if ts == 0 {
		ts = time.Now().UnixNano()
	}
	price, _ := strconv.ParseFloat(t.Price, 64)
	qty, _ := strconv.ParseFloat(t.Quantity, 64)
	side := types.SideBuy
	if t.IsBuyerMM { // buyer was maker → aggressor was a seller
		side = types.SideSell
	}
	if bi.OnTrade != nil {
		bi.OnTrade(price, qty, t.IsBuyerMM, ts)
	}
	if bi.OnTick != nil {
		bi.OnTick(types.TickData{
			Timestamp: ts,
			Source:    types.SourceBinance,
			Type:      types.TickTrade,
			Price:     price,
			Amount:    qty,
			Side:      side,
		})
	}
}
