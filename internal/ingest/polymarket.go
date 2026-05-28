package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cicn/polytr/internal/book"
	"github.com/cicn/polytr/internal/types"
	"github.com/gorilla/websocket"
)

const (
	polyWSBase = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)

// errSwap is returned by PolymarketIngestor.session when SetToken closed the
// active WS to force a re-dial with a new tokenID. loopUntilDone treats it as
// an intentional reset (no backoff penalty).
var errSwap = errors.New("polymarket: token swap")

// PolymarketIngestor connects to the Polymarket CLOB WS feed for a single
// market (the active BTC 5m binary outcome). Polymarket prices are
// probabilities in [0, 1]; the L2 book is denominated in shares.
//
// Subscription protocol (CLOB websockets):
//
//	{ "type": "market", "assets_ids": ["<tokenID>"] }
//
// Inbound message kinds we care about:
//   - "book"          full snapshot
//   - "price_change"  delta updates to bids/asks
//   - "last_trade_price" / "tick_size_change" — informational
type PolymarketIngestor struct {
	Book   *book.Book
	OnTick TickHandler
	OnSwap func(newTokenID string)
	Logf   func(string, ...any)

	mu       sync.Mutex
	tokenID  string
	conn     *websocket.Conn // current WS, nil between sessions
	swapping bool            // set by SetToken; consumed by session on return
}

func NewPolymarket(tokenID string, b *book.Book, onTick TickHandler) *PolymarketIngestor {
	return &PolymarketIngestor{
		tokenID: tokenID,
		Book:    b,
		OnTick:  onTick,
		Logf:    log.Printf,
	}
}

// Token returns the currently subscribed CLOB tokenID.
func (pi *PolymarketIngestor) Token() string {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	return pi.tokenID
}

// SetToken swaps the subscribed market id. If a session is currently active,
// its WS conn is closed so loopUntilDone re-dials with the new tokenID.
// Returns true if the token actually changed (false = no-op).
func (pi *PolymarketIngestor) SetToken(tokenID string) bool {
	pi.mu.Lock()
	if pi.tokenID == tokenID {
		pi.mu.Unlock()
		return false
	}
	pi.tokenID = tokenID
	c := pi.conn
	if c != nil {
		pi.swapping = true
	}
	pi.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
	if pi.OnSwap != nil {
		pi.OnSwap(tokenID)
	}
	return true
}

func (pi *PolymarketIngestor) Run(ctx context.Context) {
	loopUntilDone(ctx, "polymarket", pi.session, pi.Logf)
}

func (pi *PolymarketIngestor) session(ctx context.Context) error {
	tok := pi.Token()
	if tok == "" {
		// nothing to subscribe to yet; back off and let discovery / scheduler set one
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return fmt.Errorf("no tokenID configured")
		}
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	c, _, err := dialer.DialContext(ctx, polyWSBase, nil)
	if err != nil {
		return err
	}
	pi.mu.Lock()
	pi.conn = c
	pi.swapping = false
	pi.mu.Unlock()
	defer func() {
		pi.mu.Lock()
		pi.conn = nil
		pi.mu.Unlock()
		_ = c.Close()
	}()

	sub := map[string]any{
		"type":       "market",
		"assets_ids": []string{tok},
	}
	if err := c.WriteJSON(sub); err != nil {
		return pi.wrapErr(err)
	}

	c.SetPongHandler(func(string) error {
		_ = c.SetReadDeadline(time.Now().Add(75 * time.Second))
		return nil
	})
	_ = c.SetReadDeadline(time.Now().Add(75 * time.Second))

	pingT := time.NewTicker(20 * time.Second)
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
			return pi.wrapErr(err)
		}
		pi.dispatch(msg)
	}
}

// wrapErr converts a session-ending error into errSwap if the close was
// triggered by SetToken, so loopUntilDone reconnects immediately without
// counting against the backoff attempt.
func (pi *PolymarketIngestor) wrapErr(err error) error {
	pi.mu.Lock()
	swap := pi.swapping
	pi.swapping = false
	pi.mu.Unlock()
	if swap {
		return errSwap
	}
	return err
}

// Messages can arrive as a JSON object or a top-level array of objects.
func (pi *PolymarketIngestor) dispatch(msg []byte) {
	if len(msg) == 0 {
		return
	}
	if msg[0] == '[' {
		var arr []json.RawMessage
		if err := json.Unmarshal(msg, &arr); err != nil {
			return
		}
		for _, m := range arr {
			pi.handleOne(m)
		}
		return
	}
	pi.handleOne(msg)
}

type polyEnvelope struct {
	EventType string `json:"event_type"`
	AssetID   string `json:"asset_id"`
}

type polyBookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type polyBookSnapshot struct {
	EventType string          `json:"event_type"`
	AssetID   string          `json:"asset_id"`
	Bids      []polyBookLevel `json:"bids"`
	Asks      []polyBookLevel `json:"asks"`
	Timestamp string          `json:"timestamp"`
}

type polyPriceChange struct {
	EventType string `json:"event_type"`
	AssetID   string `json:"asset_id"`
	Changes   []struct {
		Price string `json:"price"`
		Side  string `json:"side"`
		Size  string `json:"size"`
	} `json:"changes"`
	Timestamp string `json:"timestamp"`
}

type polyLastTrade struct {
	EventType  string `json:"event_type"`
	AssetID    string `json:"asset_id"`
	Price      string `json:"price"`
	Size       string `json:"size"`
	Side       string `json:"side"`
	FeeRateBps string `json:"fee_rate_bps"`
	Timestamp  string `json:"timestamp"`
}

func (pi *PolymarketIngestor) handleOne(raw []byte) {
	var env polyEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}
	switch env.EventType {
	case "book":
		var b polyBookSnapshot
		if err := json.Unmarshal(raw, &b); err != nil {
			return
		}
		pi.applyBook(b)
	case "price_change":
		var pc polyPriceChange
		if err := json.Unmarshal(raw, &pc); err != nil {
			return
		}
		pi.applyPriceChange(pc)
	case "last_trade_price":
		var t polyLastTrade
		if err := json.Unmarshal(raw, &t); err != nil {
			return
		}
		pi.applyTrade(t)
	}
}

func parseTs(s string) int64 {
	if s == "" {
		return time.Now().UnixNano()
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Now().UnixNano()
	}
	// Polymarket emits ms epoch; convert to ns.
	return v * int64(time.Millisecond)
}

func (pi *PolymarketIngestor) applyBook(b polyBookSnapshot) {
	ts := parseTs(b.Timestamp)
	bids := make([]types.BookLevel, 0, len(b.Bids))
	asks := make([]types.BookLevel, 0, len(b.Asks))
	for _, lv := range b.Bids {
		p, _ := strconv.ParseFloat(lv.Price, 64)
		s, _ := strconv.ParseFloat(lv.Size, 64)
		if s > 0 {
			bids = append(bids, types.BookLevel{Price: p, Amount: s})
		}
	}
	for _, lv := range b.Asks {
		p, _ := strconv.ParseFloat(lv.Price, 64)
		s, _ := strconv.ParseFloat(lv.Size, 64)
		if s > 0 {
			asks = append(asks, types.BookLevel{Price: p, Amount: s})
		}
	}
	pi.Book.Reset(ts, bids, asks)
	if mid, ok := pi.Book.Mid(); ok && pi.OnTick != nil {
		pi.OnTick(types.TickData{
			Timestamp: ts, Source: types.SourcePolymarket, Type: types.TickL2,
			Price: mid, Amount: 0, Side: types.SideNone,
		})
	}
}

func (pi *PolymarketIngestor) applyPriceChange(pc polyPriceChange) {
	ts := parseTs(pc.Timestamp)
	for _, ch := range pc.Changes {
		p, _ := strconv.ParseFloat(ch.Price, 64)
		s, _ := strconv.ParseFloat(ch.Size, 64)
		switch ch.Side {
		case "BUY":
			pi.Book.ApplyBid(p, s, ts)
		case "SELL":
			pi.Book.ApplyAsk(p, s, ts)
		}
	}
	if mid, ok := pi.Book.Mid(); ok && pi.OnTick != nil {
		pi.OnTick(types.TickData{
			Timestamp: ts, Source: types.SourcePolymarket, Type: types.TickL2,
			Price: mid, Amount: 0, Side: types.SideNone,
		})
	}
}

func (pi *PolymarketIngestor) applyTrade(t polyLastTrade) {
	ts := parseTs(t.Timestamp)
	p, _ := strconv.ParseFloat(t.Price, 64)
	s, _ := strconv.ParseFloat(t.Size, 64)
	side := types.SideBuy
	if t.Side == "SELL" {
		side = types.SideSell
	}
	if pi.OnTick != nil {
		pi.OnTick(types.TickData{
			Timestamp: ts, Source: types.SourcePolymarket, Type: types.TickTrade,
			Price: p, Amount: s, Side: side,
		})
	}
}
