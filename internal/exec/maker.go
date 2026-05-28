// Package exec implements the Maker-Only execution engine.
//
// Hard contract: every order this package emits is POST-ONLY. If the venue's
// book has moved such that our limit would cross the spread, the order is
// either repriced to one tick *behind* best-of-side, or canceled outright.
// We NEVER pay the spread.
//
//   - On Polymarket CLOB, orderType:"GTC" with postOnly:true is the only
//     submission mode used.
//   - The Submit function is a pluggable closure so this package stays
//     transport-agnostic (the bot wires in either a signed REST client or a
//     paper-trading shim).
//
// Re-quote cadence: we never chase. If our resting order's price is no longer
// at the front of the book by more than `RequoteTicks` ticks, we cancel and
// re-place. Chasing the inside would otherwise cause repeated post-only
// rejections and waste rate budget.
package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cicn/polytr/internal/book"
	"github.com/cicn/polytr/internal/signal"
)

// ErrWouldCross is returned by VenueClient.Place when the venue rejects a
// post-only order that would have crossed. The Manager treats this as
// "not our fault, the market moved" and re-quotes after a cooldown.
var ErrWouldCross = errors.New("post-only would cross")

// Side mirrors the venue order side.
type Side int

const (
	SideBuy  Side = 1
	SideSell Side = -1
)

func (s Side) String() string {
	if s == SideBuy {
		return "BUY"
	}
	return "SELL"
}

// Order is the working snapshot of one resting maker quote.
type Order struct {
	ID       string
	Side     Side
	Price    float64
	Size     float64
	PlacedAt time.Time
}

// VenueClient is what the manager calls to actually talk to Polymarket
// (or a paper-trading shim during dev/replay).
//
// ALL implementations MUST honor post-only semantics. If the venue does not
// expose a post-only flag natively, the implementation must pre-check the
// top of the live book and return ErrWouldCross instead of submitting a
// taker.
type VenueClient interface {
	// Place submits a POST-ONLY limit order. Returns the venue's order ID.
	Place(ctx context.Context, side Side, price, size float64) (string, error)
	// Cancel cancels a resting order. No-op on already-filled IDs.
	Cancel(ctx context.Context, id string) error
}

// Config controls the maker logic.
type Config struct {
	TickSize       float64 // minimum price increment (Polymarket: 0.01 typical)
	Size           float64 // order size in shares
	RequoteTicks   int     // re-quote when our quote is this many ticks behind best
	RequoteCooldown time.Duration // min interval between re-quotes (rate limit safety)
	MaxQueueAge    time.Duration // cancel-and-replace if order rests this long
}

func DefaultConfig() Config {
	return Config{
		TickSize:        0.01,
		Size:            100,
		RequoteTicks:    1,
		RequoteCooldown: 200 * time.Millisecond,
		MaxQueueAge:     30 * time.Second,
	}
}

// Manager owns at most one resting order per side and keeps it pegged
// (post-only) one tick behind the best opposing price.
type Manager struct {
	cfg    Config
	client VenueClient
	polyBk *book.Book

	mu        sync.Mutex
	working   *Order
	position  int     // 1 long YES, -1 short YES, 0 flat
	avgPrice  float64
	lastQuote time.Time
}

func NewManager(cfg Config, c VenueClient, b *book.Book) *Manager {
	return &Manager{cfg: cfg, client: c, polyBk: b}
}

// Position returns the current direction (+1/0/-1).
func (m *Manager) Position() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.position
}

// OnFill is invoked by the venue client (or its wrapper) when a resting order
// fills. It updates the manager's position.
func (m *Manager) OnFill(side Side, price, size float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if side == SideBuy {
		m.position = 1
		m.avgPrice = price
	} else {
		m.position = -1
		m.avgPrice = price
	}
	m.working = nil
}

// OnSettle resets position state at strike settlement.
func (m *Manager) OnSettle() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.position = 0
	m.avgPrice = 0
	m.working = nil
}

// Apply consumes a signal decision and adjusts working orders accordingly.
//
// The maker rule:
//   - BUY  → post a bid one tick BELOW best ask (i.e. join the bid side at
//     best bid, or step one tick above current best bid if the spread permits).
//     We must NEVER price at or above best ask.
//   - SELL → mirror: post one tick ABOVE best bid, never at or below it.
//   - EXIT → cancel any working order; place a flattening order on the
//     opposite side under the same post-only rule.
func (m *Manager) Apply(ctx context.Context, d signal.Decision, logf func(string, ...any)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if time.Since(m.lastQuote) < m.cfg.RequoteCooldown {
		return
	}

	switch d.Action {
	case signal.ActionHold:
		m.maintain(ctx, logf)
	case signal.ActionBuy:
		m.placeOrReprice(ctx, SideBuy, logf)
	case signal.ActionSell:
		m.placeOrReprice(ctx, SideSell, logf)
	case signal.ActionExit:
		m.flatten(ctx, logf)
	}
}

// maintain checks if a working order is still well-placed; cancels & repricies
// if it has drifted off the inside.
func (m *Manager) maintain(ctx context.Context, logf func(string, ...any)) {
	if m.working == nil {
		return
	}
	if time.Since(m.working.PlacedAt) > m.cfg.MaxQueueAge {
		m.cancelWorking(ctx, "queue age", logf)
		return
	}
	want, ok := m.makerPrice(m.working.Side)
	if !ok {
		return
	}
	drift := absInt(int((want - m.working.Price) / m.cfg.TickSize))
	if drift >= m.cfg.RequoteTicks {
		side := m.working.Side
		m.cancelWorking(ctx, "drift", logf)
		m.place(ctx, side, want, logf)
	}
}

func (m *Manager) placeOrReprice(ctx context.Context, side Side, logf func(string, ...any)) {
	want, ok := m.makerPrice(side)
	if !ok {
		return
	}
	if m.working != nil {
		if m.working.Side == side && pricesClose(m.working.Price, want, m.cfg.TickSize, m.cfg.RequoteTicks) {
			return
		}
		m.cancelWorking(ctx, "side/price change", logf)
	}
	m.place(ctx, side, want, logf)
}

func (m *Manager) flatten(ctx context.Context, logf func(string, ...any)) {
	if m.working != nil {
		m.cancelWorking(ctx, "flatten", logf)
	}
	if m.position == 0 {
		return
	}
	// Close direction is opposite of the open direction.
	closeSide := SideSell
	if m.position < 0 {
		closeSide = SideBuy
	}
	want, ok := m.makerPrice(closeSide)
	if !ok {
		return
	}
	m.place(ctx, closeSide, want, logf)
}

// makerPrice computes a price that is guaranteed not to cross.
//
//	BUY:  bestBid + 1tick if (bestAsk − (bestBid+tick)) >= tick, else bestBid
//	SELL: bestAsk − 1tick if (bestAsk−tick − bestBid) >= tick, else bestAsk
//
// Returns ok=false if we don't have both sides of the book.
func (m *Manager) makerPrice(side Side) (float64, bool) {
	bb, okb := m.polyBk.BestBid()
	ba, oka := m.polyBk.BestAsk()
	if !okb || !oka {
		return 0, false
	}
	tick := m.cfg.TickSize
	if side == SideBuy {
		cand := bb.Price + tick
		if cand+tick > ba.Price {
			// would cross or sit on the ask — fall back to joining the bid
			return bb.Price, true
		}
		return cand, true
	}
	// SELL
	cand := ba.Price - tick
	if cand-tick < bb.Price {
		return ba.Price, true
	}
	return cand, true
}

func (m *Manager) place(ctx context.Context, side Side, price float64, logf func(string, ...any)) {
	id, err := m.client.Place(ctx, side, price, m.cfg.Size)
	m.lastQuote = time.Now()
	if errors.Is(err, ErrWouldCross) {
		logf("exec: post-only rejected (would cross) side=%s price=%.4f — backing off", side, price)
		return
	}
	if err != nil {
		logf("exec: place error side=%s price=%.4f err=%v", side, price, err)
		return
	}
	m.working = &Order{
		ID: id, Side: side, Price: price, Size: m.cfg.Size, PlacedAt: time.Now(),
	}
	logf("exec: POST-ONLY %s %.4f size=%.4f id=%s", side, price, m.cfg.Size, id)
}

func (m *Manager) cancelWorking(ctx context.Context, why string, logf func(string, ...any)) {
	if m.working == nil {
		return
	}
	if err := m.client.Cancel(ctx, m.working.ID); err != nil {
		logf("exec: cancel error id=%s err=%v", m.working.ID, err)
	} else {
		logf("exec: cancel id=%s (%s)", m.working.ID, why)
	}
	m.working = nil
}

func pricesClose(a, b, tick float64, n int) bool {
	d := absf(a - b)
	return d < float64(n)*tick
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func absf(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// ---------- Paper client (for dev/replay) ----------

// PaperClient is an in-memory VenueClient that simulates post-only placements
// against a snapshot book. It NEVER crosses: if the proposed price would be a
// taker, it returns ErrWouldCross.
type PaperClient struct {
	mu     sync.Mutex
	bk     *book.Book
	tick   float64
	seq    int
	live   map[string]Order
	OnEvent func(format string, args ...any)
}

func NewPaperClient(bk *book.Book, tick float64) *PaperClient {
	return &PaperClient{bk: bk, tick: tick, live: map[string]Order{}}
}

func (p *PaperClient) Place(ctx context.Context, side Side, price, size float64) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	bb, okb := p.bk.BestBid()
	ba, oka := p.bk.BestAsk()
	if okb && side == SideSell && price <= bb.Price {
		return "", ErrWouldCross
	}
	if oka && side == SideBuy && price >= ba.Price {
		return "", ErrWouldCross
	}
	p.seq++
	id := fmt.Sprintf("paper-%d", p.seq)
	p.live[id] = Order{ID: id, Side: side, Price: price, Size: size, PlacedAt: time.Now()}
	if p.OnEvent != nil {
		p.OnEvent("PaperPlace id=%s side=%s price=%.4f size=%.4f", id, side, price, size)
	}
	return id, nil
}

func (p *PaperClient) Cancel(ctx context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.live, id)
	if p.OnEvent != nil {
		p.OnEvent("PaperCancel id=%s", id)
	}
	return nil
}
