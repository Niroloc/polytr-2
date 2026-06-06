package main

import (
	"sync"
	"time"
)

// strikeWindow is the active BTC 5m bet window. Polymarket's BTC up/down 5m
// markets resolve "Up" iff Chainlink BTC/USD at window end ≥ Chainlink
// BTC/USD at window start — so Strike is just the spot price recorded at the
// instant the window opens, NOT a rounded round-number. We snapshot Binance
// mid at window open as a proxy for the Chainlink start-price (within ~$5-10
// on a 5m horizon). When Chainlink Data Streams credentials are wired in,
// the proxy gets swapped for the authoritative oracle reading.
type strikeWindow struct {
	Start  time.Time
	End    time.Time
	Strike float64
	// Direction: +1 means "above" market, -1 means "below". Caller toggles
	// based on which Polymarket outcome token is YES.
	Direction int
}

// strikeScheduler maintains the rolling 5m strike windows.
type strikeScheduler struct {
	mu        sync.RWMutex
	current   strikeWindow
	durMin    int // window duration in minutes
	listeners []func(strikeWindow)
}

func newStrikeScheduler(durMin int) *strikeScheduler {
	return &strikeScheduler{durMin: durMin}
}

func (s *strikeScheduler) Current() strikeWindow {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// Tick reconciles state against wall-clock + latest spot. Returns true if a
// new window opened.
func (s *strikeScheduler) Tick(now time.Time, spot float64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	end := s.current.End
	if !end.IsZero() && now.Before(end) {
		return false
	}
	start := now.Truncate(time.Duration(s.durMin) * time.Minute)
	s.current = strikeWindow{
		Start:     start,
		End:       start.Add(time.Duration(s.durMin) * time.Minute),
		Strike:    spot, // unrounded — see strikeWindow doc comment for rationale
		Direction: 1,    // default to "above"
	}
	w := s.current
	for _, fn := range s.listeners {
		go fn(w)
	}
	return true
}

// OnNewWindow registers a listener invoked (in its own goroutine) on every
// rollover. Multiple listeners are supported and fired in registration order.
func (s *strikeScheduler) OnNewWindow(fn func(strikeWindow)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, fn)
}

// settlementTracker keeps a 24h-rolling count of ITM outcomes for the EV model.
type settlementTracker struct {
	mu      sync.Mutex
	records []settlementRecord
}

type settlementRecord struct {
	at  time.Time
	itm bool
}

func newSettlementTracker() *settlementTracker { return &settlementTracker{} }

func (t *settlementTracker) Record(at time.Time, itm bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.records = append(t.records, settlementRecord{at: at, itm: itm})
	cutoff := at.Add(-24 * time.Hour)
	i := 0
	for i < len(t.records) && t.records[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		t.records = append(t.records[:0], t.records[i:]...)
	}
}

func (t *settlementTracker) Counts() (itm, total uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.records {
		total++
		if r.itm {
			itm++
		}
	}
	return
}
