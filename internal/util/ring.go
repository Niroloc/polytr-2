package util

import "sync"

// PriceRing is a thread-safe ring buffer of (timestamp, price) pairs with a
// time-based eviction policy. Used by the FP committee for KDE and Z-score
// over the last 2h, and by realized-vol over the last 10min.
type PriceRing struct {
	mu       sync.Mutex
	tsNs     []int64
	prices   []float64
	maxSpan  int64 // nanoseconds
	maxCount int   // hard cap as a safety net
}

func NewPriceRing(maxSpanNs int64, maxCount int) *PriceRing {
	return &PriceRing{
		tsNs:     make([]int64, 0, maxCount),
		prices:   make([]float64, 0, maxCount),
		maxSpan:  maxSpanNs,
		maxCount: maxCount,
	}
}

func (r *PriceRing) Push(ts int64, p float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tsNs = append(r.tsNs, ts)
	r.prices = append(r.prices, p)
	cutoff := ts - r.maxSpan
	i := 0
	for i < len(r.tsNs) && r.tsNs[i] < cutoff {
		i++
	}
	if i > 0 {
		r.tsNs = append(r.tsNs[:0], r.tsNs[i:]...)
		r.prices = append(r.prices[:0], r.prices[i:]...)
	}
	if len(r.tsNs) > r.maxCount {
		drop := len(r.tsNs) - r.maxCount
		r.tsNs = append(r.tsNs[:0], r.tsNs[drop:]...)
		r.prices = append(r.prices[:0], r.prices[drop:]...)
	}
}

// SnapshotPrices returns a copy of the buffered prices.
func (r *PriceRing) SnapshotPrices() []float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]float64, len(r.prices))
	copy(out, r.prices)
	return out
}

// SinceN returns the last n prices (or fewer if not enough buffered).
func (r *PriceRing) SinceN(n int) []float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n >= len(r.prices) {
		out := make([]float64, len(r.prices))
		copy(out, r.prices)
		return out
	}
	out := make([]float64, n)
	copy(out, r.prices[len(r.prices)-n:])
	return out
}

func (r *PriceRing) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.prices)
}
