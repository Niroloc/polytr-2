// Package book implements a sorted L2 order book.
//
// Both sides are kept as slices sorted by price (bids descending, asks
// ascending). Updates use binary search for O(log n) lookup + O(n) splice;
// in practice n is bounded by the venue's depth stream (~20 levels) so this
// outperforms a map for the hot path: TopN(), Mid(), Imbalance().
package book

import (
	"sort"
	"sync"

	"github.com/cicn/polytr/internal/types"
)

type Book struct {
	mu        sync.RWMutex
	src       types.Source
	bids      []types.BookLevel // descending price
	asks      []types.BookLevel // ascending price
	updatedAt int64
}

func New(src types.Source) *Book {
	return &Book{src: src}
}

// Reset replaces both sides with a full snapshot. Caller's slices may be sorted
// in any order — we normalize.
func (b *Book) Reset(ts int64, bids, asks []types.BookLevel) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bids = append(b.bids[:0], bids...)
	b.asks = append(b.asks[:0], asks...)
	sort.Slice(b.bids, func(i, j int) bool { return b.bids[i].Price > b.bids[j].Price })
	sort.Slice(b.asks, func(i, j int) bool { return b.asks[i].Price < b.asks[j].Price })
	b.updatedAt = ts
}

// ApplyBid upserts (amount > 0) or removes (amount == 0) a bid level.
func (b *Book) ApplyBid(price, amount float64, ts int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bids = upsertDesc(b.bids, price, amount)
	b.updatedAt = ts
}

// ApplyAsk upserts (amount > 0) or removes (amount == 0) an ask level.
func (b *Book) ApplyAsk(price, amount float64, ts int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.asks = upsertAsc(b.asks, price, amount)
	b.updatedAt = ts
}

func upsertDesc(s []types.BookLevel, price, amount float64) []types.BookLevel {
	// descending: search for first index where s[i].Price <= price
	i := sort.Search(len(s), func(i int) bool { return s[i].Price <= price })
	if i < len(s) && s[i].Price == price {
		if amount == 0 {
			return append(s[:i], s[i+1:]...)
		}
		s[i].Amount = amount
		return s
	}
	if amount == 0 {
		return s
	}
	s = append(s, types.BookLevel{})
	copy(s[i+1:], s[i:])
	s[i] = types.BookLevel{Price: price, Amount: amount}
	return s
}

func upsertAsc(s []types.BookLevel, price, amount float64) []types.BookLevel {
	i := sort.Search(len(s), func(i int) bool { return s[i].Price >= price })
	if i < len(s) && s[i].Price == price {
		if amount == 0 {
			return append(s[:i], s[i+1:]...)
		}
		s[i].Amount = amount
		return s
	}
	if amount == 0 {
		return s
	}
	s = append(s, types.BookLevel{})
	copy(s[i+1:], s[i:])
	s[i] = types.BookLevel{Price: price, Amount: amount}
	return s
}

// BestBid returns the highest bid; ok=false if empty.
func (b *Book) BestBid() (types.BookLevel, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.bids) == 0 {
		return types.BookLevel{}, false
	}
	return b.bids[0], true
}

// BestAsk returns the lowest ask; ok=false if empty.
func (b *Book) BestAsk() (types.BookLevel, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.asks) == 0 {
		return types.BookLevel{}, false
	}
	return b.asks[0], true
}

// Mid returns (bid+ask)/2; ok=false if either side is empty.
func (b *Book) Mid() (float64, bool) {
	bb, okb := b.BestBid()
	ba, oka := b.BestAsk()
	if !okb || !oka {
		return 0, false
	}
	return (bb.Price + ba.Price) / 2, true
}

// Spread returns ask - bid.
func (b *Book) Spread() (float64, bool) {
	bb, okb := b.BestBid()
	ba, oka := b.BestAsk()
	if !okb || !oka {
		return 0, false
	}
	return ba.Price - bb.Price, true
}

// Imbalance returns (Vbid - Vask) / (Vbid + Vask) over the top n levels.
// Returns 0 if both sides are empty.
func (b *Book) Imbalance(n int) float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var vb, va float64
	for i := 0; i < n && i < len(b.bids); i++ {
		vb += b.bids[i].Amount
	}
	for i := 0; i < n && i < len(b.asks); i++ {
		va += b.asks[i].Amount
	}
	if vb+va == 0 {
		return 0
	}
	return (vb - va) / (vb + va)
}

// Snapshot returns a copy of the top-n levels of each side, plus venue/ts.
func (b *Book) Snapshot(n int) *types.BookSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := &types.BookSnapshot{Source: b.src, Timestamp: b.updatedAt}
	nb := n
	if nb > len(b.bids) {
		nb = len(b.bids)
	}
	na := n
	if na > len(b.asks) {
		na = len(b.asks)
	}
	out.Bids = append(out.Bids, b.bids[:nb]...)
	out.Asks = append(out.Asks, b.asks[:na]...)
	return out
}
