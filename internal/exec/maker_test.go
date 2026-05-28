package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/cicn/polytr/internal/book"
	"github.com/cicn/polytr/internal/types"
)

// TestPostOnlyNeverCrosses is the critical safety invariant: even if we ask
// the paper client to place an order at or beyond the opposite side of the
// book, it must reject with ErrWouldCross.
func TestPostOnlyNeverCrosses(t *testing.T) {
	bk := book.New(types.SourcePolymarket)
	bk.Reset(1,
		[]types.BookLevel{{Price: 0.50, Amount: 100}},
		[]types.BookLevel{{Price: 0.52, Amount: 100}},
	)
	c := NewPaperClient(bk, 0.01)

	if _, err := c.Place(context.Background(), SideBuy, 0.52, 10); !errors.Is(err, ErrWouldCross) {
		t.Fatalf("expected ErrWouldCross on bid at ask, got %v", err)
	}
	if _, err := c.Place(context.Background(), SideBuy, 0.55, 10); !errors.Is(err, ErrWouldCross) {
		t.Fatalf("expected ErrWouldCross on bid above ask, got %v", err)
	}
	if _, err := c.Place(context.Background(), SideSell, 0.50, 10); !errors.Is(err, ErrWouldCross) {
		t.Fatalf("expected ErrWouldCross on ask at bid, got %v", err)
	}
	// legal: bid below ask, ask above bid
	if _, err := c.Place(context.Background(), SideBuy, 0.51, 10); err != nil {
		t.Fatalf("legal bid rejected: %v", err)
	}
	if _, err := c.Place(context.Background(), SideSell, 0.51, 10); err != nil {
		t.Fatalf("legal ask rejected: %v", err)
	}
}

func TestMakerPriceStaysInside(t *testing.T) {
	bk := book.New(types.SourcePolymarket)
	bk.Reset(1,
		[]types.BookLevel{{Price: 0.50, Amount: 100}},
		[]types.BookLevel{{Price: 0.52, Amount: 100}},
	)
	m := NewManager(DefaultConfig(), NewPaperClient(bk, 0.01), bk)
	p, ok := m.makerPrice(SideBuy)
	if !ok || p >= 0.52 {
		t.Fatalf("BUY maker price must stay below ask 0.52, got %v", p)
	}
	p, ok = m.makerPrice(SideSell)
	if !ok || p <= 0.50 {
		t.Fatalf("SELL maker price must stay above bid 0.50, got %v", p)
	}
}
