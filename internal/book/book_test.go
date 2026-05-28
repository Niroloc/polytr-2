package book

import (
	"testing"

	"github.com/cicn/polytr/internal/types"
)

func TestApplyAndImbalance(t *testing.T) {
	b := New(types.SourceBinance)
	b.ApplyBid(100, 5, 1)
	b.ApplyBid(99, 5, 1)
	b.ApplyAsk(101, 2, 1)
	b.ApplyAsk(102, 2, 1)

	bb, _ := b.BestBid()
	if bb.Price != 100 {
		t.Fatalf("BestBid = %v", bb.Price)
	}
	ba, _ := b.BestAsk()
	if ba.Price != 101 {
		t.Fatalf("BestAsk = %v", ba.Price)
	}
	mid, _ := b.Mid()
	if mid != 100.5 {
		t.Fatalf("Mid = %v", mid)
	}
	imb := b.Imbalance(5)
	// (10-4)/(10+4) ≈ 0.4286
	if imb < 0.4 || imb > 0.45 {
		t.Fatalf("imbalance = %v", imb)
	}
}

func TestRemoveLevel(t *testing.T) {
	b := New(types.SourceBinance)
	b.ApplyBid(100, 5, 1)
	b.ApplyBid(99, 5, 1)
	b.ApplyBid(100, 0, 2) // remove
	bb, ok := b.BestBid()
	if !ok || bb.Price != 99 {
		t.Fatalf("after remove, best bid should be 99, got %v / ok=%v", bb.Price, ok)
	}
}

func TestSnapshotOrdering(t *testing.T) {
	b := New(types.SourceBinance)
	b.Reset(1, []types.BookLevel{{Price: 99, Amount: 1}, {Price: 100, Amount: 1}, {Price: 98, Amount: 1}},
		[]types.BookLevel{{Price: 102, Amount: 1}, {Price: 101, Amount: 1}})
	s := b.Snapshot(10)
	if s.Bids[0].Price != 100 || s.Bids[1].Price != 99 || s.Bids[2].Price != 98 {
		t.Fatalf("bids not descending: %+v", s.Bids)
	}
	if s.Asks[0].Price != 101 || s.Asks[1].Price != 102 {
		t.Fatalf("asks not ascending: %+v", s.Asks)
	}
}
