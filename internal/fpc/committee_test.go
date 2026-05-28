package fpc

import (
	"math"
	"testing"
	"time"

	"github.com/cicn/polytr/internal/types"
)

func TestNormalizeWeights(t *testing.T) {
	w := Weights{1, 1, 1, 1, 1}.Normalize()
	sum := w.EV + w.BS + w.Imb + w.Z + w.KDE
	if math.Abs(sum-1) > 1e-12 {
		t.Fatalf("weights do not sum to 1: %v", sum)
	}
	if math.Abs(w.EV-0.2) > 1e-12 {
		t.Fatalf("equal weights should each be 0.2, got %v", w.EV)
	}
}

func TestBlackScholesAtMoney(t *testing.T) {
	bs := NewBlackScholes()
	// At S=K with positive T and positive vol, N(d2) should be slightly under 0.5
	// (because of the −½σ²T drift term).
	m := types.MarketContext{
		Now:        time.Now(),
		BinanceMid: 70000,
		Strike:     70000,
		StrikeEnd:  time.Now().Add(150 * time.Second),
		RecentPrices: synthPrices(64, 70000, 50),
	}
	p := bs.Estimate(m)
	if p < 0.3 || p > 0.5 {
		t.Fatalf("ATM BS prob should be near 0.5, got %v", p)
	}
}

func TestEV(t *testing.T) {
	ev := NewEV()
	if got := ev.Estimate(types.MarketContext{}); got != 0.5 {
		t.Fatalf("empty EV should be 0.5, got %v", got)
	}
	if got := ev.Estimate(types.MarketContext{HistITM: 6, HistTotal: 10}); got != 0.6 {
		t.Fatalf("EV 6/10 = %v", got)
	}
}

func TestImbalance(t *testing.T) {
	im := NewImbalance(0.5, 0.5)
	m := types.MarketContext{
		BinanceBook: &types.BookSnapshot{
			Bids: []types.BookLevel{{Price: 100, Amount: 10}},
			Asks: []types.BookLevel{{Price: 101, Amount: 1}},
		},
		PolyBook: &types.BookSnapshot{
			Bids: []types.BookLevel{{Price: 0.5, Amount: 1}},
			Asks: []types.BookLevel{{Price: 0.51, Amount: 1}},
		},
	}
	p := im.Estimate(m)
	// I_bin = (10-1)/11 ≈ 0.818, I_poly = 0 → FP = 0.5 + 0.5*0.818 = 0.909
	if p < 0.85 || p > 0.95 {
		t.Fatalf("imbalance ~0.91 expected, got %v", p)
	}
}

func TestZScoreReversion(t *testing.T) {
	z := NewZScore()
	prices := synthPrices(64, 100, 1)
	// Push the latest spot well above the mean so Z >> 0
	m := types.MarketContext{
		BinanceMid:   105,
		RecentPrices: prices,
	}
	p := z.Estimate(m)
	if p > 0.4 { // we expect mean reversion → low prob of staying above
		t.Fatalf("high-Z reversion prob should be low, got %v", p)
	}
}

func TestKDEAboveStrike(t *testing.T) {
	k := NewKDE()
	prices := synthPrices(128, 100, 1)
	m := types.MarketContext{
		Strike:       95, // strike well below the cluster
		RecentPrices: prices,
	}
	p := k.Estimate(m)
	if p < 0.95 {
		t.Fatalf("KDE prob above-strike should be near 1, got %v", p)
	}
}

func TestCommitteeAggregates(t *testing.T) {
	c := New(Weights{1, 1, 1, 1, 1})
	m := types.MarketContext{
		BinanceMid:   70000,
		Strike:       70000,
		StrikeEnd:    time.Now().Add(150 * time.Second),
		RecentPrices: synthPrices(64, 70000, 50),
		HistITM:      5, HistTotal: 10,
		BinanceBook: &types.BookSnapshot{
			Bids: []types.BookLevel{{Price: 70000, Amount: 1}},
			Asks: []types.BookLevel{{Price: 70001, Amount: 1}},
		},
		PolyBook: &types.BookSnapshot{
			Bids: []types.BookLevel{{Price: 0.5, Amount: 1}},
			Asks: []types.BookLevel{{Price: 0.51, Amount: 1}},
		},
	}
	est := c.Estimate(m)
	if est.FinalFP < 0 || est.FinalFP > 1 {
		t.Fatalf("FinalFP out of bounds: %v", est.FinalFP)
	}
}

// synthPrices produces n prices ≈ mean with deterministic small oscillation.
func synthPrices(n int, mean, amp float64) []float64 {
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = mean + amp*math.Sin(float64(i)*0.31)
	}
	return out
}
