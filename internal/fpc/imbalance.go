package fpc

import (
	"github.com/cicn/polytr/internal/types"
	"github.com/cicn/polytr/internal/util"
)

// Imbalance models the lead-lag arbitrage between Binance (lead) and
// Polymarket (lag):
//
//	I = (V_bid − V_ask) / (V_bid + V_ask)        per-venue
//	FP_Imb = 0.5 + α·I_Binance − β·I_Polymarket
//
// Intuition: if Binance bids are stacked relative to asks (I_Binance > 0)
// but Polymarket hasn't caught up (I_Polymarket ≈ 0), the binary is
// under-priced for the up-strike.
type Imbalance struct {
	Alpha float64 // sensitivity to Binance imbalance
	Beta  float64 // sensitivity to Polymarket imbalance
	Depth int     // levels to aggregate (default 5)
}

func NewImbalance(alpha, beta float64) *Imbalance {
	return &Imbalance{Alpha: alpha, Beta: beta, Depth: 5}
}

func (im *Imbalance) Name() string { return "Imb" }

func (im *Imbalance) Estimate(m types.MarketContext) float64 {
	depth := im.Depth
	if depth <= 0 {
		depth = 5
	}
	ibin := bookImbalance(m.BinanceBook, depth)
	ipoly := bookImbalance(m.PolyBook, depth)
	fp := 0.5 + im.Alpha*ibin - im.Beta*ipoly
	return util.Clamp(fp, 0, 1)
}

func bookImbalance(b *types.BookSnapshot, depth int) float64 {
	if b == nil {
		return 0
	}
	var vb, va float64
	for i := 0; i < depth && i < len(b.Bids); i++ {
		vb += b.Bids[i].Amount
	}
	for i := 0; i < depth && i < len(b.Asks); i++ {
		va += b.Asks[i].Amount
	}
	if vb+va == 0 {
		return 0
	}
	return (vb - va) / (vb + va)
}
