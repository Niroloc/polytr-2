package fpc

import (
	"github.com/cicn/polytr/internal/types"
	"github.com/cicn/polytr/internal/util"
)

// ZScore is the mean-reversion model:
//
//	Z = (S − μ) / σ      over the recent window (RecentPrices)
//	FP_Z = 1 − Φ(Z)      (probability of returning toward mean)
//
// Caveat for the BTC 5m up-strike: a high Z (price extended above μ) implies
// pullback risk → lower FP_Z for "above". The formula `1 − Φ(Z)` produces
// exactly that monotone shape: Z = +2 → ~0.025, Z = 0 → 0.5, Z = −2 → ~0.975.
type ZScore struct{}

func NewZScore() *ZScore { return &ZScore{} }

func (z *ZScore) Name() string { return "Z" }

func (z *ZScore) Estimate(m types.MarketContext) float64 {
	if len(m.RecentPrices) < 8 || m.BinanceMid <= 0 {
		return 0.5
	}
	mean, sd := util.MeanStd(m.RecentPrices)
	if sd == 0 {
		return 0.5
	}
	zs := (m.BinanceMid - mean) / sd
	return util.Clamp(1-util.NormCDF(zs), 0, 1)
}
