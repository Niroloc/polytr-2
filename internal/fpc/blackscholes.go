package fpc

import (
	"math"

	"github.com/cicn/polytr/internal/types"
	"github.com/cicn/polytr/internal/util"
)

// BlackScholes models the binary cash-or-nothing probability of finishing ITM.
//
// For an "above" digital with strike K, expiry T, vol σ, rate r:
//
//	d2 = [ ln(S/K) + (r − ½σ²) · T ] / (σ · √T)
//	FP_BS = N(d2)
//
// Notes:
//   - T is in years. We pass seconds-left in MarketContext.TimeLeft();
//     here we convert with year = 365·86400.
//   - σ should be annualized. We estimate it from a rolling stddev of
//     log-returns of recent prices (see RealizedVol).
//   - r ≈ 0 for a 5m horizon; configurable.
type BlackScholes struct {
	RiskFree   float64 // annual risk-free; ~0 over 5min is fine
	MinVol     float64 // floor on σ to keep d2 finite
	WindowSecs float64 // realized-vol lookback in seconds (for return spacing)
}

func NewBlackScholes() *BlackScholes {
	return &BlackScholes{
		RiskFree:   0.0,
		MinVol:     0.05,
		WindowSecs: 600,
	}
}

func (b *BlackScholes) Name() string { return "BS" }

func (b *BlackScholes) Estimate(m types.MarketContext) float64 {
	S := m.BinanceMid
	K := m.Strike
	if S <= 0 || K <= 0 {
		return 0.5
	}
	T := m.TimeLeft() / (365.0 * 86400.0)
	if T <= 0 {
		// Settlement instant: ITM = 1 if S>K else 0
		if S > K {
			return 1
		}
		return 0
	}
	sigma := realizedVol(m.RecentPrices, b.WindowSecs)
	if sigma < b.MinVol {
		sigma = b.MinVol
	}
	d2 := (math.Log(S/K) + (b.RiskFree-0.5*sigma*sigma)*T) / (sigma * math.Sqrt(T))
	return util.Clamp(util.NormCDF(d2), 0, 1)
}

// realizedVol returns the annualized stddev of log-returns of `prices`,
// assuming roughly uniform spacing over `windowSecs` seconds.
//
// Annualization factor: √(yearSecs / sampleSpacingSecs)
func realizedVol(prices []float64, windowSecs float64) float64 {
	if len(prices) < 8 {
		return 0
	}
	rets := make([]float64, 0, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		if prices[i-1] <= 0 || prices[i] <= 0 {
			continue
		}
		rets = append(rets, math.Log(prices[i]/prices[i-1]))
	}
	if len(rets) < 4 {
		return 0
	}
	_, sd := util.MeanStd(rets)
	if windowSecs <= 0 {
		windowSecs = 600
	}
	spacing := windowSecs / float64(len(rets))
	if spacing <= 0 {
		spacing = 1
	}
	return sd * math.Sqrt(365.0*86400.0/spacing)
}
