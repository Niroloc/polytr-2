package fpc

import (
	"math"

	"github.com/cicn/polytr/internal/types"
	"github.com/cicn/polytr/internal/util"
)

// KDE estimates the probability mass above the strike using a Gaussian
// kernel density estimate over the last 2h of price points:
//
//	KDE(x) = (1 / N·h) · Σ φ((x − xᵢ) / h)
//	FP_KDE = ∫_K^∞ KDE(x) dx
//	       = (1/N) · Σ [1 − Φ((K − xᵢ) / h)]
//
// (The integral collapses because each Gaussian kernel integrates analytically.)
//
// Bandwidth h uses Silverman's rule: h = 1.06 · σ · N^(−1/5).
//
// The model captures "stickiness" around clusters of recent prints — local
// support/resistance — that a parametric model (BS) misses.
type KDE struct {
	MinSamples int
}

func NewKDE() *KDE { return &KDE{MinSamples: 32} }

func (k *KDE) Name() string { return "KDE" }

func (k *KDE) Estimate(m types.MarketContext) float64 {
	xs := m.RecentPrices
	if len(xs) < k.MinSamples || m.Strike <= 0 {
		return 0.5
	}
	_, sd := util.MeanStd(xs)
	if sd <= 0 {
		// degenerate — all prints at the same value
		if xs[0] > m.Strike {
			return 1
		}
		if xs[0] < m.Strike {
			return 0
		}
		return 0.5
	}
	n := float64(len(xs))
	h := 1.06 * sd * math.Pow(n, -0.2)
	if h <= 0 {
		h = sd
	}
	var sum float64
	K := m.Strike
	for _, x := range xs {
		z := (K - x) / h
		sum += 1 - util.NormCDF(z)
	}
	return util.Clamp(sum/n, 0, 1)
}
