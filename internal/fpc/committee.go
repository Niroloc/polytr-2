// Package fpc implements the Fair Price Committee — a weighted average of
// five independent probability models for the BTC 5m binary option.
//
// All models output a probability in [0, 1] that the option settles ITM
// (i.e. spot closes above strike for an "above" market, or below for a
// "below" market — caller orients the strike accordingly).
//
//	Final_FP = Σ wᵢ · FPᵢ   with Σ wᵢ = 1
package fpc

import "github.com/cicn/polytr/internal/types"

// Model is one of the five FP estimators.
type Model interface {
	Name() string
	// Estimate returns probability in [0, 1].
	Estimate(types.MarketContext) float64
}

// Weights are normalized at construction.
type Weights struct {
	EV      float64
	BS      float64
	Imb     float64
	Z       float64
	KDE     float64
}

func (w Weights) Normalize() Weights {
	s := w.EV + w.BS + w.Imb + w.Z + w.KDE
	if s == 0 {
		return Weights{0.2, 0.2, 0.2, 0.2, 0.2}
	}
	return Weights{w.EV / s, w.BS / s, w.Imb / s, w.Z / s, w.KDE / s}
}

// Committee aggregates the five models.
type Committee struct {
	W   Weights
	EV  *EV
	BS  *BlackScholes
	Imb *Imbalance
	Z   *ZScore
	KDE *KDE
}

// Estimate is the canonical output of the committee: the weighted Final_FP
// plus the component breakdown (useful for the simulator UI and the signal
// module's diagnostics).
type Estimate struct {
	FinalFP  float64
	EV       float64
	BS       float64
	Imb      float64
	Z        float64
	KDE      float64
}

func New(w Weights) *Committee {
	return &Committee{
		W:   w.Normalize(),
		EV:  NewEV(),
		BS:  NewBlackScholes(),
		Imb: NewImbalance(0.5, 0.5),
		Z:   NewZScore(),
		KDE: NewKDE(),
	}
}

func (c *Committee) Estimate(m types.MarketContext) Estimate {
	ev := c.EV.Estimate(m)
	bs := c.BS.Estimate(m)
	im := c.Imb.Estimate(m)
	z := c.Z.Estimate(m)
	kd := c.KDE.Estimate(m)
	final := c.W.EV*ev + c.W.BS*bs + c.W.Imb*im + c.W.Z*z + c.W.KDE*kd
	if final < 0 {
		final = 0
	}
	if final > 1 {
		final = 1
	}
	return Estimate{
		FinalFP: final,
		EV:      ev, BS: bs, Imb: im, Z: z, KDE: kd,
	}
}
