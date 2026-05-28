package fpc

import "github.com/cicn/polytr/internal/types"

// EV is the Daily Expected Value model.
//
//	FP_EV = ITM_Count / Total_Intervals   over the last 24h.
//
// Captures intraday bias: if the up-strike has hit ITM 60% of recent
// 5m windows, our prior for the next one is 0.60.
//
// The counters are maintained by the bot (see signal/strike loop). EV
// itself is stateless — it just reads MarketContext.HistITM / HistTotal.
type EV struct{}

func NewEV() *EV { return &EV{} }

func (e *EV) Name() string { return "EV" }

func (e *EV) Estimate(m types.MarketContext) float64 {
	if m.HistTotal == 0 {
		return 0.5 // no prior → max-entropy
	}
	p := float64(m.HistITM) / float64(m.HistTotal)
	if p < 0 {
		return 0
	}
	if p > 1 {
		return 1
	}
	return p
}
