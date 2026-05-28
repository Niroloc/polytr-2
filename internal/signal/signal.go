// Package signal converts FP estimates into trade decisions.
//
//	Entry: Final_FP − Market_Price >  Threshold_Entry  →  BUY YES   (binary cheap)
//	       Market_Price − Final_FP >  Threshold_Entry  →  SELL YES  (binary rich)
//	Exit/Hedge: |Final_FP − Market_Price| < Threshold_Exit  OR  TimeLeft < MinSeconds
package signal

import (
	"github.com/cicn/polytr/internal/fpc"
	"github.com/cicn/polytr/internal/types"
)

type Action int

const (
	ActionHold Action = iota
	ActionBuy         // open long YES (or hedge short)
	ActionSell        // open short YES (or hedge long)
	ActionExit        // unwind existing exposure
)

func (a Action) String() string {
	switch a {
	case ActionBuy:
		return "BUY"
	case ActionSell:
		return "SELL"
	case ActionExit:
		return "EXIT"
	default:
		return "HOLD"
	}
}

type Config struct {
	EntryEdge  float64 // probability-points needed to enter (e.g. 0.03 = 3¢)
	ExitEdge   float64 // probability-points at which to flatten
	MinSeconds float64 // close everything when TimeLeft < this
}

func DefaultConfig() Config {
	return Config{EntryEdge: 0.03, ExitEdge: 0.005, MinSeconds: 5}
}

type Decision struct {
	Action    Action
	Final     float64 // Final_FP from committee
	Market    float64 // current Polymarket mid
	Edge      float64 // Final - Market (signed)
	TimeLeft  float64 // seconds until settlement
	Reason    string
	Committee fpc.Estimate
}

// Decide computes the action given the FP estimate, current market mid, and
// existing position direction (1 = long YES, -1 = short YES, 0 = flat).
func Decide(cfg Config, est fpc.Estimate, m types.MarketContext, position int) Decision {
	market := m.PolyMid
	edge := est.FinalFP - market
	tl := m.TimeLeft()
	d := Decision{
		Final: est.FinalFP, Market: market, Edge: edge,
		TimeLeft: tl, Committee: est,
	}

	if tl < cfg.MinSeconds && position != 0 {
		d.Action = ActionExit
		d.Reason = "time-to-settlement under floor"
		return d
	}

	if position != 0 && absf(edge) < cfg.ExitEdge {
		d.Action = ActionExit
		d.Reason = "edge collapsed below exit threshold"
		return d
	}

	if position == 0 {
		if edge > cfg.EntryEdge {
			d.Action = ActionBuy
			d.Reason = "FP > market by entry edge"
			return d
		}
		if -edge > cfg.EntryEdge {
			d.Action = ActionSell
			d.Reason = "market > FP by entry edge"
			return d
		}
	}

	d.Action = ActionHold
	return d
}

func absf(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
