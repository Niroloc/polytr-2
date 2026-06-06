// Command replay reads the binary tick log and computes the FP committee
// estimate at every tick, then serves an interactive web chart of:
//
//	* Binance mid (left axis, USD)
//	* Polymarket mid (right axis, probability 0..1)
//	* Final_FP and the 5 component FPs (right axis)
//
// Usage:
//
//	replay --data ./data --from 2026-05-22T00:00:00Z --to 2026-05-23T00:00:00Z
//
// Then open http://localhost:8080/.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/cicn/polytr/internal/book"
	"github.com/cicn/polytr/internal/fpc"
	"github.com/cicn/polytr/internal/signal"
	"github.com/cicn/polytr/internal/storage"
	"github.com/cicn/polytr/internal/types"
	"github.com/cicn/polytr/internal/util"
)

type sample struct {
	T          int64   `json:"t"`
	BinMid     float64 `json:"bin"`
	PolyMid    float64 `json:"poly"`
	Strike     float64 `json:"k"`
	FinalFP    float64 `json:"fp"`
	EV         float64 `json:"ev"`
	BS         float64 `json:"bs"`
	Imb        float64 `json:"imb"`
	Z          float64 `json:"z"`
	KDE        float64 `json:"kde"`
	SignalAction string `json:"act"`
}

func main() {
	var (
		dataDir  = flag.String("data", "./data", "binary tick log root")
		fromStr  = flag.String("from", "", "start time RFC3339 (default: 24h ago)")
		toStr    = flag.String("to", "", "end time RFC3339 (default: now)")
		listen   = flag.String("listen", ":8080", "HTTP listen address")
		strikeMin  = flag.Int("strike-minutes", 5, "binary window minutes")
		entryEdge  = flag.Float64("entry-edge", 0.03, "")
		exitEdge   = flag.Float64("exit-edge", 0.005, "")
		minSecs    = flag.Float64("min-seconds", 5, "")
		stride     = flag.Int("stride", 50, "emit one sample per N ticks (downsamples UI payload)")
		logFile    = flag.String("log-file", "", "if set, append logs to this file in addition to stderr")
	)
	flag.Parse()

	logCloser, err := util.SetupFileLogging(*logFile)
	if err != nil {
		log.Fatalf("log setup: %v", err)
	}
	defer logCloser.Close()

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now
	if *fromStr != "" {
		t, err := time.Parse(time.RFC3339, *fromStr)
		if err != nil {
			log.Fatalf("--from: %v", err)
		}
		from = t
	}
	if *toStr != "" {
		t, err := time.Parse(time.RFC3339, *toStr)
		if err != nil {
			log.Fatalf("--to: %v", err)
		}
		to = t
	}

	log.Printf("replay: data=%s from=%s to=%s", *dataDir, from, to)
	samples, err := runReplay(*dataDir, from, to, *strikeMin, signal.Config{
		EntryEdge: *entryEdge, ExitEdge: *exitEdge, MinSeconds: *minSecs,
	}, *stride)
	if err != nil {
		log.Fatalf("replay: %v", err)
	}
	log.Printf("replay: generated %d samples", len(samples))

	mux := http.NewServeMux()
	mux.HandleFunc("/", indexHandler)
	mux.HandleFunc("/api/samples", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(samples)
	})
	log.Printf("serving on %s", *listen)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		log.Fatal(err)
	}
}

func runReplay(root string, from, to time.Time, strikeMin int, sigCfg signal.Config, stride int) ([]sample, error) {
	r, err := storage.NewReader(root, from, to)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	bnBook := book.New(types.SourceBinance)
	pmBook := book.New(types.SourcePolymarket)
	ring2h := util.NewPriceRing(int64(2*time.Hour), 4096)
	committee := fpc.New(fpc.Weights{EV: 1, BS: 1, Imb: 1, Z: 1, KDE: 1})

	type winState struct {
		start, end time.Time
		strike     float64
	}
	var win winState
	var itmCount, totalCount uint32

	out := make([]sample, 0, 1<<16)
	var t types.TickData
	count := 0
	for {
		ok, err := r.Next(&t)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		count++
		now := time.Unix(0, t.Timestamp).UTC()

		// Update orderbooks. Without diff streams we treat each L2 tick as a
		// price observation that doesn't mutate the book — books are reset by
		// proper L2 events at ingest time. For replay, we approximate the
		// state by using the L2 record's price as the most-recent mid and
		// pushing it into the 2h ring.
		switch t.Source {
		case types.SourceBinance:
			if t.Type == types.TickTrade || t.Type == types.TickL2 {
				ring2h.Push(t.Timestamp, t.Price)
				bnBook.ApplyBid(t.Price-0.5, t.Amount+1, t.Timestamp)
				bnBook.ApplyAsk(t.Price+0.5, t.Amount+1, t.Timestamp)
			}
		case types.SourcePolymarket:
			if t.Type == types.TickL2 || t.Type == types.TickTrade {
				pmBook.ApplyBid(clamp(t.Price-0.005, 0.001, 0.999), 100, t.Timestamp)
				pmBook.ApplyAsk(clamp(t.Price+0.005, 0.001, 0.999), 100, t.Timestamp)
			}
		}

		// Window rollover
		if win.end.IsZero() || !now.Before(win.end) {
			start := now.Truncate(time.Duration(strikeMin) * time.Minute)
			if !win.end.IsZero() {
				mid, _ := bnBook.Mid()
				if mid > win.strike {
					itmCount++
				}
				totalCount++
				if totalCount > 288 { // ~24h of 5m windows
					totalCount = 288
				}
			}
			mid, _ := bnBook.Mid()
			win = winState{
				start:  start,
				end:    start.Add(time.Duration(strikeMin) * time.Minute),
				strike: mid, // unrounded — see strikeWindow in cmd/bot/strike.go
			}
		}

		if count%stride != 0 {
			continue
		}

		bnMid, _ := bnBook.Mid()
		pmMid, _ := pmBook.Mid()
		if bnMid == 0 {
			continue
		}
		m := types.MarketContext{
			Now: now, BinanceMid: bnMid,
			BinanceBook:  bnBook.Snapshot(10),
			PolyMid:      pmMid,
			PolyBook:     pmBook.Snapshot(10),
			Strike:       win.strike,
			StrikeStart:  win.start,
			StrikeEnd:    win.end,
			RecentPrices: ring2h.SnapshotPrices(),
			HistITM:      itmCount,
			HistTotal:    totalCount,
		}
		est := committee.Estimate(m)
		dec := signal.Decide(sigCfg, est, m, 0)
		out = append(out, sample{
			T: t.Timestamp / int64(time.Millisecond), BinMid: bnMid, PolyMid: pmMid,
			Strike: win.strike,
			FinalFP: est.FinalFP, EV: est.EV, BS: est.BS, Imb: est.Imb, Z: est.Z, KDE: est.KDE,
			SignalAction: dec.Action.String(),
		})
	}
	return out, nil
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

// ---------- HTTP UI ----------

var indexOnce sync.Once
var indexBody []byte

func indexHandler(w http.ResponseWriter, r *http.Request) {
	indexOnce.Do(func() { indexBody = []byte(indexHTML) })
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexBody)
}

const indexHTML = `<!doctype html>
<html><head>
<meta charset="utf-8">
<title>polytr — FP vs Market</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2"></script>
<style>
  body { background:#0e0e10; color:#ddd; font-family: ui-monospace, monospace; margin:0; padding:16px; }
  h1 { font-size:14px; font-weight:600; color:#9af; margin:0 0 8px 0 }
  .row { display:flex; gap:8px; align-items:center; margin-bottom:8px; font-size:12px }
  .legend { display:flex; gap:12px; flex-wrap:wrap; font-size:11px; color:#888 }
  .swatch { display:inline-block; width:10px; height:10px; margin-right:4px; vertical-align:middle }
  #chart-wrap { height: 78vh; background:#141417; border:1px solid #222; padding:8px }
  button { background:#1a1a1f; color:#ddd; border:1px solid #333; padding:4px 8px; cursor:pointer; font-family:inherit }
  button:hover { background:#222 }
</style>
</head><body>
<h1>polytr — FP committee vs Polymarket BTC 5m</h1>
<div class="row">
  <button onclick="chart.resetZoom()">reset zoom</button>
  <span id="meta"></span>
  <span class="legend" id="legend"></span>
</div>
<div id="chart-wrap"><canvas id="chart"></canvas></div>
<script>
async function load() {
  const res = await fetch('/api/samples');
  const data = await res.json();
  document.getElementById('meta').textContent = ' samples: ' + data.length;
  const t = data.map(d => d.t);
  const ds = (label, key, color, axis) => ({
    label, data: data.map(d => ({x: d.t, y: d[key]})),
    borderColor: color, backgroundColor: color, pointRadius: 0,
    borderWidth: 1, tension: 0, yAxisID: axis
  });
  const ctx = document.getElementById('chart');
  window.chart = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: [
        ds('Binance Mid (USD)', 'bin',  '#5af', 'yUSD'),
        ds('Strike (USD)',      'k',    '#888', 'yUSD'),
        ds('Polymarket Mid',    'poly', '#fa5', 'yProb'),
        ds('Final FP',          'fp',   '#fff', 'yProb'),
        ds('FP_EV',  'ev',  '#7c7', 'yProb'),
        ds('FP_BS',  'bs',  '#c77', 'yProb'),
        ds('FP_Imb', 'imb', '#77c', 'yProb'),
        ds('FP_Z',   'z',   '#cc7', 'yProb'),
        ds('FP_KDE', 'kde', '#7cc', 'yProb'),
      ]
    },
    options: {
      animation: false, parsing: false, responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'nearest', intersect: false },
      plugins: {
        legend: { labels: { color:'#ccc', font:{size:10}, boxWidth:10 } },
        zoom: {
          pan: { enabled: true, mode: 'x' },
          zoom: { wheel:{enabled:true}, pinch:{enabled:true}, mode:'x' }
        },
        tooltip: { callbacks: {
          label: c => c.dataset.label + ': ' + (c.parsed.y!==null ? c.parsed.y.toFixed(4) : '')
        }}
      },
      scales: {
        x: { type:'time', ticks: {color:'#888'}, grid:{color:'#222'} },
        yUSD:  { position:'left',  ticks:{color:'#5af'}, grid:{color:'#222'}, title:{display:true,text:'USD',color:'#5af'} },
        yProb: { position:'right', min:0, max:1, ticks:{color:'#fa5'}, grid:{drawOnChartArea:false}, title:{display:true,text:'probability',color:'#fa5'} },
      }
    }
  });
}
load().catch(err => document.body.innerHTML = '<pre>'+err+'</pre>');
</script>
</body></html>`

// Suppress unused import warnings for Sprintf in case future formatting is added.
var _ = fmt.Sprintf
