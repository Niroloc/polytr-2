// Command replay reads the binary tick log and computes the FP committee
// estimate at every tick, then serves an interactive web chart of:
//
//	* Binance mid (left axis, USD)
//	* Polymarket mid (right axis, probability 0..1)
//	* Final_FP and the 5 component FPs (right axis)
//
// Usage:
//
//	# bounded historical replay
//	replay --data ./data --from 2026-05-22T00:00:00Z --to 2026-05-23T00:00:00Z
//
//	# open-ended live tail (omit --to or set --live; the dashboard polls /api/samples?since=...
//	# every 2s and appends new ticks to the chart as the running bot writes them)
//	replay --data ./data --live
//
// Then open http://localhost:8080/.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cicn/polytr/internal/book"
	"github.com/cicn/polytr/internal/fpc"
	"github.com/cicn/polytr/internal/signal"
	"github.com/cicn/polytr/internal/storage"
	"github.com/cicn/polytr/internal/types"
	"github.com/cicn/polytr/internal/util"
)

type sample struct {
	T            int64   `json:"t"`
	BinMid       float64 `json:"bin"`
	PolyMid      float64 `json:"poly"`
	Strike       float64 `json:"k"`
	FinalFP      float64 `json:"fp"`
	EV           float64 `json:"ev"`
	BS           float64 `json:"bs"`
	Imb          float64 `json:"imb"`
	Z            float64 `json:"z"`
	KDE          float64 `json:"kde"`
	SignalAction string  `json:"act"`
}

// engine streams ticks through the FP committee incrementally. State carries
// across calls to Advance so the same orderbooks / rolling rings / window
// counters seen during the initial historical pass keep accumulating as new
// ticks land in the tick log.
//
// Concurrency: Advance acquires `mu` for the duration of the read+compute,
// so concurrent HTTP polls don't double-consume the tick stream.
type engine struct {
	mu sync.Mutex

	root      string
	strikeMin int
	stride    int
	sigCfg    signal.Config

	bnBook    *book.Book
	pmBook    *book.Book
	ring2h    *util.PriceRing
	committee *fpc.Committee

	win     winState
	itm     uint32
	total   uint32
	tickIdx int

	samples []sample
	lastTs  int64 // ns; next read starts at lastTs+1ns

	bootstrapDone atomic.Bool // gates live tailing until the historical pass finishes
}

type winState struct {
	start, end time.Time
	strike     float64
}

func newEngine(root string, strikeMin, stride int, sigCfg signal.Config, from time.Time) *engine {
	return &engine{
		root:      root,
		strikeMin: strikeMin,
		stride:    stride,
		sigCfg:    sigCfg,
		bnBook:    book.New(types.SourceBinance),
		pmBook:    book.New(types.SourcePolymarket),
		ring2h:    util.NewPriceRing(int64(2*time.Hour), 4096),
		committee: fpc.New(fpc.Weights{EV: 1, BS: 1, Imb: 1, Z: 1, KDE: 1}),
		samples:   make([]sample, 0, 1<<16),
		lastTs:    from.UnixNano() - 1,
	}
}

// Bootstrap replays the historical window [from, to] in fixed-size chunks,
// releasing the engine lock between chunks so the HTTP server stays responsive
// and /api/samples can return progress as it accumulates. Runs in its own
// goroutine; sets bootstrapDone when complete so live tailing can begin.
func (e *engine) Bootstrap(from, to time.Time, chunk time.Duration) {
	defer e.bootstrapDone.Store(true)
	for cur := from; cur.Before(to); cur = cur.Add(chunk) {
		end := cur.Add(chunk)
		if end.After(to) {
			end = to
		}
		// Advance keys its low bound off e.lastTs, so passing successive chunk
		// ends walks the whole window; the lock is dropped after each chunk.
		if err := e.Advance(end); err != nil {
			log.Printf("replay: bootstrap chunk ending %s failed: %v", end.Format(time.RFC3339), err)
			return
		}
	}
}

// Advance consumes every tick with ts in (e.lastTs, to.UnixNano()], appends
// any produced samples to e.samples, and returns the indices of newly-added
// samples (start, end) so callers can slice the response window cheaply.
func (e *engine) Advance(to time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	from := time.Unix(0, e.lastTs+1).UTC()
	if !from.Before(to) {
		return nil
	}
	r, err := storage.NewReader(e.root, from, to)
	if err != nil {
		return err
	}
	defer r.Close()

	var t types.TickData
	for {
		ok, err := r.Next(&t)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		e.tickIdx++
		e.lastTs = t.Timestamp
		now := time.Unix(0, t.Timestamp).UTC()

		// Approximate book state from the L2 record's price.
		switch t.Source {
		case types.SourceBinance:
			if t.Type == types.TickTrade || t.Type == types.TickL2 {
				e.ring2h.Push(t.Timestamp, t.Price)
				e.bnBook.ApplyBid(t.Price-0.5, t.Amount+1, t.Timestamp)
				e.bnBook.ApplyAsk(t.Price+0.5, t.Amount+1, t.Timestamp)
			}
		case types.SourcePolymarket:
			if t.Type == types.TickL2 || t.Type == types.TickTrade {
				e.pmBook.ApplyBid(clamp(t.Price-0.005, 0.001, 0.999), 100, t.Timestamp)
				e.pmBook.ApplyAsk(clamp(t.Price+0.005, 0.001, 0.999), 100, t.Timestamp)
			}
		}

		// Window rollover.
		if e.win.end.IsZero() || !now.Before(e.win.end) {
			start := now.Truncate(time.Duration(e.strikeMin) * time.Minute)
			if !e.win.end.IsZero() {
				mid, _ := e.bnBook.Mid()
				if mid > e.win.strike {
					e.itm++
				}
				e.total++
				if e.total > 288 { // ~24h of 5m windows
					e.total = 288
				}
			}
			mid, _ := e.bnBook.Mid()
			e.win = winState{
				start:  start,
				end:    start.Add(time.Duration(e.strikeMin) * time.Minute),
				strike: mid,
			}
		}

		if e.tickIdx%e.stride != 0 {
			continue
		}
		bnMid, _ := e.bnBook.Mid()
		pmMid, _ := e.pmBook.Mid()
		if bnMid == 0 {
			continue
		}
		m := types.MarketContext{
			Now: now, BinanceMid: bnMid,
			BinanceBook:  e.bnBook.Snapshot(10),
			PolyMid:      pmMid,
			PolyBook:     e.pmBook.Snapshot(10),
			Strike:       e.win.strike,
			StrikeStart:  e.win.start,
			StrikeEnd:    e.win.end,
			RecentPrices: e.ring2h.SnapshotPrices(),
			HistITM:      e.itm,
			HistTotal:    e.total,
		}
		est := e.committee.Estimate(m)
		dec := signal.Decide(e.sigCfg, est, m, 0)
		e.samples = append(e.samples, sample{
			T:            t.Timestamp / int64(time.Millisecond),
			BinMid:       bnMid,
			PolyMid:      pmMid,
			Strike:       e.win.strike,
			FinalFP:      est.FinalFP,
			EV:           est.EV,
			BS:           est.BS,
			Imb:          est.Imb,
			Z:            est.Z,
			KDE:          est.KDE,
			SignalAction: dec.Action.String(),
		})
	}
	return nil
}

// SampleCount returns the number of samples produced so far (lock-safe).
func (e *engine) SampleCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.samples)
}

// Since returns samples with millisecond timestamp strictly greater than
// `sinceMs`. Caller must NOT mutate the returned slice.
func (e *engine) Since(sinceMs int64) []sample {
	e.mu.Lock()
	defer e.mu.Unlock()
	if sinceMs <= 0 {
		return e.samples
	}
	// samples are append-only and timestamp-ordered → binary search
	lo, hi := 0, len(e.samples)
	for lo < hi {
		mid := (lo + hi) / 2
		if e.samples[mid].T <= sinceMs {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return e.samples[lo:]
}

func main() {
	var (
		dataDir = flag.String("data", "./data", "binary tick log root")
		fromStr = flag.String("from", "", "start time RFC3339 (default: 24h ago)")
		toStr   = flag.String("to", "", "end time RFC3339 (empty + --live=true → open-ended live tail)")
		live    = flag.Bool("live", false, "keep advancing the engine on new ticks (poll cadence controlled by client)")
		listen  = flag.String("listen", ":8080", "HTTP listen address")
		strikeMin = flag.Int("strike-minutes", 5, "binary window minutes")
		entryEdge = flag.Float64("entry-edge", 0.03, "")
		exitEdge  = flag.Float64("exit-edge", 0.005, "")
		minSecs   = flag.Float64("min-seconds", 5, "")
		stride    = flag.Int("stride", 50, "emit one sample per N ticks (downsamples UI payload)")
		logFile   = flag.String("log-file", "", "if set, append logs to this file in addition to stderr")
	)
	flag.Parse()

	logCloser, err := util.SetupFileLogging(*logFile)
	if err != nil {
		log.Fatalf("log setup: %v", err)
	}
	defer logCloser.Close()

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	if *fromStr != "" {
		t, err := time.Parse(time.RFC3339, *fromStr)
		if err != nil {
			log.Fatalf("--from: %v", err)
		}
		from = t
	}
	var to time.Time
	if *toStr != "" {
		t, err := time.Parse(time.RFC3339, *toStr)
		if err != nil {
			log.Fatalf("--to: %v", err)
		}
		to = t
	} else {
		to = now
	}

	mode := "bounded"
	if *live || *toStr == "" {
		mode = "live"
	}
	log.Printf("replay: mode=%s data=%s from=%s to=%s", mode, *dataDir, from, to)

	eng := newEngine(*dataDir, *strikeMin, *stride, signal.Config{
		EntryEdge: *entryEdge, ExitEdge: *exitEdge, MinSeconds: *minSecs,
	}, from)

	// Bootstrap the historical window in the background so the HTTP server can
	// start serving immediately. Without this, a large tick log would block
	// ListenAndServe for minutes and the container reads as unhealthy / dead.
	go func() {
		start := time.Now()
		eng.Bootstrap(from, to, time.Hour)
		log.Printf("replay: bootstrap complete in %s (%d samples)", time.Since(start).Round(time.Millisecond), eng.SampleCount())
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/", indexHandler)
	mux.HandleFunc("/api/samples", func(w http.ResponseWriter, r *http.Request) {
		// Tail new ticks only once the historical pass is done — otherwise a
		// live poll would jump the cursor to "now" and swallow the rest of the
		// backlog in one locked read, defeating the chunked bootstrap.
		if mode == "live" && eng.bootstrapDone.Load() {
			if err := eng.Advance(time.Now().UTC()); err != nil {
				log.Printf("replay: advance: %v", err)
			}
		}
		var sinceMs int64
		if s := r.URL.Query().Get("since"); s != "" {
			if v, err := strconv.ParseInt(s, 10, 64); err == nil {
				sinceMs = v
			}
		}
		out := eng.Since(sinceMs)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/api/mode", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":      mode,
			"bootstrap": eng.bootstrapDone.Load(),
		})
	})
	log.Printf("serving on %s (bootstrap running in background)", *listen)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		log.Fatal(err)
	}
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
  .row { display:flex; gap:8px; align-items:center; margin-bottom:8px; font-size:12px; flex-wrap:wrap }
  .toggles { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:8px }
  .toggles label { display:inline-flex; align-items:center; gap:4px; padding:2px 6px; border:1px solid #333; border-radius:3px; cursor:pointer; font-size:11px; user-select:none }
  .toggles label.on { background:#1a1a1f }
  .toggles label.off { opacity:0.45 }
  .toggles input { display:none }
  .swatch { display:inline-block; width:10px; height:10px; vertical-align:middle }
  #chart-wrap { height: 78vh; background:#141417; border:1px solid #222; padding:8px }
  button { background:#1a1a1f; color:#ddd; border:1px solid #333; padding:4px 8px; cursor:pointer; font-family:inherit; font-size:11px }
  button:hover { background:#222 }
  .dot { display:inline-block; width:6px; height:6px; border-radius:50%; vertical-align:middle; margin-right:4px }
  .dot.live { background:#5f5; box-shadow:0 0 6px #5f5 }
  .dot.bounded { background:#888 }
  #banner { display:none; padding:10px 12px; margin-bottom:8px; border-radius:4px; font-size:12px; line-height:1.5 }
  #banner.err  { display:block; background:#3a1414; border:1px solid #832; color:#fbb }
  #banner.warn { display:block; background:#33300f; border:1px solid #875; color:#fe9 }
</style>
</head><body>
<h1>polytr — FP committee vs Polymarket BTC 5m</h1>
<div id="banner"></div>
<div class="row">
  <button id="btn-reset">reset zoom</button>
  <button id="btn-all-on">all on</button>
  <button id="btn-all-off">all off</button>
  <span id="mode-indicator"></span>
  <span id="meta"></span>
</div>
<div class="toggles" id="toggles"></div>
<div id="chart-wrap"><canvas id="chart"></canvas></div>
<script>
function showBanner(kind, html) {
  const b = document.getElementById('banner');
  b.className = kind;
  b.innerHTML = html;
}

// Fail loudly if the CDN-hosted Chart.js didn't load (e.g. the host blocks
// external CDNs) instead of throwing an opaque "Chart is not defined".
if (typeof Chart === 'undefined') {
  showBanner('err', '<b>Chart.js failed to load.</b> The chart library is served from ' +
    'cdn.jsdelivr.net — this host appears unable to reach it. Check the browser ' +
    'console / network tab. (If the box is behind a TLS-intercepting proxy, the CDN ' +
    'request is likely being blocked the same way gamma-api was.)');
  throw new Error('Chart.js unavailable');
}

const DATASETS = [
  {label:'Binance Mid (USD)', key:'bin',  color:'#5af', axis:'yUSD'},
  {label:'Strike (USD)',      key:'k',    color:'#888', axis:'yUSD'},
  {label:'Polymarket Mid',    key:'poly', color:'#fa5', axis:'yProb'},
  {label:'Final FP',          key:'fp',   color:'#fff', axis:'yProb'},
  {label:'FP_EV',  key:'ev',  color:'#7c7', axis:'yProb'},
  {label:'FP_BS',  key:'bs',  color:'#c77', axis:'yProb'},
  {label:'FP_Imb', key:'imb', color:'#77c', axis:'yProb'},
  {label:'FP_Z',   key:'z',   color:'#cc7', axis:'yProb'},
  {label:'FP_KDE', key:'kde', color:'#7cc', axis:'yProb'},
];
let chart, lastT = 0, mode = 'bounded', totalSamples = 0;

function buildToggles() {
  const wrap = document.getElementById('toggles');
  wrap.innerHTML = '';
  // restore previous selection from localStorage; default = all visible
  const saved = JSON.parse(localStorage.getItem('polytr.toggles') || '{}');
  chart.data.datasets.forEach((ds, i) => {
    if (saved[ds.label] === false) {
      chart.setDatasetVisibility(i, false);
    }
    const lbl = document.createElement('label');
    lbl.className = chart.isDatasetVisible(i) ? 'on' : 'off';
    lbl.innerHTML =
      '<input type="checkbox" data-i="'+i+'" '+(chart.isDatasetVisible(i)?'checked':'')+'>' +
      '<span class="swatch" style="background:'+ds.borderColor+'"></span>' +
      '<span>'+ds.label+'</span>';
    lbl.querySelector('input').addEventListener('change', e => {
      const idx = +e.target.dataset.i;
      chart.setDatasetVisibility(idx, e.target.checked);
      lbl.className = e.target.checked ? 'on' : 'off';
      saved[chart.data.datasets[idx].label] = e.target.checked;
      localStorage.setItem('polytr.toggles', JSON.stringify(saved));
      chart.update('none');
    });
    wrap.appendChild(lbl);
  });
  chart.update('none');
}

function toggleAll(on) {
  chart.data.datasets.forEach((_, i) => chart.setDatasetVisibility(i, on));
  buildToggles();
}

async function fetchMode() {
  try {
    const r = await fetch('/api/mode');
    const j = await r.json();
    mode = j.mode || 'bounded';
  } catch (_) {}
  const el = document.getElementById('mode-indicator');
  el.innerHTML = '<span class="dot '+mode+'"></span>' + mode;
}

async function bootstrap() {
  await fetchMode();
  const res = await fetch('/api/samples');
  const data = await res.json();
  totalSamples = data.length;
  if (data.length) lastT = data[data.length-1].t;
  const datasets = DATASETS.map(d => ({
    label: d.label,
    data: data.map(s => ({x: s.t, y: s[d.key]})),
    borderColor: d.color, backgroundColor: d.color,
    pointRadius: 0, borderWidth: 1, tension: 0, yAxisID: d.axis,
    _key: d.key,
  }));
  const ctx = document.getElementById('chart');
  chart = new Chart(ctx, {
    type: 'line',
    data: { datasets },
    options: {
      animation: false, parsing: false, responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'nearest', intersect: false },
      plugins: {
        legend: { display: false }, // we render our own toggles below
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
  buildToggles();
  updateMeta();

  // Wire buttons here (not inline onclick) so they always resolve the chart
  // variable, regardless of how the browser scopes top-level let for inline handlers.
  document.getElementById('btn-reset').addEventListener('click', () => chart.resetZoom());
  document.getElementById('btn-all-on').addEventListener('click', () => toggleAll(true));
  document.getElementById('btn-all-off').addEventListener('click', () => toggleAll(false));

  // Empty-data state is the #1 "nothing shows up" cause: the chart renders
  // but has no lines. Say so explicitly instead of leaving a blank canvas.
  if (totalSamples === 0) {
    if (mode === 'live') {
      showBanner('warn', '<b>No tick data yet.</b> The chart is empty because the log has ' +
        'no samples in range. If the bot is running, it may not be collecting ticks ' +
        '(e.g. Binance/Polymarket WS can\'t connect). Lines will appear automatically ' +
        'as data lands — this view is polling every 2s.');
    } else {
      showBanner('warn', '<b>No samples in the selected range.</b> Widen --from/--to, or ' +
        'confirm the tick log actually has data for this window.');
    }
  }
}

function updateMeta() {
  document.getElementById('meta').textContent = ' samples: ' + totalSamples +
    (lastT ? ' · last: ' + new Date(lastT).toISOString().substr(11,8) : '');
}

async function tick() {
  if (mode !== 'live') return;
  try {
    const res = await fetch('/api/samples?since=' + lastT);
    const data = await res.json();
    if (!data.length) return;
    if (totalSamples === 0) document.getElementById('banner').className = ''; // clear "no data" warn
    for (const s of data) {
      chart.data.datasets.forEach(ds => {
        ds.data.push({x: s.t, y: s[ds._key]});
      });
    }
    totalSamples += data.length;
    lastT = data[data.length-1].t;
    chart.update('none');
    updateMeta();
  } catch (err) {
    console.warn('poll failed', err);
  }
}

bootstrap().then(() => {
  if (mode === 'live') setInterval(tick, 2000);
}).catch(err => {
  console.error(err);
  showBanner('err', '<b>Failed to initialise dashboard:</b> ' + (err && err.message ? err.message : err));
});
</script>
</body></html>`

// Suppress unused import warnings for Sprintf in case future formatting is added.
var _ = fmt.Sprintf
