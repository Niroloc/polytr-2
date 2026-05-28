// Command bot is the live trading process.
//
// It wires:
//
//	ingestion (Binance + Polymarket WS)
//	  ↓
//	orderbooks + tick store (binary, 7d retention)
//	  ↓
//	FP Committee (every Binance L2/trade or Polymarket book update)
//	  ↓
//	signal module
//	  ↓
//	Maker-only execution manager (Post-Only orders to the venue or paper client)
//
// All sub-systems are independent goroutines communicating via channels;
// the main goroutine just waits for SIGINT.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	ossignal "os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/cicn/polytr/internal/book"
	"github.com/cicn/polytr/internal/exec"
	"github.com/cicn/polytr/internal/fpc"
	"github.com/cicn/polytr/internal/ingest"
	"github.com/cicn/polytr/internal/signal"
	"github.com/cicn/polytr/internal/storage"
	"github.com/cicn/polytr/internal/types"
	"github.com/cicn/polytr/internal/util"
)

func main() {
	var (
		dataDir          = flag.String("data", "./data", "binary tick log root")
		polyDiscoverInt  = flag.Duration("poly-discover-interval", 60*time.Second, "safety-net auto-discovery poll cadence")
		strikeStep       = flag.Float64("strike-step", 100.0, "USD step for strike rounding")
		strikeMinutes    = flag.Int("strike-minutes", 5, "binary window length in minutes")
		entryEdge        = flag.Float64("entry-edge", 0.03, "min FP-vs-market edge to enter (probability)")
		exitEdge         = flag.Float64("exit-edge", 0.005, "edge under which to flatten")
		minSeconds       = flag.Float64("min-seconds", 5, "exit when window time-left under this")
		tickSize         = flag.Float64("tick", 0.01, "venue tick size (Polymarket)")
		orderSize        = flag.Float64("size", 100, "order size in shares")
		paper            = flag.Bool("paper", true, "use paper-trading venue client (no real orders)")
		logFile          = flag.String("log-file", "", "if set, append logs to this file in addition to stderr")
	)
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	runtime.GOMAXPROCS(runtime.NumCPU())

	logCloser, err := util.SetupFileLogging(*logFile)
	if err != nil {
		log.Fatalf("log setup: %v", err)
	}
	defer logCloser.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts, err := storage.Open(*dataDir)
	if err != nil {
		log.Fatalf("storage open: %v", err)
	}
	defer ts.Close()

	bnBook := book.New(types.SourceBinance)
	pmBook := book.New(types.SourcePolymarket)

	priceRing2h := util.NewPriceRing(int64(2*time.Hour), 4096)
	priceRing10m := util.NewPriceRing(int64(10*time.Minute), 2048)
	_ = priceRing10m // reserved for realized-vol consumers; BS pulls from 2h ring

	bnIngestor := ingest.NewBinance(bnBook, func(tick types.TickData) {
		ts.Write(tick)
		if tick.Type == types.TickTrade {
			priceRing2h.Push(tick.Timestamp, tick.Price)
			priceRing10m.Push(tick.Timestamp, tick.Price)
		} else if tick.Type == types.TickL2 {
			priceRing2h.Push(tick.Timestamp, tick.Price)
		}
	})
	pmIngestor := ingest.NewPolymarket("", pmBook, func(tick types.TickData) {
		ts.Write(tick)
	})

	go bnIngestor.Run(ctx)
	go pmIngestor.Run(ctx)

	committee := fpc.New(fpc.Weights{EV: 1, BS: 1, Imb: 1, Z: 1, KDE: 1})
	sigCfg := signal.Config{
		EntryEdge:  *entryEdge,
		ExitEdge:   *exitEdge,
		MinSeconds: *minSeconds,
	}

	scheduler := newStrikeScheduler(*strikeStep, *strikeMinutes)
	settle := newSettlementTracker()

	var venueClient exec.VenueClient
	paperClient := exec.NewPaperClient(pmBook, *tickSize)
	paperClient.OnEvent = func(f string, a ...any) { log.Printf("[paper] "+f, a...) }
	if *paper {
		venueClient = paperClient
	} else {
		log.Fatal("live venue client not implemented in this build; run with --paper")
	}
	mgr := exec.NewManager(exec.Config{
		TickSize:        *tickSize,
		Size:            *orderSize,
		RequoteTicks:    1,
		RequoteCooldown: 200 * time.Millisecond,
		MaxQueueAge:     30 * time.Second,
	}, venueClient, pmBook)

	scheduler.OnNewWindow(func(w strikeWindow) {
		log.Printf("[strike] window opened start=%s end=%s strike=%.2f",
			w.Start.Format(time.RFC3339), w.End.Format(time.RFC3339), w.Strike)
		mgr.OnSettle()
	})

	// Marker write on every real Polymarket token swap. Side encodes the
	// direction the subscribed token represents (gamma currently always
	// resolves to "Up" → +1; switch to -1 here if we ever subscribe to Down).
	// That's all backtests need: window boundary is derived from the marker's
	// timestamp truncated to the 5m window; ticks between two markers belong
	// to the most-recent marker's direction.
	onTokenSwap := func(_ time.Time, _ string) {
		ts.Write(types.TickData{
			Timestamp: time.Now().UnixNano(),
			Source:    types.SourcePolymarket,
			Type:      types.TickStrike,
			Side:      types.SideBuy,
		})
	}

	log.Printf("[discover] gamma-api auto-discovery enabled (safety-net interval=%s)", *polyDiscoverInt)
	runDiscovery(ctx, ingest.NewGammaClient(), pmIngestor, scheduler, *strikeMinutes, *polyDiscoverInt, onTokenSwap)

	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		var lastWindowStart time.Time
		var lastLogSecond int
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				bnMid, okB := bnBook.Mid()
				if !okB {
					continue
				}
				scheduler.Tick(now, bnMid)
				win := scheduler.Current()
				pmMid, _ := pmBook.Mid()
				itm, total := settle.Counts()
				m := types.MarketContext{
					Now:          now,
					BinanceMid:   bnMid,
					BinanceBook:  bnBook.Snapshot(10),
					PolyMid:      pmMid,
					PolyBook:     pmBook.Snapshot(10),
					Strike:       win.Strike,
					StrikeStart:  win.Start,
					StrikeEnd:    win.End,
					RecentPrices: priceRing2h.SnapshotPrices(),
					HistITM:      itm,
					HistTotal:    total,
				}
				est := committee.Estimate(m)
				dec := signal.Decide(sigCfg, est, m, mgr.Position())
				mgr.Apply(ctx, dec, log.Printf)
				if !lastWindowStart.IsZero() && !win.Start.Equal(lastWindowStart) {
					itmOutcome := bnMid > win.Strike
					settle.Record(now, itmOutcome)
				}
				lastWindowStart = win.Start
				if now.Second() != lastLogSecond && now.Second()%5 == 0 {
					lastLogSecond = now.Second()
					log.Printf("[fp] mid=%.2f K=%.2f tl=%.0fs final=%.4f ev=%.4f bs=%.4f imb=%.4f z=%.4f kde=%.4f pmMid=%.4f act=%s",
						bnMid, win.Strike, m.TimeLeft(),
						est.FinalFP, est.EV, est.BS, est.Imb, est.Z, est.KDE, pmMid, dec.Action)
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	ossignal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer ossignal.Stop(sigCh)
	<-sigCh
	log.Println("shutting down")
	cancel()
	time.Sleep(500 * time.Millisecond)
}
