package main

import (
	"context"
	"log"
	"time"

	"github.com/cicn/polytr/internal/ingest"
)

// onSwapFn is invoked once per real token swap (i.e. when pm.SetToken returns
// true). The marker write happens here, not on every successful resolve, so we
// don't spam duplicates from concurrent paths.
type onSwapFn func(windowStart time.Time, tokenID string)

// resolveAndSet tries up to `attempts` times (1s apart) to resolve the gamma
// tokenID for the 5m window starting at `windowStart`, then calls SetToken if
// the result differs from what's already subscribed. Failures are logged once
// at the end so transient gaps (e.g. window not yet listed) don't spam.
func resolveAndSet(
	ctx context.Context,
	gamma *ingest.GammaClient,
	pm *ingest.PolymarketIngestor,
	windowStart time.Time,
	attempts int,
	onSwap onSwapFn,
) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if ctx.Err() != nil {
			return
		}
		tok, err := gamma.ResolveBTC5mToken(ctx, windowStart)
		if err == nil {
			if pm.SetToken(tok) {
				log.Printf("[discover] window=%s token=%.16s…", windowStart.UTC().Format(time.RFC3339), tok)
				if onSwap != nil {
					onSwap(windowStart, tok)
				}
			}
			return
		}
		lastErr = err
		if i+1 < attempts {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
	}
	if lastErr != nil {
		log.Printf("[discover] resolve failed for %s after %d tries: %v",
			windowStart.UTC().Format(time.RFC3339), attempts, lastErr)
	}
}

// runDiscovery wires gamma-api auto-resolution of the current Polymarket BTC
// 5m token into four independent paths:
//
//  1. Bootstrap — when the strike scheduler opens its first window (i.e. once
//     the first Binance mid arrives), do a bounded-retry resolve so the
//     Polymarket WS can subscribe ASAP.
//  2. Rollover  — every strikeScheduler.OnNewWindow fires a bounded-retry
//     resolve in its own goroutine.
//  3. Safety net — a low-cadence ticker (interval) re-checks the wall-clock
//     window; covers the case where scheduler.Tick missed a boundary or gamma
//     listed the market late. No-op when nothing changed.
//  4. Wall-clock boundary — sleeps until the next UTC 5m boundary and fires a
//     bounded-retry resolve there. Independent of Binance mid availability:
//     if Binance is disconnected at the boundary, scheduler.Tick won't roll
//     the window over (Path 2 silent), but Path 4 still resolves the new
//     market so Polymarket WS swaps on time.
func runDiscovery(
	ctx context.Context,
	gamma *ingest.GammaClient,
	pm *ingest.PolymarketIngestor,
	scheduler *strikeScheduler,
	durMin int,
	interval time.Duration,
	onSwap onSwapFn,
) {
	// Path 2: rollover listener — runs in its own goroutine (scheduler fires it that way).
	scheduler.OnNewWindow(func(w strikeWindow) {
		resolveAndSet(ctx, gamma, pm, w.Start, 30, onSwap)
	})

	// Path 1: bootstrap as soon as the scheduler has a window.
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if w := scheduler.Current(); !w.Start.IsZero() {
					resolveAndSet(ctx, gamma, pm, w.Start, 30, onSwap)
					return
				}
			}
		}
	}()

	// Path 3: safety-net poll.
	if interval > 0 {
		go func() {
			t := time.NewTicker(interval)
			defer t.Stop()
			d := time.Duration(durMin) * time.Minute
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-t.C:
					resolveAndSet(ctx, gamma, pm, now.UTC().Truncate(d), 1, onSwap)
				}
			}
		}()
	}

	// Path 4: wall-clock 5m boundary — fires resolveAndSet exactly at each
	// UTC window boundary, independent of scheduler/Binance availability.
	go func() {
		d := time.Duration(durMin) * time.Minute
		for {
			now := time.Now().UTC()
			next := now.Truncate(d).Add(d)
			select {
			case <-ctx.Done():
				return
			case <-time.After(next.Sub(now)):
				resolveAndSet(ctx, gamma, pm, next, 30, onSwap)
			}
		}
	}()
}
