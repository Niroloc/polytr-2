// Package ingest hosts WebSocket ingestion for Binance Spot and Polymarket CLOB.
//
// Both ingestors share the same contract: they push types.TickData into a
// channel and update an L2 *book.Book. Reconnect/backoff is handled inline;
// the consumer never needs to know about transport hiccups.
package ingest

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// Sink is the consumer interface the ingestors push into.
//
// Concrete implementations live in cmd/bot — typically a fan-out to the
// tick store, the orderbook update path, and the FP recompute trigger.
type Sink interface {
	OnTick(tick tickIn)
}

// tickIn is the internal carrier used by ingestors; cmd/bot adapts it.
type tickIn struct {
	Ts     int64
	Source uint8
	Type   uint8
	Price  float64
	Amount float64
	Side   int8
}

// backoff yields an exponential delay with jitter, capped at 30s.
func backoff(attempt int) time.Duration {
	base := time.Duration(1<<attempt) * 100 * time.Millisecond
	if base > 30*time.Second {
		base = 30 * time.Second
	}
	jitter := time.Duration(rand.Int63n(int64(base) / 4))
	return base + jitter
}

// loopUntilDone runs `do` in a reconnect loop, honoring ctx cancellation.
// A returned errSwap (token swap initiated by the caller) is treated as a
// clean restart: no log line, no backoff, and the attempt counter resets.
func loopUntilDone(ctx context.Context, name string, do func(ctx context.Context) error, log func(format string, v ...any)) {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		err := do(ctx)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errSwap) {
			attempt = 0
			continue
		}
		attempt++
		d := backoff(attempt)
		if attempt > 6 {
			attempt = 6
		}
		log("ingest[%s]: disconnected (%v), retry in %s", name, err, d)
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}
