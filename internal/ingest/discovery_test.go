package ingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestGamma(t *testing.T, h http.HandlerFunc) *GammaClient {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	g := NewGammaClient()
	g.BaseURL = srv.URL
	return g
}

func TestBTC5mSlug(t *testing.T) {
	// 2025-12-19 16:35:00 UTC == unix 1766162100; verified against Polymarket live.
	ts := time.Date(2025, 12, 19, 16, 37, 21, 0, time.UTC) // mid-window
	got := BTC5mSlug(ts)
	want := "btc-updown-5m-1766162100"
	if got != want {
		t.Fatalf("slug mismatch: got %s want %s", got, want)
	}
}

func TestResolveBTC5mToken_UpFirst(t *testing.T) {
	g := newTestGamma(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "slug=btc-updown-5m-") {
			t.Errorf("unexpected query: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"slug":"btc-updown-5m-1","markets":[{"outcomes":"[\"Up\", \"Down\"]","clobTokenIds":"[\"UPTOK\", \"DOWNTOK\"]"}]}]`))
	})
	tok, err := g.ResolveBTC5mToken(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if tok != "UPTOK" {
		t.Fatalf("want UPTOK got %s", tok)
	}
}

func TestResolveBTC5mToken_DownFirst(t *testing.T) {
	g := newTestGamma(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"markets":[{"outcomes":"[\"Down\", \"Up\"]","clobTokenIds":"[\"DOWNTOK\", \"UPTOK\"]"}]}]`))
	})
	tok, err := g.ResolveBTC5mToken(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if tok != "UPTOK" {
		t.Fatalf("want UPTOK got %s", tok)
	}
}

func TestResolveBTC5mToken_EmptyEvents(t *testing.T) {
	g := newTestGamma(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})
	_, err := g.ResolveBTC5mToken(context.Background(), time.Now())
	if !errors.Is(err, ErrNoEvent) {
		t.Fatalf("want ErrNoEvent got %v", err)
	}
}

func TestResolveBTC5mToken_NoMarkets(t *testing.T) {
	g := newTestGamma(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"slug":"x","markets":[]}]`))
	})
	_, err := g.ResolveBTC5mToken(context.Background(), time.Now())
	if !errors.Is(err, ErrNoMarket) {
		t.Fatalf("want ErrNoMarket got %v", err)
	}
}

func TestResolveBTC5mToken_MismatchedArrays(t *testing.T) {
	g := newTestGamma(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"markets":[{"outcomes":"[\"Up\", \"Down\"]","clobTokenIds":"[\"UPTOK\"]"}]}]`))
	})
	_, err := g.ResolveBTC5mToken(context.Background(), time.Now())
	if !errors.Is(err, ErrNoOutcomes) {
		t.Fatalf("want ErrNoOutcomes got %v", err)
	}
}

func TestResolveBTC5mToken_NoUp(t *testing.T) {
	g := newTestGamma(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"markets":[{"outcomes":"[\"Yes\", \"No\"]","clobTokenIds":"[\"A\", \"B\"]"}]}]`))
	})
	_, err := g.ResolveBTC5mToken(context.Background(), time.Now())
	if !errors.Is(err, ErrNoUpOutcome) {
		t.Fatalf("want ErrNoUpOutcome got %v", err)
	}
}

func TestResolveBTC5mToken_HTTP500(t *testing.T) {
	g := newTestGamma(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	_, err := g.ResolveBTC5mToken(context.Background(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "http 500") {
		t.Fatalf("want http 500 error got %v", err)
	}
}

func TestResolveBTC5mToken_ContextCanceled(t *testing.T) {
	g := newTestGamma(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := g.ResolveBTC5mToken(ctx, time.Now())
	if err == nil {
		t.Fatal("want error from canceled ctx")
	}
}
