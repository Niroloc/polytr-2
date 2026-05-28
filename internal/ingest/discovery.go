package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

const (
	gammaDefaultBase = "https://gamma-api.polymarket.com"
	gammaTimeout     = 5 * time.Second
)

// Errors returned by GammaClient.ResolveBTC5mToken.
var (
	ErrNoEvent     = errors.New("gamma: no event for slug")
	ErrNoMarket    = errors.New("gamma: event has no markets")
	ErrNoOutcomes  = errors.New("gamma: market missing outcomes/clobTokenIds")
	ErrNoUpOutcome = errors.New("gamma: market has no Up outcome")
)

// GammaClient calls gamma-api.polymarket.com to resolve the active BTC 5m
// binary market for a given window. Stateless; safe for concurrent use.
type GammaClient struct {
	HTTP    *http.Client
	BaseURL string
}

// NewGammaClient builds a client with a reusable HTTP transport sized for
// occasional short-lived requests.
func NewGammaClient() *GammaClient {
	tr := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: 4 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          4,
		IdleConnTimeout:       60 * time.Second,
	}
	return &GammaClient{
		HTTP:    &http.Client{Timeout: gammaTimeout, Transport: tr},
		BaseURL: gammaDefaultBase,
	}
}

// BTC5mSlug derives the gamma event/market slug for the 5-minute window that
// contains `windowStart`. Polymarket emits one event per 5m window with slug
// `btc-updown-5m-{unix_seconds_of_window_start}`.
func BTC5mSlug(windowStart time.Time) string {
	w := windowStart.UTC().Truncate(5 * time.Minute)
	return fmt.Sprintf("btc-updown-5m-%d", w.Unix())
}

type gammaEvent struct {
	Slug    string        `json:"slug"`
	Markets []gammaMarket `json:"markets"`
}

type gammaMarket struct {
	// Both fields arrive as stringified JSON arrays, not native arrays.
	OutcomesRaw     string `json:"outcomes"`
	ClobTokenIDsRaw string `json:"clobTokenIds"`
}

// ResolveBTC5mToken returns the YES (Up) CLOB tokenID for the 5-minute window
// containing `windowStart`. Returns a typed error if the event isn't listed
// yet, the market is malformed, or the network call fails; the caller is
// expected to retry on its own cadence.
func (g *GammaClient) ResolveBTC5mToken(ctx context.Context, windowStart time.Time) (string, error) {
	slug := BTC5mSlug(windowStart)
	u := fmt.Sprintf("%s/events?slug=%s", g.BaseURL, url.QueryEscape(slug))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "polytr/1.0")
	resp, err := g.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return "", fmt.Errorf("gamma: http %d", resp.StatusCode)
	}
	var events []gammaEvent
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		return "", fmt.Errorf("gamma: decode: %w", err)
	}
	if len(events) == 0 {
		return "", fmt.Errorf("%w: %s", ErrNoEvent, slug)
	}
	if len(events[0].Markets) == 0 {
		return "", ErrNoMarket
	}
	m := events[0].Markets[0]
	var outcomes []string
	if err := json.Unmarshal([]byte(m.OutcomesRaw), &outcomes); err != nil {
		return "", fmt.Errorf("gamma: outcomes parse: %w", err)
	}
	var tokens []string
	if err := json.Unmarshal([]byte(m.ClobTokenIDsRaw), &tokens); err != nil {
		return "", fmt.Errorf("gamma: clobTokenIds parse: %w", err)
	}
	if len(outcomes) == 0 || len(tokens) == 0 || len(outcomes) != len(tokens) {
		return "", ErrNoOutcomes
	}
	for i, o := range outcomes {
		if o == "Up" {
			if tokens[i] == "" {
				return "", ErrNoOutcomes
			}
			return tokens[i], nil
		}
	}
	return "", ErrNoUpOutcome
}
