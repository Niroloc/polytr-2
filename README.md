# polytr — Polymarket BTC 5m HFT Bot

Go-based high-frequency trading bot for Polymarket BTC 5-minute binary options.
Combines a live trader and an offline replay/simulator behind a single binary tick log.

```
ingestion (Binance WS + Polymarket CLOB WS)
   │
   ▼
orderbooks   ──►   binary tick store (7d retention)
   │                       │
   ▼                       ▼
FP Committee (5 models) ◄──┘     replay simulator (web chart)
   │
   ▼
signal module  ─►  Maker-Only execution (Post-Only)
```

## Components

| Path | Role |
|---|---|
| [`internal/ingest/`](internal/ingest/) | Binance Spot WS (`depth20@100ms` + `aggTrade`) and Polymarket CLOB WS ingestion with reconnect/backoff. |
| [`internal/book/`](internal/book/) | Sorted-slice L2 order book — O(log n) upsert, O(1) top-of-book. |
| [`internal/storage/`](internal/storage/) | Fixed-size 27-byte binary tick records, hour-bucketed files, 7-day retention janitor. |
| [`internal/fpc/`](internal/fpc/) | Fair Price Committee — EV, Black-Scholes binary delta, Imbalance, Z-Score, KDE. |
| [`internal/signal/`](internal/signal/) | Entry/exit/time-floor decision logic. |
| [`internal/exec/`](internal/exec/) | Maker-only execution manager (strict Post-Only, never crosses). |
| [`cmd/bot/`](cmd/bot/) | Live trader binary. |
| [`cmd/replay/`](cmd/replay/) | Replay simulator + interactive Chart.js dashboard. |

## Fair Price Committee

`Final_FP = w₁·FP_EV + w₂·FP_BS + w₃·FP_Imb + w₄·FP_Z + w₅·FP_KDE`
(weights auto-normalize; default is 0.2 each.)

| Model | Formula | Purpose |
|---|---|---|
| **EV** | `ITM_count / total_windows` over 24h | Intraday bias / prior |
| **Black-Scholes** | `N(d2)` with realized vol from last 10min | Theoretical baseline |
| **Imbalance** | `0.5 + α·I_Binance − β·I_Polymarket` | Lead-lag arbitrage |
| **Z-Score** | `1 − Φ((S − μ) / σ)` over 2h | Mean reversion |
| **KDE** | `(1/N)·Σ [1 − Φ((K − xᵢ)/h)]`, Silverman h | Local support/resistance |

## Prerequisites

- Go ≥ 1.23 (for native build)
- Docker ≥ 24 (for container deployment)
- Outbound TLS to `stream.binance.com:9443` and `ws-subscriptions-clob.polymarket.com:443`

## Native Build & Run

```bash
# build both binaries
go build ./...

# unit tests (FP math, codec, book ordering, post-only safety)
go test ./...

# run the live bot in paper-trading mode (auto-discovers the active
# Polymarket BTC 5m market via gamma-api on startup and every 5m rollover)
go run ./cmd/bot \
  --data ./data \
  --paper \
  --strike-step 100 \
  --strike-minutes 5 \
  --entry-edge 0.03 \
  --exit-edge 0.005

# replay yesterday's logs with the web dashboard on :8080
go run ./cmd/replay \
  --data ./data \
  --from 2026-05-22T00:00:00Z \
  --to   2026-05-23T00:00:00Z \
  --listen :8080
# then open http://localhost:8080/
```

### Bot flags

| Flag | Default | Notes |
|---|---|---|
| `--data` | `./data` | Tick log root dir |
| `--paper` | `true` | Paper-trading client; flip to `false` only after wiring a live `exec.VenueClient` |
| `--poly-discover-interval` | `60s` | Safety-net poll cadence for gamma-api auto-discovery. |
| `--strike-step` | `100` | USD step for strike rounding |
| `--strike-minutes` | `5` | Window length |
| `--entry-edge` | `0.03` | Probability-points needed to enter |
| `--exit-edge` | `0.005` | Probability-points to flatten |
| `--min-seconds` | `5` | Force-exit when window time-left under this |
| `--tick` | `0.01` | Venue tick size |
| `--size` | `100` | Order size in shares |

### Replay flags

| Flag | Default | Notes |
|---|---|---|
| `--data` | `./data` | Tick log root |
| `--from` / `--to` | last 24h | RFC3339 timestamps |
| `--listen` | `:8080` | HTTP listen address |
| `--stride` | `50` | Emit one sample per N ticks (keeps the UI payload small) |

The dashboard plots Binance mid + strike (left axis, USD) against Polymarket
mid, `Final_FP`, and the 5 component FPs (right axis, probability). Pan/zoom
with the mouse; click "reset zoom" to re-fit.

## Docker

### Build the image

```bash
docker build -t polytr:latest .
```

The image is a multi-stage Alpine build (~25 MB). Final stage runs as the
unprivileged `polytr` user; tick data lives in the `/data` volume.

### Run the bot

```bash
docker run --rm -d \
  --name polytr-bot \
  -v polytr-data:/data \
  polytr:latest \
  bot --data /data --paper
```

### Run the replay UI

```bash
docker run --rm -p 8080:8080 \
  -v polytr-data:/data \
  polytr:latest \
  replay --data /data --listen :8080 \
         --from 2026-05-22T00:00:00Z --to 2026-05-23T00:00:00Z
```

The `polytr-data` volume is shared so the replay sees whatever the bot logged.

### docker compose

The stack ships with two services sharing a named volume:

| Service | Profile | Purpose |
|---|---|---|
| `bot` | default | Live ingestion + FP committee + maker-only execution. Writes ticks to `/data`. |
| `replay` | `replay` | Reads `/data`, computes FP at every stored tick, serves the Chart.js dashboard on `:8080`. |

**First run:**

```bash
cp .env.example .env
$EDITOR .env                          # tune thresholds, paper-mode toggle
mkdir -p logs && sudo chown 10001:10001 logs   # see "Host logs" below
docker compose up -d                  # builds the image, starts the bot
docker compose logs -f bot            # follow live FP / signal output via docker
tail -f logs/bot.log                  # ...or directly from the host filesystem
```

**Add the replay dashboard:**

```bash
docker compose --profile replay up -d
# UI on http://localhost:8080
```

**Replay a specific window without touching `.env`:**

```bash
REPLAY_FROM=2026-05-22T00:00:00Z REPLAY_TO=2026-05-23T00:00:00Z \
  docker compose --profile replay up replay
```

**Operations:**

```bash
docker compose ps                       # status + healthchecks
docker compose restart bot              # bounce just the trader
docker compose down                     # stop everything (keeps the volume)
docker compose down -v                  # ⚠ also wipes the tick log
docker volume inspect polytr-data       # locate ticks on the host
docker compose exec bot ls -lh /data    # peek at on-disk hour files
```

Both services have JSON-file log rotation (10MB × 5 files for the bot, 10MB ×
3 for replay) and healthchecks — the bot's healthcheck fails if no `.bin`
file has been touched in the last 2 minutes, so a wedged ingestor will be
surfaced by `docker compose ps`. Adjust resource limits in the commented
`deploy:` block of [`docker-compose.yml`](docker-compose.yml) if you pin to
a small host.

See [`.env.example`](.env.example) for every tunable; the same variables
work whether you use `docker compose` or run the binaries natively.

### Host logs

Each container writes its log stream to **both** stderr (visible via
`docker compose logs`) and a file inside `/logs`, which is bind-mounted from
the host directory `HOST_LOGS_DIR` (default `./logs`).

```
./logs/
├── bot.log       ← live trader output, appended forever
└── replay.log    ← replay simulator output
```

The container runs as **uid/gid 10001** (`polytr`), so the bind-mount target
must be writable by that uid. The simplest one-time setup on Linux:

```bash
mkdir -p logs
sudo chown 10001:10001 logs
```

To park logs somewhere else (e.g. `/var/log/polytr`), set `HOST_LOGS_DIR` in
`.env`:

```bash
HOST_LOGS_DIR=/var/log/polytr
```

Rotation of the host-side log files is **not** done automatically — use the
host's `logrotate` (example snippet for `/etc/logrotate.d/polytr`):

```
/var/log/polytr/*.log {
    daily
    rotate 14
    compress
    missingok
    notifempty
    copytruncate
}
```

`copytruncate` is required because the bot keeps an open file handle (it
appends with `O_APPEND`), so logrotate can't safely `rename → reopen`.

If you only want host logs and want to disable docker's own json-file
buffering (saves disk on noisy days), set `logging.driver: none` in
[`docker-compose.yml`](docker-compose.yml).

## On-disk layout

```
data/
└── 2026-05-23/
    ├── 00.bin   ── append-only fixed 27-byte records
    ├── 01.bin
    ├── ...
    └── 23.bin
```

Day directories older than 7 days are pruned automatically by a background
janitor (`internal/storage/tickstore.go`).

## Maker-Only invariant

The execution engine is built around one hard rule: **never cross the spread**.

- `exec.Manager.makerPrice` only returns prices strictly inside the spread.
- `exec.PaperClient.Place` returns `ErrWouldCross` if asked to place at or
  beyond the opposite side.
- Re-quotes are rate-limited via `RequoteCooldown` (200ms default) to avoid
  chasing and burning rate-limit budget.
- Test [`internal/exec/maker_test.go`](internal/exec/maker_test.go) pins this
  invariant against regressions.

To go live you must implement `exec.VenueClient` with a signed Polymarket
CLOB REST client that sets `orderType:"GTC", postOnly:true`, then replace the
`PaperClient` wiring in [`cmd/bot/main.go`](cmd/bot/main.go).

## Caveats

1. The live CLOB submission client is intentionally **not implemented** — `--paper=false` exits with an error. Wiring real-money orders silently would be a footgun.
2. Polymarket token auto-discovery resolves the YES outcome of the active `btc-updown-5m-{epoch}` market via `gamma-api.polymarket.com/events?slug=…`. If the bot starts mid-window, the first few seconds of Polymarket ticks are missed (Binance keeps recording); the gap is recoverable from the tick log timestamps.
3. Replay reconstructs an approximate book from L2 mid prints; full-fidelity backtesting needs periodic snapshots in the tick format (cheap to add — bump `TickType` enum).
