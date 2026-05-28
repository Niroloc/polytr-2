# syntax=docker/dockerfile:1.6
#
# Multi-stage build:
#   1) builder — full Go toolchain, compiles static binaries
#   2) runtime — minimal Alpine with CA bundle (TLS for WSS) and tzdata
#
# Build:   docker build -t polytr:latest .
# Run bot: docker run --rm -v polytr-data:/data polytr:latest bot --paper --poly-token <TOKEN>
# Replay:  docker run --rm -p 8080:8080 -v polytr-data:/data polytr:latest replay --listen :8080

FROM golang:1.23-alpine AS builder
WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOFLAGS=-trimpath

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -o /out/bot    ./cmd/bot && \
    go build -ldflags="-s -w" -o /out/replay ./cmd/replay

# ---------- runtime ----------
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 10001 polytr && \
    adduser -D -u 10001 -G polytr polytr && \
    mkdir -p /data /logs && chown -R polytr:polytr /data /logs

COPY --from=builder /out/bot    /usr/local/bin/bot
COPY --from=builder /out/replay /usr/local/bin/replay

# Entry point selects which binary by first arg ("bot" or "replay").
COPY <<'EOF' /usr/local/bin/entrypoint.sh
#!/bin/sh
set -eu
cmd="${1:-bot}"
shift || true
exec "/usr/local/bin/${cmd}" "$@"
EOF
RUN chmod +x /usr/local/bin/entrypoint.sh

USER polytr
WORKDIR /data
VOLUME ["/data", "/logs"]
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["bot", "--data", "/data", "--paper"]
