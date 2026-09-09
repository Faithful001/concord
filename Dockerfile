# syntax=docker/dockerfile:1

# ─── Stage 1: build ──────────────────────────────────────────────────────────
# Use golang:1.25-alpine when that image is available; otherwise use 1.24-alpine.
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Cache module downloads before copying source.
COPY go.mod ./
RUN go mod download

# Copy source and build a statically-linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /concord \
      ./cmd/concord

# ─── Stage 2: minimal runtime ─────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /concord /usr/local/bin/concord

# Raft peer-to-peer port | HTTP API port
EXPOSE 8001 9001

# Snapshot data lives here — mount a volume for persistence.
VOLUME ["/data"]

ENTRYPOINT ["concord"]
