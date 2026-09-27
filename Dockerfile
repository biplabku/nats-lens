# ── Stage 1: dependency cache (cargo-chef) ────────────────────────────────────
FROM rust:1.81-alpine AS chef
RUN apk add --no-cache musl-dev && cargo install cargo-chef --locked
WORKDIR /app

# ── Stage 2: compute dependency recipe ───────────────────────────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: build dependencies (cached layer) ───────────────────────────────
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build only the nats-lens binary
COPY . .
RUN cargo build --release -p nats-lens

# ── Stage 4: minimal runtime image ───────────────────────────────────────────
FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates tzdata

# Non-root user for security
RUN addgroup -S nats-lens && adduser -S nats-lens -G nats-lens
USER nats-lens

COPY --from=builder /app/target/release/nats-lens /usr/local/bin/nats-lens

# Web UI + API
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["nats-lens"]
CMD ["--nats", "nats://nats:4222", "--port", "8080"]
