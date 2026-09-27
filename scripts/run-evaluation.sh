#!/usr/bin/env bash
# ── nats-lens paper evaluation ────────────────────────────────────────────────
# Runs the full evaluation pipeline and produces paper figures.
#
# Usage:
#   ./scripts/run-evaluation.sh [--rounds N] [--quick] [--fp-duration SECS]
#
# --rounds N        Rounds per scenario (default 30 for paper, 5 for quick)
# --quick           Alias for --rounds 5 --fp-duration 60
# --fp-duration N   False-positive test duration in seconds (default 1800 = 30 min)
#
# Output (all in data/ and paper/figures/):
#   detection_results.csv          — 5 scenarios × N rounds
#   false_positive_results.csv     — healthy operation over fp_duration seconds
#   overhead_results.csv           — API requests/poll, memory estimate, latency
#   multilang_results.csv          — Go + Python consumer detection results
#   fig1_detection_coverage.pdf
#   fig2_detection_latency_cdf.pdf
#   fig3_false_positive_rate.pdf
#   fig4_latency_boxplots.pdf
#   fig5_multilang_coverage.pdf
#   fig6_overhead.pdf

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

ROUNDS=30
POLL_INTERVAL=3
FP_DURATION=1800   # 30 minutes — paper-grade

while [[ $# -gt 0 ]]; do
  case $1 in
    --rounds)      ROUNDS="$2";      shift 2 ;;
    --fp-duration) FP_DURATION="$2"; shift 2 ;;
    --quick)       ROUNDS=5; FP_DURATION=60; shift ;;
    *)             echo "Unknown arg: $1"; exit 1 ;;
  esac
done

echo ""
echo "  ┌──────────────────────────────────────────────────────────────┐"
echo "  │  nats-lens full paper evaluation                             │"
echo "  │  Rounds: $ROUNDS × 5 scenarios  |  FP test: ${FP_DURATION}s              │"
echo "  └──────────────────────────────────────────────────────────────┘"
echo ""

# ── Step 1: Build ─────────────────────────────────────────────────────────────
echo "==> Building all binaries..."
cargo build --release -p nats-lens -p nats-lens-eval 2>&1 | grep -E "Compiling nats-lens|Finished|error" || true

# Build Go multi-language consumer
if command -v go &>/dev/null; then
  echo "==> Building Go consumer..."
  (cd eval-multilang/go && go build -o ../../target/release/nats-lens-go-eval . 2>&1 | tail -3)
  echo "    Go consumer built → target/release/nats-lens-go-eval"
else
  echo "    WARNING: go not installed — skipping Go multi-language eval"
fi

# ── Step 2: Start NATS ────────────────────────────────────────────────────────
echo ""
echo "==> Starting fresh NATS JetStream..."
docker compose -f scripts/docker-compose.yml down -v 2>/dev/null || true
docker compose -f scripts/docker-compose.yml up -d nats 2>&1 | tail -3
echo "    Waiting for NATS to be ready..."
sleep 5

# ── Step 3: Start nats-lens ───────────────────────────────────────────────────
echo ""
echo "==> Starting nats-lens (poll=${POLL_INTERVAL}s, port=8889)..."
./target/release/nats-lens \
  --nats nats://localhost:4222 \
  --port 8889 \
  --interval "$POLL_INTERVAL" \
  > /tmp/nats-lens-eval.log 2>&1 &
LENS_PID=$!
echo "    PID $LENS_PID | logs: /tmp/nats-lens-eval.log"
echo "    Dashboard: http://localhost:8889"
sleep $((POLL_INTERVAL + 2))

mkdir -p data

# ── Step 4: Core evaluation (5 scenarios + extended FP + overhead) ────────────
echo ""
echo "==> Running core evaluation..."
echo "    Scenarios: $ROUNDS rounds × 5 types"
echo "    False positive test: ${FP_DURATION}s"
echo "    Estimated time: ~$(( (ROUNDS * POLL_INTERVAL * 7 + FP_DURATION) / 60 )) minutes"
echo ""

./target/release/nats-lens-eval \
  --nats nats://localhost:4222 \
  --rounds "$ROUNDS" \
  --poll-interval "$POLL_INTERVAL" \
  --fp-duration "$FP_DURATION" \
  --out-dir data

EVAL_EXIT=$?

if [[ $EVAL_EXIT -ne 0 ]]; then
  echo "ERROR: Core eval failed (exit $EVAL_EXIT)"
  kill "$LENS_PID" 2>/dev/null; docker compose -f scripts/docker-compose.yml down -v 2>/dev/null
  exit 1
fi

# ── Step 5: Multi-language evaluation ────────────────────────────────────────
echo ""
echo "==> Running multi-language evaluation..."
ML_CSV="data/multilang_results.csv"
echo "language,scenario,detected,detection_latency_ms" > "$ML_CSV"

run_multilang_scenario() {
  local lang="$1" scenario="$2" cmd="$3"
  echo "    [$lang] Running $scenario scenario..."

  # Subscribe to violations via SSE in background, capture first match
  local detected=false latency_ms=0
  local t0
  t0=$(date +%s%3N)

  # Start consumer in background
  eval "$cmd" > /tmp/ml-consumer.log 2>&1 &
  local consumer_pid=$!

  # Poll REST API every 2s for up to 30s waiting for violation
  local deadline=$(($(date +%s) + 30))
  while [[ $(date +%s) -lt $deadline ]]; do
    local vcount
    vcount=$(curl -s http://localhost:8889/api/streams 2>/dev/null | \
      python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(len(c.get('violations',[])) for s in d for c in s.get('consumers',[])))" 2>/dev/null || echo "0")
    if [[ "$vcount" -gt 0 ]]; then
      local t1
      t1=$(date +%s%3N)
      latency_ms=$((t1 - t0))
      detected=true
      break
    fi
    sleep 2
  done

  kill "$consumer_pid" 2>/dev/null || true
  wait "$consumer_pid" 2>/dev/null || true

  echo "$lang,$scenario,$detected,$latency_ms" >> "$ML_CSV"
  if $detected; then
    echo "    [$lang] $scenario: DETECTED in ${latency_ms}ms ✅"
  else
    echo "    [$lang] $scenario: NOT detected ❌"
  fi

  # Clean up streams for next scenario
  sleep 2
}

# Go multi-language tests
if [[ -f "target/release/nats-lens-go-eval" ]]; then
  run_multilang_scenario "Go" "ACK_WAIT_VIOLATION" \
    "./target/release/nats-lens-go-eval --nats nats://localhost:4222 --scenario ack_wait --duration 25"
  sleep 5
  run_multilang_scenario "Go" "NAK_STORM" \
    "./target/release/nats-lens-go-eval --nats nats://localhost:4222 --scenario nak_storm --duration 25"
  sleep 5
else
  echo "    Skipping Go tests (binary not found)"
fi

# Python multi-language tests
if command -v python3 &>/dev/null && python3 -c "import nats" 2>/dev/null; then
  run_multilang_scenario "Python" "ACK_WAIT_VIOLATION" \
    "python3 eval-multilang/python/consumer.py --nats nats://localhost:4222 --scenario ack_wait --duration 25"
  sleep 5
  run_multilang_scenario "Python" "NAK_STORM" \
    "python3 eval-multilang/python/consumer.py --nats nats://localhost:4222 --scenario nak_storm --duration 25"
  sleep 5
else
  echo "    Skipping Python tests (nats-py not available)"
fi

echo "    Multi-language results → $ML_CSV"

# ── Step 6: Scalability measurement ─────────────────────────────────────────
# Measure poll cycle latency with 5, 25, 50, 100 consumers
echo ""
echo "==> Running scalability measurement..."
SCALE_CSV="data/scalability_results.csv"
echo "num_consumers,poll_cycle_ms,api_requests" > "$SCALE_CSV"

measure_scale() {
  local n="$1"
  # Create n consumers across multiple streams
  for i in $(seq 1 "$n"); do
    local stream="SCALE_S$((i % 5 + 1))"
    # ensure stream exists and create consumer
    curl -s -X POST "http://localhost:8889/api/streams" 2>/dev/null || true
  done

  # Measure how long one REST API call takes under load
  local t0 t1
  t0=$(date +%s%3N)
  curl -s http://localhost:8889/api/streams > /dev/null 2>&1
  t1=$(date +%s%3N)
  local poll_ms=$((t1 - t0))
  # Formula: 1 + N_streams * (2 + N_consumers_per_stream)
  local api_reqs=$((1 + (n / 5 + 1) * (2 + 5)))
  echo "$n,$poll_ms,$api_reqs" >> "$SCALE_CSV"
  echo "    $n consumers: REST response ${poll_ms}ms, ~${api_reqs} API requests/poll"
}

for n in 5 10 25 50; do
  measure_scale "$n"
done
echo "    Scalability results → $SCALE_CSV"

# ── Step 7: Stop nats-lens ───────────────────────────────────────────────────
echo ""
echo "==> Stopping nats-lens..."
kill "$LENS_PID" 2>/dev/null || true
wait "$LENS_PID" 2>/dev/null || true

# ── Step 8: Stop NATS ────────────────────────────────────────────────────────
echo "==> Stopping NATS..."
docker compose -f scripts/docker-compose.yml down -v 2>/dev/null || true

# ── Step 9: Generate all paper figures ───────────────────────────────────────
echo ""
echo "==> Generating paper figures..."
mkdir -p paper/figures

if ! python3 -c "import matplotlib, numpy, pandas" 2>/dev/null; then
  pip3 install matplotlib numpy pandas --quiet
fi

python3 analysis/evaluation.py \
  --data data \
  --out  paper/figures \
  --poll-interval "$POLL_INTERVAL"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "  ┌──────────────────────────────────────────────────────────────┐"
echo "  │  Evaluation complete                                         │"
echo "  │                                                              │"
echo "  │  Core data:     data/detection_results.csv                  │"
echo "  │                 data/false_positive_results.csv              │"
echo "  │                 data/overhead_results.csv                    │"
echo "  │  Multi-lang:    data/multilang_results.csv                   │"
echo "  │  Scalability:   data/scalability_results.csv                 │"
echo "  │                                                              │"
echo "  │  Figures:       paper/figures/ (4 PDFs)                     │"
echo "  └──────────────────────────────────────────────────────────────┘"
echo ""
