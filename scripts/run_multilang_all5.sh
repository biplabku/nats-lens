#!/usr/bin/env bash
# Run multi-language verification for all 5 violation classes.
# Starts nats-lens on port 8889, then tests each scenario with Go and Python.
# Results written to data/multilang_all5_results.csv

set -euo pipefail
cd "$(dirname "$0")/.."

NATS="${NATS_URL:-nats://localhost:4222}"
OUT="data/multilang_all5_results.csv"
LENS_PORT=8889
LENS_PID=""
LENS_BIN="./target/release/nats-lens"
GO_BIN="./target/release/nats-lens-go-eval"
PY_BIN="eval-multilang/python/consumer.py"

cleanup() {
  [[ -n "$LENS_PID" ]] && kill "$LENS_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "language,scenario,detected,detection_latency_ms,violation_type" > "$OUT"

# Start nats-lens
"$LENS_BIN" --nats "$NATS" --port "$LENS_PORT" --interval 3 > /tmp/lens_ml.log 2>&1 &
LENS_PID=$!
sleep 15
echo "[INFO] nats-lens started (PID $LENS_PID)"

run_scenario() {
  local lang="$1" scenario="$2" cmd="$3"
  echo "[$(date +%H:%M:%S)] [$lang] $scenario..."

  local t0 t1 latency detected=false vtype=""
  t0=$(python3 -c "import time; print(int(time.time()*1000))")

  eval "$cmd" > /tmp/ml_consumer.log 2>&1 &
  local cpid=$!

  local deadline=$(( $(date +%s) + 40 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local result
    result=$(curl -s "http://localhost:${LENS_PORT}/api/streams" 2>/dev/null | \
      python3 -c "
import json,sys
try:
  d=json.load(sys.stdin)
  for s in d:
    for c in s.get('consumers',[]):
      if c.get('violations'):
        v=c['violations'][0]
        vname=v.get('violation','unknown')
        if isinstance(vname,dict): vname=str(vname)
        print('DETECTED:'+str(vname))
        sys.exit(0)
  print('NONE')
except Exception as e:
  print('NONE')
" 2>/dev/null)
    if [[ "$result" == DETECTED:* ]]; then
      t1=$(python3 -c "import time; print(int(time.time()*1000))")
      latency=$((t1 - t0))
      detected=true
      vtype="${result#DETECTED:}"
      break
    fi
    sleep 1
  done

  kill "$cpid" 2>/dev/null || true
  wait "$cpid" 2>/dev/null || true
  sleep 2

  echo "$lang,$scenario,$detected,$latency,$vtype" >> "$OUT"
  if $detected; then
    echo "  → DETECTED in ${latency}ms ($vtype)"
  else
    echo "  → NOT detected (30s timeout)"
  fi
}

SCENARIOS=("ack_wait:ACK_WAIT_VIOLATION" "nak_storm:NAK_STORM"
           "seq_gap:SEQUENCE_GAP" "max_pending:MAX_PENDING_THROTTLE"
           "missing_progress:MISSING_PROGRESS")

echo "=== Go consumers ==="
if [[ -f "$GO_BIN" ]]; then
  for entry in "${SCENARIOS[@]}"; do
    scenario="${entry%%:*}"
    run_scenario "Go" "${entry##*:}" \
      "$GO_BIN --nats $NATS --scenario $scenario --duration 30"
    sleep 15
  done
else
  echo "Go binary not found: $GO_BIN"
fi

echo ""
echo "=== Python consumers ==="
if python3 -c "import nats" 2>/dev/null; then
  for entry in "${SCENARIOS[@]}"; do
    scenario="${entry%%:*}"
    run_scenario "Python" "${entry##*:}" \
      "python3 $PY_BIN --nats $NATS --scenario $scenario --duration 30"
    sleep 15
  done
else
  echo "nats-py not installed — run: pip install nats-py"
fi

echo ""
echo "Results written to $OUT"
cat "$OUT"
