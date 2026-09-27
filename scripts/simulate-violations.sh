#!/usr/bin/env bash
# Simulate NATS JetStream violations so nats-lens can detect and display them.
#
# Runs inside the nats-box container (no local NATS CLI needed).
# Keep this running in a separate terminal while nats-lens is open.
#
# What this triggers:
#   ORDERS stream  → AckWaitViolation + MaxPendingThrottle
#     Pull 3 messages but hold them for 10s (ack_wait=5s → redeliveries start)
#
#   EVENTS stream  → SequenceGap
#     Flood 300 more messages into a 50-msg stream while consumer has unacked msgs
#
#   PAYMENTS stream → MissingProgress
#     Pull 18 messages (of max_pending=20), hold them without acking

set -euo pipefail

SERVER="nats://nats:4222"

echo "==> Simulating violations. Open http://localhost:8888 to watch."
echo "    Press Ctrl-C to stop."
echo ""

# ── AckWaitViolation + MaxPendingThrottle on ORDERS ──────────────────────────
simulate_orders() {
    while true; do
        echo "[ORDERS] pulling 3 messages (will hold > 5s ack_wait)..."
        # Pull all 3 available pending slots, hold without acking
        docker exec nats-lens-nats \
            nats --server "$SERVER" consumer next ORDERS order-processor \
            --count 3 --no-ack 2>/dev/null || true

        # Seed more messages so the stream stays active
        for i in $(seq 1 20); do
            docker exec nats-lens-nats \
                nats --server "$SERVER" pub orders.new \
                "{\"order_id\": $i}" 2>/dev/null || true
        done

        echo "[ORDERS] sleeping 12s (exceeds ack_wait=5s, redeliveries will happen)"
        sleep 12
    done
}

# ── SequenceGap on EVENTS ─────────────────────────────────────────────────────
simulate_events() {
    while true; do
        echo "[EVENTS] flooding 300 messages into 50-msg stream..."
        for i in $(seq 1 300); do
            docker exec nats-lens-nats \
                nats --server "$SERVER" pub events.click \
                "{\"seq\": $i}" 2>/dev/null || true
        done
        # Pull one message to establish ack_floor, then flood again to create gap
        docker exec nats-lens-nats \
            nats --server "$SERVER" consumer next EVENTS event-handler \
            --count 1 2>/dev/null || true
        sleep 15
    done
}

# ── MissingProgress on PAYMENTS ───────────────────────────────────────────────
simulate_payments() {
    while true; do
        echo "[PAYMENTS] pulling 18/20 pending slots without acking..."
        for i in $(seq 1 50); do
            docker exec nats-lens-nats \
                nats --server "$SERVER" pub payments.charge \
                "{\"payment_id\": $i, \"amount\": $((RANDOM % 10000))}" 2>/dev/null || true
        done
        docker exec nats-lens-nats \
            nats --server "$SERVER" consumer next PAYMENTS payment-processor \
            --count 18 --no-ack 2>/dev/null || true
        echo "[PAYMENTS] holding 18 ack-pending slots (ack_wait=120s)..."
        sleep 30
    done
}

# Run all three in parallel
simulate_orders &
PID1=$!
simulate_events &
PID2=$!
simulate_payments &
PID3=$!

trap "kill $PID1 $PID2 $PID3 2>/dev/null; echo 'Stopped.'" INT TERM
wait
