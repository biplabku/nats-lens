#!/usr/bin/env bash
# Full TNSM paper evaluation suite.
# Runs all experiments and produces all data files needed for the journal paper.
# Total runtime: ~3-4 hours.
#
# Usage:
#   ./scripts/run-tnsm-evaluation.sh [--nats nats://localhost:4222]

set -euo pipefail

NATS="${1:-nats://localhost:4222}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BASE_DIR="data/tnsm_${TIMESTAMP}"
mkdir -p "$BASE_DIR"

echo "═══════════════════════════════════════════════════════════"
echo "  nats-lens TNSM Full Evaluation Suite"
echo "  Output: $BASE_DIR/"
echo "  NATS:   $NATS"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Build release binaries
echo "▶ Building..."
cargo build --release --bin nats-lens-eval --bin nats-lens-eval-extended --bin nats-lens 2>&1 | tail -3
echo ""

# Start nats-lens in background (needed for multilang eval)
./target/release/nats-lens --nats "$NATS" --port 8891 --interval 3 &
LENS_PID=$!
trap "kill $LENS_PID 2>/dev/null || true" EXIT

echo "▶ Experiment 1: Baseline (100 rounds, poll=3s)"
./target/release/nats-lens-eval \
    --nats "$NATS" \
    --rounds 100 \
    --poll-interval 3 \
    --fp-duration 86400 \
    --out-dir "$BASE_DIR/baseline"
echo ""

echo "▶ Experiment 2: Extended (recovery, multi-vio, adversarial FP, sensitivity)"
./target/release/nats-lens-eval-extended \
    --nats "$NATS" \
    --rounds 10 \
    --poll-interval 3 \
    --out-dir "$BASE_DIR/extended"
echo ""

echo "▶ Experiment 3: 3-Node Cluster (100 rounds, poll=3s)"
echo "   Start cluster first: docker-compose -f scripts/docker-compose-cluster.yml up -d"
echo "   Then press Enter..."
read -r
CLUSTER_URL="${2:-nats://localhost:4222}"
./target/release/nats-lens-eval \
    --nats "$CLUSTER_URL" \
    --rounds 100 \
    --poll-interval 3 \
    --fp-duration 3600 \
    --out-dir "$BASE_DIR/cluster"
echo ""

echo "▶ Experiment 4: Scale benchmark"
python3 scripts/scale_benchmark.py \
    --nats "$NATS" \
    --nats-lens ./target/release/nats-lens \
    --out "$BASE_DIR/scale_results.csv" \
    --counts 10,50,100,200,500,1000
echo ""

echo "═══════════════════════════════════════════════════════════"
echo "  All experiments complete."
echo "  Generate figures: python3 analysis/plot_tnsm_figures.py --data $BASE_DIR"
echo "═══════════════════════════════════════════════════════════"
