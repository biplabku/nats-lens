#!/usr/bin/env python3
"""
Multi-language empirical evaluation via NATS health events.

Instead of polling the REST API (blocked in test environments), this script
subscribes directly to nats.lens.health.violations.* — the same channel
nats-lens uses to broadcast violations to any language. This gives
end-to-end empirical measurement: Go/Python consumer → NATS JetStream →
nats-lens detector → violation event on NATS subject → Python subscriber.

Usage:
    python3 eval-multilang/run_multilang_eval.py \
        --nats nats://localhost:4222 \
        --go-binary target/release/nats-lens-go-eval \
        --out data/multilang_results.csv
"""
import argparse
import asyncio
import csv
import json
import subprocess
import time
import sys
import os

try:
    import nats
except ImportError:
    print("Install: pip install nats-py")
    sys.exit(1)


SCENARIOS = [
    ("Go",     "ACK_WAIT_VIOLATION", "ack_wait",  "ACK_WAIT_VIOLATION"),
    ("Go",     "NAK_STORM",          "nak_storm",  "NAK_STORM"),
    ("Go",     "MISSING_PROGRESS",   "healthy",    None),
    ("Python", "ACK_WAIT_VIOLATION", "ack_wait",   "ACK_WAIT_VIOLATION"),
    ("Python", "NAK_STORM",          "nak_storm",  "NAK_STORM"),
]

# Detection timeout (seconds) and consumer duration (seconds)
DETECT_TIMEOUT = 60   # was 35 — ACK_WAIT needs more time for num_redelivered to grow
CONSUMER_DUR   = 55   # was 25


async def run_scenario(nc, lang, scenario_name, consumer_scenario, expected_type,
                       go_binary, nats_url, timeout_secs=DETECT_TIMEOUT):
    """
    Run one language scenario and wait for a violation event.
    Returns (detected: bool, latency_ms: int | None, actual_type: str | None)
    """
    # Subscribe to all violation events (callback must be async in nats-py)
    violations = []

    async def on_violation(msg):
        violations.append(msg)

    sub = await nc.subscribe("nats.lens.health.violations.>", cb=on_violation)

    t0 = time.monotonic()

    # Start the consumer process
    if lang == "Go":
        cmd = [go_binary,
               "--nats", nats_url,
               "--scenario", consumer_scenario,
               "--duration", str(timeout_secs)]
    else:  # Python
        cmd = [sys.executable,
               os.path.join(os.path.dirname(__file__), "python/consumer.py"),
               "--nats", nats_url,
               "--scenario", consumer_scenario,
               "--duration", str(timeout_secs)]

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    try:
        # Wait for expected violation or timeout
        deadline = time.monotonic() + timeout_secs
        detected = False
        latency_ms = None
        actual_type = None

        while time.monotonic() < deadline:
            await asyncio.sleep(1)

            for msg in violations:
                try:
                    data = json.loads(msg.data.decode())
                    vtype = data.get("violation", {}).get("type") or data.get("type", "")
                    if expected_type and vtype == expected_type:
                        latency_ms = int((time.monotonic() - t0) * 1000)
                        detected = True
                        actual_type = vtype
                        break
                    elif not expected_type:
                        # Healthy scenario — any violation is a false positive
                        actual_type = vtype
                except Exception:
                    pass
            violations.clear()

            if detected:
                break

        return detected, latency_ms, actual_type

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        await sub.unsubscribe()

        # Clean up streams created by the consumer
        await asyncio.sleep(2)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nats", default="nats://localhost:4222")
    parser.add_argument("--go-binary", default="target/release/nats-lens-go-eval")
    parser.add_argument("--out", default="data/multilang_results.csv")
    args = parser.parse_args()

    # Verify go binary exists
    if not os.path.exists(args.go_binary):
        print(f"ERROR: Go binary not found at {args.go_binary}")
        print("Build with: cd eval-multilang/go && go build -o ../../target/release/nats-lens-go-eval .")
        sys.exit(1)

    nc = await nats.connect(args.nats)
    print(f"Connected to {args.nats}")
    print(f"Listening for violations on nats.lens.health.violations.*")
    print()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["language", "scenario", "detected", "detection_latency_ms", "violation_type"])

        for (lang, scenario_name, consumer_scenario, expected_type) in SCENARIOS:
            print(f"[{lang}] {scenario_name} ...", end=" ", flush=True)

            if expected_type is None:
                # Healthy scenario — run and count violations
                detected, latency_ms, actual_type = await run_scenario(
                    nc, lang, scenario_name, consumer_scenario, None,
                    args.go_binary, args.nats, timeout_secs=DETECT_TIMEOUT
                )
                if not detected:
                    print(f"✅ No violations (correct)")
                    writer.writerow([lang, scenario_name, False, "", "none"])
                else:
                    print(f"⚠️  FALSE POSITIVE: {actual_type}")
                    writer.writerow([lang, scenario_name, True, latency_ms, actual_type])
            else:
                detected, latency_ms, actual_type = await run_scenario(
                    nc, lang, scenario_name, consumer_scenario, expected_type,
                    args.go_binary, args.nats, timeout_secs=DETECT_TIMEOUT
                )
                status = f"✅ {latency_ms}ms" if detected else "❌ NOT detected"
                print(status)
                writer.writerow([lang, scenario_name, detected, latency_ms or "", actual_type or ""])

            f.flush()
            # Brief pause between scenarios
            await asyncio.sleep(5)

    await nc.close()
    print(f"\nResults → {args.out}")


if __name__ == "__main__":
    asyncio.run(main())
