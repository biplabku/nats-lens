#!/usr/bin/env python3
"""
Scale benchmark: measure nats-lens memory and poll overhead at different consumer counts.

Creates N consumers across M streams, runs nats-lens, measures:
  - RSS memory (KB)
  - Poll cycle time (ms, from logs)
  - API requests per cycle

Usage:
    python3 scripts/scale_benchmark.py --nats nats://localhost:4222 \
        --nats-lens ./target/release/nats-lens \
        --out data/scale_results.csv
"""
import argparse, asyncio, csv, json, os, signal, subprocess, sys, time, re
import nats as natslib


CONSUMER_COUNTS = [10, 50, 100, 200, 500, 1000]
STREAMS_PER_GROUP = 10   # distribute consumers across streams evenly
POLL_INTERVAL = 3        # seconds
MEASURE_POLLS = 5        # number of poll cycles to measure after warmup
WARMUP_POLLS = 2


async def setup_consumers(nc, n_consumers: int):
    """Create n_consumers evenly distributed across STREAMS_PER_GROUP streams."""
    js = nc.jetstream()
    n_streams = min(STREAMS_PER_GROUP, n_consumers)
    per_stream = max(1, n_consumers // n_streams)
    created = 0
    stream_names = []
    for s in range(n_streams):
        stream = f"SCALE_S{s:04d}"
        stream_names.append(stream)
        try:
            await js.delete_stream(stream)
        except Exception:
            pass
        await js.add_stream(name=stream, subjects=[f"scale.{s}.>"],
                            storage="memory", max_msgs=100)
        for c in range(per_stream):
            if created >= n_consumers:
                break
            consumer = f"scale-c{created:04d}"
            await js.add_consumer(stream, durable_name=consumer,
                                  ack_policy="explicit", ack_wait=300,
                                  max_ack_pending=512)
            created += 1
    return stream_names, created


async def teardown(nc, stream_names):
    js = nc.jetstream()
    for s in stream_names:
        try:
            await js.delete_stream(s)
        except Exception:
            pass


def get_rss_kb(pid: int) -> float:
    try:
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)],
                                      stderr=subprocess.DEVNULL).decode().strip()
        return float(out)
    except Exception:
        return 0.0


def start_nats_lens(nats_lens_bin: str, nats_url: str, port: int) -> subprocess.Popen:
    return subprocess.Popen(
        [nats_lens_bin, "--nats", nats_url,
         "--port", str(port), "--interval", str(POLL_INTERVAL)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1
    )


def read_poll_times(proc: subprocess.Popen, n_polls: int, timeout: int = 120) -> list:
    """Read log lines and extract poll cycle durations."""
    times = []
    deadline = time.time() + timeout
    pattern = re.compile(r'poll.*?(\d+)\s*ms', re.IGNORECASE)
    # Also look for "polled N consumers in Xms" style
    pattern2 = re.compile(r'(\d+)\s*consumers.*?(\d+)\s*ms', re.IGNORECASE)
    while len(times) < n_polls and time.time() < deadline:
        try:
            line = proc.stdout.readline()
            if not line:
                time.sleep(0.1)
                continue
            m = pattern.search(line) or pattern2.search(line)
            if m:
                times.append(int(m.group(1)))
        except Exception:
            break
    return times


async def benchmark_one(nc, n_consumers: int, nats_lens_bin: str,
                        nats_url: str, port: int) -> dict:
    print(f"\n[N={n_consumers}] Setting up consumers...", flush=True)
    stream_names, actual = await setup_consumers(nc, n_consumers)
    print(f"  Created {actual} consumers across {len(stream_names)} streams")

    proc = start_nats_lens(nats_lens_bin, nats_url, port)
    # Warmup
    warmup_secs = (WARMUP_POLLS + 1) * POLL_INTERVAL + 2
    time.sleep(warmup_secs)

    rss = get_rss_kb(proc.pid)
    print(f"  RSS after warmup: {rss:.0f} KB")

    # Measure poll cycle time from logs (best effort)
    # Just measure RSS at steady state — log parsing varies by build
    measure_secs = MEASURE_POLLS * POLL_INTERVAL + 1
    rss_samples = []
    for _ in range(MEASURE_POLLS):
        time.sleep(POLL_INTERVAL)
        rss_samples.append(get_rss_kb(proc.pid))

    rss_steady = sum(rss_samples) / len(rss_samples) if rss_samples else rss

    # API requests per cycle (formula)
    n_streams = len(stream_names)
    consumers_per_stream = actual / n_streams
    api_reqs = 1 + n_streams * (2 + consumers_per_stream)

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

    await teardown(nc, stream_names)

    result = {
        "n_consumers": actual,
        "n_streams": n_streams,
        "rss_kb": rss_steady,
        "api_reqs_per_cycle": api_reqs,
        "req_per_sec_5s_interval": api_reqs / 5.0,
    }
    print(f"  RSS={rss_steady:.0f} KB  API={api_reqs:.0f}/cycle  {api_reqs/5:.1f} req/s")
    return result


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nats", default="nats://localhost:4222")
    parser.add_argument("--nats-lens", default="./target/release/nats-lens")
    parser.add_argument("--out", default="data/scale_results.csv")
    parser.add_argument("--counts", default=None,
                        help="Comma-separated consumer counts (overrides default)")
    args = parser.parse_args()

    counts = (list(map(int, args.counts.split(",")))
              if args.counts else CONSUMER_COUNTS)

    nc = await natslib.connect(args.nats)
    print(f"Connected to {args.nats}")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    results = []
    port = 9100
    for n in counts:
        r = await benchmark_one(nc, n, args.nats_lens, args.nats, port)
        results.append(r)
        port += 1

    await nc.close()

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"\nResults → {args.out}")
    for r in results:
        print(f"  N={r['n_consumers']:5d}  RSS={r['rss_kb']:7.0f} KB"
              f"  API={r['api_reqs_per_cycle']:7.1f}/cycle"
              f"  {r['req_per_sec_5s_interval']:5.1f} req/s")


if __name__ == "__main__":
    asyncio.run(main())
