#!/usr/bin/env python3
"""
Python consumer that triggers NATS JetStream violations for nats-lens multi-language eval.
Demonstrates that nats-lens detects violations from consumers in any language.

Usage:
    python consumer.py --scenario ack_wait --nats nats://localhost:4222
    python consumer.py --scenario nak_storm --nats nats://localhost:4222
    python consumer.py --scenario healthy   --nats nats://localhost:4222

Install: pip install nats-py
"""
import argparse
import asyncio
import json
import sys
import time


async def run_ack_wait(nc, duration: int):
    """
    AckWaitViolation: pull messages, hold for 6s without acking.
    ack_wait=4s fires → NATS redelivers → num_redelivered grows.
    nats-lens should detect AckWaitViolation within 2 poll cycles.
    """
    js = nc.jetstream()
    stream = "PYTHON_ACK"
    consumer = "py-ack-consumer"

    await js.add_stream(name=stream, subjects=["py.ack.>"], storage="memory", max_msgs=5000)
    # Delete stale consumer first to ensure fresh config
    try:
        await js.delete_consumer(stream, consumer)
    except Exception:
        pass
    await js.purge_stream(stream)
    # ack_wait=2s: short enough that redeliveries happen quickly.
    # max_ack_pending=20: allows pulling new messages each cycle so
    # num_redelivered grows as different messages get their first redeliver.
    await js.add_consumer(stream, durable_name=consumer, ack_policy="explicit",
                          ack_wait=2, max_ack_pending=20)

    for i in range(50):
        await js.publish("py.ack.msg", json.dumps({"i": i}).encode())

    psub = await js.pull_subscribe("py.ack.>", consumer, stream=stream)
    deadline = time.time() + duration

    print(f"[Python AckWait] Running. nats-lens should detect AckWaitViolation.")
    while time.time() < deadline:
        try:
            msgs = await psub.fetch(5, timeout=1)
            print(f"[Python AckWait] pulled {len(msgs)} messages, holding 3s")
            await asyncio.sleep(3)  # exceeds ack_wait=2s → NATS redelivers
        except Exception:
            await asyncio.sleep(0.3)

    await js.delete_consumer(stream, consumer)
    await js.purge_stream(stream)
    print("[Python AckWait] Done.")


async def run_nak_storm(nc, duration: int):
    """
    NakStorm: NAK every message with 500ms delay.
    num_redelivered stays elevated → nats-lens detects NakStorm.
    """
    js = nc.jetstream()
    stream = "PYTHON_NAK"
    consumer = "py-nak-consumer"

    await js.add_stream(name=stream, subjects=["py.nak.>"], storage="memory", max_msgs=5000)
    await js.add_consumer(stream, durable_name=consumer, ack_policy="explicit",
                          ack_wait=30, max_ack_pending=50)

    for i in range(50):
        await js.publish("py.nak.msg", json.dumps({"i": i}).encode())

    psub = await js.pull_subscribe("py.nak.>", consumer, stream=stream)
    deadline = time.time() + duration

    print("[Python NakStorm] Running. nats-lens should detect NakStorm.")
    while time.time() < deadline:
        try:
            msgs = await psub.fetch(5, timeout=1)
            for msg in msgs:
                await msg.nak(delay=0.5)  # NAK with 500ms backoff
            print(f"[Python NakStorm] NAK'd {len(msgs)} messages")
            await asyncio.sleep(0.6)
        except Exception:
            await asyncio.sleep(0.2)

    await js.delete_consumer(stream, consumer)
    await js.purge_stream(stream)
    print("[Python NakStorm] Done.")


async def run_seq_gap(nc, duration: int):
    """
    SequenceGap: stream evicts messages before consumer pulls them.
    stream.first_seq > consumer.ack_floor.stream_seq + 1 → gap detected.
    """
    js = nc.jetstream()
    stream = "PYTHON_SEQ"
    consumer = "py-seq-consumer"

    await js.add_stream(name=stream, subjects=["py.seq.>"], storage="memory",
                        max_msgs=10)  # tiny retention: keeps only last 10
    try:
        await js.delete_consumer(stream, consumer)
    except Exception:
        pass
    await js.purge_stream(stream)
    await js.add_consumer(stream, durable_name=consumer, ack_policy="explicit",
                          ack_wait=300, max_ack_pending=512)

    # Publish first batch and ACK some so ack_floor.stream_seq > 0
    for i in range(5):
        await js.publish("py.seq.msg", json.dumps({"i": i}).encode())

    psub = await js.pull_subscribe("py.seq.>", consumer, stream=stream)
    try:
        msgs = await psub.fetch(3, timeout=2)
        for m in msgs:
            await m.ack()  # ack_floor.stream_seq now = 3
    except Exception:
        pass

    # Now publish flood → stream evicts msg 1-3 (already acked) + many more
    print("[Python SeqGap] Publishing 200 messages into a 10-msg stream...")
    for i in range(5, 205):
        await js.publish("py.seq.msg", json.dumps({"i": i}).encode())
    # Stream first_seq ≈ 196; consumer ack_floor = 3 → gap = 196 > 3+1 ✓

    deadline = time.time() + duration
    print("[Python SeqGap] Gap created. nats-lens should detect SequenceGap.")
    while time.time() < deadline:
        await asyncio.sleep(2)

    try:
        await js.delete_consumer(stream, consumer)
        await js.purge_stream(stream)
    except Exception:
        pass
    print("[Python SeqGap] Done.")


async def run_max_pending(nc, duration: int):
    """
    MaxPendingThrottle: num_ack_pending == max_ack_pending AND num_pending > 0.
    Consumer pulls but never acks, filling the pending window.
    """
    js = nc.jetstream()
    stream = "PYTHON_MAX"
    consumer = "py-max-consumer"

    await js.add_stream(name=stream, subjects=["py.max.>"], storage="memory", max_msgs=5000)
    try:
        await js.delete_consumer(stream, consumer)
    except Exception:
        pass
    await js.purge_stream(stream)
    await js.add_consumer(stream, durable_name=consumer, ack_policy="explicit",
                          ack_wait=300, max_ack_pending=5)  # tiny window

    for i in range(100):
        await js.publish("py.max.msg", json.dumps({"i": i}).encode())

    psub = await js.pull_subscribe("py.max.>", consumer, stream=stream)
    # Pull up to max_ack_pending without ACKing → throttle fires
    try:
        msgs = await psub.fetch(5, timeout=2)
        print(f"[Python MaxPending] Pulled {len(msgs)} msgs, NOT acking → throttle")
    except Exception:
        pass

    deadline = time.time() + duration
    print("[Python MaxPending] Window full. nats-lens should detect MaxPendingThrottle.")
    while time.time() < deadline:
        await asyncio.sleep(2)

    try:
        await js.delete_consumer(stream, consumer)
        await js.purge_stream(stream)
    except Exception:
        pass
    print("[Python MaxPending] Done.")


async def run_missing_progress(nc, duration: int):
    """
    MissingProgress: num_ack_pending/max_ack_pending >= 0.9 AND ack_wait > 30s.
    Consumer holds messages without acking, ratio near capacity.
    """
    js = nc.jetstream()
    stream = "PYTHON_MISS"
    consumer = "py-miss-consumer"

    await js.add_stream(name=stream, subjects=["py.miss.>"], storage="memory", max_msgs=5000)
    try:
        await js.delete_consumer(stream, consumer)
    except Exception:
        pass
    await js.purge_stream(stream)
    await js.add_consumer(stream, durable_name=consumer, ack_policy="explicit",
                          ack_wait=300, max_ack_pending=20)  # long ack_wait, larger window

    for i in range(50):
        await js.publish("py.miss.msg", json.dumps({"i": i}).encode())

    psub = await js.pull_subscribe("py.miss.>", consumer, stream=stream)
    # Pull 18/20 = 0.90 of max_ack_pending WITHOUT acking → ratio ≥ 0.9
    # num_ack_pending(18) < max_ack_pending(20) → MAX_PENDING_THROTTLE does NOT fire
    # ack_wait=300s > 30s → MISSING_PROGRESS fires alone
    try:
        msgs = await psub.fetch(18, timeout=2)
        print(f"[Python MissingProgress] Pulled {len(msgs)}/20 msgs (ratio={len(msgs)/20:.2f}), NOT acking")
    except Exception:
        pass

    deadline = time.time() + duration
    print("[Python MissingProgress] nats-lens should detect MissingProgress.")
    while time.time() < deadline:
        await asyncio.sleep(2)

    try:
        await js.delete_consumer(stream, consumer)
        await js.purge_stream(stream)
    except Exception:
        pass
    print("[Python MissingProgress] Done.")


async def run_healthy(nc, duration: int):
    """
    Healthy consumer: promptly acks all messages. No violations expected.
    """
    js = nc.jetstream()
    stream = "PYTHON_HEALTHY"
    consumer = "py-healthy-consumer"

    await js.add_stream(name=stream, subjects=["py.healthy.>"], storage="memory", max_msgs=10000)
    await js.add_consumer(stream, durable_name=consumer, ack_policy="explicit",
                          ack_wait=300, max_ack_pending=512)

    async def publisher():
        i = 0
        while True:
            await js.publish("py.healthy.msg", json.dumps({"seq": i}).encode())
            i += 1
            await asyncio.sleep(0.2)

    pub_task = asyncio.create_task(publisher())
    psub = await js.pull_subscribe("py.healthy.>", consumer, stream=stream)
    deadline = time.time() + duration

    print("[Python Healthy] Running correctly. nats-lens should NOT detect violations.")
    while time.time() < deadline:
        try:
            msgs = await psub.fetch(10, timeout=0.5)
            for msg in msgs:
                await msg.ack()  # promptly ack — no violation
        except Exception:
            await asyncio.sleep(0.1)

    pub_task.cancel()
    await js.delete_consumer(stream, consumer)
    await js.purge_stream(stream)
    print("[Python Healthy] Done. 0 violations expected.")


async def main():
    import nats

    parser = argparse.ArgumentParser()
    parser.add_argument("--nats",     default="nats://localhost:4222")
    parser.add_argument("--scenario", default="ack_wait",
                        choices=["ack_wait", "nak_storm", "healthy",
                                 "seq_gap", "max_pending", "missing_progress"])
    parser.add_argument("--duration", type=int, default=30)
    args = parser.parse_args()

    nc = await nats.connect(args.nats)
    try:
        if args.scenario == "ack_wait":
            await run_ack_wait(nc, args.duration)
        elif args.scenario == "nak_storm":
            await run_nak_storm(nc, args.duration)
        elif args.scenario == "seq_gap":
            await run_seq_gap(nc, args.duration)
        elif args.scenario == "max_pending":
            await run_max_pending(nc, args.duration)
        elif args.scenario == "missing_progress":
            await run_missing_progress(nc, args.duration)
        else:
            await run_healthy(nc, args.duration)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
