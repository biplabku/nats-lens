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
    await js.add_consumer(stream, durable_name=consumer, ack_policy="explicit",
                          ack_wait=4, max_ack_pending=10)

    for i in range(20):
        await js.publish("py.ack.msg", json.dumps({"i": i}).encode())

    psub = await js.pull_subscribe("py.ack.>", consumer, stream=stream)
    deadline = time.time() + duration

    print(f"[Python AckWait] Running. nats-lens should detect AckWaitViolation.")
    while time.time() < deadline:
        try:
            msgs = await psub.fetch(3, timeout=2)
            print(f"[Python AckWait] pulled {len(msgs)} messages, holding 6s")
            await asyncio.sleep(6)  # exceeds ack_wait=4s → NATS redelivers
        except Exception:
            await asyncio.sleep(0.5)

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
                        choices=["ack_wait", "nak_storm", "healthy"])
    parser.add_argument("--duration", type=int, default=30)
    args = parser.parse_args()

    nc = await nats.connect(args.nats)
    try:
        if args.scenario == "ack_wait":
            await run_ack_wait(nc, args.duration)
        elif args.scenario == "nak_storm":
            await run_nak_storm(nc, args.duration)
        else:
            await run_healthy(nc, args.duration)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
