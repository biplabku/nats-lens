#!/usr/bin/env python3
"""
Subscribe to nats-lens violation events from Python.

Install: pip install nats-py
Run:     python subscribe.py
"""
import asyncio, json
import nats


async def on_violation(msg):
    event    = json.loads(msg.data)
    vtype    = event["violation"]["type"]
    stream   = event["stream_name"]
    consumer = event["consumer_name"]
    severity = event["severity"]
    fix      = event["violation"].get("fix_command", "see dashboard")

    print(f"[{severity}] {vtype} on {stream}/{consumer}")
    print(f"  Fix: {fix}")


async def main():
    nc = await nats.connect("nats://localhost:4222")
    await nc.subscribe("nats.lens.health.violations.>", cb=on_violation)
    print("Listening for nats-lens violations...")
    await asyncio.sleep(3600)   # run for 1 hour


if __name__ == "__main__":
    asyncio.run(main())
