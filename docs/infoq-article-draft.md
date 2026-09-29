# Five Ways NATS JetStream Silently Breaks Your At-Least-Once Guarantee

**Draft for InfoQ — Practitioner Article**
**Author: Biplab Kumar Das**
**Target: 1800–2200 words**

---

## Key Takeaways

- NATS JetStream's at-least-once delivery guarantee depends on two configuration values most teams set once and forget: `ack_wait` and `max_ack_pending`
- Five specific misconfiguration patterns silently violate the guarantee — no errors in application logs, nothing in Prometheus, nothing in Datadog
- Standard monitoring tools expose server throughput metrics, not consumer delivery state — they cannot detect any of these failures by design
- nats-lens is an open-source tool that reads from JetStream's management API and detects all five patterns in real time, from any consumer regardless of language

---

## The Incident That Started This

We had a financial transaction processor running on NATS JetStream. It had been in production for months, passing every health check, green on every dashboard. One Monday morning, a customer noticed they'd been charged twice for the same order. Then another. Then twelve more.

No exceptions in the logs. No error rates spiking. No dead letter queue filling up. The application thought it had processed each message exactly once. NATS agreed — delivery confirmed, acknowledged, done.

Except it hadn't been. The same messages had been delivered twice, processed twice, and charged twice. The application had no idea.

I spent the better part of two days looking at the wrong things. Pod restarts, network blips, database transaction isolation. The answer turned out to be three lines in a config file from nine months ago:

```yaml
ack_wait: 30s
max_ack_pending: 1000
```

Our P99 processing time had grown from 12 seconds to 47 seconds over those nine months as data volume increased. `ack_wait` was still 30 seconds — the value someone picked at launch because it "seemed reasonable." NATS had been redelivering messages that were still being processed, and our consumers had been processing them again without complaint.

The frustrating part was not the bug. Bugs happen. The frustrating part was that nothing in our observability stack had any idea this was occurring. Not Prometheus. Not Datadog. Not the NATS dashboard. Not our application traces. Two months of silent duplicate processing, invisible to everything.

That's what pushed me to build nats-lens.

---

## Why Monitoring Misses This

The Prometheus NATS exporter — the standard tool for NATS observability — exposes a solid set of server metrics: connection counts, message rates, memory usage, route health. These are genuinely useful for understanding your NATS server's health.

What they don't expose is per-consumer delivery state. Specifically, none of them include:

- `num_redelivered` — how many distinct messages have been redelivered at least once
- `num_ack_pending` — how many messages have been delivered but not yet acknowledged
- `ack_floor.stream_seq` — the sequence number up to which this consumer has acknowledged

These three fields live in the JetStream consumer info API (`$JS.API.CONSUMER.INFO`), not in the server metrics that Prometheus scrapes. There's no aggregation or summary of them anywhere in the standard tooling stack.

This isn't a criticism of the exporter — it's monitoring a different layer. The exporter tells you whether NATS is healthy. It cannot tell you whether your delivery guarantees are intact. Those are genuinely different questions, and confusing them is easy to do.

---

## The Five Failure Patterns

Over the process of investigating our incident and talking to other teams running JetStream at scale, five patterns kept coming up. They're distinct enough that each requires a different fix, but they share the same property: standard monitoring has no visibility into any of them.

### 1. ACK_WAIT_VIOLATION — The Silent Duplicate

This is what bit us. `ack_wait` is the time limit NATS gives a consumer to acknowledge a message before assuming the consumer is dead and redelivering. If your actual processing time grows past that limit, every message gets delivered twice — to the same consumer, in sequence, with no indication from NATS that anything is wrong.

The detection signal is `num_redelivered` growing steadily over time. Not jumping — growing. Each new message that crosses the `ack_wait` threshold increments the count. If you see this metric climbing while your consumers are running normally, you have a processing time vs `ack_wait` mismatch.

The fix is simple once you know: set `ack_wait` to your P99 processing time plus a generous buffer, and use `msg.ack_with(AckKind::Progress)` for long-running tasks to reset the timer mid-processing.

### 2. SEQUENCE_GAP — The Silent Data Loss

JetStream streams have retention policies. If a stream is configured with `MaxAge` or `MaxMsgs` and fills up, it starts evicting old messages. If a slow consumer hasn't pulled those messages yet, they're gone — the sequence numbers have a gap, and the consumer will skip over them with no error.

The detection signal is `S.first_seq > C.ack_floor.stream_seq + 1`. In plain terms: the earliest message still on the stream has a higher sequence number than the last one the consumer acknowledged. The messages in between have been evicted.

This one is particularly nasty because the consumer doesn't know it's missing anything. It continues from where it left off, which is now past the gap. If you're processing financial records or audit logs, this is undetected data loss.

### 3. MAX_PENDING_THROTTLE — The Invisible Bottleneck

`max_ack_pending` caps how many messages NATS will deliver to a consumer before requiring acknowledgments. It's a backpressure mechanism, and it's necessary. The problem comes when teams set it based on some default or gut feeling rather than actual concurrency.

If `max_ack_pending` is smaller than `concurrency × prefetch_size`, your consumer will hit the cap and stall — NATS stops delivering new messages. The consumer isn't overloaded. It has capacity. It's just throttled by a configuration value that doesn't match the workload.

The frustrating thing about this one is that it looks like normal operation from outside. Message rates are lower than expected, but there's no error, no backpressure signal, nothing obviously wrong. Teams often blame network issues or underpowered consumers when the fix is changing one number in consumer config.

### 4. NAK_STORM — The Redelivery Loop

When a consumer NAKs a message, NATS redelivers it. Usually with a backoff. That's the intended behavior for transient failures. But if the reason for the NAK doesn't resolve — stale cached data, a dependency that's still down, a message that's genuinely unprocessable — you end up in a loop.

`num_redelivered` stays elevated and stable. Unlike the ACK_WAIT case where it grows, here the same messages keep cycling. Same N messages, delivered, NAKed, redelivered, NAKed again. The consumer is burning CPU and the messages aren't making progress.

This one is interesting because the metric signature is different from ACK_WAIT_VIOLATION despite both involving `num_redelivered`. ACK_WAIT shows growth (new messages hitting the limit each cycle). NAK_STORM shows stability (same messages cycling). nats-lens distinguishes between the two.

### 5. MISSING_PROGRESS — The Long Task Trap

Long-running tasks are common in data pipelines: ML inference jobs, video transcoding, report generation. NATS's `ack_wait` is a single timer that starts when the message is delivered and doesn't pause when you're doing legitimate work.

If your task takes 10 minutes but `ack_wait` is 30 seconds, you'll get the message redelivered 20 times before you finish — assuming the previous deliveries didn't kill the job with duplicate execution first. The fix is sending in-progress acknowledgments periodically: `msg.ack_with(AckKind::Progress)` in Rust, `msg.in_progress()` in Go. These reset the `ack_wait` timer.

The detection signal is `num_ack_pending / max_ack_pending ≥ 0.9` combined with a large `ack_wait` setting. When the pending ratio is near the cap and the ack_wait is long, it's usually a long-running task that isn't sending progress acks.

---

## How nats-lens Works

The JetStream management API (`$JS.API.CONSUMER.INFO`) exposes the full consumer state — `num_redelivered`, `num_ack_pending`, `ack_floor`, and more — for every consumer on every stream. It's available to any NATS client with read access, regardless of what language your consumers are written in.

nats-lens is a single Rust binary that connects to NATS as a read-only observer. On each poll cycle it:

1. Lists all streams via `$JS.API.STREAM.LIST`
2. Fetches full state for every consumer via `$JS.API.CONSUMER.INFO`
3. Maintains a ring buffer of the last 30 snapshots per consumer
4. Runs five targeted detectors against the snapshot history
5. Broadcasts any violations it finds

The "read-only observer" part matters. nats-lens never subscribes to your application subjects, never reads message payloads, never touches your consumer subscriptions. It watches the management API, which is separate from the data plane. You add it to your cluster without touching any existing code.

At 1,000 active consumers it uses under 13MB of RSS and sends roughly 200 API requests per second at a 5-second poll interval — well within NATS's management plane capacity.

---

## What It Looks Like in Practice

Start nats-lens against any NATS server with JetStream:

```bash
docker run -p 8080:8080 ghcr.io/biplabku/nats-lens:latest \
  --nats nats://your-nats-server:4222
```

Open `http://localhost:8080`. You get a live dashboard showing every stream and consumer with health status. Green means clean. A violation card shows up when any of the five patterns is detected, with the exact NATS CLI command to fix it:

```
ACK_WAIT_VIOLATION on PAYMENTS/order-processor
  28 redeliveries/min — ack_wait (30s) shorter than processing time
  Fix: nats consumer edit PAYMENTS order-processor --ack-wait 3m
```

You can also receive violations as NATS events from any language. nats-lens publishes to `nats.lens.health.violations.{stream}.{consumer}`:

```python
async def on_violation(msg):
    event = json.loads(msg.data)
    # page your on-call, create a ticket, update a dashboard
    print(event["violation"]["fix_command"])

await nc.subscribe("nats.lens.health.violations.>", cb=on_violation)
```

And Prometheus metrics at `/metrics` if you want to build Grafana alerts:

```
nats_lens_violations_active{stream="PAYMENTS",consumer="order-processor",type="ACK_WAIT_VIOLATION"} 1
nats_lens_redelivery_rate{stream="PAYMENTS",consumer="order-processor"} 28.4
```

---

## A Note on the Go Client Behavior

During testing across multiple language clients, I found something worth documenting. The Go NATS client (`nats.go`) implements `PullSubscribe.Fetch()` with sequence-ordered delivery — after `ack_wait` fires, redelivered messages keep their original sequence position and are delivered before new messages on the next fetch.

The practical effect: when you fetch N messages and hold them past `ack_wait`, the same N messages cycle continuously. `num_redelivered` stays stable at N rather than growing. This matches the NAK_STORM metric signature, not ACK_WAIT_VIOLATION.

nats-lens detects it correctly either way. But it means the violation type you see depends on your client library's fetch semantics, not just the underlying configuration error. Both patterns represent consumer delivery degradation; the metric signature differs based on whether distinct new messages are accumulating or the same messages are cycling.

If you're running Go consumers and see NAK_STORM violations while your consumers aren't actually NAKing anything — check your `ack_wait` first.

---

## Getting Started

The full tool is open source at [github.com/biplabku/nats-lens](https://github.com/biplabku/nats-lens).

```bash
# via cargo
cargo install nats-lens
nats-lens --nats nats://localhost:4222

# via docker
docker run -p 8080:8080 ghcr.io/biplabku/nats-lens:latest \
  --nats nats://localhost:4222
```

The repository includes working examples in Rust, Go, Python, Node.js, and Java for subscribing to violation events, a Grafana dashboard JSON, and a one-command evaluation script that injects each violation type and verifies detection.

The formal characterization of all five violation classes, detection proofs, and empirical evaluation results are available in the accompanying paper on arXiv.

---

## Closing Thought

NATS JetStream is genuinely good software. The delivery guarantees it offers are real — when the configuration matches the workload. The gap was never in NATS. It was in the feedback loop between "configuration set at launch" and "workload that evolved for months afterward," with no tooling in between to notice the drift.

That's the problem nats-lens is for. Not to replace your existing NATS monitoring — keep that. But to add the delivery correctness layer that wasn't there before.

---

*Biplab Kumar Das is a distributed systems engineer and independent researcher. He works on observability tooling for cloud-native messaging systems.*

*[github.com/biplabku/nats-lens](https://github.com/biplabku/nats-lens) | [@biplabku](https://twitter.com/biplabku)*
