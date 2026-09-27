# Understanding JetStream Violations

nats-lens detects five classes of delivery guarantee violations. This document explains what each one means, why it happens, and exactly how to fix it.

---

## 1. ACK_WAIT_VIOLATION

### What it means
Your `ack_wait` setting is shorter than the time your consumer actually takes to process a message. When `ack_wait` expires, NATS thinks the consumer is dead and redelivers the message to another consumer — even though the first consumer is still processing it.

**Result**: The same message is processed twice. If your processing is not idempotent, this causes data corruption or duplicate side effects.

### Why it's hard to notice
Your consumer never logs an error. NATS never logs an error. The only signal is `num_redelivered` steadily climbing in NATS consumer info — a metric most teams don't watch.

### How to diagnose
nats-lens shows: `28 redeliveries/min — ack_wait (30s) shorter than processing time`

The redelivery rate tells you how often duplicates are happening right now. If you see >0 redeliveries/min on a consumer that isn't NAKing messages, this is the likely cause.

### How to fix

**Step 1**: Find your actual P99 processing time. Look at your application's processing latency metrics, or use nats-lens's history endpoint:
```bash
curl http://localhost:8080/api/history/ORDERS/order-processor | jq '.[-10:] | .[].ack_pending_ratio'
```

**Step 2**: Set `ack_wait` to at least `max(P99_processing_time, 120s) + 60s`:
```bash
# If your P99 is 47s:
# ack_wait = max(47, 120) + 60 = 180s = 3 minutes
nats consumer edit ORDERS order-processor --ack-wait 3m
```

**Step 3**: For long-running tasks, send in_progress acks periodically:
```rust
// Rust (async-nats)
msg.ack_with(AckKind::Progress).await?;  // resets the ack_wait timer
```
```go
// Go (nats.go)
msg.InProgress()  // call every 30s for long tasks
```
```python
# Python (nats.py)
await msg.in_progress()
```

**Step 4**: Verify the fix — `num_redelivered` should stop climbing.

---

## 2. SEQUENCE_GAP

### What it means
Your stream hit its retention limit (`max_msgs` or `max_bytes`) and evicted old messages before your consumer could pull them. Those messages are permanently gone — your consumer will never process them.

**Result**: Silent data loss. Your consumer thinks it has processed everything. It hasn't.

### Why it's hard to notice
From your consumer's perspective, nothing is wrong. It pulls messages, processes them, acks them. It has no way to know that sequences 45,821–45,830 were deleted before it ever saw them. The gap is invisible in your application logs.

### How to diagnose
nats-lens shows: `299 messages silently evicted (seq 462–760) before consumer could pull them`

The sequence numbers tell you exactly which messages were lost.

### How to fix

You have two options — fix the retention limit, or fix the consumer throughput:

**Option A: Increase retention**
```bash
# Increase max_msgs to accommodate your expected backlog
nats stream edit EVENTS --max-msgs 10000
```

**Option B: Increase consumer throughput**
Increase `max_ack_pending` to allow more parallel processing:
```bash
nats consumer edit EVENTS event-handler --max-pending 100
```
And increase your consumer's worker count so it can process faster.

**Option C: Use a different discard policy**
Change from `discard: old` (evicts oldest) to `discard: new` (rejects new publishes when full). This prevents data loss but causes publisher backpressure instead:
```bash
nats stream edit EVENTS --discard new
```

**Which to choose**: If losing messages is unacceptable (payments, audit logs), use `discard: new` + fix the throughput. If you can afford to lose old events (analytics, logs), increase retention.

---

## 3. MAX_PENDING_THROTTLE

### What it means
`max_ack_pending` is too small for your consumer's concurrency + prefetch configuration. NATS is artificially throttling how many messages it delivers, even though your consumer has capacity to process more.

**Result**: Reduced throughput, growing lag, consumers sitting idle while messages queue up in NATS.

### The correct formula
```
max_ack_pending ≥ max_concurrency + (num_pull_workers × prefetch_size)
```

For example, if you have 48 concurrent workers, 4 pull worker goroutines, and prefetch of 8:
```
max_ack_pending ≥ 48 + (4 × 8) = 80
```

### How to fix
```bash
# Replace 80 with your calculated value
nats consumer edit ORDERS order-processor --max-pending 80
```

In your consumer code, set `MaxAckPending` to match:
```go
// Go
sub, _ := js.PullSubscribe("ORDERS", "order-processor",
    nats.MaxAckPending(80),
)
```
```rust
// Rust
let consumer = stream.get_or_create_consumer("order-processor",
    pull::Config {
        max_ack_pending: 80,
        ..Default::default()
    },
).await?;
```

---

## 4. NAK_STORM

### What it means
Your consumer is NAKing (negatively acknowledging) messages — usually because they are stale or unparseable — and NATS is redelivering them. If the redelivered messages are *still* stale, your consumer NAKs them again. NATS redelivers again. This creates an infinite redelivery loop.

**Result**: CPU and network consumed by redelivering messages that will never be processed. Growing lag as the consumer spends all its capacity handling redeliveries instead of new messages.

### Why it happens
The most common cause: a consumer falls behind during a traffic spike. Messages pile up. By the time the consumer processes them, they are time-expired (stale by business logic). The consumer NAKs them. NATS redelivers. They are still stale. Loop.

### How to fix

**The key insight**: Stale messages must be ACKed (consumed and discarded), not NAKed. NAKing a stale message cannot make it fresher — it will always be redelivered stale.

```go
// Go — check staleness, ACK (not NAK) if expired
msg, _ := sub.NextMsgWithContext(ctx)
if isStale(msg) {
    msg.Ack()  // ✅ ACK — prevent redelivery
    return
}
// process...
msg.Ack()
```

```rust
// Rust
if is_stale(&msg, stale_threshold) {
    msg.ack().await?;  // ACK stale messages
    continue;
}
// process...
msg.ack().await?;
```

**Also set a max_deliver limit** to bound the redelivery count for truly broken messages:
```bash
nats consumer edit EVENTS event-handler --max-deliver 5
```

After 5 deliveries, NATS stops redelivering. You can route these to a dead-letter subject for manual inspection.

---

## 5. MISSING_PROGRESS

### What it means
Your tasks take a long time to complete (longer than `ack_wait`) and are not sending in_progress acks periodically. The `ack_pending` slot fills up and NATS redelivers messages to other consumers while the first consumer is still working.

This differs from ACK_WAIT_VIOLATION: that's caused by `ack_wait` being misconfigured. MISSING_PROGRESS is caused by long-running tasks that simply don't use the in_progress mechanism.

**Result**: Duplicate processing for long-running tasks.

### How to diagnose
nats-lens shows: `ack_pending at 95% of max with ack_wait=120s — long tasks should send in_progress acks`

A high `ack_pending` ratio near `max_ack_pending` combined with a long `ack_wait` is the signature.

### How to fix

Send in_progress acks every 30 seconds for long-running tasks:

```go
// Go — in_progress heartbeat goroutine
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            msg.InProgress()
        case <-done:
            return
        }
    }
}()
// ... do your long task ...
msg.Ack()
```

```rust
// Rust — spawn a progress ack task
let msg_clone = msg.clone();
let progress_task = tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        let _ = msg_clone.ack_with(AckKind::Progress).await;
    }
});
// ... do your long task ...
progress_task.abort();
msg.ack().await?;
```

```python
# Python — asyncio background task
async def send_progress(msg, stop_event):
    while not stop_event.is_set():
        await msg.in_progress()
        await asyncio.sleep(30)

stop = asyncio.Event()
asyncio.create_task(send_progress(msg, stop))
# ... do your long task ...
stop.set()
await msg.ack()
```

---

## Severity Guide

| Violation | Severity | Why |
|---|---|---|
| ACK_WAIT_VIOLATION | 🔴 Critical | Active duplicate execution happening right now |
| SEQUENCE_GAP | 🔴 Critical | Data already lost, cannot be recovered |
| NAK_STORM | 🔴 Critical | Throughput collapsing, self-reinforcing loop |
| MAX_PENDING_THROTTLE | 🟡 Warning | Throughput degraded, no data loss yet |
| MISSING_PROGRESS | 🟡 Warning | Potential for duplicates under load |

---

## Proactive Configuration Checklist

Before deploying a JetStream consumer, verify these five things:

- [ ] `ack_wait` ≥ P99 processing time + 60s safety margin
- [ ] `max_ack_pending` ≥ concurrency × prefetch
- [ ] Stream `max_msgs`/`max_bytes` large enough for expected lag at peak
- [ ] Long-running tasks send `in_progress` acks every ~30s
- [ ] Stale messages are ACKed (not NAKed) before processing
