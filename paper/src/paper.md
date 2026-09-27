# Configuration-Induced Delivery Failures in NATS JetStream: Detection and Remediation

**Biplab Das**  
Palo Alto Networks  
biplab.das@paloaltonetworks.com

---

## Abstract

NATS JetStream provides at-least-once message delivery guarantees. In practice, five classes of configuration mistakes silently violate this guarantee — causing duplicate execution, data loss, or redelivery storms — without any error visible in application logs or standard monitoring dashboards. Standard Prometheus exporters for NATS expose throughput metrics but are blind to all five failure classes. We present **nats-lens**, a standalone delivery correctness monitor that connects to any NATS deployment, works with consumers written in any language, and detects all five violation classes in real time. We formally characterize each violation class, prove that standard monitors cannot detect any of them, implement five targeted detectors, and evaluate on a controlled NATS cluster with 30 rounds × 5 scenarios. nats-lens achieves **100% detection coverage** across all five classes — versus 0% for the standard Prometheus NATS exporter — with **zero false positives** over 30 minutes of healthy operation. Detection latency ranges from 2,002 ms (SEQUENCE_GAP) to 8,013 ms (ACK_WAIT_VIOLATION), within three poll cycles of violation onset. We verify language-agnostic detection empirically using consumers in Rust, Go, and Python. The tool is open source, deployable via a single Docker command, and exposes findings through four output channels (web dashboard, Prometheus, REST API, NATS health events) usable from any language or framework.

---

## 1. Introduction

NATS JetStream [cite:nats-docs] is a persistent messaging layer for NATS that claims at-least-once message delivery. Engineering teams building distributed systems on NATS rely on this guarantee for correctness: each message must be processed at least once, and consumers must not miss messages silently.

Despite this guarantee, we observe in production NATS deployments that five common configuration mistakes silently violate at-least-once semantics:

1. **ACK_WAIT_VIOLATION** — `ack_wait` shorter than consumer processing time causes NATS to redeliver messages that are still being processed, producing duplicate execution.
2. **SEQUENCE_GAP** — stream retention limits cause NATS to evict messages before a slow consumer can pull them, producing silent data loss.
3. **MAX_PENDING_THROTTLE** — undersized `max_ack_pending` causes NATS to throttle message delivery even when the consumer has processing capacity.
4. **NAK_STORM** — consumers NAKing messages (refusing them for business reasons) get stuck in a redelivery loop, consuming resources without making progress.
5. **MISSING_PROGRESS** — long-running tasks that do not send periodic in_progress acknowledgments trigger ack_wait expiry and duplicate execution.

What makes these failures particularly dangerous is their invisibility. Standard NATS monitoring tools — the Prometheus NATS exporter [cite:prometheus-nats], NATS Surveyor [cite:surveyor] — expose connection counts, message throughput, and byte rates. None expose the consumer-level state signals needed to detect these five violation classes. Application logs show no errors. Operators discover the problem only after observing downstream corruption or duplicate side effects.

We make the following contributions:

1. **Formal characterization** of five NATS JetStream delivery violation classes with precise mathematical conditions for each (Section 3).
2. **Proof** that standard Prometheus NATS metrics cannot detect any of the five classes (Section 3.6).
3. **nats-lens**: a standalone detection tool implementing five targeted detectors, language-agnostic output channels, and a one-command deployment model (Section 4).
4. **Evaluation** demonstrating 100% detection coverage with zero false positives, with detection latency within two poll cycles (Section 5).
5. **Open-source release** at [github.com/biplabku/nats-lens] with pre-built binaries, Docker image, Grafana dashboard, and client code for Go, Python, Java, Node.js, and Rust.

---

## 2. Background

### 2.1 NATS JetStream Pull Consumer Model

JetStream provides persistent message storage over a NATS stream. A *pull consumer* is a durable subscription that explicitly fetches messages from the stream using `$JS.API.CONSUMER.MSG.NEXT` requests. This contrasts with push consumers where the server delivers messages proactively.

Pull consumers have four key configuration parameters relevant to delivery correctness:

- **`ack_wait`**: Maximum time the server waits for a consumer to acknowledge a message before considering it failed and redelivering it. Default: 30 seconds.
- **`max_ack_pending`**: Maximum number of unacknowledged messages the server will deliver to a consumer at one time. Default: 1,000.
- **`max_deliver`**: Maximum number of delivery attempts for a single message before it is considered permanently failed. Default: unlimited.
- **`ack_policy`**: Must be `AckExplicit` for at-least-once semantics.

Three acknowledgment types are available:
- **ACK** (`+ACK`): Message processed successfully; remove from pending.
- **NAK** (`-NAK [delay]`): Message rejected; redeliver after `delay` (or `ack_wait` if no delay).
- **WIP** (`+WPI`): Still processing; reset the `ack_wait` timer.

Consumer state, exposed via `$JS.API.CONSUMER.INFO`, includes:
- `num_pending`: Messages in the stream not yet delivered to this consumer.
- `num_ack_pending`: Delivered messages awaiting acknowledgment.
- `num_redelivered`: Count of distinct messages currently in active redelivery state (gauge, not cumulative counter).

### 2.2 The Delivery Guarantee and Its Limits

JetStream's at-least-once guarantee means: if a message is published to a stream with replicas ≥ 1 and storage type Memory or File, and a consumer with `ack_policy=AckExplicit` and `max_deliver > 1` pulls the message, the message will be delivered at least once and remain in the consumer's pending state until acknowledged.

This guarantee holds **under correct configuration**. Our work characterizes the five classes of configuration mistakes that silently violate it.

### 2.3 Related Monitoring Approaches

The **Prometheus NATS Exporter** [cite:prometheus-nats] exposes server-level metrics: active connections, message rates, subscription counts, server memory. It does not expose per-consumer `num_redelivered`, `num_ack_pending`, or `num_pending`, making all five violation classes invisible.

**NATS Surveyor** [cite:surveyor] provides a lightweight monitoring agent that exposes slightly more per-stream information (message counts, storage bytes) but does not model per-consumer delivery state.

**Kafka monitoring tools** (Burrow [cite:burrow], kminion [cite:kminion]) solve a similar problem for Kafka: detecting consumer group lag and offset commit patterns. However, Kafka's push-based consumer model with explicit offset management creates fundamentally different failure modes than NATS JetStream's pull model with ack_wait and max_ack_pending.

To our knowledge, nats-lens is the first tool to formally characterize and systematically detect delivery correctness violations in NATS JetStream.

---

## 3. Violation Characterization

We define five violation classes formally. Let *C* be a pull consumer with configuration (ack_wait = *W*, max_ack_pending = *P*), and let *S* denote the stream *C* subscribes to.

### 3.1 ACK_WAIT_VIOLATION

**Definition.** An ACK_WAIT_VIOLATION occurs when consumer *C*'s actual message processing time *T_proc* exceeds its configured `ack_wait` *W*.

*Formally:* ∃ message *m* delivered to *C* such that *T_proc(m) > W*.

**Consequence.** When *T_proc(m) > W*, the server marks *m* for redelivery before *C* completes processing. A second consumer (or the same consumer's next pull) receives *m* simultaneously with the ongoing processing. If processing is not idempotent, this causes duplicate side effects.

**Detection signal.** `num_redelivered` grows monotonically between consecutive snapshots. The rate: Δ`num_redelivered` / Δ*t* > *threshold*.

**Proposition 1** (ACK_WAIT is invisible to standard monitors): The Prometheus NATS exporter does not expose `num_redelivered` for individual consumers. Therefore, ACK_WAIT_VIOLATION cannot be detected by any system using only standard Prometheus NATS metrics.

*Proof.* The Prometheus NATS exporter exports only server-level metrics (connections, bytes, subscriptions). Consumer-level `num_redelivered` is accessible only via `$JS.API.CONSUMER.INFO`, which requires a JetStream management API call. No standard exporter makes this call. □

### 3.2 SEQUENCE_GAP

**Definition.** A SEQUENCE_GAP occurs when stream *S* evicts message sequences [*seq_low*, *seq_high*] due to retention limits before consumer *C* pulls them.

*Formally:* S.`first_seq` > C.`ack_floor.stream_seq` + 1.

**Consequence.** Messages with sequence numbers in [C.`ack_floor.stream_seq` + 1, S.`first_seq` - 1] are permanently inaccessible to *C*. These messages are silently skipped; *C* processes subsequent messages as if the gap never existed.

**Detection signal.** S.`first_seq` > C.`ack_floor.stream_seq` + 1 after at least one message has been acknowledged (C.`ack_floor.stream_seq` > 0).

**Proposition 2** (SEQUENCE_GAP is invisible to standard monitors): Stream-level `first_seq` is not exposed by the Prometheus NATS exporter. Consumer-level `ack_floor.stream_seq` is not exposed. Therefore, SEQUENCE_GAP cannot be detected by standard Prometheus NATS metrics.

### 3.3 MAX_PENDING_THROTTLE

**Definition.** A MAX_PENDING_THROTTLE occurs when consumer *C*'s `num_ack_pending` equals `max_ack_pending` *P* while `num_pending` > 0, causing the server to withhold further message delivery.

*Formally:* C.`num_ack_pending` = *P* ∧ C.`num_pending` > 0.

**Consequence.** Consumer *C*'s effective processing rate drops to zero for new messages, even if *C* has available processing capacity. Lag (`num_pending`) grows until in-flight messages are acknowledged.

**Root cause.** `max_ack_pending` is misconfigured relative to the consumer's actual concurrency × prefetch size. The correct formula: *P* ≥ *concurrency* × *prefetch_size*.

**Detection signal.** C.`num_ack_pending` / *P* ≥ *threshold* (typically 0.95) while C.`num_pending` > 0.

### 3.4 NAK_STORM

**Definition.** A NAK_STORM occurs when consumer *C* continuously NAKs messages (explicitly rejecting them), causing a sustained redelivery cycle.

*Formally:* C.`num_redelivered` ≥ *threshold* (gauge) ∧ C.`num_ack_pending` > 0 across ≥ 2 consecutive snapshots.

**Important:** `num_redelivered` in NATS JetStream is a **gauge** — it reports the count of distinct messages currently in an active redelivery cycle, not a cumulative counter. A NAK storm manifests as a stable non-zero value (the same N messages cycling), not as a growing value.

**Consequence.** CPU, network, and NATS server capacity are consumed by redeliveries that never succeed. Consumer throughput drops to zero for messages in the NAK cycle.

**Root cause.** Common causes: stale messages (processing deadline passed, consumer rejects them), malformed messages the consumer cannot parse, or business-logic rejection of all messages of a particular type.

**Detection signal.** C.`num_redelivered` ≥ *N_min* ∧ C.`num_ack_pending` > 0, sustained across 2+ poll intervals.

### 3.5 MISSING_PROGRESS

**Definition.** A MISSING_PROGRESS condition occurs when long-running tasks do not send periodic WIP acknowledgments, causing `ack_wait` to fire mid-processing.

*Formally:* C.`num_ack_pending` / *P* ≥ *ratio_threshold* ∧ *W* > *W_min* (e.g., 30s).

**Consequence.** Same as ACK_WAIT_VIOLATION: duplicate execution when `ack_wait` fires before the task completes. Unlike ACK_WAIT_VIOLATION, the root cause is absent WIP heartbeats rather than misconfigured `ack_wait`.

**Detection signal.** C.`num_ack_pending` / *P* ≥ 0.9 ∧ C.`ack_wait` > 30s.

### 3.6 Proof of Standard Monitor Blindness

**Theorem 1.** No system that uses only the metrics exported by the Prometheus NATS exporter can detect any of the five violation classes defined in Sections 3.1–3.5.

*Proof.* The Prometheus NATS exporter v0.15 exposes the following metric families: `gnatsd_connz_*` (connection counts), `gnatsd_routez_*` (route metrics), `gnatsd_subz_*` (subscription counts), `gnatsd_varz_*` (server variables including message rates and memory). No metric family includes per-consumer state: `num_redelivered`, `num_ack_pending`, `num_pending`, `ack_floor.stream_seq`, or `max_ack_pending`. Each violation class (Sections 3.1–3.5) is defined solely in terms of these per-consumer state variables. Therefore, no violation class is detectable from the Prometheus NATS exporter metrics alone. □

---

## 4. nats-lens Design

### 4.1 Architecture Overview

nats-lens is a standalone Rust binary that connects to a NATS server as an independent monitoring observer. It makes no changes to the monitored streams or consumers and requires only read access to the JetStream management API.

```
NATS Server
    ↓ $JS.API.STREAM.LIST
    ↓ $JS.API.CONSUMER.INFO.*
nats-lens Engine
    ↓ DetectorPipeline (5 detectors)
    ├── Web Dashboard (http://localhost:8080)
    ├── Prometheus /metrics endpoint
    ├── SSE violation stream (/api/violations/stream)
    └── NATS health events (nats.lens.health.violations.*)
```

The engine polls every configured interval (default 5 seconds). On each poll:
1. `$JS.API.STREAM.LIST` → discover all streams
2. For each stream: `$JS.API.CONSUMER.NAMES.{stream}` → list consumers
3. For each consumer: `$JS.API.CONSUMER.INFO.{stream}.{consumer}` → full consumer state
4. Append snapshot to per-consumer history ring (max 30 snapshots)
5. Run all five detectors on the current snapshot + history
6. Broadcast detected violations via all output channels

### 4.2 Language Agnosticism

nats-lens uses only public JetStream management API subjects (`$JS.API.*`). Any NATS consumer — regardless of implementation language (Go, Python, Java, Rust, Node.js, C#) — appears identically in the management API. The tool does not instrument consumer code and requires no changes to existing applications.

### 4.3 History Store and Trend Detection

The `HistoryStore` maintains a bounded ring buffer (max 30 entries) of `ConsumerSnapshot` structs per consumer key (`stream/consumer`). Each snapshot captures: `num_pending`, `num_ack_pending`, `num_redelivered`, `max_ack_pending`, `ack_wait_secs`, stream sequence numbers, and a wall-clock timestamp.

When a consumer is deleted and recreated (as during rolling deployments), `num_redelivered` resets to 0. The history store detects this reset using `trim_to_monotone`: before running detectors, the history ring is trimmed to its monotonically increasing suffix from the end. This prevents stale high values from producing false negative detection (saturating_sub returns 0).

### 4.4 Detector Implementations

**ACK_WAIT_VIOLATION detector**: Requires ≥ 2 history snapshots. Computes Δ`num_redelivered` / Δ*t* between the two most recent snapshots. Fires when Δ`num_redelivered` > 2 AND rate > 2/minute.

**SEQUENCE_GAP detector**: Fires immediately when S.`first_seq` > C.`ack_floor.stream_seq` + 1 AND C.`ack_floor.stream_seq` > 0. No history needed — a single snapshot is sufficient.

**MAX_PENDING_THROTTLE detector**: Fires when C.`num_ack_pending` ≥ C.`max_ack_pending` AND C.`num_pending` > 0. Recommends new `max_ack_pending` = floor(1.5 × current).

**NAK_STORM detector**: Fires when C.`num_redelivered` ≥ 2 AND C.`num_ack_pending` > 0 across ≥ 2 consecutive snapshots. Uses the gauge nature of `num_redelivered` directly.

**MISSING_PROGRESS detector**: Fires when C.`num_ack_pending` / C.`max_ack_pending` ≥ 0.9 AND C.`ack_wait_secs` > 30.

### 4.5 Output Channels

Four language-agnostic output channels surface violations:

1. **Web dashboard** — dark-theme browser UI with stream health badges, consumer metrics table, sparkline lag charts, violation cards with copy-button fix commands, and a live SSE-fed violation feed.

2. **Prometheus metrics** — `nats_lens_consumer_lag_msgs`, `nats_lens_redeliveries_per_min`, `nats_lens_ack_pending_ratio`, `nats_lens_violations_active{type}` — consumable by any Prometheus-compatible monitoring stack.

3. **REST API** — `GET /api/streams` returns all stream and consumer health as JSON; `GET /api/history/{stream}/{consumer}` returns the last 30 snapshots for trend analysis.

4. **NATS health events** — violations published to `nats.lens.health.violations.{stream}.{consumer}` as structured JSON. Any NATS client in any language subscribes and routes to alerting systems (PagerDuty, Slack, OpsGenie).

### 4.6 Apply Now

For three of the five violation classes (ACK_WAIT_VIOLATION, MAX_PENDING_THROTTLE, SEQUENCE_GAP), nats-lens can apply the recommended fix directly via the NATS management API (`$JS.API.CONSUMER.UPDATE`, `$JS.API.STREAM.UPDATE`) through a REST endpoint (`POST /api/fix/{stream}/{consumer}/ack-wait`). NAK_STORM and MISSING_PROGRESS require application code changes and are surfaced as recommendations only. Input validation enforces safe bounds (ack_wait ≥ 1s, max_ack_pending ≥ 1) before any mutation.

### 4.7 Pre-Deployment Audit Mode

`nats-lens init` performs a one-shot configuration audit against live consumer configurations without requiring the monitoring engine to run. It checks five configuration rules (ack_wait threshold, max_ack_pending formula, stream retention relative to consumer rate, max_deliver limits, missing in_progress guidance) and outputs human-readable findings with exact NATS CLI fix commands. The `--fail-on-critical` flag exits with code 1 when critical issues are found, enabling integration into CI/CD pipelines to block deployments with misconfigured consumers before they reach production.

---

## 5. Evaluation

### 5.1 Experimental Setup

We evaluate nats-lens on a single NATS server 2.10 (JetStream enabled, in-memory storage) running in a Docker container on a MacBook Pro M3 Pro (Apple Silicon, 18 GB unified memory). nats-lens polls every 3 seconds (`--interval 3`). All experiments use fresh NATS state (prior consumer history purged between runs). The evaluation harness is open-source at the same repository as nats-lens.

For each violation class, we run 30 controlled injection rounds. Each round:
1. Creates a fresh stream and consumer with configuration designed to be vulnerable to the violation
2. Starts a background injector that produces the violation condition
3. Records time-to-detection (from injection start to first violation broadcast)
4. Cleans up and resets history between rounds

We evaluate the false positive rate by running a correctly-configured consumer (ack_wait=300s, max_ack_pending=512, promptly acking all messages) for **30 minutes** and counting any violations detected.

### 5.2 Detection Coverage

Table 1 shows detection results for 30 rounds per scenario. nats-lens achieves 100% detection coverage across all five violation classes. The baseline — the standard Prometheus NATS exporter [cite:prometheus-nats] — detects 0 of 5 classes (Theorem 1).

**Table 1: Detection Coverage (30 rounds each, poll interval = 3s)**

| Violation Type | nats-lens | Prometheus NATS Exporter |
|---|---|---|
| ACK_WAIT_VIOLATION | **30/30 (100%)** | 0/30 (0%) |
| SEQUENCE_GAP | **30/30 (100%)** | 0/30 (0%) |
| MAX_PENDING_THROTTLE | **30/30 (100%)** | 0/30 (0%) |
| NAK_STORM | **30/30 (100%)** | 0/30 (0%) |
| MISSING_PROGRESS | **30/30 (100%)** | 0/30 (0%) |

The baseline's 0% detection is structurally guaranteed by Theorem 1: all five violation classes are defined in terms of per-consumer state variables (`num_redelivered`, `num_ack_pending`, `ack_floor.stream_seq`) that the Prometheus NATS exporter does not expose at any granularity.

### 5.3 Detection Latency

Detection latency is the wall-clock time from violation injection to the first violation broadcast on `nats.lens.health.violations.*`. Figure 2 shows the CDF per violation class.

**Table 2: Detection Latency Percentiles (milliseconds)**

| Violation Type | P50 | P95 | P99 | Poll cycles required |
|---|---|---|---|---|
| SEQUENCE_GAP | 2,002 | 2,008 | 2,010 | 1 |
| MAX_PENDING_THROTTLE | 2,006 | 2,010 | 2,011 | 1 |
| MISSING_PROGRESS | 2,008 | 2,015 | 2,017 | 1 |
| NAK_STORM | 6,022 | 6,026 | 6,029 | 2 |
| ACK_WAIT_VIOLATION | 8,013 | 8,018 | 8,021 | 2–3 |

Three violation classes (SEQUENCE_GAP, MAX_PENDING_THROTTLE, MISSING_PROGRESS) are detected in a single poll cycle because their detectors require only one snapshot: the violation condition is visible in the current state without any historical comparison. The remaining two classes (NAK_STORM, ACK_WAIT_VIOLATION) require ≥ 2 consecutive snapshots to confirm the condition is sustained rather than transient.

With the default 5-second poll interval, all five classes are detected within 25 seconds of onset. Operators can reduce this to 10 seconds by setting `--interval 5` to 2.

### 5.4 False Positive Rate

We ran a correctly-configured consumer (ack_wait=300s, max_ack_pending=512) actively processing 5 messages/second for **30 minutes** (1,800 seconds). nats-lens detected **0 violations** during the entire window (0.00/min).

The five detector thresholds are calibrated to require unambiguous multi-snapshot signal:

- **ACK_WAIT**: Δnum_redelivered ≥ 2 AND rate ≥ 2/min across two consecutive snapshots
- **SEQUENCE_GAP**: stream.first_seq > ack_floor + 1 (zero false-positive risk — either the gap exists or it doesn't)
- **MAX_PENDING_THROTTLE**: ack_pending ≥ max_ack_pending AND pending > 0 simultaneously
- **NAK_STORM**: num_redelivered ≥ 2 AND ack_pending > 0 across two consecutive snapshots
- **MISSING_PROGRESS**: ack_pending_ratio > 0.9 AND ack_wait > 30s

A healthy consumer with a 5-second processing time and ack_wait=300s has num_redelivered=0 at all times, ack_pending well below max, and no sequence gaps. No threshold is triggered.

### 5.5 Multi-Language Verification

A core claim is that nats-lens detects violations regardless of the consumer implementation language. We verify this empirically by running ACK_WAIT_VIOLATION and NAK_STORM scenarios using consumers written in three languages, all connecting to the same NATS server monitored by nats-lens.

**Table 3: Multi-Language Detection Results**

| Consumer language | Client library | ACK_WAIT_VIOLATION | NAK_STORM |
|---|---|---|---|
| Rust | async-nats 0.38 | ✅ Detected | ✅ Detected |
| Go | nats.go 1.37 | ✅ Detected | ✅ Detected |
| Python | nats-py 2.x | ✅ Detected | ✅ Detected |

This result is structurally guaranteed: nats-lens reads only the NATS server's JetStream management API (`$JS.API.CONSUMER.INFO`), which exposes consumer state independently of the client library used. The server does not expose client identity or library version in its monitoring API.

### 5.6 Operational Overhead

**API request volume.** nats-lens issues the following requests per poll cycle:

```
requests_per_poll = 1 + N_streams × (2 + N_consumers_per_stream)
```

| Deployment size | N_streams | N_consumers | Requests/poll | At 5s interval |
|---|---|---|---|---|
| Small | 5 | 10 | 61 | 12.2 req/s |
| Medium | 10 | 50 | 71 | 14.2 req/s |
| Large | 50 | 200 | 251 | 50.2 req/s |

NATS server throughput is typically measured in millions of messages per second [cite:nats-docs]. nats-lens's monitoring traffic is negligible at any deployment scale.

**Memory.** The history store holds at most 30 snapshots per consumer. Each `ConsumerSnapshot` is approximately 120 bytes. For 200 consumers: 200 × 30 × 120 = 720 KB — well within the memory budget of any monitoring container.

**Poll cycle latency.** We measured REST API response time (`GET /api/streams`) as a proxy for end-to-end poll cycle latency:

| N consumers | API response time |
|---|---|
| 5 | < 5 ms |
| 25 | < 8 ms |
| 50 | < 12 ms |

Poll cycle latency is dominated by NATS network round-trips, not computation. The detection pipeline (sorting, detector evaluation, history update) adds < 1 ms for any realistic consumer count.

### 5.7 Comparison with NATS Surveyor

NATS Surveyor [cite:surveyor] is the most capable existing NATS monitoring tool. We compare directly:

| Feature | NATS Surveyor | nats-lens |
|---|---|---|
| Per-consumer num_redelivered | ❌ | ✅ |
| Per-consumer num_ack_pending | ❌ | ✅ |
| Sequence gap detection | ❌ | ✅ |
| ACK_WAIT_VIOLATION | ❌ | ✅ |
| NAK_STORM | ❌ | ✅ |
| Fix recommendations | ❌ | ✅ |
| Language-agnostic alerts | ❌ | ✅ |
| Apply Now (auto-fix) | ❌ | ✅ |

NATS Surveyor focuses on account-level throughput and storage metrics. nats-lens focuses exclusively on delivery correctness — a complementary concern not addressed by any existing tool.

---

## 6. Related Work

### 6.1 Message Queue Monitoring

Burrow [cite:burrow] monitors Kafka consumer group lag and offset commit rates, alerting when lag grows consistently. kminion [cite:kminion] provides more granular Kafka consumer monitoring. These tools are tailored to Kafka's offset-based model and do not apply to NATS JetStream's ack_wait / pull-consumer model.

RabbitMQ provides a built-in management API and Prometheus plugin that expose queue depth, consumer utilization, and message rates. RabbitMQ's acknowledgment model is simpler (no ack_wait, no max_ack_pending), so the five violation classes we characterize do not apply.

### 6.2 Distributed Systems Monitoring

Dapper [cite:dapper] and Jaeger [cite:jaeger] provide distributed tracing — end-to-end visibility of request propagation. They observe message *flows* rather than message *delivery guarantees*. A message that is redelivered and processed twice would appear as two successful traces.

### 6.3 Transactional Outbox Pattern

The Transactional Outbox pattern [cite:outbox] ensures reliable message publication from a database transaction. It addresses the publisher side of the delivery guarantee (ensuring messages are published). nats-lens addresses the consumer side (ensuring delivered messages are processed correctly).

### 6.4 NATS-Specific Prior Work

NATS Surveyor [cite:surveyor] provides account-level metrics (message counts, storage usage, connections). It does not model per-consumer delivery state. The official Prometheus NATS exporter [cite:prometheus-nats] exposes server-level metrics only. To our knowledge, nats-lens is the first published work to characterize and detect consumer-level delivery correctness violations in NATS JetStream.

---

## 7. Conclusion

We formally characterized five classes of configuration-induced delivery failures in NATS JetStream and proved that standard monitoring tools are structurally incapable of detecting any of them. We implemented nats-lens, a standalone detector achieving 100% coverage with zero false positives, language-agnostic deployment, and detection latency within two poll cycles. The tool is open source with full documentation, pre-built binaries, and client code in five languages.

The five violation classes defined in this paper can serve as a checklist for any team operating NATS JetStream in production: verify that ack_wait exceeds P99 processing time, that max_ack_pending accommodates actual concurrency, that stream retention exceeds expected consumer lag, that stale messages are ACKed (not NAKed), and that long-running tasks send periodic WIP acknowledgments.

---

## References

[cite:nats-docs] NATS.io. *NATS JetStream Documentation*. https://docs.nats.io/nats-concepts/jetstream

[cite:prometheus-nats] NATS.io. *Prometheus NATS Exporter*. https://github.com/nats-io/prometheus-nats-exporter

[cite:surveyor] NATS.io. *NATS Surveyor*. https://github.com/nats-io/nats-surveyor

[cite:burrow] LinkedIn. *Burrow: Kafka Consumer Lag Checking*. https://github.com/linkedin/Burrow

[cite:kminion] Cloudhut GmbH. *kminion: Kafka monitoring tool*. https://github.com/cloudhut/kminion

[cite:dapper] B. H. Sigelman et al. *Dapper, a Large-Scale Distributed Systems Tracing Infrastructure*. Google Technical Report, 2010.

[cite:jaeger] CNCF. *Jaeger: Distributed Tracing Platform*. https://www.jaegertracing.io/

[cite:outbox] C. Richardson. *Transactional Outbox Pattern*. https://microservices.io/patterns/data/transactional-outbox.html
