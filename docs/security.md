# Security Guide

nats-lens is designed to be safe to run in production environments. This page explains what it does and doesn't access, and how to deploy it securely.

---

## What nats-lens Accesses

**Read-only JetStream management API subjects:**
- `$JS.API.STREAM.LIST` — lists stream names
- `$JS.API.STREAM.INFO.{name}` — reads stream config and state (message counts, sequence numbers)
- `$JS.API.CONSUMER.NAMES.{stream}` — lists consumer names
- `$JS.API.CONSUMER.INFO.{stream}.{consumer}` — reads consumer state (pending count, redelivery count, ack floor)

**Publishes to:**
- `nats.lens.health.violations.{stream}.{consumer}` — violation events (structured JSON, no payload content)

**Never reads:**
- Message payloads (your business data)
- Stream message content
- Any subject outside `$JS.API.*` and `nats.lens.*`

---

## Credentials

nats-lens uses the NATS credentials you provide via the `--nats` URL. It needs at minimum:

**Minimum required permissions (NATS authorization config):**
```
authorization {
  users = [
    {
      user: "nats-lens"
      password: "your-password"
      permissions: {
        publish: ["nats.lens.health.violations.>"]
        subscribe: ["$JS.API.STREAM.LIST",
                    "$JS.API.STREAM.INFO.>",
                    "$JS.API.CONSUMER.NAMES.>",
                    "$JS.API.CONSUMER.INFO.>"]
      }
    }
  ]
}
```

This prevents nats-lens from reading or publishing to any of your application subjects.

---

## Deployment Best Practices

**Run inside your network.** nats-lens should run as a pod/container inside your Kubernetes cluster or VPC, not as an external service. This means:
- NATS credentials never leave your network
- The web UI is accessible only from inside your cluster (use `kubectl port-forward` for access)
- No data is sent to any external service

**Expose the dashboard behind authentication** if you open it to a wider audience:
```yaml
# Use an ingress with auth
annotations:
  nginx.ingress.kubernetes.io/auth-type: basic
  nginx.ingress.kubernetes.io/auth-secret: nats-lens-auth
```

**Prometheus metrics are safe to expose** to your internal Prometheus — they contain only numeric metrics and stream/consumer names, not message content.

**NATS violation events** contain only metadata (stream name, consumer name, counts, severity) — never message payloads. They are safe to forward to Slack, PagerDuty, or any alerting system.

---

## Data Retention

nats-lens holds the last 30 snapshots per consumer in memory (about 150 bytes each). For a deployment with 100 consumers, this is ~450 KB of RAM. No data is written to disk.

When nats-lens restarts, history resets. The dashboard starts fresh, and trend-based detectors (NAK_STORM, ACK_WAIT_VIOLATION) need 2–3 poll cycles to re-establish baselines.
