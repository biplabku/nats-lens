# Getting Started with nats-lens

This guide walks you from zero to a running nats-lens instance monitoring your NATS JetStream deployment.

---

## Prerequisites

- NATS server 2.9+ with JetStream enabled
- One of: pre-built binary, Docker, or Rust toolchain

---

## Option A: Pre-built Binary (Fastest)

```bash
# macOS (Homebrew)
brew install biplabku/tap/nats-lens
nats-lens --nats nats://localhost:4222

# Linux
curl -L https://github.com/biplabku/nats-lens/releases/latest/download/nats-lens-linux-amd64 \
  -o nats-lens && chmod +x nats-lens
./nats-lens --nats nats://localhost:4222
```

---

## Option B: Docker (No installation)

```bash
docker run -p 8080:8080 ghcr.io/biplabku/nats-lens:latest \
  --nats nats://host.docker.internal:4222
```

> **Note**: Use `host.docker.internal` to reach NATS running on your host machine.
> For NATS also in Docker, use the container name or docker network.

---

## Option C: Build from Source

```bash
git clone https://github.com/biplabku/nats-lens
cd nats-lens
cargo build --release
./target/release/nats-lens --nats nats://localhost:4222
```

---

## Step 1: Enable JetStream on Your NATS Server

If JetStream isn't already enabled:

**Single server:**
```bash
nats-server --jetstream
```

**Docker:**
```bash
docker run -p 4222:4222 nats:latest --jetstream
```

**Config file:**
```
# nats-server.conf
jetstream {
  store_dir: /data/jetstream
  max_mem: 1GB
  max_file: 10GB
}
```

---

## Step 2: Start nats-lens

```bash
nats-lens --nats nats://localhost:4222 --port 8080
```

Output:
```
2026-09-26T10:00:00Z  INFO Engine started (poll_interval=5s)
2026-09-26T10:00:00Z  INFO Connected to nats://localhost:4222
2026-09-26T10:00:05Z  INFO Dashboard: http://localhost:8080
```

Open `http://localhost:8080` in your browser. You'll see all streams and consumers discovered automatically.

---

## Step 3: Connect Your Application

nats-lens requires **zero changes to your application**. It discovers streams and consumers from the NATS server's management API. Your existing producers and consumers continue running unchanged.

---

## Step 4: Subscribe to Health Events (Optional)

To receive violation alerts in your application or alerting system:

```bash
# NATS CLI — see violations as they happen
nats sub "nats.lens.health.violations.>"

# Filter by specific stream
nats sub "nats.lens.health.violations.ORDERS.>"
```

Each event is JSON — see [language-guides.md](language-guides.md) for Go, Python, Java, Node.js examples.

---

## Step 5: Set Up Grafana (Optional)

If you have a Prometheus + Grafana stack:

1. Add nats-lens as a Prometheus scrape target:
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'nats-lens'
    static_configs:
      - targets: ['localhost:8080']
```

2. Import the dashboard:
   - Open Grafana → Dashboards → Import
   - Upload `docs/grafana-dashboard.json`
   - Select your Prometheus data source

---

## Step 6: Deploy in Kubernetes

Add nats-lens as a deployment in your cluster:

```yaml
# nats-lens-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nats-lens
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nats-lens
  template:
    metadata:
      labels:
        app: nats-lens
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: nats-lens
        image: ghcr.io/biplabku/nats-lens:latest
        args:
          - "--nats"
          - "nats://nats.default.svc.cluster.local:4222"
          - "--port"
          - "8080"
        ports:
        - containerPort: 8080
        env:
        - name: RUST_LOG
          value: info
---
apiVersion: v1
kind: Service
metadata:
  name: nats-lens
  namespace: monitoring
spec:
  selector:
    app: nats-lens
  ports:
  - port: 8080
    targetPort: 8080
```

```bash
kubectl apply -f nats-lens-deployment.yaml
kubectl port-forward svc/nats-lens 8080:8080 -n monitoring
```

---

## Understanding the Dashboard

**Left panel — Stream list**
Each stream shows a colored health indicator:
- 🟢 **Healthy** — all consumers operating within normal parameters
- 🟡 **Warning** — degraded throughput, no data loss
- 🔴 **Critical** — active data loss or duplicate execution

**Main panel — Consumer detail** (click any stream)
- Consumer table: lag, redeliveries/min, ack pending ratio, health
- Lag trend sparklines: last 30 poll cycles
- Violation cards: what's wrong + exact fix command

**Bottom bar — Live violation feed**
Real-time feed of violations as they are detected, with timestamps.

**Top-right — Toast notifications**
Pop-up alerts for new critical violations.

---

## Configuration Reference

| Flag | Default | Description |
|---|---|---|
| `--nats` | `nats://localhost:4222` | NATS server URL |
| `--port` | `8080` | Web UI and API port |
| `--interval` | `5` | Poll interval in seconds |

**Authentication:**
```bash
# Token auth
nats-lens --nats nats://mytoken@localhost:4222

# Username/password
nats-lens --nats nats://user:password@localhost:4222

# NKey credentials file
nats-lens --nats nats://localhost:4222 --creds /path/to/user.creds
```

---

## API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Web dashboard |
| `/health` | GET | Liveness probe `{"status":"ok"}` |
| `/api/streams` | GET | All streams + consumers + violations as JSON |
| `/api/history/:stream/:consumer` | GET | Last 30 snapshots for a consumer |
| `/api/violations/stream` | GET | SSE stream of violations (real-time) |
| `/metrics` | GET | Prometheus text format |

---

## Troubleshooting

**"No streams found"**
→ JetStream is not enabled on your NATS server. Add `--jetstream` flag or enable in config.

**Dashboard shows "Disconnected"**
→ nats-lens lost connection to NATS. It reconnects automatically. Check NATS server logs.

**Violations not appearing**
→ Violations require at least 2 poll cycles of history for trend-based detection (NAK_STORM). Wait 10–15 seconds after starting.

**"healthy" even though I can see issues**
→ Some detectors (ACK_WAIT_VIOLATION, NAK_STORM) require active consumers pulling messages. If no consumer is actively processing, there are no redeliveries to detect.
