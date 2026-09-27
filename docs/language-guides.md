# Receiving Health Events: Language Guides

nats-lens publishes violation events to `nats.lens.health.violations.{stream}.{consumer}` as structured JSON. Any NATS client in any language can subscribe.

No code changes to your existing producers or consumers are required.

---

## Event Format

Every violation event is a JSON object with this structure:

```json
{
  "stream_name": "ORDERS",
  "consumer_name": "order-processor",
  "severity": "Critical",
  "description": "28 redeliveries/min — ack_wait (30s) shorter than processing time. Recommended: 117s",
  "detected_at": "2026-09-26T15:32:00Z",
  "violation": {
    "type": "ACK_WAIT_VIOLATION",
    "redeliveries_per_min": 28.4,
    "current_ack_wait_secs": 30,
    "recommended_ack_wait_secs": 117,
    "fix_command": "nats consumer edit ORDERS order-processor --ack-wait 1m57s"
  }
}
```

**Subject hierarchy:**
- `nats.lens.health.violations.*.*` — all violations
- `nats.lens.health.violations.ORDERS.*` — all violations for the ORDERS stream
- `nats.lens.health.violations.ORDERS.order-processor` — specific consumer

---

## Go

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "github.com/nats-io/nats.go"
)

type ViolationEvent struct {
    StreamName   string `json:"stream_name"`
    ConsumerName string `json:"consumer_name"`
    Severity     string `json:"severity"`
    Description  string `json:"description"`
    DetectedAt   string `json:"detected_at"`
    Violation    struct {
        Type                    string  `json:"type"`
        RedeliveriesPerMin      float64 `json:"redeliveries_per_min,omitempty"`
        CurrentAckWaitSecs      uint64  `json:"current_ack_wait_secs,omitempty"`
        RecommendedAckWaitSecs  uint64  `json:"recommended_ack_wait_secs,omitempty"`
        FixCommand              string  `json:"fix_command,omitempty"`
    } `json:"violation"`
}

func main() {
    nc, err := nats.Connect("nats://localhost:4222")
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    // Subscribe to all violations
    nc.Subscribe("nats.lens.health.violations.>", func(msg *nats.Msg) {
        var event ViolationEvent
        if err := json.Unmarshal(msg.Data, &event); err != nil {
            log.Println("parse error:", err)
            return
        }

        fmt.Printf("[%s] %s/%s: %s\n",
            event.Severity,
            event.StreamName,
            event.ConsumerName,
            event.Description,
        )

        // Route critical violations to PagerDuty, Slack, etc.
        if event.Severity == "Critical" {
            alertOncall(event)
        }
    })

    // Block forever
    select {}
}

func alertOncall(e ViolationEvent) {
    // Send to Slack, PagerDuty, OpsGenie, etc.
    fmt.Printf("ALERT: %s on %s/%s\n", e.Violation.Type, e.StreamName, e.ConsumerName)
}
```

---

## Python

```python
import asyncio
import json
import nats

async def handle_violation(msg):
    event = json.loads(msg.data.decode())
    print(f"[{event['severity']}] {event['stream_name']}/{event['consumer_name']}")
    print(f"  {event['description']}")

    violation = event.get('violation', {})
    if violation.get('fix_command'):
        print(f"  Fix: {violation['fix_command']}")

    # Critical violations → send alert
    if event['severity'] == 'Critical':
        await send_slack_alert(event)

async def send_slack_alert(event):
    # Use your preferred HTTP client
    print(f"Would send Slack alert for {event['violation']['type']}")

async def main():
    nc = await nats.connect("nats://localhost:4222")

    # Subscribe to all violations
    await nc.subscribe("nats.lens.health.violations.>", cb=handle_violation)

    print("Subscribed to nats-lens violations. Waiting...")
    await asyncio.sleep(3600)  # run for 1 hour

asyncio.run(main())
```

**Install**: `pip install nats-py`

---

## Java

```java
import io.nats.client.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;

public class NatsLensSubscriber {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .build();

        try (Connection nc = Nats.connect(options)) {
            Dispatcher d = nc.createDispatcher(msg -> {
                try {
                    var event = mapper.readTree(msg.getData());
                    String severity = event.get("severity").asText();
                    String stream   = event.get("stream_name").asText();
                    String consumer = event.get("consumer_name").asText();
                    String desc     = event.get("description").asText();

                    System.out.printf("[%s] %s/%s: %s%n", severity, stream, consumer, desc);

                    if ("Critical".equals(severity)) {
                        sendAlert(event);
                    }
                } catch (Exception e) {
                    System.err.println("Parse error: " + e.getMessage());
                }
            });

            // Subscribe to all violations
            d.subscribe("nats.lens.health.violations.>");

            System.out.println("Subscribed. Press Enter to exit.");
            System.in.read();
        }
    }

    private static void sendAlert(com.fasterxml.jackson.databind.JsonNode event) {
        // Implement your alerting logic here
        System.out.println("ALERT: " + event.get("violation").get("type").asText());
    }
}
```

**Dependencies** (Maven):
```xml
<dependency>
    <groupId>io.nats</groupId>
    <artifactId>jnats</artifactId>
    <version>2.17.0</version>
</dependency>
```

---

## Node.js / TypeScript

```typescript
import { connect, StringCodec } from 'nats';

interface ViolationEvent {
  stream_name: string;
  consumer_name: string;
  severity: 'Critical' | 'Warning';
  description: string;
  detected_at: string;
  violation: {
    type: string;
    fix_command?: string;
    redeliveries_per_min?: number;
    current_ack_wait_secs?: number;
    recommended_ack_wait_secs?: number;
  };
}

async function main() {
  const nc = await connect({ servers: 'nats://localhost:4222' });
  const sc = StringCodec();

  const sub = nc.subscribe('nats.lens.health.violations.>');

  console.log('Subscribed to nats-lens violations');

  for await (const msg of sub) {
    const event: ViolationEvent = JSON.parse(sc.decode(msg.data));

    console.log(`[${event.severity}] ${event.stream_name}/${event.consumer_name}`);
    console.log(`  ${event.description}`);

    if (event.violation.fix_command) {
      console.log(`  Fix: ${event.violation.fix_command}`);
    }

    if (event.severity === 'Critical') {
      await sendAlert(event);
    }
  }
}

async function sendAlert(event: ViolationEvent) {
  // POST to Slack webhook, PagerDuty, etc.
  console.log(`Would alert: ${event.violation.type}`);
}

main().catch(console.error);
```

**Install**: `npm install nats`

---

## Rust

```rust
use async_nats::jetstream;
use futures_util::StreamExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ViolationEvent {
    stream_name:   String,
    consumer_name: String,
    severity:      String,
    description:   String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    let mut subscriber = client
        .subscribe("nats.lens.health.violations.>")
        .await?;

    println!("Subscribed to nats-lens violations");

    while let Some(msg) = subscriber.next().await {
        if let Ok(event) = serde_json::from_slice::<ViolationEvent>(&msg.payload) {
            println!(
                "[{}] {}/{}: {}",
                event.severity, event.stream_name,
                event.consumer_name, event.description
            );

            if event.severity == "Critical" {
                send_alert(&event).await;
            }
        }
    }

    Ok(())
}

async fn send_alert(event: &ViolationEvent) {
    println!("ALERT: {}/{}", event.stream_name, event.consumer_name);
}
```

---

## REST API Polling (Any Language / curl)

If you prefer polling over push:

```bash
# All streams + violations
curl -s http://localhost:8080/api/streams | jq '.[] | {name: .stream_name, health: .health}'

# Consumer history for lag trend
curl -s http://localhost:8080/api/history/ORDERS/order-processor | \
  jq '.[-5:] | .[].lag'

# Filter critical violations
curl -s http://localhost:8080/api/streams | \
  jq '[.[] | .consumers[] | .violations[] | select(.severity == "Critical")]'
```

---

## Webhook Bridge

If your alerting system uses webhooks (PagerDuty, Opsgenie, VictorOps), bridge nats-lens events with a small script:

```python
# webhook-bridge.py — forward nats-lens violations to a webhook
import asyncio, json, nats
import httpx

WEBHOOK_URL = "https://events.pagerduty.com/v2/enqueue"
ROUTING_KEY = "your-routing-key"

async def main():
    nc = await nats.connect("nats://localhost:4222")

    async def forward(msg):
        event = json.loads(msg.data.decode())
        if event["severity"] != "Critical":
            return

        payload = {
            "routing_key": ROUTING_KEY,
            "event_action": "trigger",
            "payload": {
                "summary": f"nats-lens: {event['violation']['type']} on {event['stream_name']}/{event['consumer_name']}",
                "severity": "critical",
                "source": "nats-lens",
                "custom_details": event,
            }
        }
        async with httpx.AsyncClient() as client:
            await client.post(WEBHOOK_URL, json=payload)

    await nc.subscribe("nats.lens.health.violations.>", cb=forward)
    await asyncio.sleep(86400)  # run for 24h

asyncio.run(main())
```
