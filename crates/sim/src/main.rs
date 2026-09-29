/// nats-lens-sim — violation simulator for nats-lens paper evaluation.
///
/// Connects to NATS and deliberately triggers all five violation classes
/// so nats-lens can detect and display them in the dashboard.
///
///   cargo run -p nats-lens-sim -- --nats nats://localhost:4222
use std::time::Duration;

use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull, stream};
use clap::Parser;
use futures_util::StreamExt;
use tracing::info;

#[derive(Parser)]
#[command(
    name = "nats-lens-sim",
    about = "Trigger NATS JetStream violations for nats-lens demo"
)]
struct Args {
    #[arg(long, default_value = "nats://localhost:4222")]
    nats: String,
    /// How many rounds to run (0 = infinite)
    #[arg(long, default_value_t = 0)]
    rounds: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let args = Args::parse();
    info!("Connecting to {}", args.nats);
    let client = async_nats::connect(&args.nats).await?;
    let js = jetstream::new(client);
    info!("Connected. Open http://localhost:8888 and watch violations appear.");

    ensure_streams(&js).await?;

    let mut round = 0u32;
    loop {
        round += 1;
        if args.rounds > 0 && round > args.rounds {
            break;
        }
        info!("── Round {} ──────────────────────────────────────", round);

        tokio::join!(
            simulate_orders(&js),
            simulate_events(&js),
            simulate_payments(&js),
        );

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    Ok(())
}

// ── Ensure streams exist ──────────────────────────────────────────────────────

async fn ensure_streams(js: &jetstream::Context) -> Result<()> {
    js.get_or_create_stream(stream::Config {
        name: "ORDERS".into(),
        subjects: vec!["orders.>".into()],
        max_messages: 5_000,
        storage: stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    js.get_or_create_stream(stream::Config {
        name: "EVENTS".into(),
        subjects: vec!["events.>".into()],
        max_messages: 50, // tiny — easy to overflow
        storage: stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    js.get_or_create_stream(stream::Config {
        name: "PAYMENTS".into(),
        subjects: vec!["payments.>".into()],
        max_messages: 10_000,
        storage: stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    info!("Streams ready (ORDERS / EVENTS / PAYMENTS)");
    Ok(())
}

// ── ORDERS: AckWaitViolation + MaxPendingThrottle ────────────────────────────
// Pull 3 messages (fills max_ack_pending=3), hold 9s without acking.
// ack_wait=5s fires → NATS redelivers → redelivery count grows → violations detected.
async fn simulate_orders(js: &jetstream::Context) {
    // Publish
    for i in 0..10u32 {
        let _ = js
            .publish("orders.new", format!(r#"{{"order_id":{i}}}"#).into())
            .await;
    }
    info!("[ORDERS] published 10 messages");

    let stream = match js.get_stream("ORDERS").await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("[ORDERS] {e}");
            return;
        }
    };
    let consumer = match stream
        .get_or_create_consumer(
            "order-processor",
            pull::Config {
                durable_name: Some("order-processor".into()),
                ack_wait: Duration::from_secs(5),
                max_ack_pending: 3,
                ..Default::default()
            },
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("[ORDERS] consumer: {e}");
            return;
        }
    };

    // Pull 3 — fills all pending slots — do NOT ack
    let Ok(mut batch) = consumer.fetch().max_messages(3).messages().await else {
        return;
    };
    let mut n = 0usize;
    while let Some(Ok(_msg)) = batch.next().await {
        n += 1;
    }
    info!("[ORDERS] pulled {n} messages, holding (ack_wait=5s will trigger redeliveries)");

    tokio::time::sleep(Duration::from_secs(9)).await;
    info!("[ORDERS] done — AckWaitViolation + MaxPendingThrottle should be visible");
}

// ── EVENTS: SequenceGap ───────────────────────────────────────────────────────
// ACK one message to establish ack_floor, then flood 300 into a 50-msg stream.
// ~250 messages get evicted → stream_first_seq jumps past ack_floor → gap detected.
async fn simulate_events(js: &jetstream::Context) {
    // Seed a few, pull+ack one to establish ack_floor
    for i in 0..5u32 {
        let _ = js
            .publish("events.click", format!(r#"{{"seq":{i}}}"#).into())
            .await;
    }

    let stream = match js.get_stream("EVENTS").await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("[EVENTS] {e}");
            return;
        }
    };
    let consumer = match stream
        .get_or_create_consumer(
            "event-handler",
            pull::Config {
                durable_name: Some("event-handler".into()),
                ack_wait: Duration::from_secs(10),
                max_ack_pending: 25,
                ..Default::default()
            },
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("[EVENTS] consumer: {e}");
            return;
        }
    };

    // Pull and ACK one to set ack_floor above 0
    let Ok(mut batch) = consumer.fetch().max_messages(1).messages().await else {
        return;
    };
    if let Some(Ok(msg)) = batch.next().await {
        let _ = msg.ack().await;
        info!("[EVENTS] acked 1 message — ack_floor established");
    }

    // Flood 300 into a 50-msg stream — first ~250 evicted → SequenceGap
    for i in 0..300u32 {
        let _ = js
            .publish("events.flood", format!(r#"{{"flood":{i}}}"#).into())
            .await;
    }
    info!("[EVENTS] flooded 300 msgs into 50-msg stream — SequenceGap should appear");

    tokio::time::sleep(Duration::from_secs(3)).await;
}

// ── PAYMENTS: MissingProgress ────────────────────────────────────────────────
// Pull 18 of 20 max_ack_pending slots, hold without acking.
// pending_ratio = 18/20 = 90% with ack_wait=120s → MissingProgress fires.
async fn simulate_payments(js: &jetstream::Context) {
    for i in 0..25u32 {
        let _ = js
            .publish("payments.charge", format!(r#"{{"payment_id":{i}}}"#).into())
            .await;
    }
    info!("[PAYMENTS] published 25 messages");

    let stream = match js.get_stream("PAYMENTS").await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("[PAYMENTS] {e}");
            return;
        }
    };
    let consumer = match stream
        .get_or_create_consumer(
            "payment-processor",
            pull::Config {
                durable_name: Some("payment-processor".into()),
                ack_wait: Duration::from_secs(120),
                max_ack_pending: 20,
                ..Default::default()
            },
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("[PAYMENTS] consumer: {e}");
            return;
        }
    };

    // Pull 18 — fills 90% of pending slots — do NOT ack (simulates long tasks)
    let Ok(mut batch) = consumer.fetch().max_messages(18).messages().await else {
        return;
    };
    let mut n = 0usize;
    while let Some(Ok(_msg)) = batch.next().await {
        n += 1;
    }
    info!("[PAYMENTS] holding {n}/20 pending slots without in_progress acks — MissingProgress fires at >90%");

    tokio::time::sleep(Duration::from_secs(5)).await;
}
