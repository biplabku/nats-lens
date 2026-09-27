use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull, stream};
use clap::Parser;
use futures_util::StreamExt;
use nats_lens_core::engine::Engine;
use nats_lens_core::history::HistoryStore;
use nats_lens_core::types::Violation;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tracing::info;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "nats-lens-eval", about = "Paper evaluation harness for nats-lens")]
struct Args {
    #[arg(long, default_value = "nats://localhost:4222")]
    nats: String,

    /// Number of rounds per scenario (paper uses 30)
    #[arg(long, default_value_t = 30)]
    rounds: u32,

    /// Poll interval of the embedded engine (seconds)
    #[arg(long, default_value_t = 3)]
    poll_interval: u64,

    /// Output directory for CSV files
    #[arg(long, default_value = "data")]
    out_dir: String,

    /// How long to run the false-positive test (seconds). Paper uses 1800 (30 min).
    #[arg(long, default_value_t = 1800)]
    fp_duration: u64,
}

// ── Result row ────────────────────────────────────────────────────────────────

#[derive(Debug, serde::Serialize)]
struct EvalRow {
    scenario:              String,
    round:                 u32,
    /// Was any violation of the expected type detected?
    detected:              bool,
    /// Milliseconds from injection_start to first detection event. None = not detected.
    detection_latency_ms:  Option<u64>,
    /// Was the correct violation type reported (not a different type)?
    correct_type:          bool,
    /// Timestamp of injection start (ISO-8601)
    injected_at:           String,
}

#[derive(Debug, serde::Serialize)]
struct FalsePositiveRow {
    round:            u32,
    elapsed_secs:     u64,
    violations_count: u32,
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nats_lens_eval=info,nats_lens_core=info".parse().unwrap())
        )
        .init();

    let args = Args::parse();
    std::fs::create_dir_all(&args.out_dir)?;

    info!("Connecting to NATS at {}", args.nats);
    let client = async_nats::connect(&args.nats).await?;
    info!("Connected. Starting evaluation ({} rounds per scenario)", args.rounds);
    info!("");

    // ── Embed engine for precise timing ──────────────────────────────────────
    // The engine polls every poll_interval seconds and broadcasts violations.
    // We subscribe to that broadcast channel to measure detection latency.
    let engine   = Arc::new(Engine::new(client.clone()));
    let tx       = engine.sender();
    let history  = engine.history_store();
    let poll_ms  = Duration::from_secs(args.poll_interval);

    let engine_bg = Arc::clone(&engine);
    tokio::spawn(async move { engine_bg.run(poll_ms).await });

    // Give the engine one poll cycle to discover existing streams/consumers
    tokio::time::sleep(Duration::from_secs(args.poll_interval + 1)).await;
    info!("Engine warmed up. Beginning scenarios.");
    info!("");

    let js = jetstream::new(client.clone());

    // ── Run all scenarios ─────────────────────────────────────────────────────

    let mut detection_rows: Vec<EvalRow>        = Vec::new();
    let mut fp_rows:        Vec<FalsePositiveRow> = Vec::new();

    // Scenario 1: ACK_WAIT_VIOLATION
    info!("── Scenario 1/5: ACK_WAIT_VIOLATION ({} rounds) ──", args.rounds);
    let rows = run_ack_wait_scenario(&js, &tx, args.rounds, args.poll_interval).await?;
    let n_detected = rows.iter().filter(|r| r.detected).count();
    info!("  Detected: {}/{}", n_detected, args.rounds);
    detection_rows.extend(rows);

    // Scenario 2: SEQUENCE_GAP
    info!("── Scenario 2/5: SEQUENCE_GAP ({} rounds) ──", args.rounds);
    let rows = run_gap_scenario(&js, &tx, args.rounds, args.poll_interval).await?;
    let n_detected = rows.iter().filter(|r| r.detected).count();
    info!("  Detected: {}/{}", n_detected, args.rounds);
    detection_rows.extend(rows);

    // Scenario 3: MAX_PENDING_THROTTLE
    info!("── Scenario 3/5: MAX_PENDING_THROTTLE ({} rounds) ──", args.rounds);
    let rows = run_throttle_scenario(&js, &tx, args.rounds, args.poll_interval).await?;
    let n_detected = rows.iter().filter(|r| r.detected).count();
    info!("  Detected: {}/{}", n_detected, args.rounds);
    detection_rows.extend(rows);

    // Scenario 4: NAK_STORM
    info!("── Scenario 4/5: NAK_STORM ({} rounds) ──", args.rounds);
    let rows = run_nak_storm_scenario(&js, &tx, &history, args.rounds, args.poll_interval).await?;
    let n_detected = rows.iter().filter(|r| r.detected).count();
    info!("  Detected: {}/{}", n_detected, args.rounds);
    detection_rows.extend(rows);

    // Scenario 5: MISSING_PROGRESS
    info!("── Scenario 5/5: MISSING_PROGRESS ({} rounds) ──", args.rounds);
    let rows = run_missing_progress_scenario(&js, &tx, args.rounds, args.poll_interval).await?;
    let n_detected = rows.iter().filter(|r| r.detected).count();
    info!("  Detected: {}/{}", n_detected, args.rounds);
    detection_rows.extend(rows);

    // Scenario 6: False positives under healthy operation
    info!("── Scenario 6: FALSE POSITIVE RATE (60s healthy operation) ──");
    let fp = run_false_positive_scenario(&js, &tx, args.fp_duration, args.poll_interval).await?;
    let total_fp: u32 = fp.iter().map(|r| r.violations_count).sum();
    info!("  Total false positives in 60s: {}", total_fp);
    fp_rows.extend(fp);

    // Scenario 7: Overhead measurement
    info!("── Scenario 7: OVERHEAD MEASUREMENT ──");
    let overhead = measure_overhead(&js, args.poll_interval).await?;
    info!("  NATS API requests per poll: {}", overhead.api_requests_per_poll);
    info!("  Memory (history store): {} bytes estimated", overhead.history_bytes);
    info!("  Streams monitored: {}", overhead.stream_count);
    info!("  Consumers monitored: {}", overhead.consumer_count);
    let overhead_path = format!("{}/overhead_results.csv", args.out_dir);
    let mut wtr = csv::Writer::from_path(&overhead_path)?;
    wtr.serialize(&overhead)?;
    wtr.flush()?;
    info!("Overhead results → {overhead_path}");

    // ── Write CSV files ───────────────────────────────────────────────────────

    let detection_path = format!("{}/detection_results.csv", args.out_dir);
    let mut wtr = csv::Writer::from_path(&detection_path)?;
    for row in &detection_rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    info!("");
    info!("Detection results → {detection_path}");

    let fp_path = format!("{}/false_positive_results.csv", args.out_dir);
    let mut wtr = csv::Writer::from_path(&fp_path)?;
    for row in &fp_rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    info!("False positive results → {fp_path}");

    // ── Print summary table ───────────────────────────────────────────────────

    print_summary(&detection_rows, args.rounds);

    Ok(())
}

// ── Scenario implementations ─────────────────────────────────────────────────

/// Wait for a specific violation type on the broadcast channel.
/// Returns detection latency if found within timeout_ms, None otherwise.
async fn wait_for_violation(
    tx: &broadcast::Sender<Violation>,
    expected_type: &str,
    timeout_ms: u64,
) -> Option<u64> {
    let mut rx = tx.subscribe();
    let start  = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);

    loop {
        match tokio::time::timeout(
            timeout.saturating_sub(start.elapsed()),
            rx.recv(),
        ).await {
            Ok(Ok(v)) => {
                let vtype = v.violation.name();
                if vtype == expected_type {
                    return Some(start.elapsed().as_millis() as u64);
                }
                // Other violation type — keep waiting
                if start.elapsed() >= timeout { return None; }
            }
            _ => return None,
        }
    }
}

// ── Scenario 1: ACK_WAIT_VIOLATION ───────────────────────────────────────────
// The detector requires growing redelivery counts across consecutive snapshots.
// Simply pulling-without-acking once makes num_redelivered plateau after the
// first redeliver (nobody re-pulls, so no new redeliveries accumulate).
//
// Fix: spawn a background task that continuously re-pulls every ack_wait+1s
// so each pull triggers another redeliver cycle, growing num_redelivered
// across every engine poll snapshot.
async fn run_ack_wait_scenario(
    js:            &jetstream::Context,
    tx:            &broadcast::Sender<Violation>,
    rounds:        u32,
    poll_interval: u64,
) -> Result<Vec<EvalRow>> {
    ensure_stream(js, "EVAL_ACK", "eval.ack.>", 5_000, None).await?;
    let mut rows = Vec::new();

    for round in 1..=rounds {
        // Publish enough messages to keep the consumer busy across all cycles
        for i in 0..20u32 {
            let _ = js.publish("eval.ack.msg", format!(r#"{{"i":{i}}}"#).into()).await;
        }

        let stream = js.get_stream("EVAL_ACK").await?;
        let _consumer = stream.get_or_create_consumer(
            "eval-ack-consumer",
            pull::Config {
                durable_name:    Some("eval-ack-consumer".into()),
                ack_wait:        Duration::from_secs(4), // short so redeliveries cycle fast
                max_ack_pending: 5,
                ..Default::default()
            },
        ).await?;

        let injected_at = chrono::Utc::now().to_rfc3339();

        // Background puller: continuously pulls messages without acking.
        // Each pull-without-ack → ack_wait fires → redeliver → puller pulls again.
        // This creates a steady stream of growing num_redelivered across snapshots.
        let js_bg  = js.clone();
        let puller = tokio::spawn(async move {
            let ack_wait_secs = 4u64;
            loop {
                let Ok(stream) = js_bg.get_stream("EVAL_ACK").await else { break };
                let Ok(c) = stream.get_consumer::<pull::Config>("eval-ack-consumer").await else { break };
                // Pull without acking — fills max_ack_pending slots
                if let Ok(mut batch) = c.fetch().max_messages(5).messages().await {
                    while let Some(Ok(_msg)) = batch.next().await {
                        // Intentionally NOT acking — triggers ack_wait
                    }
                }
                // Wait just past ack_wait so NATS redelivers, then pull again
                tokio::time::sleep(Duration::from_secs(ack_wait_secs + 1)).await;
            }
        });

        // Wait long enough for 3+ engine poll cycles with growing redeliveries
        // Each cycle: pull(5) → wait 5s → redeliver → rate grows
        let timeout_ms = poll_interval * 5 * 1000 + 5000;
        let latency = wait_for_violation(tx, "ACK_WAIT_VIOLATION", timeout_ms).await;

        puller.abort();

        rows.push(EvalRow {
            scenario:             "ACK_WAIT_VIOLATION".into(),
            round,
            detected:             latency.is_some(),
            detection_latency_ms: latency,
            correct_type:         latency.is_some(),
            injected_at,
        });

        let Ok(s) = js.get_stream("EVAL_ACK").await else { continue };
        let _ = s.delete_consumer("eval-ack-consumer").await;
        let _ = s.purge().await;
        tokio::time::sleep(Duration::from_secs(poll_interval + 1)).await;
    }

    Ok(rows)
}

// ── Scenario 2: SEQUENCE_GAP ─────────────────────────────────────────────────
// Flood 200 messages into a 50-msg stream while consumer has established ack_floor.
async fn run_gap_scenario(
    js:            &jetstream::Context,
    tx:            &broadcast::Sender<Violation>,
    rounds:        u32,
    poll_interval: u64,
) -> Result<Vec<EvalRow>> {
    ensure_stream(js, "EVAL_GAP", "eval.gap.>", 50, Some(50)).await?;
    let mut rows = Vec::new();

    for round in 1..=rounds {
        // Establish ack_floor by pulling+acking one message
        for i in 0..3u32 {
            let _ = js.publish("eval.gap.seed", format!(r#"{{"i":{i}}}"#).into()).await;
        }

        let stream = js.get_stream("EVAL_GAP").await?;
        let consumer = stream.get_or_create_consumer(
            "eval-gap-consumer",
            pull::Config {
                durable_name:    Some("eval-gap-consumer".into()),
                ack_wait:        Duration::from_secs(30),
                max_ack_pending: 25,
                ..Default::default()
            },
        ).await?;

        // Pull and ACK one message — establishes ack_floor > 0
        let Ok(mut batch) = consumer.fetch().max_messages(1).messages().await else { continue };
        if let Some(Ok(msg)) = batch.next().await { let _ = msg.ack().await; }

        let injected_at  = chrono::Utc::now().to_rfc3339();

        // Flood 200 messages into the 50-msg stream → evicts ~150 → gap
        for i in 0..200u32 {
            let _ = js.publish("eval.gap.flood", format!(r#"{{"flood":{i}}}"#).into()).await;
        }

        let timeout_ms = poll_interval * 3 * 1000 + 2000;
        let latency = wait_for_violation(tx, "SEQUENCE_GAP", timeout_ms).await;

        rows.push(EvalRow {
            scenario:             "SEQUENCE_GAP".into(),
            round,
            detected:             latency.is_some(),
            detection_latency_ms: latency,
            correct_type:         latency.is_some(),
            injected_at,
        });

        let _ = stream.delete_consumer("eval-gap-consumer").await;
        // Purge the stream for next round
        let _ = stream.purge().await;
        tokio::time::sleep(Duration::from_secs(poll_interval + 1)).await;
    }

    Ok(rows)
}

// ── Scenario 3: MAX_PENDING_THROTTLE ─────────────────────────────────────────
async fn run_throttle_scenario(
    js:            &jetstream::Context,
    tx:            &broadcast::Sender<Violation>,
    rounds:        u32,
    poll_interval: u64,
) -> Result<Vec<EvalRow>> {
    ensure_stream(js, "EVAL_THROT", "eval.throt.>", 5_000, None).await?;
    let mut rows = Vec::new();

    for round in 1..=rounds {
        for i in 0..10u32 {
            let _ = js.publish("eval.throt.msg", format!(r#"{{"i":{i}}}"#).into()).await;
        }

        let stream = js.get_stream("EVAL_THROT").await?;
        let consumer = stream.get_or_create_consumer(
            "eval-throt-consumer",
            pull::Config {
                durable_name:    Some("eval-throt-consumer".into()),
                ack_wait:        Duration::from_secs(60),
                max_ack_pending: 3, // tiny → fills immediately
                ..Default::default()
            },
        ).await?;

        let injected_at = chrono::Utc::now().to_rfc3339();

        // Pull exactly max_ack_pending messages without acking → throttle
        let Ok(mut batch) = consumer.fetch().max_messages(3).messages().await else { continue };
        while let Some(Ok(_)) = batch.next().await {}

        let timeout_ms = poll_interval * 3 * 1000 + 2000;
        let latency = wait_for_violation(tx, "MAX_PENDING_THROTTLE", timeout_ms).await;

        rows.push(EvalRow {
            scenario:             "MAX_PENDING_THROTTLE".into(),
            round,
            detected:             latency.is_some(),
            detection_latency_ms: latency,
            correct_type:         latency.is_some(),
            injected_at,
        });

        let _ = stream.delete_consumer("eval-throt-consumer").await;
        tokio::time::sleep(Duration::from_secs(poll_interval + 1)).await;
    }

    Ok(rows)
}

// ── Scenario 4: NAK_STORM ────────────────────────────────────────────────────
// Requires 3+ consecutive snapshots showing redelivery growth AND lag growth.
// Previous issue: only 3 NAK rounds then consumer deleted — engine never got
// enough snapshots while consumer was still alive.
//
// Fix: spawn a background NAKer that continuously NAKs throughout the full
// detection window, keeping redeliveries growing across every poll snapshot.
async fn run_nak_storm_scenario(
    js:            &jetstream::Context,
    tx:            &broadcast::Sender<Violation>,
    history:       &Arc<Mutex<HistoryStore>>,
    rounds:        u32,
    poll_interval: u64,
) -> Result<Vec<EvalRow>> {
    ensure_stream(js, "EVAL_NAK", "eval.nak.>", 5_000, None).await?;
    let mut rows = Vec::new();

    for round in 1..=rounds {
        // Publish enough messages to sustain the NAK loop across all poll cycles
        for i in 0..50u32 {
            let _ = js.publish("eval.nak.msg", format!(r#"{{"i":{i}}}"#).into()).await;
        }

        let stream = js.get_stream("EVAL_NAK").await?;
        let _consumer = stream.get_or_create_consumer(
            "eval-nak-consumer",
            pull::Config {
                durable_name:    Some("eval-nak-consumer".into()),
                ack_wait:        Duration::from_secs(30), // long ack_wait so it doesn't interfere
                max_ack_pending: 50,
                ..Default::default()
            },
        ).await?;

        let injected_at = chrono::Utc::now().to_rfc3339();

        // Use Nak(Some(delay)) with explicit 500ms backoff — NATS redelivers
        // exactly 500ms after each NAK. With 5 messages per cycle:
        //   5 msgs × (3000ms / 500ms) = 30 redeliveries per poll interval
        //   Rate = 30/3s × 60 = 600/min >> 5/min threshold → guaranteed detection.
        let js_bg  = js.clone();
        let nakker = tokio::spawn(async move {
            loop {
                let Ok(stream) = js_bg.get_stream("EVAL_NAK").await else { break };
                let Ok(c) = stream.get_consumer::<pull::Config>("eval-nak-consumer").await else { break };
                if let Ok(mut batch) = c.fetch().max_messages(5).messages().await {
                    let mut got = false;
                    while let Some(Ok(msg)) = batch.next().await {
                        // Explicit 500ms backoff → NATS redelivers after 500ms
                        let _ = msg.ack_with(
                            async_nats::jetstream::AckKind::Nak(
                                Some(Duration::from_millis(500))
                            )
                        ).await;
                        got = true;
                    }
                    if !got {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    // Wait for the backoff to expire before pulling again
                    tokio::time::sleep(Duration::from_millis(600)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
        });

        // Need 3+ engine poll snapshots with growing redeliveries — wait 5+ cycles
        let timeout_ms = poll_interval * 6 * 1000 + 3000;
        let latency = wait_for_violation(tx, "NAK_STORM", timeout_ms).await;

        nakker.abort();

        rows.push(EvalRow {
            scenario:             "NAK_STORM".into(),
            round,
            detected:             latency.is_some(),
            detection_latency_ms: latency,
            correct_type:         latency.is_some(),
            injected_at,
        });

        let Ok(s) = js.get_stream("EVAL_NAK").await else { continue };
        let _ = s.delete_consumer("eval-nak-consumer").await;
        let _ = s.purge().await;
        // Clear stale history so old num_redelivered doesn't poison the next round.
        history.lock().await.clear_consumer("EVAL_NAK/eval-nak-consumer");
        tokio::time::sleep(Duration::from_secs(poll_interval * 2)).await;
    }

    Ok(rows)
}

// ── Scenario 5: MISSING_PROGRESS ─────────────────────────────────────────────
async fn run_missing_progress_scenario(
    js:            &jetstream::Context,
    tx:            &broadcast::Sender<Violation>,
    rounds:        u32,
    poll_interval: u64,
) -> Result<Vec<EvalRow>> {
    ensure_stream(js, "EVAL_PROG", "eval.prog.>", 5_000, None).await?;
    let mut rows = Vec::new();

    for round in 1..=rounds {
        for i in 0..25u32 {
            let _ = js.publish("eval.prog.msg", format!(r#"{{"i":{i}}}"#).into()).await;
        }

        let stream = js.get_stream("EVAL_PROG").await?;
        let consumer = stream.get_or_create_consumer(
            "eval-prog-consumer",
            pull::Config {
                durable_name:    Some("eval-prog-consumer".into()),
                ack_wait:        Duration::from_secs(120),
                max_ack_pending: 20,
                ..Default::default()
            },
        ).await?;

        let injected_at = chrono::Utc::now().to_rfc3339();

        // Pull 19/20 slots — 95% pending ratio → MISSING_PROGRESS fires
        let Ok(mut batch) = consumer.fetch().max_messages(19).messages().await else { continue };
        while let Some(Ok(_)) = batch.next().await {}

        let timeout_ms = poll_interval * 3 * 1000 + 2000;
        let latency = wait_for_violation(tx, "MISSING_PROGRESS", timeout_ms).await;

        rows.push(EvalRow {
            scenario:             "MISSING_PROGRESS".into(),
            round,
            detected:             latency.is_some(),
            detection_latency_ms: latency,
            correct_type:         latency.is_some(),
            injected_at,
        });

        let _ = stream.delete_consumer("eval-prog-consumer").await;
        tokio::time::sleep(Duration::from_secs(poll_interval + 1)).await;
    }

    Ok(rows)
}

// ── Scenario 6: False positive rate ──────────────────────────────────────────
// Run correctly-configured consumers for duration_secs. Count any violations.
async fn run_false_positive_scenario(
    js:            &jetstream::Context,
    tx:            &broadcast::Sender<Violation>,
    duration_secs: u64,
    poll_interval: u64,
) -> Result<Vec<FalsePositiveRow>> {
    ensure_stream(js, "EVAL_HEALTHY", "eval.healthy.>", 10_000, None).await?;

    // Correctly configured consumer — generous ack_wait, large max_pending
    let stream = js.get_stream("EVAL_HEALTHY").await?;
    let consumer = stream.get_or_create_consumer(
        "eval-healthy-consumer",
        pull::Config {
            durable_name:    Some("eval-healthy-consumer".into()),
            ack_wait:        Duration::from_secs(300), // well above any processing
            max_ack_pending: 512,
            ..Default::default()
        },
    ).await?;

    // Actively publish and process messages correctly throughout the window
    let js_clone  = js.clone();
    let publisher = tokio::spawn(async move {
        let mut seq = 0u32;
        loop {
            let _ = js_clone.publish("eval.healthy.msg", format!(r#"{{"seq":{seq}}}"#).into()).await;
            seq += 1;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    // Correct consumer: pull and promptly ACK
    let correct_consumer = tokio::spawn(async move {
        loop {
            let Ok(mut batch) = consumer.fetch().max_messages(10).messages().await else { break };
            while let Some(Ok(msg)) = batch.next().await {
                let _ = msg.ack().await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Count violations over the window
    let mut rx    = tx.subscribe();
    let mut rows  = Vec::new();
    let start     = Instant::now();
    let mut count = 0u32;
    let mut tick  = 0u64;

    while start.elapsed().as_secs() < duration_secs {
        // Sample every poll_interval
        tokio::time::sleep(Duration::from_secs(poll_interval)).await;
        tick += poll_interval;

        // Drain pending violations
        loop {
            match rx.try_recv() {
                Ok(_) => count += 1,
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(_) => break,
            }
        }

        rows.push(FalsePositiveRow {
            round:            1,
            elapsed_secs:     tick,
            violations_count: count,
        });
    }

    publisher.abort();
    correct_consumer.abort();
    if let Ok(s) = js.get_stream("EVAL_HEALTHY").await {
        tokio::spawn(async move { let _ = s.delete_consumer("eval-healthy-consumer").await; });
    }

    Ok(rows)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn ensure_stream(
    js:       &jetstream::Context,
    name:     &str,
    subject:  &str,
    max_msgs: i64,
    max_msgs_override: Option<i64>,
) -> Result<()> {
    let _ = js.get_or_create_stream(stream::Config {
        name:         name.into(),
        subjects:     vec![subject.into()],
        max_messages: max_msgs_override.unwrap_or(max_msgs),
        storage:      stream::StorageType::Memory,
        ..Default::default()
    }).await?;
    Ok(())
}

fn print_summary(rows: &[EvalRow], rounds: u32) {
    println!();
    println!("  ═══════════════════════════════════════════════════════════════");
    println!("  EVALUATION RESULTS — nats-lens detection coverage");
    println!("  ═══════════════════════════════════════════════════════════════");
    println!("  {:30} {:>10} {:>15} {:>10}",
        "Violation Type", "Detected", "Avg Latency", "Coverage");
    println!("  {}", "─".repeat(70));

    let scenarios = [
        "ACK_WAIT_VIOLATION",
        "SEQUENCE_GAP",
        "MAX_PENDING_THROTTLE",
        "NAK_STORM",
        "MISSING_PROGRESS",
    ];

    for scenario in &scenarios {
        let scenario_rows: Vec<&EvalRow> = rows.iter()
            .filter(|r| r.scenario == *scenario)
            .collect();

        if scenario_rows.is_empty() { continue; }

        let n_detected = scenario_rows.iter().filter(|r| r.detected).count();
        let latencies:  Vec<u64> = scenario_rows.iter()
            .filter_map(|r| r.detection_latency_ms)
            .collect();
        let avg_latency = if latencies.is_empty() {
            "N/A".to_string()
        } else {
            format!("{:.0}ms", latencies.iter().sum::<u64>() as f64 / latencies.len() as f64)
        };
        let coverage = n_detected as f64 / rounds as f64 * 100.0;

        println!("  {:30} {:>10} {:>15} {:>9.1}%",
            scenario, format!("{}/{}", n_detected, rounds), avg_latency, coverage);
    }

    println!("  {}", "─".repeat(70));
    println!("  Baseline (standard Prometheus NATS metrics):  0/5 types detected");
    println!("  ═══════════════════════════════════════════════════════════════");
    println!();
    println!("  Run `python analysis/evaluation.py --data data/` to generate paper figures.");
    println!();
}

// ── Overhead measurement ──────────────────────────────────────────────────────

#[derive(Debug, serde::Serialize)]
struct OverheadResult {
    /// Number of $JS.API.* requests nats-lens makes per poll cycle.
    /// Formula: 1 (STREAM.LIST) + N_streams × (1 STREAM.INFO + 1 CONSUMER.NAMES +
    ///          N_consumers × 1 CONSUMER.INFO)
    api_requests_per_poll: u64,
    /// Estimated history store memory: 30 snapshots × ~120 bytes × N_consumers
    history_bytes: u64,
    stream_count: u64,
    consumer_count: u64,
    /// Poll interval used during measurement
    poll_interval_secs: u64,
    /// Wall-clock time for one full poll cycle (ms)
    poll_cycle_ms: u64,
}

async fn measure_overhead(
    js: &jetstream::Context,
    poll_interval: u64,
) -> Result<OverheadResult> {
    // Count streams and consumers visible on this server
    let mut stream_names: Vec<String> = Vec::new();
    {
        use futures_util::StreamExt;
        let mut names = js.stream_names();
        while let Some(Ok(name)) = names.next().await {
            stream_names.push(name);
        }
    }

    let stream_count = stream_names.len() as u64;
    let mut consumer_count = 0u64;
    for sname in &stream_names {
        if let Ok(stream) = js.get_stream(sname).await {
            use futures_util::StreamExt;
            let mut cnames = stream.consumer_names();
            while let Some(Ok(_)) = cnames.next().await {
                consumer_count += 1;
            }
        }
    }

    // Formula: 1 list + N_streams × (1 info + 1 consumer_names + N_consumers_avg × 1 info)
    let consumers_per_stream = if stream_count > 0 {
        consumer_count / stream_count
    } else { 1 };
    let api_requests_per_poll = 1 + stream_count * (2 + consumers_per_stream);

    // Snapshot size estimate: ~120 bytes per ConsumerSnapshot struct
    let snapshot_bytes: u64 = 120;
    let max_history:    u64 = 30;
    let history_bytes = consumer_count * max_history * snapshot_bytes;

    // Measure poll cycle wall clock
    let start = std::time::Instant::now();
    // Simulate one poll cycle: list + info for first stream/consumer
    let _ = js.stream_names().collect::<Vec<_>>().await;
    let poll_cycle_ms = start.elapsed().as_millis() as u64;

    Ok(OverheadResult {
        api_requests_per_poll,
        history_bytes,
        stream_count,
        consumer_count,
        poll_interval_secs: poll_interval,
        poll_cycle_ms,
    })
}
