/// Extended evaluation harness — TNSM journal experiments.
///
/// Adds four new scenarios on top of the baseline eval:
///   1. Recovery detection  — violation fires, config fixed, alert clears
///   2. Simultaneous multi-violation — 3 classes injected concurrently
///   3. Adversarial false positives — boundary configs that must NOT trigger
///   4. Poll-interval sensitivity — detection latency at 1s/3s/5s/10s/30s
///
/// Run:
///   cargo run --bin nats-lens-eval-extended -- --nats nats://localhost:4222
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull, stream};
use clap::Parser;
use futures_util::StreamExt;
use nats_lens_core::engine::Engine;
use nats_lens_core::history::HistoryStore;
use nats_lens_core::types::Violation;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tracing::info;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "nats-lens-eval-extended",
    about = "TNSM extended evaluation: recovery, multi-violation, adversarial FP, sensitivity"
)]
struct Args {
    #[arg(long, default_value = "nats://localhost:4222")]
    nats: String,

    /// Rounds per scenario
    #[arg(long, default_value_t = 10)]
    rounds: u32,

    /// Base poll interval (seconds) for non-sensitivity experiments
    #[arg(long, default_value_t = 3)]
    poll_interval: u64,

    /// Output directory
    #[arg(long, default_value = "data/extended")]
    out_dir: String,
}

// ── CSV row types ─────────────────────────────────────────────────────────────

#[derive(Debug, serde::Serialize)]
struct RecoveryRow {
    round: u32,
    violation_detected: bool,
    violation_latency_ms: Option<u64>,
    recovery_detected: bool,
    recovery_latency_ms: Option<u64>, // ms from fix to alert silence
    recovery_poll_cycles: Option<u32>,
}

#[derive(Debug, serde::Serialize)]
struct MultiVioRow {
    round: u32,
    ack_wait_detected: bool,
    ack_wait_latency_ms: Option<u64>,
    nak_storm_detected: bool,
    nak_storm_latency_ms: Option<u64>,
    seq_gap_detected: bool,
    seq_gap_latency_ms: Option<u64>,
    all_detected: bool,
}

#[derive(Debug, serde::Serialize)]
struct AdversarialRow {
    boundary_case: String,
    rounds_tested: u32,
    false_positives: u32,
    /// Always 0 for a correct detector
    fp_rate: f64,
}

#[derive(Debug, serde::Serialize)]
struct SensitivityRow {
    poll_interval_secs: u64,
    scenario: String,
    rounds: u32,
    detected: u32,
    detection_rate_pct: f64,
    p50_latency_ms: u64,
    p95_latency_ms: u64,
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter("nats_lens_eval=info,nats_lens_core=info")
        .init();

    let args = Args::parse();
    std::fs::create_dir_all(&args.out_dir)?;

    info!("Connecting to NATS at {}", args.nats);
    let client = async_nats::connect(&args.nats).await?;

    let engine = Arc::new(Engine::new(client.clone()));
    let tx = engine.sender();
    let history = engine.history_store();
    let poll_ms = Duration::from_secs(args.poll_interval);

    let engine_bg = Arc::clone(&engine);
    tokio::spawn(async move { engine_bg.run(poll_ms).await });
    tokio::time::sleep(Duration::from_secs(args.poll_interval + 1)).await;
    info!("Engine warmed up.");

    let js = jetstream::new(client.clone());

    // ── Experiment 1: Recovery Detection ─────────────────────────────────────
    info!("");
    info!(
        "══ Experiment 1: Recovery Detection ({} rounds) ══",
        args.rounds
    );
    let recovery_rows =
        run_recovery_experiment(&js, &tx, &history, args.rounds, args.poll_interval).await?;
    let rec_ok = recovery_rows.iter().filter(|r| r.recovery_detected).count();
    info!(
        "  Violation detected: {}/{}",
        recovery_rows
            .iter()
            .filter(|r| r.violation_detected)
            .count(),
        args.rounds
    );
    info!("  Recovery detected:  {}/{}", rec_ok, args.rounds);

    let path = format!("{}/recovery_results.csv", args.out_dir);
    let mut w = csv::Writer::from_path(&path)?;
    for r in &recovery_rows {
        w.serialize(r)?;
    }
    w.flush()?;
    info!("  → {path}");

    // ── Experiment 2: Simultaneous Multi-Violation ────────────────────────────
    info!("");
    info!(
        "══ Experiment 2: Simultaneous Multi-Violation ({} rounds) ══",
        args.rounds
    );
    let multi_rows =
        run_multi_violation_experiment(&js, &tx, args.rounds, args.poll_interval).await?;
    let all_ok = multi_rows.iter().filter(|r| r.all_detected).count();
    info!("  All 3 classes detected: {}/{}", all_ok, args.rounds);

    let path = format!("{}/multi_violation_results.csv", args.out_dir);
    let mut w = csv::Writer::from_path(&path)?;
    for r in &multi_rows {
        w.serialize(r)?;
    }
    w.flush()?;
    info!("  → {path}");

    // ── Experiment 3: Adversarial False Positives ─────────────────────────────
    info!("");
    info!("══ Experiment 3: Adversarial Boundary Conditions ══");
    info!("  Purging all eval streams for clean isolation...");
    purge_all_eval_streams(&js).await;
    // Wait 3 poll cycles for engine to settle with no active violations
    tokio::time::sleep(Duration::from_secs(args.poll_interval * 3 + 2)).await;
    info!("  Engine settled. Starting adversarial tests.");
    let adv_rows = run_adversarial_experiment(&js, &tx, args.poll_interval).await?;
    for r in &adv_rows {
        info!(
            "  [{}] FP={}/{} ({:.1}%)",
            r.boundary_case, r.false_positives, r.rounds_tested, r.fp_rate
        );
    }

    let path = format!("{}/adversarial_fp_results.csv", args.out_dir);
    let mut w = csv::Writer::from_path(&path)?;
    for r in &adv_rows {
        w.serialize(r)?;
    }
    w.flush()?;
    info!("  → {path}");

    // ── Experiment 4: Poll-Interval Sensitivity (single-snapshot) ────────────────
    info!("");
    info!("══ Experiment 4: Poll-Interval Sensitivity — Single-snapshot (k=1) ══");
    let sens_rows = run_sensitivity_experiment(&client, args.rounds).await?;
    for r in &sens_rows {
        info!(
            "  poll={}s  {}  rate={:.0}%  P50={}ms",
            r.poll_interval_secs, r.scenario, r.detection_rate_pct, r.p50_latency_ms
        );
    }
    let path = format!("{}/sensitivity_results.csv", args.out_dir);
    let mut w = csv::Writer::from_path(&path)?;
    for r in &sens_rows {
        w.serialize(r)?;
    }
    w.flush()?;
    info!("  → {path}");

    // ── Experiment 5: Poll-Interval Sensitivity (two-snapshot) ───────────────
    // Validates Theorem 2 k=2 bound: detection latency ≤ 2×T_poll
    // Runs NAK_STORM at poll intervals 1s, 3s, 5s, 10s (5 rounds each).
    // Skips 30s to keep total runtime < 30 minutes.
    info!("");
    info!("══ Experiment 5: Poll-Interval Sensitivity — Two-snapshot (k=2) ══");
    let sens2_rows = run_two_snapshot_sensitivity(&client, args.rounds.min(5)).await?;
    for r in &sens2_rows {
        info!(
            "  poll={}s  {}  rate={:.0}%  P50={}ms  bound={:.0}ms",
            r.poll_interval_secs,
            r.scenario,
            r.detection_rate_pct,
            r.p50_latency_ms,
            r.poll_interval_secs as f64 * 2000.0
        );
    }
    let path = format!("{}/sensitivity_two_snapshot_results.csv", args.out_dir);
    let mut w = csv::Writer::from_path(&path)?;
    for r in &sens2_rows {
        w.serialize(r)?;
    }
    w.flush()?;
    info!("  → {path}");

    info!("");
    info!(
        "All extended experiments complete. Results in {}/",
        args.out_dir
    );
    Ok(())
}

// ── Experiment 1: Recovery Detection ─────────────────────────────────────────
//
// Protocol:
//   1. Inject ACK_WAIT_VIOLATION (ack_wait=2s, puller holds 4s)
//   2. Wait for first violation event
//   3. Fix: abort puller + recreate consumer with ack_wait=30s
//   4. Wait for violations to stop (2 consecutive quiet poll cycles = "cleared")
//   5. Measure recovery_latency_ms = time from fix to first quiet poll

async fn run_recovery_experiment(
    js: &jetstream::Context,
    tx: &broadcast::Sender<Violation>,
    history: &Arc<Mutex<HistoryStore>>,
    rounds: u32,
    poll_interval: u64,
) -> Result<Vec<RecoveryRow>> {
    ensure_stream(js, "EVAL_REC", "eval.rec.>", 5_000, None).await?;
    let mut rows = Vec::new();

    for round in 1..=rounds {
        info!("  Round {}/{}", round, rounds);

        // Publish messages
        for i in 0..20u32 {
            let _ = js
                .publish("eval.rec.msg", format!(r#"{{"i":{i}}}"#).into())
                .await;
        }

        // Create bad consumer: ack_wait=2s
        let stream = js.get_stream("EVAL_REC").await?;
        let _ = stream
            .get_or_create_consumer(
                "eval-rec-consumer",
                pull::Config {
                    durable_name: Some("eval-rec-consumer".into()),
                    ack_wait: Duration::from_secs(2),
                    max_ack_pending: 10,
                    ..Default::default()
                },
            )
            .await?;

        // Puller holds messages past ack_wait
        let js_bg = js.clone();
        let puller = tokio::spawn(async move {
            loop {
                let Ok(stream) = js_bg.get_stream("EVAL_REC").await else {
                    break;
                };
                let Ok(c) = stream
                    .get_consumer::<pull::Config>("eval-rec-consumer")
                    .await
                else {
                    break;
                };
                if let Ok(mut batch) = c.fetch().max_messages(5).messages().await {
                    while let Some(Ok(_)) = batch.next().await {}
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });

        // Phase 1: detect violation
        let timeout_ms = poll_interval * 8 * 1000;
        let viol_latency = wait_for_violation_timed(tx, "ACK_WAIT_VIOLATION", timeout_ms).await;
        let violation_detected = viol_latency.is_some();

        // Phase 2: fix — abort puller, recreate consumer with good ack_wait
        puller.abort();
        let fix_time = Instant::now();

        let stream = js.get_stream("EVAL_REC").await?;
        let _ = stream.delete_consumer("eval-rec-consumer").await;
        history
            .lock()
            .await
            .clear_consumer("EVAL_REC/eval-rec-consumer");

        // New consumer with safe ack_wait — no messages pulled (nothing to redeliver)
        let _ = js
            .get_or_create_stream(stream::Config {
                name: "EVAL_REC".into(),
                subjects: vec!["eval.rec.>".into()],
                max_messages: 5_000,
                storage: stream::StorageType::Memory,
                ..Default::default()
            })
            .await;

        let stream = js.get_stream("EVAL_REC").await?;
        let _ = stream
            .get_or_create_consumer(
                "eval-rec-consumer",
                pull::Config {
                    durable_name: Some("eval-rec-consumer".into()),
                    ack_wait: Duration::from_secs(30), // safe
                    max_ack_pending: 10,
                    ..Default::default()
                },
            )
            .await?;

        // Phase 3: wait for silence (no ACK_WAIT violation for 2 consecutive poll cycles)
        let recovery_latency = wait_for_silence(tx, "ACK_WAIT_VIOLATION", poll_interval).await;
        let recovery_detected = recovery_latency.is_some();
        let recovery_ms = recovery_latency.map(|_| fix_time.elapsed().as_millis() as u64);
        let recovery_cycles =
            recovery_ms.map(|ms| (ms as f64 / (poll_interval * 1000) as f64).ceil() as u32);

        rows.push(RecoveryRow {
            round,
            violation_detected,
            violation_latency_ms: viol_latency,
            recovery_detected,
            recovery_latency_ms: recovery_ms,
            recovery_poll_cycles: recovery_cycles,
        });

        let stream = js.get_stream("EVAL_REC").await?;
        let _ = stream.delete_consumer("eval-rec-consumer").await;
        let _ = stream.purge().await;
        history
            .lock()
            .await
            .clear_consumer("EVAL_REC/eval-rec-consumer");
        tokio::time::sleep(Duration::from_secs(poll_interval * 2)).await;
    }

    Ok(rows)
}

// ── Experiment 2: Simultaneous Multi-Violation ────────────────────────────────
//
// Injects ACK_WAIT + NAK_STORM + SEQUENCE_GAP at the same time on 3 streams.
// Verifies the engine detects all three concurrently within the timeout window.

async fn run_multi_violation_experiment(
    js: &jetstream::Context,
    tx: &broadcast::Sender<Violation>,
    rounds: u32,
    poll_interval: u64,
) -> Result<Vec<MultiVioRow>> {
    ensure_stream(js, "EVAL_MULT_ACK", "eval.mult.ack.>", 5_000, None).await?;
    ensure_stream(js, "EVAL_MULT_NAK", "eval.mult.nak.>", 5_000, None).await?;
    ensure_stream(js, "EVAL_MULT_GAP", "eval.mult.gap.>", 50, Some(50)).await?;

    let mut rows = Vec::new();

    for round in 1..=rounds {
        info!("  Round {}/{}", round, rounds);

        // Seed messages for all three streams
        for i in 0..20u32 {
            let _ = js
                .publish("eval.mult.ack.msg", format!(r#"{{"i":{i}}}"#).into())
                .await;
            let _ = js
                .publish("eval.mult.nak.msg", format!(r#"{{"i":{i}}}"#).into())
                .await;
        }
        for i in 0..3u32 {
            let _ = js
                .publish("eval.mult.gap.seed", format!(r#"{{"i":{i}}}"#).into())
                .await;
        }

        // Create consumers
        let sa = js.get_stream("EVAL_MULT_ACK").await?;
        let _ = sa
            .get_or_create_consumer(
                "eval-mult-ack",
                pull::Config {
                    durable_name: Some("eval-mult-ack".into()),
                    ack_wait: Duration::from_secs(2),
                    max_ack_pending: 10,
                    ..Default::default()
                },
            )
            .await?;

        let sn = js.get_stream("EVAL_MULT_NAK").await?;
        let _ = sn
            .get_or_create_consumer(
                "eval-mult-nak",
                pull::Config {
                    durable_name: Some("eval-mult-nak".into()),
                    ack_wait: Duration::from_secs(30),
                    max_ack_pending: 20,
                    ..Default::default()
                },
            )
            .await?;

        let sg = js.get_stream("EVAL_MULT_GAP").await?;
        let c_gap = sg
            .get_or_create_consumer(
                "eval-mult-gap",
                pull::Config {
                    durable_name: Some("eval-mult-gap".into()),
                    ack_wait: Duration::from_secs(30),
                    max_ack_pending: 25,
                    ..Default::default()
                },
            )
            .await?;
        // Establish ack_floor for gap detection
        if let Ok(mut batch) = c_gap.fetch().max_messages(1).messages().await {
            if let Some(Ok(msg)) = batch.next().await {
                let _ = msg.ack().await;
            }
        }

        // Launch three concurrent injectors
        let js_ack = js.clone();
        let ack_task = tokio::spawn(async move {
            loop {
                let Ok(s) = js_ack.get_stream("EVAL_MULT_ACK").await else {
                    break;
                };
                let Ok(c) = s.get_consumer::<pull::Config>("eval-mult-ack").await else {
                    break;
                };
                if let Ok(mut b) = c.fetch().max_messages(5).messages().await {
                    while let Some(Ok(_)) = b.next().await {}
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });

        let js_nak = js.clone();
        let nak_task = tokio::spawn(async move {
            loop {
                let Ok(s) = js_nak.get_stream("EVAL_MULT_NAK").await else {
                    break;
                };
                let Ok(c) = s.get_consumer::<pull::Config>("eval-mult-nak").await else {
                    break;
                };
                if let Ok(mut b) = c.fetch().max_messages(5).messages().await {
                    while let Some(Ok(msg)) = b.next().await {
                        let _ = msg
                            .ack_with(async_nats::jetstream::AckKind::Nak(Some(
                                Duration::from_millis(500),
                            )))
                            .await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(600)).await;
            }
        });

        // Flood the gap stream
        let js_gap = js.clone();
        let gap_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            for i in 0..200u32 {
                let _ = js_gap
                    .publish("eval.mult.gap.flood", format!(r#"{{"flood":{i}}}"#).into())
                    .await;
            }
        });

        // Wait for all three with individual latency tracking
        let timeout_ms = poll_interval * 10 * 1000 + 5000;
        let (ack_lat, nak_lat, gap_lat) = tokio::join!(
            wait_for_violation_timed(tx, "ACK_WAIT_VIOLATION", timeout_ms),
            wait_for_violation_timed(tx, "NAK_STORM", timeout_ms),
            wait_for_violation_timed(tx, "SEQUENCE_GAP", timeout_ms),
        );

        ack_task.abort();
        nak_task.abort();
        gap_task.abort();

        let all = ack_lat.is_some() && nak_lat.is_some() && gap_lat.is_some();
        rows.push(MultiVioRow {
            round,
            ack_wait_detected: ack_lat.is_some(),
            ack_wait_latency_ms: ack_lat,
            nak_storm_detected: nak_lat.is_some(),
            nak_storm_latency_ms: nak_lat,
            seq_gap_detected: gap_lat.is_some(),
            seq_gap_latency_ms: gap_lat,
            all_detected: all,
        });

        // Cleanup
        for (sname, cname) in &[
            ("EVAL_MULT_ACK", "eval-mult-ack"),
            ("EVAL_MULT_NAK", "eval-mult-nak"),
            ("EVAL_MULT_GAP", "eval-mult-gap"),
        ] {
            if let Ok(s) = js.get_stream(sname).await {
                let _ = s.delete_consumer(cname).await;
                let _ = s.purge().await;
            }
        }
        tokio::time::sleep(Duration::from_secs(poll_interval * 2)).await;
    }

    Ok(rows)
}

// ── Experiment 3: Adversarial Boundary Conditions ─────────────────────────────
//
// Tests configs that are just BELOW detection thresholds.
// A correct detector fires zero false positives on these cases.

async fn run_adversarial_experiment(
    js: &jetstream::Context,
    tx: &broadcast::Sender<Violation>,
    poll_interval: u64,
) -> Result<Vec<AdversarialRow>> {
    let rounds = 10u32; // poll cycles per boundary case
    let mut rows = Vec::new();

    // ── Case A: MAX_PENDING at cap but num_pending=0 ─────────────────────────
    // MAX_PENDING_THROTTLE fires ONLY when num_pending > 0.
    // If all messages are already pulled, num_pending=0 → should NOT fire.
    {
        ensure_stream(js, "EVAL_ADV_A", "eval.adv.a.>", 5_000, None).await?;
        for i in 0..5u32 {
            let _ = js
                .publish("eval.adv.a.msg", format!(r#"{{"i":{i}}}"#).into())
                .await;
        }
        let stream = js.get_stream("EVAL_ADV_A").await?;
        let consumer = stream
            .get_or_create_consumer(
                "eval-adv-a",
                pull::Config {
                    durable_name: Some("eval-adv-a".into()),
                    ack_wait: Duration::from_secs(60),
                    max_ack_pending: 5,
                    ..Default::default()
                },
            )
            .await?;
        // Pull exactly max_ack_pending msgs → num_ack_pending=5=P, but num_pending=0
        if let Ok(mut b) = consumer.fetch().max_messages(5).messages().await {
            while let Some(Ok(_)) = b.next().await {}
        }

        let fp = count_false_positives(tx, "MAX_PENDING_THROTTLE", poll_interval, rounds).await;
        rows.push(AdversarialRow {
            boundary_case: "MAX_PENDING: pending=P but num_pending=0".into(),
            rounds_tested: rounds,
            false_positives: fp,
            fp_rate: fp as f64 / rounds as f64,
        });

        let _ = stream.delete_consumer("eval-adv-a").await;
        let _ = stream.purge().await;
    }

    // ── Case B: num_redelivered=1 stable (NAK_STORM needs >=2) ───────────────
    {
        ensure_stream(js, "EVAL_ADV_B", "eval.adv.b.>", 5_000, None).await?;
        for i in 0..2u32 {
            let _ = js
                .publish("eval.adv.b.msg", format!(r#"{{"i":{i}}}"#).into())
                .await;
        }
        let stream = js.get_stream("EVAL_ADV_B").await?;
        let consumer = stream
            .get_or_create_consumer(
                "eval-adv-b",
                pull::Config {
                    durable_name: Some("eval-adv-b".into()),
                    ack_wait: Duration::from_secs(2),
                    max_ack_pending: 50,
                    ..Default::default()
                },
            )
            .await?;
        // Pull exactly 1 message and hold it — only 1 distinct message redelivered
        if let Ok(mut b) = consumer.fetch().max_messages(1).messages().await {
            if let Some(Ok(_)) = b.next().await {} // hold but don't ack
        }

        let fp = count_false_positives(tx, "NAK_STORM", poll_interval, rounds).await;
        rows.push(AdversarialRow {
            boundary_case: "NAK_STORM: num_redelivered=1 (threshold is >=2)".into(),
            rounds_tested: rounds,
            false_positives: fp,
            fp_rate: fp as f64 / rounds as f64,
        });

        let _ = stream.delete_consumer("eval-adv-b").await;
        let _ = stream.purge().await;
    }

    // ── Case C: MISSING_PROGRESS: ratio=0.85 (threshold is >=0.9) ────────────
    {
        ensure_stream(js, "EVAL_ADV_C", "eval.adv.c.>", 5_000, None).await?;
        for i in 0..20u32 {
            let _ = js
                .publish("eval.adv.c.msg", format!(r#"{{"i":{i}}}"#).into())
                .await;
        }
        let stream = js.get_stream("EVAL_ADV_C").await?;
        let consumer = stream
            .get_or_create_consumer(
                "eval-adv-c",
                pull::Config {
                    durable_name: Some("eval-adv-c".into()),
                    ack_wait: Duration::from_secs(120),
                    max_ack_pending: 20,
                    ..Default::default()
                },
            )
            .await?;
        // Pull 17/20 = 85% → below 90% threshold
        if let Ok(mut b) = consumer.fetch().max_messages(17).messages().await {
            while let Some(Ok(_)) = b.next().await {}
        }

        let fp = count_false_positives(tx, "MISSING_PROGRESS", poll_interval, rounds).await;
        rows.push(AdversarialRow {
            boundary_case: "MISSING_PROGRESS: ratio=0.85 (threshold 0.90)".into(),
            rounds_tested: rounds,
            false_positives: fp,
            fp_rate: fp as f64 / rounds as f64,
        });

        let _ = stream.delete_consumer("eval-adv-c").await;
        let _ = stream.purge().await;
    }

    // ── Case D: Healthy consumer — ack everything promptly ───────────────────
    {
        ensure_stream(js, "EVAL_ADV_D", "eval.adv.d.>", 10_000, None).await?;
        let stream = js.get_stream("EVAL_ADV_D").await?;
        let _consumer = stream
            .get_or_create_consumer(
                "eval-adv-d",
                pull::Config {
                    durable_name: Some("eval-adv-d".into()),
                    ack_wait: Duration::from_secs(300),
                    max_ack_pending: 512,
                    ..Default::default()
                },
            )
            .await?;

        // Continuously publish and promptly ack
        let js_bg = js.clone();
        let healthy = tokio::spawn(async move {
            let mut i = 0u32;
            loop {
                let _ = js_bg
                    .publish("eval.adv.d.msg", format!(r#"{{"i":{i}}}"#).into())
                    .await;
                i += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });

        let stream_d = js.get_stream("EVAL_ADV_D").await?;
        let c2 = stream_d
            .get_consumer::<pull::Config>("eval-adv-d")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let acker = tokio::spawn(async move {
            loop {
                if let Ok(mut b) = c2.fetch().max_messages(10).messages().await {
                    while let Some(Ok(msg)) = b.next().await {
                        let _ = msg.ack().await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        // Count any violation on EVAL_ADV_D only (should be 0 for a healthy consumer)
        let mut total_fp = 0u32;
        for vtype in &[
            "ACK_WAIT_VIOLATION",
            "SEQUENCE_GAP",
            "MAX_PENDING_THROTTLE",
            "NAK_STORM",
            "MISSING_PROGRESS",
        ] {
            total_fp +=
                count_false_positives_filtered(tx, vtype, "EVAL_ADV_D", poll_interval, 3).await;
        }

        healthy.abort();
        acker.abort();

        rows.push(AdversarialRow {
            boundary_case: "Healthy consumer (all classes, prompt ACKs)".into(),
            rounds_tested: 15,
            false_positives: total_fp,
            fp_rate: total_fp as f64 / 15.0,
        });

        let _ = stream.delete_consumer("eval-adv-d").await;
        let _ = stream.purge().await;
    }

    Ok(rows)
}

// ── Experiment 4: Poll-Interval Sensitivity ────────────────────────────────────
//
// Runs the two fastest-to-inject violations (SEQUENCE_GAP, MAX_PENDING_THROTTLE)
// at poll intervals of 1s, 3s, 5s, 10s, 30s.
// Tests the theoretical guarantee: detection within 1×T_poll for single-snapshot
// detectors and 2×T_poll for two-snapshot detectors.

async fn run_sensitivity_experiment(
    client: &async_nats::Client,
    rounds: u32,
) -> Result<Vec<SensitivityRow>> {
    let poll_intervals: &[u64] = &[1, 3, 5, 10, 30];
    let mut rows = Vec::new();

    for &interval in poll_intervals {
        info!("  poll_interval={}s ...", interval);

        // Fresh engine for each poll interval
        let engine = Arc::new(Engine::new(client.clone()));
        let tx = engine.sender();
        let poll_ms = Duration::from_secs(interval);
        let eng_bg = Arc::clone(&engine);
        let handle = tokio::spawn(async move { eng_bg.run(poll_ms).await });
        tokio::time::sleep(Duration::from_secs(interval + 1)).await;

        let js = jetstream::new(client.clone());

        // Test SEQUENCE_GAP (single-snapshot, should detect in 1×T_poll)
        let gap_rows = run_gap_sensitivity(&js, &tx, rounds, interval).await?;
        rows.push(summarize_sensitivity(interval, "SEQUENCE_GAP", &gap_rows));

        // Test MAX_PENDING_THROTTLE (single-snapshot, should detect in 1×T_poll)
        let throt_rows = run_throttle_sensitivity(&js, &tx, rounds, interval).await?;
        rows.push(summarize_sensitivity(
            interval,
            "MAX_PENDING_THROTTLE",
            &throt_rows,
        ));

        handle.abort();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    Ok(rows)
}

// helpers for sensitivity sub-runs ──

async fn run_gap_sensitivity(
    js: &jetstream::Context,
    tx: &broadcast::Sender<Violation>,
    rounds: u32,
    poll_interval: u64,
) -> Result<Vec<Option<u64>>> {
    let sname = format!("EVAL_SENS_GAP_{poll_interval}");
    let subj = format!("eval.sens.gap.{poll_interval}.>");
    ensure_stream(js, &sname, &subj, 50, Some(50)).await?;

    let mut latencies = Vec::new();
    for _ in 0..rounds {
        for i in 0..3u32 {
            let _ = js
                .publish(
                    format!("eval.sens.gap.{poll_interval}.seed"),
                    format!(r#"{{"i":{i}}}"#).into(),
                )
                .await;
        }
        let stream = js.get_stream(&sname).await?;
        let consumer = stream
            .get_or_create_consumer(
                "eval-sens-gap",
                pull::Config {
                    durable_name: Some("eval-sens-gap".into()),
                    ack_wait: Duration::from_secs(30),
                    max_ack_pending: 25,
                    ..Default::default()
                },
            )
            .await?;
        if let Ok(mut b) = consumer.fetch().max_messages(1).messages().await {
            if let Some(Ok(msg)) = b.next().await {
                let _ = msg.ack().await;
            }
        }
        for i in 0..200u32 {
            let _ = js
                .publish(
                    format!("eval.sens.gap.{poll_interval}.flood"),
                    format!(r#"{{"f":{i}}}"#).into(),
                )
                .await;
        }
        let timeout = poll_interval * 4 * 1000;
        latencies.push(wait_for_violation_timed(tx, "SEQUENCE_GAP", timeout).await);
        let _ = stream.delete_consumer("eval-sens-gap").await;
        let _ = stream.purge().await;
        tokio::time::sleep(Duration::from_secs(poll_interval + 1)).await;
    }
    Ok(latencies)
}

async fn run_throttle_sensitivity(
    js: &jetstream::Context,
    tx: &broadcast::Sender<Violation>,
    rounds: u32,
    poll_interval: u64,
) -> Result<Vec<Option<u64>>> {
    let sname = format!("EVAL_SENS_THR_{poll_interval}");
    let subj = format!("eval.sens.thr.{poll_interval}.>");
    ensure_stream(js, &sname, &subj, 5_000, None).await?;

    let mut latencies = Vec::new();
    for _ in 0..rounds {
        for i in 0..10u32 {
            let _ = js
                .publish(
                    format!("eval.sens.thr.{poll_interval}.msg"),
                    format!(r#"{{"i":{i}}}"#).into(),
                )
                .await;
        }
        let stream = js.get_stream(&sname).await?;
        let consumer = stream
            .get_or_create_consumer(
                "eval-sens-thr",
                pull::Config {
                    durable_name: Some("eval-sens-thr".into()),
                    ack_wait: Duration::from_secs(60),
                    max_ack_pending: 3,
                    ..Default::default()
                },
            )
            .await?;
        if let Ok(mut b) = consumer.fetch().max_messages(3).messages().await {
            while let Some(Ok(_)) = b.next().await {}
        }
        let timeout = poll_interval * 4 * 1000;
        latencies.push(wait_for_violation_timed(tx, "MAX_PENDING_THROTTLE", timeout).await);
        let _ = stream.delete_consumer("eval-sens-thr").await;
        tokio::time::sleep(Duration::from_secs(poll_interval + 1)).await;
    }
    Ok(latencies)
}

fn summarize_sensitivity(
    poll_interval: u64,
    scenario: &str,
    latencies: &[Option<u64>],
) -> SensitivityRow {
    let detected: Vec<u64> = latencies.iter().filter_map(|x| *x).collect();
    let n = latencies.len() as u32;
    let d = detected.len() as u32;
    let mut sorted = detected.clone();
    sorted.sort_unstable();
    let p50 = sorted.get(sorted.len() / 2).copied().unwrap_or(0);
    let p95 = sorted
        .get((sorted.len() as f64 * 0.95) as usize)
        .copied()
        .unwrap_or(0);
    SensitivityRow {
        poll_interval_secs: poll_interval,
        scenario: scenario.into(),
        rounds: n,
        detected: d,
        detection_rate_pct: d as f64 / n as f64 * 100.0,
        p50_latency_ms: p50,
        p95_latency_ms: p95,
    }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

/// Wait for a specific violation type. Returns elapsed ms or None on timeout.
async fn wait_for_violation_timed(
    tx: &broadcast::Sender<Violation>,
    vtype: &str,
    timeout_ms: u64,
) -> Option<u64> {
    let mut rx = tx.subscribe();
    let start = Instant::now();
    let deadline = Duration::from_millis(timeout_ms);
    loop {
        match tokio::time::timeout(deadline.saturating_sub(start.elapsed()), rx.recv()).await {
            Ok(Ok(v)) if v.violation.name() == vtype => {
                return Some(start.elapsed().as_millis() as u64);
            }
            Ok(Ok(_)) => {
                if start.elapsed() >= deadline {
                    return None;
                }
            }
            _ => return None,
        }
    }
}

/// Wait until a given violation type has NOT fired for 2 consecutive poll cycles.
/// Returns Some(elapsed_ms_since_call) when silence confirmed, None on timeout.
async fn wait_for_silence(
    tx: &broadcast::Sender<Violation>,
    vtype: &str,
    poll_interval: u64,
) -> Option<u64> {
    let start = Instant::now();
    let quiet_needed = Duration::from_secs(poll_interval * 2);
    let timeout = Duration::from_secs(poll_interval * 8);
    let mut last_seen = Instant::now();
    let mut rx = tx.subscribe();

    loop {
        if start.elapsed() > timeout {
            return None;
        }

        let remaining = quiet_needed.saturating_sub(last_seen.elapsed());
        if remaining.is_zero() {
            return Some(start.elapsed().as_millis() as u64);
        }

        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(v)) if v.violation.name() == vtype => {
                last_seen = Instant::now(); // violation still firing
            }
            Ok(Ok(_)) => {} // different type, ignore
            _ => {
                // timeout without seeing the vtype — quiet period reached
                if last_seen.elapsed() >= quiet_needed {
                    return Some(start.elapsed().as_millis() as u64);
                }
            }
        }
    }
}

/// Count false positives for a given violation type, restricted to a stream prefix.
/// The stream_prefix filter is critical: it prevents background simulator processes
/// from contaminating the count with legitimate violations on their own streams.
async fn count_false_positives(
    tx: &broadcast::Sender<Violation>,
    vtype: &str,
    poll_interval: u64,
    poll_cycles: u32,
) -> u32 {
    // Only count violations on EVAL_ADV_* streams (the adversarial test streams).
    count_false_positives_filtered(tx, vtype, "EVAL_ADV_", poll_interval, poll_cycles).await
}

async fn count_false_positives_filtered(
    tx: &broadcast::Sender<Violation>,
    vtype: &str,
    stream_prefix: &str,
    poll_interval: u64,
    poll_cycles: u32,
) -> u32 {
    let mut rx = tx.subscribe();
    let deadline = Duration::from_secs(poll_interval * poll_cycles as u64 + 2);
    let start = Instant::now();
    let mut count = 0u32;

    loop {
        match tokio::time::timeout(deadline.saturating_sub(start.elapsed()), rx.recv()).await {
            Ok(Ok(v))
                if v.violation.name() == vtype && v.stream_name.starts_with(stream_prefix) =>
            {
                count += 1;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    count
}

// ── Experiment 5: Two-snapshot sensitivity ───────────────────────────────────
// Validates Theorem 2 k=2: detection within 2×T_poll for NAK_STORM.
// Uses NAK_STORM (most reliable two-snapshot detector) at 1s, 3s, 5s, 10s.

async fn run_two_snapshot_sensitivity(
    client: &async_nats::Client,
    rounds: u32,
) -> Result<Vec<SensitivityRow>> {
    // Only go up to 10s — at 30s the test would take 60s per round × rounds
    let poll_intervals: &[u64] = &[1, 3, 5, 10];
    let mut rows = Vec::new();

    for &interval in poll_intervals {
        info!("  poll_interval={}s (two-snapshot) ...", interval);

        let engine = Arc::new(Engine::new(client.clone()));
        let tx = engine.sender();
        let history = engine.history_store();
        let poll_ms = Duration::from_secs(interval);
        let eng_bg = Arc::clone(&engine);
        let handle = tokio::spawn(async move { eng_bg.run(poll_ms).await });
        tokio::time::sleep(Duration::from_secs(interval + 1)).await;

        let js = jetstream::new(client.clone());
        let latencies = run_nak_storm_sensitivity(&js, &tx, &history, rounds, interval).await?;
        rows.push(summarize_sensitivity(interval, "NAK_STORM", &latencies));

        handle.abort();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(rows)
}

async fn run_nak_storm_sensitivity(
    js: &jetstream::Context,
    tx: &broadcast::Sender<Violation>,
    history: &Arc<Mutex<HistoryStore>>,
    rounds: u32,
    poll_interval: u64,
) -> Result<Vec<Option<u64>>> {
    let sname = format!("EVAL_SENS_NAK_{poll_interval}");
    let subj = format!("eval.sens.nak.{poll_interval}.>");
    ensure_stream(js, &sname, &subj, 5_000, None).await?;

    let mut latencies = Vec::new();
    for _ in 0..rounds {
        for i in 0..50u32 {
            let _ = js
                .publish(
                    format!("eval.sens.nak.{poll_interval}.msg"),
                    format!(r#"{{"i":{i}}}"#).into(),
                )
                .await;
        }
        let stream = js.get_stream(&sname).await?;
        let _ = stream
            .get_or_create_consumer(
                "eval-sens-nak",
                pull::Config {
                    durable_name: Some("eval-sens-nak".into()),
                    ack_wait: Duration::from_secs(30),
                    max_ack_pending: 50,
                    ..Default::default()
                },
            )
            .await?;

        let js_bg = js.clone();
        let s2 = sname.clone();
        let nakker = tokio::spawn(async move {
            loop {
                let Ok(stream) = js_bg.get_stream(&s2).await else {
                    break;
                };
                let Ok(c) = stream.get_consumer::<pull::Config>("eval-sens-nak").await else {
                    break;
                };
                if let Ok(mut b) = c.fetch().max_messages(5).messages().await {
                    while let Some(Ok(msg)) = b.next().await {
                        let _ = msg
                            .ack_with(async_nats::jetstream::AckKind::Nak(Some(
                                Duration::from_millis(500),
                            )))
                            .await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(600)).await;
            }
        });

        // k=2 detectors need 2×T_poll; allow generous timeout 6×T_poll
        let timeout = poll_interval * 6 * 1000;
        latencies.push(wait_for_violation_timed(tx, "NAK_STORM", timeout).await);

        nakker.abort();
        let stream = js.get_stream(&sname).await?;
        let _ = stream.delete_consumer("eval-sens-nak").await;
        let _ = stream.purge().await;
        history
            .lock()
            .await
            .clear_consumer(&format!("{sname}/eval-sens-nak"));
        tokio::time::sleep(Duration::from_secs(poll_interval * 2 + 1)).await;
    }
    Ok(latencies)
}

/// Delete and purge all streams created by any eval harness run.
/// Called before the adversarial experiment to eliminate cross-contamination.
async fn purge_all_eval_streams(js: &jetstream::Context) {
    let eval_streams = [
        "EVAL_ACK",
        "EVAL_GAP",
        "EVAL_THROT",
        "EVAL_NAK",
        "EVAL_PROG",
        "EVAL_REC",
        "EVAL_MULT_ACK",
        "EVAL_MULT_NAK",
        "EVAL_MULT_GAP",
        "EVAL_HEALTHY",
        // sensitivity streams
        "EVAL_SENS_GAP_1",
        "EVAL_SENS_GAP_3",
        "EVAL_SENS_GAP_5",
        "EVAL_SENS_GAP_10",
        "EVAL_SENS_GAP_30",
        "EVAL_SENS_THR_1",
        "EVAL_SENS_THR_3",
        "EVAL_SENS_THR_5",
        "EVAL_SENS_THR_10",
        "EVAL_SENS_THR_30",
        // scale benchmark streams
        "SCALE_S0000",
        "SCALE_S0001",
        "SCALE_S0002",
        "SCALE_S0003",
        "SCALE_S0004",
        "SCALE_S0005",
        "SCALE_S0006",
        "SCALE_S0007",
        "SCALE_S0008",
        "SCALE_S0009",
    ];
    for name in &eval_streams {
        if let Ok(s) = js.get_stream(name).await {
            // Delete all consumers first
            use futures_util::StreamExt;
            let mut cnames: Vec<String> = Vec::new();
            let mut iter = s.consumer_names();
            while let Some(Ok(c)) = iter.next().await {
                cnames.push(c);
            }
            for c in cnames {
                let _ = s.delete_consumer(&c).await;
            }
            let _ = s.purge().await;
            let _ = js.delete_stream(name).await;
        }
    }
    info!("  All eval streams purged.");
}

async fn ensure_stream(
    js: &jetstream::Context,
    name: &str,
    subject: &str,
    max_msgs: i64,
    max_msgs_override: Option<i64>,
) -> Result<()> {
    let _ = js
        .get_or_create_stream(stream::Config {
            name: name.into(),
            subjects: vec![subject.into()],
            max_messages: max_msgs_override.unwrap_or(max_msgs),
            storage: stream::StorageType::Memory,
            ..Default::default()
        })
        .await?;
    Ok(())
}
