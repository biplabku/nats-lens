/// nats-lens init — one-shot configuration audit.
///
/// Connects to NATS, scans every stream and consumer, evaluates each
/// consumer's configuration against known best practices, and prints
/// a human-readable (or JSON) report with specific fix commands.
///
/// Exits non-zero when --fail-on-critical is set and critical issues exist.
use anyhow::Result;
use nats_lens_core::client::NatsClient;
use serde::Serialize;

// ── Audit finding ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct Finding {
    pub stream: String,
    pub consumer: String,
    pub severity: &'static str,
    pub rule: &'static str,
    pub detail: String,
    pub fix_command: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Run the audit and print the report.  Returns `true` if any Critical finding
/// was found (so the caller can exit with code 1 when --fail-on-critical).
pub async fn run_audit(nats: async_nats::Client, format: &str) -> Result<bool> {
    let client = NatsClient::new(nats);
    let streams = client.list_streams().await?;

    let mut findings: Vec<Finding> = Vec::new();
    let mut total_consumers_checked: usize = 0;

    for stream in &streams {
        let sname = &stream.config.name;
        let consumers = match client.list_consumer_names(sname).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("warn: could not list consumers for {sname}: {e}");
                continue;
            }
        };

        for cname in &consumers {
            total_consumers_checked += 1;
            let info = match client.consumer_info(sname, cname).await {
                Ok(i) => i,
                Err(e) => {
                    eprintln!("warn: could not read {sname}/{cname}: {e}");
                    continue;
                }
            };

            let ack_wait_secs = info.config.ack_wait_secs();
            let max_ack_pending = info.config.max_ack_pending();

            // ── Rule 1: ack_wait very short (strict threshold = 5s) ───────────
            // 30s is the NATS default and is safe for many workloads.
            // We only flag explicitly short values (≤ 5s) as CRITICAL because
            // those are almost certainly misconfigured. Values 6–30s are WARNING:
            // common for health checks and lightweight probes, but risky for
            // heavier processing without WIP acks.
            if ack_wait_secs <= 5 {
                let recommended = 120u64;
                findings.push(Finding {
                    stream: sname.clone(),
                    consumer: cname.clone(),
                    severity: "CRITICAL",
                    rule: "ACK_WAIT_TOO_SHORT",
                    detail: format!(
                        "ack_wait={ack_wait_secs}s is extremely short. Any processing that \
                         takes longer than {ack_wait_secs}s will cause NATS to redeliver \
                         mid-processing, producing duplicate execution."
                    ),
                    fix_command: format!(
                        "nats consumer edit {sname} {cname} --ack-wait {recommended}s"
                    ),
                });
            } else if ack_wait_secs <= 30 {
                let recommended = 120u64;
                findings.push(Finding {
                    stream: sname.clone(),
                    consumer: cname.clone(),
                    severity: "WARNING",
                    rule: "ACK_WAIT_POTENTIALLY_SHORT",
                    detail: format!(
                        "ack_wait={ack_wait_secs}s (at or near the 30s default). Safe for \
                         lightweight workloads, but any task taking longer than \
                         {ack_wait_secs}s will trigger redelivery. Ensure either \
                         processing completes within {ack_wait_secs}s or consumers send \
                         in_progress acks for longer tasks."
                    ),
                    fix_command: format!(
                        "nats consumer edit {sname} {cname} --ack-wait {recommended}s  \
                         # or send msg.in_progress() every 30s in long-running tasks"
                    ),
                });
            }

            // ── Rule 2: max_ack_pending too small ─────────────────────────────
            // Minimum safe value is 64. Values below 10 will throttle almost
            // any concurrent consumer to near-serial processing.
            if max_ack_pending < 64 && max_ack_pending > 0 {
                let recommended = 256i64;
                findings.push(Finding {
                    stream: sname.clone(),
                    consumer: cname.clone(),
                    severity: if max_ack_pending < 10 {
                        "CRITICAL"
                    } else {
                        "WARNING"
                    },
                    rule: "MAX_PENDING_TOO_LOW",
                    detail: format!(
                        "max_ack_pending={max_ack_pending} is very low. \
                         NATS will throttle delivery when {max_ack_pending} messages \
                         are in-flight, even if your consumers have capacity. \
                         Formula: max_ack_pending ≥ concurrency × prefetch_size."
                    ),
                    fix_command: format!(
                        "nats consumer edit {sname} {cname} --max-pending {recommended}"
                    ),
                });
            }

            // ── Rule 3: max_deliver too high (NAK storm risk) ─────────────────
            // Unlimited redeliveries (max_deliver=-1 or very large) combined with
            // no NAK backoff can cause redelivery storms. Recommend ≤ 10.
            // Note: max_deliver is not in our ConsumerInfo yet; skip for now.

            // ── Rule 4: stream retention vs consumer throughput ───────────────
            // If the stream has very low max_msgs, a slow consumer will lose data.
            if let Some(max_msgs) = stream.config.max_msgs {
                if max_msgs > 0 && max_msgs < 1000 {
                    findings.push(Finding {
                        stream: sname.clone(),
                        consumer: cname.clone(),
                        severity: "WARNING",
                        rule: "LOW_STREAM_RETENTION",
                        detail: format!(
                            "Stream {sname} has max_msgs={max_msgs}. \
                             If this consumer falls behind by more than {max_msgs} messages, \
                             NATS will evict older messages before the consumer can process them, \
                             causing a SEQUENCE_GAP (silent data loss)."
                        ),
                        fix_command: format!(
                            "nats stream edit {sname} --max-msgs 10000  \
                             # or use --discard new to reject publishers instead"
                        ),
                    });
                }
            }

            // ── Rule 5: long ack_wait without in_progress guidance ────────────
            // ack_wait > 60s is fine IF consumers send in_progress acks.
            // We can't verify that from config alone, but we can warn.
            if ack_wait_secs > 60 {
                findings.push(Finding {
                    stream: sname.clone(),
                    consumer: cname.clone(),
                    severity: "INFO",
                    rule: "LONG_ACK_WAIT_NEEDS_PROGRESS",
                    detail: format!(
                        "ack_wait={ack_wait_secs}s is long. This is correct if your tasks \
                         take that long — but ensure your consumer sends in_progress acks \
                         every ~30s for tasks near or over {ack_wait_secs}s, otherwise \
                         NATS will redeliver mid-processing."
                    ),
                    fix_command: format!(
                        "# In your consumer code, send msg.in_progress() every 30s \
                         for long-running tasks on {sname}/{cname}"
                    ),
                });
            }
        }
    }

    let had_critical = findings.iter().any(|f| f.severity == "CRITICAL");

    match format {
        "json" => print_json(&findings)?,
        _ => print_text(&findings, streams.len(), total_consumers_checked),
    }

    Ok(had_critical)
}

// ── Output formatters ─────────────────────────────────────────────────────────

fn print_text(findings: &[Finding], stream_count: usize, consumers_checked: usize) {
    let n_critical = findings.iter().filter(|f| f.severity == "CRITICAL").count();
    let n_warning = findings.iter().filter(|f| f.severity == "WARNING").count();
    let n_info = findings.iter().filter(|f| f.severity == "INFO").count();

    println!();
    println!("  nats-lens init — JetStream Configuration Audit");
    println!("  ────────────────────────────────────────────────");
    println!("  Streams scanned:   {stream_count}");
    println!("  Consumers checked: {consumers_checked}");
    println!("  Critical:          {n_critical}");
    println!("  Warnings:          {n_warning}");
    println!("  Info:              {n_info}");
    println!();

    if findings.is_empty() {
        println!("  ✅  All consumers look healthy. No configuration issues found.");
        println!();
        return;
    }

    // Group by stream for readability
    let mut streams_seen: Vec<&str> = Vec::new();
    for f in findings {
        if !streams_seen.contains(&f.stream.as_str()) {
            streams_seen.push(&f.stream);
        }
    }

    for stream in &streams_seen {
        let stream_findings: Vec<&Finding> =
            findings.iter().filter(|f| &f.stream == stream).collect();

        println!("  Stream: {stream}");
        println!("  {}", "─".repeat(50));

        for f in stream_findings {
            let icon = match f.severity {
                "CRITICAL" => "🔴",
                "WARNING" => "🟡",
                _ => "ℹ️ ",
            };
            println!();
            println!(
                "  {icon} [{severity}] {rule}  —  consumer: {consumer}",
                severity = f.severity,
                rule = f.rule,
                consumer = f.consumer,
            );
            println!();
            // Word-wrap the detail at 72 chars
            for line in wrap(&f.detail, 68) {
                println!("     {line}");
            }
            println!();
            println!("     Fix:");
            println!("       {}", f.fix_command);
            println!();
        }
    }

    if n_critical > 0 {
        println!("  ⚠️  {n_critical} CRITICAL issue(s) found.");
        println!("     Run `nats-lens init --fail-on-critical` in CI to block deployments.");
    }
    println!();
}

fn print_json(findings: &[Finding]) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(findings)?);
    Ok(())
}

/// Very simple word wrapper — splits on spaces, respects max_width.
fn wrap(s: &str, max_width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in s.split_whitespace() {
        if current.is_empty() {
            current.push_str(word);
        } else if current.len() + 1 + word.len() <= max_width {
            current.push(' ');
            current.push_str(word);
        } else {
            lines.push(current.clone());
            current = word.to_string();
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}
