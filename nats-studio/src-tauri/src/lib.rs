use std::sync::Arc;
use std::time::Duration;

use nats_lens_core::engine::Engine;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex;

// ── App state ─────────────────────────────────────────────────────────────

pub struct AppState {
    pub engine: Arc<Mutex<Option<Arc<Engine>>>>,
}

// ── Audit finding (inlined from lens/src/audit.rs) ────────────────────────

#[derive(Debug, Serialize)]
pub struct AuditFinding {
    pub stream:      String,
    pub consumer:    String,
    pub severity:    &'static str,
    pub rule:        &'static str,
    pub detail:      String,
    pub fix_command: String,
}

// ── Commands exposed to the frontend ─────────────────────────────────────
//
// NOTE: These must be PRIVATE (no `pub`) so that tauri-macros does not
// generate `#[macro_export]` + `pub use { macro }` in the same scope.
// With pub visibility, Rust 1.90 raises E0255 ("defined multiple times")
// because `macro_rules! __cmd__X` and `pub use { __cmd__X }` in the same
// module both insert the macro into the module's macro namespace.
// Private functions only get a local `use { macro }` (no pub use), which
// Rust allows.  The generate_handler! macro still finds them via the local
// macro_rules! binding.

#[derive(Deserialize)]
pub struct ConnectArgs {
    nats_url:      String,
    poll_interval: u64,
}

#[tauri::command]
async fn connect(
    args:  ConnectArgs,
    state: State<'_, AppState>,
    app:   AppHandle,
) -> Result<String, String> {
    let client = async_nats::connect(&args.nats_url)
        .await
        .map_err(|e| format!("Connection failed: {e}"))?;

    let engine    = Arc::new(Engine::new(client));
    let engine_bg = Arc::clone(&engine);
    let app_bg    = app.clone();
    let poll_ms   = Duration::from_secs(args.poll_interval.max(1));

    // Engine runs in background, emits violation events to frontend
    tokio::spawn(async move {
        let tx = engine_bg.sender();
        let mut rx = tx.subscribe();
        tokio::spawn(async move {
            while let Ok(v) = rx.recv().await {
                let _ = app_bg.emit("violation", &v);
            }
        });
        engine_bg.run(poll_ms).await;
    });

    *state.engine.lock().await = Some(engine);
    Ok(format!("Connected to {}", args.nats_url))
}

#[tauri::command]
async fn disconnect(state: State<'_, AppState>) -> Result<(), String> {
    *state.engine.lock().await = None;
    Ok(())
}

#[tauri::command]
async fn get_streams(state: State<'_, AppState>) -> Result<serde_json::Value, String> {
    let guard = state.engine.lock().await;
    let Some(engine) = guard.as_ref() else {
        return Err("Not connected".into());
    };
    let streams = engine.state().read().await.clone();
    Ok(serde_json::to_value(&streams).map_err(|e| e.to_string())?)
}

#[tauri::command]
async fn get_history(
    stream:   String,
    consumer: String,
    state:    State<'_, AppState>,
) -> Result<serde_json::Value, String> {
    let guard = state.engine.lock().await;
    let Some(engine) = guard.as_ref() else { return Ok(serde_json::json!([])); };
    let key      = format!("{stream}/{consumer}");
    let hist_arc = engine.history_store();
    let hist     = hist_arc.lock().await;
    let snaps    = hist.get(&key);
    Ok(serde_json::to_value(snaps).map_err(|e| e.to_string())?)
}

#[tauri::command]
async fn apply_fix_ack_wait(
    stream:   String,
    consumer: String,
    secs:     u64,
    state:    State<'_, AppState>,
) -> Result<String, String> {
    if secs == 0 || secs > 315_360_000 {
        return Err("secs must be between 1 and 315360000".into());
    }
    let guard = state.engine.lock().await;
    let Some(engine) = guard.as_ref() else { return Err("Not connected".into()); };
    engine.nats_client()
        .update_ack_wait(&stream, &consumer, secs)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("Updated ack_wait={secs}s on {stream}/{consumer}"))
}

#[tauri::command]
async fn apply_fix_max_pending(
    stream:   String,
    consumer: String,
    value:    i64,
    state:    State<'_, AppState>,
) -> Result<String, String> {
    if value < 1 || value > 1_000_000 {
        return Err("value must be between 1 and 1,000,000".into());
    }
    let guard = state.engine.lock().await;
    let Some(engine) = guard.as_ref() else { return Err("Not connected".into()); };
    engine.nats_client()
        .update_max_ack_pending(&stream, &consumer, value)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("Updated max_ack_pending={value} on {stream}/{consumer}"))
}

#[tauri::command]
async fn apply_fix_max_msgs(
    stream: String,
    value:  i64,
    state:  State<'_, AppState>,
) -> Result<String, String> {
    if value < 0 {
        return Err("value must be non-negative (0 = unlimited)".into());
    }
    let guard = state.engine.lock().await;
    let Some(engine) = guard.as_ref() else { return Err("Not connected".into()); };
    engine.nats_client()
        .update_stream_max_msgs(&stream, value)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("Updated max_msgs={value} on stream {stream}"))
}

#[tauri::command]
async fn run_init_audit(state: State<'_, AppState>) -> Result<serde_json::Value, String> {
    // Clone the Arc so we can drop the Mutex lock before the async loop.
    let engine_arc = {
        let guard = state.engine.lock().await;
        guard.as_ref().map(Arc::clone)
    };
    let Some(engine) = engine_arc else {
        return Err("Not connected".into());
    };

    let client  = engine.nats_client();
    let streams = client.list_streams().await.map_err(|e| e.to_string())?;

    let mut findings: Vec<AuditFinding> = Vec::new();

    for stream in &streams {
        let sname     = &stream.config.name;
        let consumers = match client.list_consumer_names(sname).await {
            Ok(c)  => c,
            Err(e) => {
                tracing::warn!("audit: cannot list consumers for {sname}: {e}");
                continue;
            }
        };

        for cname in &consumers {
            let info = match client.consumer_info(sname, cname).await {
                Ok(i)  => i,
                Err(e) => {
                    tracing::warn!("audit: cannot read {sname}/{cname}: {e}");
                    continue;
                }
            };

            let ack_wait_secs   = info.config.ack_wait_secs();
            let max_ack_pending = info.config.max_ack_pending();

            // Rule 1 — ack_wait very short (CRITICAL ≤ 5s, WARNING ≤ 30s)
            if ack_wait_secs <= 5 {
                findings.push(AuditFinding {
                    stream: sname.clone(), consumer: cname.clone(),
                    severity: "CRITICAL", rule: "ACK_WAIT_TOO_SHORT",
                    detail: format!(
                        "ack_wait={ack_wait_secs}s is extremely short. Any processing \
                         longer than {ack_wait_secs}s will cause NATS to redeliver \
                         mid-processing, producing duplicate execution."
                    ),
                    fix_command: format!(
                        "nats consumer edit {sname} {cname} --ack-wait 120s"
                    ),
                });
            } else if ack_wait_secs <= 30 {
                findings.push(AuditFinding {
                    stream: sname.clone(), consumer: cname.clone(),
                    severity: "WARNING", rule: "ACK_WAIT_POTENTIALLY_SHORT",
                    detail: format!(
                        "ack_wait={ack_wait_secs}s (at or near the 30s default). \
                         Safe for lightweight workloads, but tasks longer than \
                         {ack_wait_secs}s will trigger redelivery."
                    ),
                    fix_command: format!(
                        "nats consumer edit {sname} {cname} --ack-wait 120s"
                    ),
                });
            }

            // Rule 2 — max_ack_pending too low
            if max_ack_pending < 64 && max_ack_pending > 0 {
                findings.push(AuditFinding {
                    stream: sname.clone(), consumer: cname.clone(),
                    severity: if max_ack_pending < 10 { "CRITICAL" } else { "WARNING" },
                    rule: "MAX_PENDING_TOO_LOW",
                    detail: format!(
                        "max_ack_pending={max_ack_pending} is very low. NATS throttles \
                         delivery when {max_ack_pending} messages are in-flight, even \
                         if consumers have capacity. Recommended: ≥256."
                    ),
                    fix_command: format!(
                        "nats consumer edit {sname} {cname} --max-pending 256"
                    ),
                });
            }

            // Rule 3 — low stream retention
            if let Some(max_msgs) = stream.config.max_msgs {
                if max_msgs > 0 && max_msgs < 1000 {
                    findings.push(AuditFinding {
                        stream: sname.clone(), consumer: cname.clone(),
                        severity: "WARNING", rule: "LOW_STREAM_RETENTION",
                        detail: format!(
                            "Stream {sname} has max_msgs={max_msgs}. If this consumer \
                             falls behind by more than {max_msgs} messages, NATS will \
                             evict older messages before they are processed (silent data loss)."
                        ),
                        fix_command: format!(
                            "nats stream edit {sname} --max-msgs 10000"
                        ),
                    });
                }
            }

            // Rule 4 — long ack_wait info hint
            if ack_wait_secs > 60 {
                findings.push(AuditFinding {
                    stream: sname.clone(), consumer: cname.clone(),
                    severity: "INFO", rule: "LONG_ACK_WAIT_NEEDS_PROGRESS",
                    detail: format!(
                        "ack_wait={ack_wait_secs}s is long. Ensure consumers send \
                         in_progress acks every ~30s for tasks approaching {ack_wait_secs}s, \
                         otherwise NATS will redeliver mid-processing."
                    ),
                    fix_command: format!(
                        "# Send msg.in_progress() every 30s in long-running tasks on {sname}/{cname}"
                    ),
                });
            }
        }
    }

    serde_json::to_value(&findings).map_err(|e| e.to_string())
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter("nats_studio=info,nats_lens_core=info")
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(AppState {
            engine: Arc::new(Mutex::new(None)),
        })
        .invoke_handler(tauri::generate_handler![
            connect,
            disconnect,
            get_streams,
            get_history,
            apply_fix_ack_wait,
            apply_fix_max_pending,
            apply_fix_max_msgs,
            run_init_audit,
        ])
        .run(tauri::generate_context!())
        .expect("error running nats-studio");
}
