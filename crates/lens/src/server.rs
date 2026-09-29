use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::http::Method;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Json, Response,
    },
    routing::get,
    Router,
};
use futures_util::StreamExt;
use nats_lens_core::client::NatsClient;
use nats_lens_core::history::HistoryStore;
use nats_lens_core::types::{StreamHealth, Violation};
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::{Any, CorsLayer};

// Embed the UI at compile time so the binary is fully self-contained.
const INDEX_HTML: &str = include_str!("../../../ui/index.html");

// ── App state ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    stream_health: Arc<RwLock<Vec<StreamHealth>>>,
    violation_tx: broadcast::Sender<Violation>,
    history: Arc<Mutex<HistoryStore>>,
    nats_client: Arc<NatsClient>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn serve(
    addr: SocketAddr,
    stream_health: Arc<RwLock<Vec<StreamHealth>>>,
    violation_tx: broadcast::Sender<Violation>,
    history: Arc<Mutex<HistoryStore>>,
    nats_client: Arc<NatsClient>,
) -> Result<()> {
    let state = Arc::new(AppState {
        stream_health,
        violation_tx,
        history,
        nats_client,
    });

    // CORS: allow cross-origin reads (dashboard, Grafana, monitoring tools) but
    // restrict state-mutating methods (POST /api/fix/*) to same-origin only.
    // Wildcard CORS on POSTs would allow any webpage on the local network to
    // reconfigure JetStream consumers via CSRF.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(index))
        .route("/health", get(health_check))
        .route("/metrics", get(metrics))
        .route("/api/streams", get(api_streams))
        .route("/api/violations/stream", get(violations_sse))
        .route("/api/history/:stream/:consumer", get(api_history))
        .route(
            "/api/fix/:stream/:consumer/ack-wait",
            axum::routing::post(apply_fix_ack_wait),
        )
        .route(
            "/api/fix/:stream/:consumer/max-pending",
            axum::routing::post(apply_fix_max_pending),
        )
        .route(
            "/api/fix/:stream/max-msgs",
            axum::routing::post(apply_fix_max_msgs),
        )
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// ── Route handlers ────────────────────────────────────────────────────────────

/// Serve the embedded dashboard HTML.
async fn index() -> impl IntoResponse {
    Html(INDEX_HTML)
}

/// Simple liveness probe.
async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// Return all stream health snapshots as JSON.
async fn api_streams(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let streams = state.stream_health.read().await.clone();
    Json(streams)
}

/// Prometheus text format metrics endpoint.
///
/// Exposes four metric families:
/// - `nats_lens_consumer_lag_msgs`       — messages waiting to be delivered
/// - `nats_lens_redeliveries_per_min`    — redelivery rate
/// - `nats_lens_ack_pending_ratio`       — ack-pending utilisation (0.0–1.0)
/// - `nats_lens_violations_active` — 1 if a given violation type is active, 0 otherwise
///
/// The output is hand-built as a plain String — no prometheus crate needed,
/// which keeps the dependency tree small.
async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let streams = state.stream_health.read().await.clone();
    let mut out = String::with_capacity(8 * 1024);

    // ── nats_lens_consumer_lag_msgs ───────────────────────────────────────────
    out.push_str("# HELP nats_lens_consumer_lag_msgs Messages waiting to be delivered\n");
    out.push_str("# TYPE nats_lens_consumer_lag_msgs gauge\n");
    for sh in &streams {
        for ch in &sh.consumers {
            out.push_str(&format!(
                "nats_lens_consumer_lag_msgs{{stream=\"{}\",consumer=\"{}\"}} {}\n",
                sh.stream_name, ch.consumer_name, ch.lag
            ));
        }
    }

    // ── nats_lens_redeliveries_per_min ────────────────────────────────────────
    out.push_str("# HELP nats_lens_redeliveries_per_min Redeliveries per minute\n");
    out.push_str("# TYPE nats_lens_redeliveries_per_min gauge\n");
    for sh in &streams {
        for ch in &sh.consumers {
            out.push_str(&format!(
                "nats_lens_redeliveries_per_min{{stream=\"{}\",consumer=\"{}\"}} {:.4}\n",
                sh.stream_name, ch.consumer_name, ch.redeliveries_per_min
            ));
        }
    }

    // ── nats_lens_ack_pending_ratio ───────────────────────────────────────────
    out.push_str("# HELP nats_lens_ack_pending_ratio Ack pending ratio (0.0-1.0)\n");
    out.push_str("# TYPE nats_lens_ack_pending_ratio gauge\n");
    for sh in &streams {
        for ch in &sh.consumers {
            out.push_str(&format!(
                "nats_lens_ack_pending_ratio{{stream=\"{}\",consumer=\"{}\"}} {:.4}\n",
                sh.stream_name,
                ch.consumer_name,
                ch.snapshot.pending_ratio()
            ));
        }
    }

    // ── nats_lens_violations_active ───────────────────────────────────────────
    const VIOLATION_TYPES: &[&str] = &[
        "ACK_WAIT_VIOLATION",
        "SEQUENCE_GAP",
        "MAX_PENDING_THROTTLE",
        "NAK_STORM",
        "MISSING_PROGRESS",
    ];

    out.push_str(
        "# HELP nats_lens_violations_active 1 if this violation type is currently active\n",
    );
    out.push_str("# TYPE nats_lens_violations_active gauge\n");
    for sh in &streams {
        for ch in &sh.consumers {
            let active: HashSet<&str> = ch.violations.iter().map(|v| v.violation.name()).collect();
            for vtype in VIOLATION_TYPES {
                let val = if active.contains(*vtype) { 1 } else { 0 };
                out.push_str(&format!(
                    "nats_lens_violations_active{{stream=\"{}\",consumer=\"{}\",type=\"{}\"}} {}\n",
                    sh.stream_name, ch.consumer_name, vtype, val
                ));
            }
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Body::from(out))
        .unwrap()
}

/// History endpoint — returns the last ≤30 snapshots for a specific consumer
/// as a JSON array of `{ts_ms, lag, redelivered, ack_pending_ratio}`.
///
/// Returns `[]` when no history has been accumulated yet.
async fn api_history(
    State(state): State<Arc<AppState>>,
    Path((stream, consumer)): Path<(String, String)>,
) -> impl IntoResponse {
    let key = format!("{stream}/{consumer}");
    let history = state.history.lock().await;
    let snaps = history.get(&key);

    let result: Vec<serde_json::Value> = snaps
        .iter()
        .map(|s| {
            let ack_pending_ratio = if s.max_ack_pending > 0 {
                s.num_ack_pending as f64 / s.max_ack_pending as f64
            } else {
                0.0
            };
            serde_json::json!({
                "ts_ms":             s.captured_at.timestamp_millis(),
                "lag":               s.num_pending,
                "redelivered":       s.num_redelivered,
                "ack_pending_ratio": ack_pending_ratio,
            })
        })
        .collect();

    Json(result)
}

/// SSE endpoint — pushes one JSON event per violation as it is detected.
///
/// Each connection creates its own broadcast receiver via `tx.subscribe()`,
/// so all concurrent browser tabs receive every violation independently.
async fn violations_sse(
    State(state): State<Arc<AppState>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.violation_tx.subscribe();

    let stream = BroadcastStream::new(rx).filter_map(|result| async move {
        match result {
            Ok(violation) => serde_json::to_string(&violation)
                .ok()
                .map(|data| Ok(Event::default().data(data))),
            // Lagged receiver (slow consumer) — skip dropped items silently.
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

// ── Apply Now handlers ────────────────────────────────────────────────────────
// Each endpoint applies one specific fix via the NATS management API.
// The dashboard "Apply Now" button POSTs to these with the recommended value.

/// Maximum safe ack_wait to prevent u64 overflow in nanosecond conversion.
/// 10 years in seconds is well above any practical value.
const MAX_ACK_WAIT_SECS: u64 = 315_360_000;
/// Minimum ack_wait: 1 second. Zero causes infinite-redelivery denial-of-service.
const MIN_ACK_WAIT_SECS: u64 = 1;
/// Minimum max_ack_pending: 1. Zero or negative disables delivery entirely.
const MIN_MAX_PENDING: i64 = 1;

#[derive(serde::Deserialize)]
struct AckWaitBody {
    secs: u64,
}

/// POST /api/fix/:stream/:consumer/ack-wait   body: {"secs": 120}
async fn apply_fix_ack_wait(
    State(state): State<Arc<AppState>>,
    Path((stream, consumer)): Path<(String, String)>,
    Json(body): Json<AckWaitBody>,
) -> impl IntoResponse {
    if body.secs < MIN_ACK_WAIT_SECS || body.secs > MAX_ACK_WAIT_SECS {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": format!("secs must be between {MIN_ACK_WAIT_SECS} and {MAX_ACK_WAIT_SECS}")
            })),
        );
    }
    match state
        .nats_client
        .update_ack_wait(&stream, &consumer, body.secs)
        .await
    {
        Ok(_) => (
            StatusCode::OK,
            Json(
                serde_json::json!({ "ok": true, "applied": format!("ack_wait={}s on {stream}/{consumer}", body.secs) }),
            ),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

#[derive(serde::Deserialize)]
struct MaxPendingBody {
    value: i64,
}

/// POST /api/fix/:stream/:consumer/max-pending   body: {"value": 256}
async fn apply_fix_max_pending(
    State(state): State<Arc<AppState>>,
    Path((stream, consumer)): Path<(String, String)>,
    Json(body): Json<MaxPendingBody>,
) -> impl IntoResponse {
    if body.value < MIN_MAX_PENDING {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": format!("value must be >= {MIN_MAX_PENDING}")
            })),
        );
    }
    match state
        .nats_client
        .update_max_ack_pending(&stream, &consumer, body.value)
        .await
    {
        Ok(_) => (
            StatusCode::OK,
            Json(
                serde_json::json!({ "ok": true, "applied": format!("max_ack_pending={} on {stream}/{consumer}", body.value) }),
            ),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

#[derive(serde::Deserialize)]
struct MaxMsgsBody {
    value: i64,
}

/// POST /api/fix/:stream/max-msgs   body: {"value": 10000}
async fn apply_fix_max_msgs(
    State(state): State<Arc<AppState>>,
    Path(stream): Path<String>,
    Json(body): Json<MaxMsgsBody>,
) -> impl IntoResponse {
    if body.value < 1 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "value must be >= 1"
            })),
        );
    }
    match state
        .nats_client
        .update_stream_max_msgs(&stream, body.value)
        .await
    {
        Ok(_) => (
            StatusCode::OK,
            Json(
                serde_json::json!({ "ok": true, "applied": format!("max_msgs={} on {stream}", body.value) }),
            ),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}
