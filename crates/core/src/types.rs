use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── Raw JetStream API response shapes ────────────────────────────────────────
// These mirror the NATS server JSON wire format exactly.
// Using raw API requests means nats-lens works with any NATS version
// and any client language — not just Rust.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsApiError {
    pub code: u16,
    pub description: String,
}

/// $JS.API.STREAM.LIST response
#[derive(Debug, Deserialize)]
pub struct StreamListResponse {
    pub total: usize,
    pub streams: Option<Vec<StreamInfo>>,
    pub error: Option<JsApiError>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamInfo {
    pub config: StreamConfig,
    pub state: StreamState,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamConfig {
    pub name: String,
    pub subjects: Option<Vec<String>>,
    pub max_msgs: Option<i64>,
    pub max_bytes: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamState {
    pub messages: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub num_deleted: Option<u64>,
}

/// $JS.API.CONSUMER.NAMES.{stream} response
#[derive(Debug, Deserialize)]
pub struct ConsumerNamesResponse {
    pub total: usize,
    pub consumers: Option<Vec<String>>,
    pub error: Option<JsApiError>,
}

/// $JS.API.CONSUMER.INFO.{stream}.{consumer} response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConsumerInfo {
    pub stream_name: String,
    pub name: String,
    pub config: ConsumerConfig,
    pub delivered: SequenceInfo,
    pub ack_floor: SequenceInfo,
    pub num_ack_pending: u64,
    pub num_redelivered: u64,
    pub num_waiting: u64,
    pub num_pending: u64,
    pub error: Option<JsApiError>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConsumerConfig {
    pub durable_name: Option<String>,
    /// ack_wait in nanoseconds as returned by NATS server
    pub ack_wait: Option<u64>,
    pub max_ack_pending: Option<i64>,
    pub filter_subject: Option<String>,
}

impl ConsumerConfig {
    pub fn ack_wait_secs(&self) -> u64 {
        self.ack_wait.unwrap_or(30_000_000_000) / 1_000_000_000
    }

    pub fn max_ack_pending(&self) -> i64 {
        self.max_ack_pending.unwrap_or(1000)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SequenceInfo {
    pub consumer_seq: u64,
    pub stream_seq: u64,
}

// ── Snapshot — point-in-time view of one consumer ────────────────────────────

/// Everything nats-lens needs about one consumer at one point in time.
/// Stored in HistoryStore to enable trend detection across polls.
#[derive(Debug, Clone, Serialize)]
pub struct ConsumerSnapshot {
    pub stream_name: String,
    pub consumer_name: String,
    pub num_pending: u64,
    pub num_ack_pending: u64,
    pub num_redelivered: u64,
    pub max_ack_pending: i64,
    pub ack_wait_secs: u64,
    pub delivered_stream_seq: u64,
    pub ack_floor_stream_seq: u64,
    pub stream_first_seq: u64,
    pub stream_last_seq: u64,
    pub captured_at: DateTime<Utc>,
}

impl ConsumerSnapshot {
    pub fn key(&self) -> String {
        format!("{}/{}", self.stream_name, self.consumer_name)
    }

    pub fn pending_ratio(&self) -> f64 {
        if self.max_ack_pending <= 0 {
            return 0.0;
        }
        self.num_ack_pending as f64 / self.max_ack_pending as f64
    }
}

// ── Violation — the core output of nats-lens ─────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Severity {
    Critical,
    Warning,
}

/// The five delivery guarantee violations nats-lens detects.
/// Published to `nats.lens.violations` as JSON — any language subscribes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ViolationType {
    /// ack_wait shorter than actual processing time → messages redelivered while
    /// still being processed → duplicate execution.
    AckWaitViolation {
        redeliveries_per_min: f64,
        current_ack_wait_secs: u64,
        recommended_ack_wait_secs: u64,
        fix_command: String,
    },

    /// Stream evicted messages before consumer could pull them → silent data loss.
    /// The consumer's ack_floor jumped past the stream's first_seq.
    SequenceGap {
        gap_start: u64,
        gap_end: u64,
        messages_lost: u64,
    },

    /// max_ack_pending too small for the consumer's concurrency + prefetch →
    /// NATS throttles delivery even when the consumer has capacity.
    MaxPendingThrottle {
        current_max_pending: i64,
        recommended_min: i64,
        pending_ratio_pct: f64,
    },

    /// Consumer is NAKing stale messages → NATS redelivers → still stale →
    /// infinite redelivery loop consuming throughput without processing work.
    NakStorm {
        redelivery_rate_per_min: f64,
        lag_growth_per_min: f64,
    },

    /// Long-running tasks not sending in_progress acks → ack_wait fires →
    /// messages redelivered mid-processing → duplicate execution.
    MissingProgress {
        ack_pending_ratio_pct: f64,
        ack_wait_secs: u64,
    },
}

impl ViolationType {
    pub fn name(&self) -> &'static str {
        match self {
            ViolationType::AckWaitViolation { .. } => "ACK_WAIT_VIOLATION",
            ViolationType::SequenceGap { .. } => "SEQUENCE_GAP",
            ViolationType::MaxPendingThrottle { .. } => "MAX_PENDING_THROTTLE",
            ViolationType::NakStorm { .. } => "NAK_STORM",
            ViolationType::MissingProgress { .. } => "MISSING_PROGRESS",
        }
    }

    pub fn severity(&self) -> Severity {
        match self {
            ViolationType::AckWaitViolation { .. } => Severity::Critical,
            ViolationType::SequenceGap { .. } => Severity::Critical,
            ViolationType::NakStorm { .. } => Severity::Critical,
            ViolationType::MaxPendingThrottle { .. } => Severity::Warning,
            ViolationType::MissingProgress { .. } => Severity::Warning,
        }
    }

    pub fn description(&self) -> String {
        match self {
            ViolationType::AckWaitViolation { redeliveries_per_min, current_ack_wait_secs, recommended_ack_wait_secs, .. } =>
                format!("{redeliveries_per_min:.0} redeliveries/min — ack_wait ({current_ack_wait_secs}s) shorter than processing time. Recommended: {recommended_ack_wait_secs}s"),
            ViolationType::SequenceGap { messages_lost, gap_start, gap_end } =>
                format!("{messages_lost} messages silently evicted (seq {gap_start}–{gap_end}) before consumer could pull them"),
            ViolationType::MaxPendingThrottle { current_max_pending, recommended_min, pending_ratio_pct } =>
                format!("Consumer throttled at {pending_ratio_pct:.0}% capacity — max_ack_pending={current_max_pending} but should be ≥{recommended_min}"),
            ViolationType::NakStorm { redelivery_rate_per_min, lag_growth_per_min } =>
                format!("NAK storm: {redelivery_rate_per_min:.0} redeliveries/min, lag growing at {lag_growth_per_min:.0} msgs/min — consumer is NAKing stale messages"),
            ViolationType::MissingProgress { ack_pending_ratio_pct, ack_wait_secs } =>
                format!("ack_pending at {ack_pending_ratio_pct:.0}% of max with ack_wait={ack_wait_secs}s — long tasks should send in_progress acks"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    pub stream_name: String,
    pub consumer_name: String,
    pub violation: ViolationType,
    pub severity: Severity,
    pub description: String,
    pub detected_at: DateTime<Utc>,
}

impl Violation {
    pub fn new(snapshot: &ConsumerSnapshot, violation: ViolationType) -> Self {
        let severity = violation.severity();
        let description = violation.description();
        Self {
            stream_name: snapshot.stream_name.clone(),
            consumer_name: snapshot.consumer_name.clone(),
            violation,
            severity,
            description,
            detected_at: Utc::now(),
        }
    }
}

// ── Health summary ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    Healthy,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerHealth {
    pub stream_name: String,
    pub consumer_name: String,
    pub health: Health,
    pub lag: u64,
    pub redeliveries_per_min: f64,
    pub violations: Vec<Violation>,
    pub snapshot: ConsumerSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamHealth {
    pub stream_name: String,
    pub config: StreamConfig,
    pub state: StreamState,
    pub health: Health,
    pub consumers: Vec<ConsumerHealth>,
}
