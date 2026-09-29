use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::{error, info, warn};

use crate::client::NatsClient;
use crate::detectors;
use crate::history::HistoryStore;
use crate::types::{
    ConsumerHealth, ConsumerSnapshot, Health, Severity, StreamHealth, StreamInfo, Violation,
};

/// Capacity of the violation broadcast channel.  Old violations are silently
/// dropped when receivers fall behind.
const BROADCAST_CAPACITY: usize = 1_024;

/// The central engine: polls every stream and consumer on each tick, runs all
/// detectors, publishes violations over a broadcast channel, and keeps the
/// latest `Vec<StreamHealth>` accessible for the REST API.
pub struct Engine {
    client: Arc<NatsClient>,
    history: Arc<Mutex<HistoryStore>>,
    tx: broadcast::Sender<Violation>,
    state: Arc<RwLock<Vec<StreamHealth>>>,
}

impl Engine {
    pub fn new(nats: async_nats::Client) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            client: Arc::new(NatsClient::new(nats)),
            history: Arc::new(Mutex::new(HistoryStore::new())),
            tx,
            state: Arc::new(RwLock::new(Vec::new())),
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Subscribe to the violation broadcast.  Each call creates an independent
    /// receiver starting from the point of subscription.
    pub fn subscribe(&self) -> broadcast::Receiver<Violation> {
        self.tx.subscribe()
    }

    /// Clone of the sender — lets the web server create per-connection
    /// receivers without holding a reference to the engine itself.
    pub fn sender(&self) -> broadcast::Sender<Violation> {
        self.tx.clone()
    }

    /// Shared handle to the current stream-health snapshot used by the REST API.
    pub fn state(&self) -> Arc<RwLock<Vec<StreamHealth>>> {
        Arc::clone(&self.state)
    }

    /// Shared handle to the history store — lets the web server serve
    /// per-consumer lag history without duplicating storage.
    /// Expose the underlying NATS client (Arc-wrapped) for the Apply Now backend.
    pub fn nats_client(&self) -> &NatsClient {
        &self.client
    }
    pub fn nats_client_arc(&self) -> Arc<NatsClient> {
        Arc::clone(&self.client)
    }

    pub fn history_store(&self) -> Arc<Mutex<HistoryStore>> {
        Arc::clone(&self.history)
    }

    /// Poll loop — runs forever, sleeping `poll_interval` between iterations.
    /// Errors on a single poll are logged and the loop continues.
    pub async fn run(&self, poll_interval: Duration) {
        info!("Engine started (poll_interval={poll_interval:?})");
        loop {
            if let Err(e) = self.poll_once().await {
                error!("Poll cycle failed: {e:#}");
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    async fn poll_once(&self) -> anyhow::Result<()> {
        let streams = self.client.list_streams().await?;
        let mut healths = Vec::with_capacity(streams.len());

        // Build the set of (stream, consumer) keys visible this poll cycle.
        // We use it below to evict deleted consumers from the history store,
        // preventing unbounded memory growth in environments with ephemeral consumers.
        let mut live_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        for stream in &streams {
            match self.poll_stream(stream, &mut live_keys).await {
                Ok(sh) => healths.push(sh),
                Err(e) => warn!("Skipping stream '{}': {e:#}", stream.config.name),
            }
        }

        // Evict consumers that no longer exist from the history store.
        {
            let mut history = self.history.lock().await;
            let stale: Vec<String> = history
                .keys()
                .filter(|k| !live_keys.contains(k.as_str()))
                .cloned()
                .collect();
            for key in stale {
                history.clear_consumer(&key);
            }
        }

        *self.state.write().await = healths;
        Ok(())
    }

    async fn poll_stream(
        &self,
        stream_info: &StreamInfo,
        live_keys: &mut std::collections::HashSet<String>,
    ) -> anyhow::Result<StreamHealth> {
        let stream_name = &stream_info.config.name;
        let consumer_names = self.client.list_consumer_names(stream_name).await?;
        let mut consumer_healths = Vec::with_capacity(consumer_names.len());

        for consumer_name in &consumer_names {
            live_keys.insert(format!("{stream_name}/{consumer_name}"));
            match self
                .poll_consumer(stream_name, consumer_name, stream_info)
                .await
            {
                Ok(ch) => consumer_healths.push(ch),
                Err(e) => warn!("Skipping consumer '{stream_name}/{consumer_name}': {e:#}"),
            }
        }

        let overall_health = aggregate_health(consumer_healths.iter().map(|ch| &ch.health));

        Ok(StreamHealth {
            stream_name: stream_name.clone(),
            config: stream_info.config.clone(),
            state: stream_info.state.clone(),
            health: overall_health,
            consumers: consumer_healths,
        })
    }

    async fn poll_consumer(
        &self,
        stream_name: &str,
        consumer_name: &str,
        stream_info: &StreamInfo,
    ) -> anyhow::Result<ConsumerHealth> {
        let info = self
            .client
            .consumer_info(stream_name, consumer_name)
            .await?;

        let snapshot = ConsumerSnapshot {
            stream_name: stream_name.to_string(),
            consumer_name: consumer_name.to_string(),
            num_pending: info.num_pending,
            num_ack_pending: info.num_ack_pending,
            num_redelivered: info.num_redelivered,
            max_ack_pending: info.config.max_ack_pending(),
            ack_wait_secs: info.config.ack_wait_secs(),
            delivered_stream_seq: info.delivered.stream_seq,
            ack_floor_stream_seq: info.ack_floor.stream_seq,
            stream_first_seq: stream_info.state.first_seq,
            stream_last_seq: stream_info.state.last_seq,
            captured_at: Utc::now(),
        };

        // Push snapshot to history, run detectors, compute rate — all in one
        // lock section to avoid holding the lock across an await.
        let (violations, redeliveries_per_min) = {
            let mut history = self.history.lock().await;
            history.push(snapshot.clone());
            // Trim the ring to the monotone suffix so that consumer recreation
            // (delete + recreate → num_redelivered resets to 0) doesn't poison
            // the delta calculation with stale high values.
            history.trim_to_monotone(&snapshot.key());

            let violations = detectors::run_all_detectors(&snapshot, &history);

            let key = snapshot.key();
            let snaps = history.get(&key);
            let redeliv_rate = compute_redelivery_rate(snaps);

            (violations, redeliv_rate)
        };

        // Broadcast each violation over the in-process channel and also publish
        // to NATS so any language (Go, Python, Java) can subscribe without
        // polling the REST API.  Both operations are best-effort; errors are
        // silently ignored so a publish failure never disrupts the poll loop.
        for v in &violations {
            let _ = self.tx.send(v.clone());
            if let Ok(payload) = serde_json::to_vec(v) {
                let subject = format!(
                    "nats.lens.health.violations.{}.{}",
                    v.stream_name, v.consumer_name
                );
                let _ = self
                    .client
                    .publish(&subject, bytes::Bytes::from(payload))
                    .await;
            }
        }

        let health = derive_health(&violations);

        Ok(ConsumerHealth {
            stream_name: stream_name.to_string(),
            consumer_name: consumer_name.to_string(),
            health,
            lag: snapshot.num_pending,
            redeliveries_per_min,
            violations,
            snapshot,
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn derive_health(violations: &[Violation]) -> Health {
    if violations.iter().any(|v| v.severity == Severity::Critical) {
        Health::Critical
    } else if !violations.is_empty() {
        Health::Warning
    } else {
        Health::Healthy
    }
}

fn aggregate_health<'a>(iter: impl Iterator<Item = &'a Health>) -> Health {
    iter.fold(Health::Healthy, |acc, h| match (&acc, h) {
        (Health::Critical, _) | (_, Health::Critical) => Health::Critical,
        (Health::Warning, _) | (_, Health::Warning) => Health::Warning,
        _ => Health::Healthy,
    })
}

fn compute_redelivery_rate(snaps: &[ConsumerSnapshot]) -> f64 {
    if snaps.len() < 2 {
        return 0.0;
    }
    let prev = &snaps[snaps.len() - 2];
    let curr = &snaps[snaps.len() - 1];
    let elapsed_secs = (curr.captured_at - prev.captured_at).num_milliseconds() as f64 / 1_000.0;
    if elapsed_secs <= 0.0 {
        return 0.0;
    }
    let delta = curr.num_redelivered.saturating_sub(prev.num_redelivered);
    (delta as f64 / elapsed_secs) * 60.0
}
