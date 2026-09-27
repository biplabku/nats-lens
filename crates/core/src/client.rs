use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::types::{ConsumerInfo, ConsumerNamesResponse, StreamInfo, StreamListResponse};

/// Thin wrapper around `async_nats::Client` that speaks the raw JetStream
/// management API (`$JS.API.*` subjects).  Using raw request-reply instead of
/// the typed high-level API means nats-lens can target any NATS server version
/// and any nats client crate release.
pub struct NatsClient {
    inner: async_nats::Client,
}

impl NatsClient {
    pub fn new(client: async_nats::Client) -> Self {
        Self { inner: client }
    }

    // ── Stream listing (paginated) ────────────────────────────────────────────

    /// Fetches ALL streams, handling JetStream pagination (default page = 256).
    /// Returns an empty vec when the server has no streams.
    pub async fn list_streams(&self) -> Result<Vec<StreamInfo>> {
        let mut all = Vec::new();
        let mut offset = 0usize;
        loop {
            let body = serde_json::json!({ "offset": offset });
            let msg = self
                .inner
                .request("$JS.API.STREAM.LIST", Bytes::from(serde_json::to_vec(&body)?))
                .await
                .map_err(|e| anyhow!("STREAM.LIST request failed: {e}"))?;

            let resp: StreamListResponse = serde_json::from_slice(&msg.payload)
                .map_err(|e| anyhow!("Failed to parse STREAM.LIST response: {e}"))?;

            if let Some(err) = resp.error {
                return Err(anyhow!("JetStream API error {}: {}", err.code, err.description));
            }

            let page = resp.streams.unwrap_or_default();
            let page_len = page.len();
            all.extend(page);

            // If we received fewer items than remain (total > received so far), fetch next page.
            if all.len() >= resp.total || page_len == 0 {
                break;
            }
            offset = all.len();
        }
        Ok(all)
    }

    // ── Consumer listing (paginated) ──────────────────────────────────────────

    /// Returns ALL consumer names for `stream`, handling JetStream pagination.
    pub async fn list_consumer_names(&self, stream: &str) -> Result<Vec<String>> {
        let subject = format!("$JS.API.CONSUMER.NAMES.{stream}");
        let mut all = Vec::new();
        let mut offset = 0usize;
        loop {
            let body = serde_json::json!({ "offset": offset });
            let msg = self
                .inner
                .request(subject.clone(), Bytes::from(serde_json::to_vec(&body)?))
                .await
                .map_err(|e| anyhow!("CONSUMER.NAMES request failed for {stream}: {e}"))?;

            let resp: ConsumerNamesResponse = serde_json::from_slice(&msg.payload)
                .map_err(|e| anyhow!("Failed to parse CONSUMER.NAMES response: {e}"))?;

            if let Some(err) = resp.error {
                return Err(anyhow!("JetStream API error {}: {}", err.code, err.description));
            }

            let page = resp.consumers.unwrap_or_default();
            let page_len = page.len();
            all.extend(page);

            if all.len() >= resp.total || page_len == 0 {
                break;
            }
            offset = all.len();
        }
        Ok(all)
    }

    // ── Consumer info ─────────────────────────────────────────────────────────

    /// Fetches full consumer metadata from
    /// `$JS.API.CONSUMER.INFO.{stream}.{consumer}`.
    ///
    /// Handles the case where the server returns an API-level error JSON
    /// (e.g. 404 consumer not found) gracefully by converting it into an
    /// `Err(anyhow::Error)` rather than a parse failure.
    pub async fn consumer_info(&self, stream: &str, consumer: &str) -> Result<ConsumerInfo> {
        let subject = format!("$JS.API.CONSUMER.INFO.{stream}.{consumer}");
        let msg = self
            .inner
            .request(subject, Bytes::from_static(b""))
            .await
            .map_err(|e| anyhow!("CONSUMER.INFO request failed for {stream}/{consumer}: {e}"))?;

        // Parse as a generic Value first so we can inspect the error field before
        // attempting to deserialize the full ConsumerInfo struct — this avoids a
        // serde failure when the server returns only {"error": {...}}.
        let value: serde_json::Value = serde_json::from_slice(&msg.payload)
            .map_err(|e| anyhow!("Failed to parse CONSUMER.INFO response: {e}"))?;

        if let Some(err_obj) = value.get("error") {
            let code = err_obj.get("code").and_then(|v| v.as_u64()).unwrap_or(0);
            let desc = err_obj
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow!("JetStream API error {code}: {desc}"));
        }

        let info: ConsumerInfo = serde_json::from_value(value)
            .map_err(|e| anyhow!("Failed to decode ConsumerInfo for {stream}/{consumer}: {e}"))?;

        Ok(info)
    }

    // ── Publishing ────────────────────────────────────────────────────────────

    pub async fn publish(&self, subject: &str, payload: Bytes) -> Result<()> {
        self.inner
            .publish(subject.to_string(), payload)
            .await
            .map_err(|e| anyhow!("publish to {subject} failed: {e}"))?;
        Ok(())
    }

    // ── Consumer update (Apply Now) ───────────────────────────────────────────

    /// Update `ack_wait` for a consumer via `$JS.API.CONSUMER.UPDATE`.
    /// `ack_wait_secs` is converted to nanoseconds as required by the NATS API.
    pub async fn update_ack_wait(
        &self,
        stream:        &str,
        consumer:      &str,
        ack_wait_secs: u64,
    ) -> Result<()> {
        let subject = format!("$JS.API.CONSUMER.UPDATE.{stream}.{consumer}");
        let body = serde_json::json!({
            "stream_name": stream,
            "config": {
                "durable_name": consumer,
                // NATS expects ack_wait in nanoseconds
                "ack_wait": ack_wait_secs * 1_000_000_000u64
            }
        });
        let payload = Bytes::from(serde_json::to_vec(&body)?);
        let msg = self.inner
            .request(subject.clone(), payload)
            .await
            .map_err(|e| anyhow!("CONSUMER.UPDATE request failed: {e}"))?;
        check_js_error(&msg.payload, &subject)
    }

    /// Update `max_ack_pending` for a consumer.
    pub async fn update_max_ack_pending(
        &self,
        stream:          &str,
        consumer:        &str,
        max_ack_pending: i64,
    ) -> Result<()> {
        let subject = format!("$JS.API.CONSUMER.UPDATE.{stream}.{consumer}");
        let body = serde_json::json!({
            "stream_name": stream,
            "config": {
                "durable_name": consumer,
                "max_ack_pending": max_ack_pending
            }
        });
        let payload = Bytes::from(serde_json::to_vec(&body)?);
        let msg = self.inner
            .request(subject.clone(), payload)
            .await
            .map_err(|e| anyhow!("CONSUMER.UPDATE request failed: {e}"))?;
        check_js_error(&msg.payload, &subject)
    }

    /// Update stream `max_msgs` via `$JS.API.STREAM.UPDATE`.
    pub async fn update_stream_max_msgs(
        &self,
        stream:   &str,
        max_msgs: i64,
    ) -> Result<()> {
        let subject = format!("$JS.API.STREAM.UPDATE.{stream}");
        let body = serde_json::json!({ "name": stream, "max_msgs": max_msgs });
        let payload = Bytes::from(serde_json::to_vec(&body)?);
        let msg = self.inner
            .request(subject.clone(), payload)
            .await
            .map_err(|e| anyhow!("STREAM.UPDATE request failed: {e}"))?;
        check_js_error(&msg.payload, &subject)
    }
}

fn check_js_error(payload: &Bytes, subject: &str) -> Result<()> {
    let v: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|e| anyhow!("Failed to parse {subject} response: {e}"))?;
    if let Some(err) = v.get("error") {
        let code = err.get("code").and_then(|c| c.as_u64()).unwrap_or(0);
        let desc = err.get("description").and_then(|d| d.as_str()).unwrap_or("unknown");
        return Err(anyhow!("JetStream API error {code}: {desc}"));
    }
    Ok(())
}
