pub mod ack_wait;
pub mod gap;
pub mod nak_storm;
pub mod progress;
pub mod throttle;

use crate::history::HistoryStore;
use crate::types::{ConsumerSnapshot, Violation};

/// Run every detector against `current` and return all violations found.
///
/// The caller is expected to have already pushed `current` into `history`
/// before calling this function so that rate-based detectors can see it as
/// the latest data point.
///
/// Detection order: gap first (data-loss, highest urgency), then redelivery
/// detectors, then configuration-quality warnings.
pub fn run_all_detectors(current: &ConsumerSnapshot, history: &HistoryStore) -> Vec<Violation> {
    let mut violations = Vec::new();

    // Critical: data already lost — check first.
    if let Some(v) = gap::detect(current, history) {
        violations.push(v);
    }

    // Critical: redelivery loop caused by under-sized ack_wait.
    if let Some(v) = ack_wait::detect(current, history) {
        violations.push(v);
    }

    // Critical: NAK storm — consumer in a pointless redelivery loop.
    if let Some(v) = nak_storm::detect(current, history) {
        violations.push(v);
    }

    // Warning: delivery throttled by max_ack_pending.
    if let Some(v) = throttle::detect(current, history) {
        violations.push(v);
    }

    // Warning: long-running tasks without in-progress acks.
    if let Some(v) = progress::detect(current, history) {
        violations.push(v);
    }

    violations
}
