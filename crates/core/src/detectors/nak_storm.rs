//! Detects **NakStorm**: the consumer has a persistent redelivery loop — messages
//! are being refused (NAKed or timed out) and redelivered continuously without
//! making progress.
//!
//! ## `num_redelivered` semantics
//!
//! In NATS JetStream, `num_redelivered` counts the number of **distinct messages
//! that have been redelivered at least once** since the consumer was created.
//! It grows when new messages enter the redelivery cycle (different messages
//! each time), but it does NOT grow when the same messages are redelivered
//! repeatedly — making it behave like a semi-gauge for persistent loops.
//!
//! ## Detection heuristic
//!
//! A NAK storm produces a stable, non-zero `num_redelivered` (same N messages
//! stuck in the cycle) combined with non-zero `num_ack_pending` (consumer is
//! actively in flight).  We require this condition to hold across ≥ 2 consecutive
//! snapshots to avoid firing on single transient redeliveries.
//!
//! Threshold: num_redelivered ≥ MIN_REDELIVERED across 2+ polls.

use crate::history::HistoryStore;
use crate::types::{ConsumerSnapshot, Violation, ViolationType};

const MIN_REDELIVERED: u64 = 2;

pub fn detect(current: &ConsumerSnapshot, history: &HistoryStore) -> Option<Violation> {
    let key   = current.key();
    let snaps = history.get(&key);

    // Require 2+ snapshots to confirm the condition is sustained.
    if snaps.len() < 2 {
        return None;
    }

    let prev = &snaps[snaps.len() - 2];

    // Both snapshots must show redeliveries — rules out a single transient event.
    if current.num_redelivered < MIN_REDELIVERED || prev.num_redelivered < MIN_REDELIVERED {
        return None;
    }

    // Consumer must be actively in-flight (not idle between polls).
    if current.num_ack_pending == 0 {
        return None;
    }

    // Compute the delta to use as the "rate" proxy for the violation description.
    // For a persistent NAK loop over the same messages, the delta is 0 (stable gauge).
    // For an expanding loop (new messages entering), the delta is positive.
    let elapsed_secs =
        (current.captured_at - prev.captured_at).num_milliseconds() as f64 / 1_000.0;
    let redeliv_delta = current.num_redelivered.saturating_sub(prev.num_redelivered);
    let redelivery_rate_per_min = if elapsed_secs > 0.0 && redeliv_delta > 0 {
        (redeliv_delta as f64 / elapsed_secs) * 60.0
    } else {
        // Stable num_redelivered: express as "N messages stuck in loop"
        current.num_redelivered as f64
    };

    Some(Violation::new(
        current,
        ViolationType::NakStorm {
            redelivery_rate_per_min,
            // lag_growth_per_min: how many more messages entered the redelivery
            // cycle per minute. Zero means the same N messages are looping.
            lag_growth_per_min: if elapsed_secs > 0.0 {
                (redeliv_delta as f64 / elapsed_secs) * 60.0
            } else {
                0.0
            },
        },
    ))
}
