//! Detects **MaxPendingThrottle**: the consumer's `max_ack_pending` cap is too
//! low for its concurrency, so NATS stops delivering new messages even when
//! the consumer still has processing capacity.
//!
//! Detection heuristic
//! -------------------
//! If `num_ack_pending >= max_ack_pending` **and** `num_pending > 0`, the
//! in-flight window is full and new messages cannot be delivered.
//!
//! The recommended minimum is `ceil(max_ack_pending * 1.5)` — a 50 % headroom
//! that accommodates burst processing without reaching the cap under normal load.

use crate::history::HistoryStore;
use crate::types::{ConsumerSnapshot, Violation, ViolationType};

pub fn detect(current: &ConsumerSnapshot, _history: &HistoryStore) -> Option<Violation> {
    let max_ap = current.max_ack_pending;
    if max_ap <= 0 {
        return None;
    }

    // Consumer is only throttled when the window is full AND there is work waiting.
    if current.num_ack_pending < max_ap as u64 || current.num_pending == 0 {
        return None;
    }

    let pending_ratio_pct = (current.num_ack_pending as f64 / max_ap as f64) * 100.0;
    let recommended_min = (max_ap as f64 * 1.5).ceil() as i64;

    Some(Violation::new(
        current,
        ViolationType::MaxPendingThrottle {
            current_max_pending: max_ap,
            recommended_min,
            pending_ratio_pct,
        },
    ))
}
