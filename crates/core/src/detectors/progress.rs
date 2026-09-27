//! Detects **MissingProgress**: tasks are long-running but the consumer is not
//! sending in-progress acks, so `ack_wait` fires and NATS redelivers the
//! message to another worker — causing duplicate execution mid-processing.
//!
//! Detection heuristic
//! -------------------
//! If the ack-pending ratio (`num_ack_pending / max_ack_pending`) exceeds 90 %
//! **and** `ack_wait` is greater than 30 seconds, the tasks are likely long
//! enough to warrant periodic `MsgInProgress` acks.
//!
//! This is a **Warning** because it does not guarantee duplicates have already
//! occurred, but the configuration is fragile under processing time variation.

use crate::history::HistoryStore;
use crate::types::{ConsumerSnapshot, Violation, ViolationType};

const RATIO_THRESHOLD: f64 = 0.9;
const MIN_ACK_WAIT_SECS: u64 = 30;

pub fn detect(current: &ConsumerSnapshot, _history: &HistoryStore) -> Option<Violation> {
    if current.pending_ratio() <= RATIO_THRESHOLD {
        return None;
    }
    if current.ack_wait_secs <= MIN_ACK_WAIT_SECS {
        return None;
    }

    let ack_pending_ratio_pct = current.pending_ratio() * 100.0;

    Some(Violation::new(
        current,
        ViolationType::MissingProgress {
            ack_pending_ratio_pct,
            ack_wait_secs: current.ack_wait_secs,
        },
    ))
}
