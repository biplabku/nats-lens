//! Detects **AckWaitViolation**: the consumer's configured `ack_wait` is
//! shorter than the actual message processing time, causing NATS to redeliver
//! messages that are still in flight — leading to duplicate execution.
//!
//! Detection heuristic
//! -------------------
//! If the number of redeliveries grew by more than 5 between the two most
//! recent snapshots **and** the implied rate exceeds 5 redeliveries/minute,
//! the consumer is likely processing slower than `ack_wait` allows.
//!
//! Recommended `ack_wait`
//! -----------------------
//! Without application-level P99 instrumentation we estimate the actual
//! processing time as `ack_wait * 0.9` (conservative lower bound — if the
//! current `ack_wait` were long enough there would be no redeliveries, so
//! the real P99 is at least 90 % of the current setting).  We then target
//! `max(estimate, 120 s) + 60 s` to give a generous headroom.
//!
//! Fix
//! ---
//! Emits a ready-to-paste `nats consumer edit` command.

use crate::history::HistoryStore;
use crate::types::{ConsumerSnapshot, Violation, ViolationType};

// A delta of 2+ redeliveries per poll cycle is enough to indicate ack_wait
// is shorter than processing time. Using 5 was too high relative to small
// max_ack_pending values (e.g., 5 messages = exactly 5 redeliveries → misses).
const MIN_REDELIVERY_DELTA: u64 = 2;
const MIN_REDELIVERY_RATE: f64 = 2.0; // per minute

pub fn detect(current: &ConsumerSnapshot, history: &HistoryStore) -> Option<Violation> {
    let key = current.key();
    let snaps = history.get(&key);

    // Need at least two data points to compute a rate.
    if snaps.len() < 2 {
        return None;
    }

    let prev = &snaps[snaps.len() - 2];
    let now = &snaps[snaps.len() - 1];

    let elapsed_secs =
        (now.captured_at - prev.captured_at).num_milliseconds() as f64 / 1_000.0;
    if elapsed_secs <= 0.0 {
        return None;
    }

    let redeliv_delta = now.num_redelivered.saturating_sub(prev.num_redelivered);
    if redeliv_delta < MIN_REDELIVERY_DELTA {
        return None;
    }

    let redeliveries_per_min = (redeliv_delta as f64 / elapsed_secs) * 60.0;
    if redeliveries_per_min < MIN_REDELIVERY_RATE {
        return None;
    }

    let ack_wait_secs = current.ack_wait_secs;

    // Conservative P99 estimate: current ack_wait * 0.9 (if ack_wait were
    // sufficient there would be no redeliveries; real P99 >= 90% of current).
    let p99_estimate_secs = (ack_wait_secs as f64 * 0.9) as u64;
    let recommended_ack_wait_secs = p99_estimate_secs.max(120) + 60;

    let fix_command = format!(
        "nats consumer edit {} {} --ack-wait {}s",
        current.stream_name, current.consumer_name, recommended_ack_wait_secs
    );

    Some(Violation::new(
        current,
        ViolationType::AckWaitViolation {
            redeliveries_per_min,
            current_ack_wait_secs: ack_wait_secs,
            recommended_ack_wait_secs,
            fix_command,
        },
    ))
}
