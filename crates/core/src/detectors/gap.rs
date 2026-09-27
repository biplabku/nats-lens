//! Detects **SequenceGap**: the stream evicted messages before the consumer
//! could pull them, causing silent data loss.
//!
//! Detection heuristic
//! -------------------
//! If `stream_first_seq > ack_floor_stream_seq + 1` **and** the consumer has
//! already started consuming (ack_floor > 0), then at least one message was
//! deleted / expired from the stream before the consumer reached it.
//!
//! ```text
//!   stream: [first_seq … last_seq]
//!   consumer ack floor:  ack_floor_stream_seq
//!
//!   gap = [ack_floor_stream_seq+1, stream_first_seq-1]
//! ```
//!
//! This is a **Critical** violation because data was irreversibly lost.

use crate::history::HistoryStore;
use crate::types::{ConsumerSnapshot, Violation, ViolationType};

pub fn detect(current: &ConsumerSnapshot, _history: &HistoryStore) -> Option<Violation> {
    // Skip consumers that have not yet processed a single message — a gap
    // cannot be attributed to eviction if the consumer never pulled anything.
    if current.ack_floor_stream_seq == 0 {
        return None;
    }

    // No gap: the consumer's ack floor is at or past the stream's first seq.
    if current.stream_first_seq <= current.ack_floor_stream_seq + 1 {
        return None;
    }

    // Guard against u64 underflow (stream_first_seq is guaranteed > 1 here).
    let gap_start = current.ack_floor_stream_seq + 1;
    let gap_end = current.stream_first_seq - 1;
    let messages_lost = gap_end - gap_start + 1;

    Some(Violation::new(
        current,
        ViolationType::SequenceGap {
            gap_start,
            gap_end,
            messages_lost,
        },
    ))
}
