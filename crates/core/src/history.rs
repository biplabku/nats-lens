use std::collections::HashMap;

use crate::types::ConsumerSnapshot;

/// Maximum number of snapshots kept per consumer key.
const MAX_HISTORY: usize = 30;

/// Stores the last [`MAX_HISTORY`] snapshots for every consumer seen,
/// keyed by `"{stream_name}/{consumer_name}"`.
///
/// The oldest snapshot is evicted when the ring is full.  Because the window
/// is small (≤ 30 entries) a plain `Vec` is used: `remove(0)` is O(n) but
/// negligible at this size, and `as_slice()` returns a contiguous borrow
/// which lets detectors do simple index arithmetic without a copy.
pub struct HistoryStore {
    data: HashMap<String, Vec<ConsumerSnapshot>>,
}

impl HistoryStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Appends `snapshot` to the ring for its consumer key, evicting the
    /// oldest entry if the ring is already at capacity.
    pub fn push(&mut self, snapshot: ConsumerSnapshot) {
        let key = snapshot.key();
        let ring = self.data.entry(key).or_default();
        ring.push(snapshot);
        if ring.len() > MAX_HISTORY {
            ring.remove(0);
        }
    }

    /// Returns a view of all stored snapshots for `key` in oldest-first order.
    /// Returns an empty slice when the key is unknown.
    pub fn get(&self, key: &str) -> &[ConsumerSnapshot] {
        self.data.get(key).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Returns the most recent snapshot for `key`, or `None` if unknown.
    pub fn latest(&self, key: &str) -> Option<&ConsumerSnapshot> {
        self.data.get(key)?.last()
    }

    /// Iterate over all consumer keys currently in the store.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.data.keys()
    }

    /// Remove all history for a specific consumer key.
    /// Used by the eval harness between rounds when a consumer is deleted and
    /// recreated — otherwise old num_redelivered values poison the next round.
    pub fn clear_consumer(&mut self, key: &str) {
        self.data.remove(key);
    }

    /// Trim the history for `key` to only keep snapshots where `num_redelivered`
    /// is monotonically increasing from the end.  This handles the case where a
    /// consumer is deleted and recreated mid-ring: the new snapshots have lower
    /// num_redelivered, which makes saturating_sub produce 0 deltas.
    pub fn trim_to_monotone(&mut self, key: &str) {
        let Some(ring) = self.data.get_mut(key) else { return };
        // Find the last reset point (where num_redelivered decreased).
        let mut reset_idx = 0;
        for i in 1..ring.len() {
            if ring[i].num_redelivered < ring[i - 1].num_redelivered {
                reset_idx = i;
            }
        }
        if reset_idx > 0 {
            ring.drain(..reset_idx);
        }
    }
}

impl Default for HistoryStore {
    fn default() -> Self {
        Self::new()
    }
}
