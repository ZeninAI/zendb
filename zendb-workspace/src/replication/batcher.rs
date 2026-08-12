//! Serialized event batching for outbound Push messages.
//!
//! Events are encoded once when admitted and stored as length-delimited bytes
//! per table. The resulting payloads are passed directly to the wire message.

use std::collections::HashMap;
use tokio::time::Instant;
use zendb_types::{Event, utils::serialize_to_vec};

use super::wire::encode_serialized_table_batch;
use crate::config::BatchConfig;
use crate::system::{INSTALLATIONS_TABLE_NAME, TABLE_CATALOG_NAME};

pub(super) struct Batcher {
    config: BatchConfig,
    pending: HashMap<String, PendingBatch>,
    pending_bytes: usize,
    deadline: Option<Instant>,
}

struct PendingBatch {
    event_count: usize,
    event_bytes: Vec<u8>,
}

impl Batcher {
    pub(super) fn new(config: BatchConfig) -> Self {
        Self {
            config,
            pending: HashMap::new(),
            pending_bytes: 0,
            deadline: None,
        }
    }

    /// Enqueue an event and report whether the resulting batch crossed its
    /// soft size threshold.
    pub(super) fn push(&mut self, table: String, event: Event) -> bool {
        let encoded = serialize_to_vec(&event).expect("replication events must be serializable");
        let encoded_len = encoded.len() + std::mem::size_of::<u32>();
        let entry = self.pending.entry(table).or_insert_with(|| PendingBatch {
            event_count: 0,
            event_bytes: Vec::new(),
        });
        entry.event_count += 1;
        entry
            .event_bytes
            .extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        entry.event_bytes.extend_from_slice(&encoded);
        self.pending_bytes = self.pending_bytes.saturating_add(encoded_len);

        if self.pending_bytes == encoded_len {
            if let Some(linger) = self.config.linger {
                self.deadline = Some(Instant::now() + linger);
            }
        }
        self.pending_bytes > self.config.batch_size
    }

    /// Returns the instant at which the batcher should be flushed, or `None`
    /// when completion is controlled only by the size threshold.
    pub(super) fn next_flush(&self) -> Option<Instant> {
        if self.pending.is_empty() {
            None
        } else {
            self.deadline
        }
    }

    pub(super) fn is_due(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| deadline <= Instant::now())
    }

    /// Drain pending serialized events into sorted table payloads.
    pub(super) fn flush(&mut self) -> Vec<Vec<u8>> {
        self.deadline = None;
        self.pending_bytes = 0;
        let mut pending: Vec<_> = std::mem::take(&mut self.pending).into_iter().collect();
        pending
            .sort_unstable_by(|(left, _), (right, _)| table_order(left).cmp(&table_order(right)));
        pending
            .into_iter()
            .map(|(table, batch)| {
                encode_serialized_table_batch(&table, batch.event_count, &batch.event_bytes)
            })
            .collect()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Ordering key: catalog first, installations second, application tables last.
pub(super) fn table_order(name: &str) -> (u8, &str) {
    if name == TABLE_CATALOG_NAME {
        (0, name)
    } else if name == INSTALLATIONS_TABLE_NAME {
        (1, name)
    } else {
        (2, name)
    }
}
