//! Serialized event batching for outbound Push messages.
//!
//! Events are encoded once when admitted and stored as length-delimited bytes
//! per table. The batch size is therefore measured from the bytes already
//! produced, without a separate sizing pass.

use std::collections::HashMap;
use tokio::time::Instant;
use zendb_types::{
    Event,
    utils::{deserialize_from, serialize_to_vec},
};

use super::wire::TableBatch;
use crate::config::BatchConfig;
use crate::system::{INSTALLATIONS_TABLE_NAME, TABLE_CATALOG_NAME};

pub(super) struct Batcher {
    config: BatchConfig,
    pending: HashMap<String, Vec<u8>>,
    pending_bytes: usize,
    deadline: Option<Instant>,
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
        let encoded_len = encoded.len() + std::mem::size_of::<u64>();
        let entry = self.pending.entry(table).or_default();
        entry.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
        entry.extend_from_slice(&encoded);
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

    /// Drain pending serialized events into sorted table batches.
    pub(super) fn flush(&mut self) -> Vec<TableBatch> {
        self.deadline = None;
        self.pending_bytes = 0;
        let pending = std::mem::take(&mut self.pending);
        let mut batches: Vec<TableBatch> = pending
            .into_iter()
            .map(|(table, bytes)| {
                let mut events = Vec::new();
                let mut offset = 0;
                while offset < bytes.len() {
                    let length = usize::try_from(u64::from_le_bytes(
                        bytes[offset..offset + std::mem::size_of::<u64>()]
                            .try_into()
                            .expect("event length prefix is complete"),
                    ))
                    .expect("event length fits in memory");
                    offset += std::mem::size_of::<u64>();
                    let end = offset + length;
                    let event: Event = deserialize_from(&bytes[offset..end])
                        .expect("serialized replication event must be decodable");
                    events.push(event);
                    offset = end;
                }
                TableBatch { table, events }
            })
            .collect();
        batches.sort_unstable_by(|a, b| {
            table_order(a.table.as_str()).cmp(&table_order(b.table.as_str()))
        });
        batches
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
