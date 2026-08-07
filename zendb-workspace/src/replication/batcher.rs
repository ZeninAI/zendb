//! Linger-based event batching for outbound Push messages.
//!
//! Events arriving from the workspace are accumulated per-table. When the
//! linger duration expires (or immediately if linger is zero), the batcher
//! produces sorted `TableBatch` vectors ready for broadcast.

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;
use zendb_types::Event;

use super::wire::TableBatch;
use crate::system::{INSTALLATIONS_TABLE_NAME, TABLE_CATALOG_NAME};

pub(super) struct Batcher {
    linger: Duration,
    pending: HashMap<String, Vec<Event>>,
    deadline: Option<Instant>,
}

impl Batcher {
    pub(super) fn new(linger: Duration) -> Self {
        Self {
            linger,
            pending: HashMap::new(),
            deadline: None,
        }
    }

    /// Enqueue an event for batched delivery.
    pub(super) fn push(&mut self, table: String, event: Event) {
        self.pending.entry(table).or_default().push(event);
        if !self.linger.is_zero() && self.deadline.is_none() {
            self.deadline = Some(Instant::now() + self.linger);
        }
    }

    /// Returns the instant at which the batcher should be flushed, or `None`
    /// if there is nothing pending.
    pub(super) fn next_flush(&self) -> Option<Instant> {
        if self.pending.is_empty() {
            return None;
        }
        if self.linger.is_zero() {
            // Immediate: schedule for "now" so tokio::select! fires instantly.
            return Some(Instant::now());
        }
        self.deadline
    }

    /// Drain all pending events into sorted table batches.
    /// System tables (catalog, installations) are ordered first so that
    /// table-creation events arrive before application events.
    pub(super) fn flush(&mut self) -> Vec<TableBatch> {
        self.deadline = None;
        let mut batches: Vec<TableBatch> = self
            .pending
            .drain()
            .map(|(table, events)| TableBatch { table, events })
            .collect();
        batches.sort_unstable_by(|a, b| table_order(a.table.as_str()).cmp(&table_order(b.table.as_str())));
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
