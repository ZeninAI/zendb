//! Pure per-table event batching for outbound replication envelopes.

use std::collections::HashMap;

use zendb_types::{CompactEvent, Envelope, Event, InstallationId, utils::serdes::serialized_size};

use super::BatchConfig;

struct PendingBatch {
    events: Vec<CompactEvent>,
    bytes: usize,
}

pub(super) struct Batcher {
    author: InstallationId,
    config: BatchConfig,
    pending: HashMap<String, PendingBatch>,
}

impl Batcher {
    pub(super) fn new(author: InstallationId, config: BatchConfig) -> Self {
        Self {
            author,
            config,
            pending: HashMap::new(),
        }
    }

    pub(super) fn push(&mut self, table: String, event: Event) -> Option<Envelope> {
        let compact = CompactEvent {
            sequence: event.stamp.id.sequence,
            time: event.stamp.time,
            primary_key: event.primary_key,
            path: event.path,
            op: event.op,
        };
        let event_bytes = serialized_size(&compact).unwrap_or(0);
        let batch = self
            .pending
            .entry(table.clone())
            .or_insert_with(|| PendingBatch {
                events: Vec::new(),
                bytes: serialized_size(&Envelope {
                    author: self.author,
                    table: table.clone(),
                    events: Vec::new(),
                })
                .unwrap_or(0),
            });
        batch.bytes = batch.bytes.saturating_add(event_bytes);
        batch.events.push(compact);
        (batch.events.len() >= self.config.max_events || batch.bytes >= self.config.max_bytes)
            .then(|| self.pending.remove(&table))
            .flatten()
            .map(|batch| Envelope {
                author: self.author,
                table,
                events: batch.events,
            })
    }

    pub(super) fn drain(&mut self) -> Vec<Envelope> {
        self.pending
            .drain()
            .map(|(table, batch)| Envelope {
                author: self.author,
                table,
                events: batch.events,
            })
            .collect()
    }
}
