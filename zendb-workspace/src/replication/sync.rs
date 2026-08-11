//! Anti-entropy: receipt comparison, missing-range computation, recent event
//! cache, and range-based fetch from durable topics.
//!
//! The sync subsystem provides eventual consistency by periodically exchanging
//! receipt summaries with connected installations, computing which event
//! sequences we are missing, and fetching them. A bounded in-memory cache of
//! recent events avoids scanning durable storage for common hot-path fetches.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use zendb_types::{Event, EventId, InstallationId};

use super::batcher::table_order;
use super::wire::{EventRange, ReceiptSummary, TableBatch};
use crate::Result;
use crate::core::WorkspaceCore;

// ─── Recent event cache ──────────────────────────────────────────────────────

/// Bounded ring buffer of recently seen events indexed by EventId.
/// Serves Fetch requests from memory without topic scans.
pub(super) struct RecentCache {
    capacity: usize,
    ring: VecDeque<(String, EventId)>,
    index: HashMap<(String, EventId), Event>,
}

impl RecentCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            ring: VecDeque::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
        }
    }

    /// Record an event. Evicts the oldest entry when at capacity.
    pub(super) fn insert(&mut self, table: &str, event: &Event) {
        let id = event.id;
        let key = (table.to_owned(), id);
        if self.index.contains_key(&key) {
            return;
        }
        if self.ring.len() >= self.capacity {
            if let Some(evicted) = self.ring.pop_front() {
                self.index.remove(&evicted);
            }
        }
        self.ring.push_back(key.clone());
        self.index.insert(key, event.clone());
    }

    /// Try to fulfill ranges from the cache. Returns matched events grouped by
    /// table and the remaining unfulfilled ranges.
    pub(super) fn fetch(&self, ranges: &[EventRange]) -> (Vec<TableBatch>, Vec<EventRange>) {
        let mut found: HashMap<String, Vec<Event>> = HashMap::new();
        let mut remaining = Vec::new();

        for range in ranges {
            let mut any_missing = false;
            for seq in range.start..=range.end {
                let id = EventId {
                    author: range.author,
                    sequence: seq,
                };
                if let Some(event) = self.index.get(&(range.table.clone(), id)) {
                    found
                        .entry(range.table.clone())
                        .or_default()
                        .push(event.clone());
                } else {
                    any_missing = true;
                }
            }
            if any_missing {
                remaining.push(range.clone());
            }
        }

        let mut batches: Vec<TableBatch> = found
            .into_iter()
            .map(|(table, events)| TableBatch { table, events })
            .collect();
        batches.sort_unstable_by(|a, b| table_order(&a.table).cmp(&table_order(&b.table)));
        (batches, remaining)
    }
}

// ─── Receipt summaries ───────────────────────────────────────────────────────

/// Build table-scoped receipt summaries from the durable table causal states.
pub(super) fn receipt_summaries(core: &WorkspaceCore) -> Vec<ReceiptSummary> {
    let mut summaries = Vec::new();
    core.table_store.for_each(|table_name, table| {
        for (author, receipt) in table.read().receipt_summaries() {
            summaries.push(ReceiptSummary {
                table: table_name.to_owned(),
                author,
                max_seen: receipt.max_seen,
                missing: receipt
                    .missing
                    .iter()
                    .map(|range| (*range.start(), *range.end()))
                    .collect(),
            });
        }
    });
    summaries
}

// ─── Missing-range computation ───────────────────────────────────────────────

/// Given a remote peer's receipt summaries, compute which event ranges we are
/// missing that the peer claims to have.
pub(super) fn compute_missing(
    core: &WorkspaceCore,
    receipts: Vec<ReceiptSummary>,
    max_ranges: usize,
) -> Vec<EventRange> {
    let mut missing = Vec::new();
    for receipt in receipts {
        if receipt.max_seen == 0 {
            continue;
        }
        let mut cursor = 1_u64;
        for &(gap_start, gap_end) in &receipt.missing {
            let gap_start = gap_start.max(1);
            let gap_end = gap_end.min(receipt.max_seen);
            if gap_start > gap_end {
                continue;
            }
            // Scan the range [cursor, gap_start) for sequences we don't have.
            scan_range(
                core,
                &receipt.table,
                receipt.author,
                cursor,
                gap_start - 1,
                &mut missing,
                max_ranges,
            );
            if missing.len() >= max_ranges {
                return missing;
            }
            cursor = gap_end + 1;
        }
        // Scan from cursor to max_seen (the tail after all gaps).
        if cursor <= receipt.max_seen {
            scan_range(
                core,
                &receipt.table,
                receipt.author,
                cursor,
                receipt.max_seen,
                &mut missing,
                max_ranges,
            );
            if missing.len() >= max_ranges {
                return missing;
            }
        }
    }
    missing
}

/// Scan a contiguous range and emit sub-ranges for sequences we don't have.
fn scan_range(
    core: &WorkspaceCore,
    table_name: &str,
    author: InstallationId,
    from: u64,
    to: u64,
    out: &mut Vec<EventRange>,
    max_ranges: usize,
) {
    let Ok(table) = core.table_store.get(table_name) else {
        return;
    };
    let mut seq = from;
    while seq <= to && out.len() < max_ranges {
        // Skip sequences we already have.
        while seq <= to
            && table.read().contains_event(EventId {
                author,
                sequence: seq,
            })
        {
            seq += 1;
        }
        if seq > to {
            break;
        }
        let range_start = seq;
        // Extend through contiguous missing sequences.
        while seq <= to
            && !table.read().contains_event(EventId {
                author,
                sequence: seq,
            })
        {
            seq += 1;
        }
        out.push(EventRange {
            table: table_name.to_owned(),
            author,
            start: range_start,
            end: seq - 1,
        });
    }
}

// ─── Range-based fetch from durable topics ───────────────────────────────────

/// Read events matching the requested ranges from durable table topics.
pub(super) fn fetch_ranges(core: &WorkspaceCore, ranges: &[EventRange]) -> Result<Vec<TableBatch>> {
    let mut batches = Vec::new();

    let mut tables = Vec::new();
    core.table_store.for_each(|name, table| {
        tables.push((name.to_owned(), Arc::clone(table)));
    });
    tables.sort_unstable_by(|(a, _), (b, _)| table_order(a).cmp(&table_order(b)));

    for (table_name, handle) in tables {
        let reader = {
            let table = handle.read();
            table.reader()
        };
        let mut events = Vec::new();
        for record in reader {
            let (_, change) = record?;
            let event = change.event;
            let matches = ranges.iter().any(|r| {
                r.table == table_name
                    && r.author == event.id.author
                    && event.id.sequence >= r.start
                    && event.id.sequence <= r.end
            });
            if !matches {
                continue;
            }
            events.push(event);
        }
        if !events.is_empty() {
            batches.push(TableBatch {
                table: table_name,
                events,
            });
        }
    }
    Ok(batches)
}
