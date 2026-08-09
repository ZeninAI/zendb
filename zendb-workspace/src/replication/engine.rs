//! Replication engine: message routing, admission control, commit, and broadcast.
//!
//! The engine is a plain struct (not a NetworkBehaviour). It processes inbound
//! messages and workspace notifications, returning outbound `(PeerId, Message)`
//! pairs that the controller dispatches through the swarm.
//!
//! Admission policy:
//! - Only peers whose installation is Active locally are accepted.
//! - Unknown or Pending peers are recorded as Pending in the installations
//!   table and immediately rejected with `Error(NotAdmitted)`.
//! - Once an installation transitions to Active, the runtime dials it.

use std::sync::Weak;

use libp2p::PeerId;
use zendb_types::{
    Installation, InstallationId, InstallationState, Multiaddr, Op, Path, PublicKey, TypeOp,
};

use super::batcher::table_order;
use super::sync::{self, RecentCache};
use super::wire::{EventRange, Message, ProtocolError, ReceiptSummary, TableBatch};
use crate::config::SyncConfig;
use crate::core::WorkspaceCore;
use crate::system::INSTALLATIONS_TABLE_NAME;
use crate::{Error, Result};

// ─── Engine ──────────────────────────────────────────────────────────────────

pub(super) struct Engine {
    core: Weak<WorkspaceCore>,
    local_peer_id: PeerId,
    config: SyncConfig,
    pub(super) cache: RecentCache,
}

impl Engine {
    pub(super) fn new(
        core: Weak<WorkspaceCore>,
        local_peer_id: PeerId,
        config: SyncConfig,
    ) -> Self {
        let cache_cap = config.recent_cache_capacity;
        Self {
            core,
            local_peer_id,
            config,
            cache: RecentCache::new(cache_cap),
        }
    }

    // ── Startup ──────────────────────────────────────────────────────────

    /// Return every active remote installation and its current route hints.
    pub(super) fn active_peer_routes(&self) -> Vec<(PeerId, Vec<libp2p::Multiaddr>)> {
        let Some(core) = self.core.upgrade() else {
            return Vec::new();
        };
        core.membership
            .list()
            .into_iter()
            .filter_map(|(_, installation)| {
                if !installation.state.is_active() {
                    return None;
                }
                let peer_id = installation.public_key.as_libp2p().to_peer_id();
                (peer_id != self.local_peer_id).then(|| {
                    (
                        peer_id,
                        installation
                            .addresses
                            .iter()
                            .map(|address| address.as_libp2p().clone())
                            .collect(),
                    )
                })
            })
            .collect()
    }

    pub(super) fn is_active_peer(&self, peer_id: PeerId) -> bool {
        self.active_peer_routes()
            .into_iter()
            .any(|(candidate, _)| candidate == peer_id)
    }

    pub(super) fn addresses(&self, peer_id: PeerId) -> Vec<libp2p::Multiaddr> {
        self.active_peer_routes()
            .into_iter()
            .find_map(|(candidate, addresses)| (candidate == peer_id).then_some(addresses))
            .unwrap_or_default()
    }

    // ── Anti-entropy ─────────────────────────────────────────────────────

    /// Send receipt summaries to every active remote installation.
    pub(super) fn send_summaries(&self) -> Vec<(PeerId, Message)> {
        let Some(core) = self.core.upgrade() else {
            return Vec::new();
        };
        let receipts = sync::receipt_summaries(&core);
        self.active_peer_routes()
            .into_iter()
            .map(|(peer_id, _)| peer_id)
            .map(|peer_id| {
                (
                    peer_id,
                    Message::Summary {
                        receipts: receipts.clone(),
                    },
                )
            })
            .collect()
    }

    // ── Inbound message handling ─────────────────────────────────────────

    /// Process a handshake from a newly connected peer. Returns outbound
    /// messages (typically an error if the peer is not admitted).
    pub(super) fn session_established(
        &mut self,
        peer_id: PeerId,
        installation_id: InstallationId,
        display_name: String,
        public_key: PublicKey,
        addresses: Vec<Multiaddr>,
    ) -> Vec<(PeerId, Message)> {
        let Some(core) = self.core.upgrade() else {
            return vec![(peer_id, Message::Error(ProtocolError::Internal))];
        };

        match core.membership.get(&installation_id) {
            Some(existing) if existing.public_key != public_key => {
                return vec![(peer_id, Message::Error(ProtocolError::IdentityMismatch))];
            }
            Some(existing) if existing.state.is_active() => {
                // Peer is admitted; session proceeds normally.
                return Vec::new();
            }
            Some(_) => {
                // Known but not Active (Pending or Rejected). Reject.
                return vec![(peer_id, Message::Error(ProtocolError::NotAdmitted))];
            }
            None => {
                // Unknown peer. Record as Pending and reject.
                let pending = Installation {
                    display_name,
                    public_key,
                    addresses,
                    state: InstallationState::Pending,
                };
                let _ = core
                    .table_store
                    .get(INSTALLATIONS_TABLE_NAME)
                    .and_then(|table| {
                        core.commit_change(
                            &table,
                            installation_id.into(),
                            Path::new(),
                            Op::Type(TypeOp::Installation(pending.set())),
                        )
                    });
                return vec![(peer_id, Message::Error(ProtocolError::NotAdmitted))];
            }
        }
    }

    /// Route an inbound protocol message from an established, active peer.
    pub(super) fn receive(&mut self, peer_id: PeerId, msg: Message) -> Vec<(PeerId, Message)> {
        let Some(core) = self.core.upgrade() else {
            return Vec::new();
        };
        if !self.is_active_peer(peer_id) {
            return vec![(peer_id, Message::Error(ProtocolError::Unauthorized))];
        }

        match msg {
            Message::Push { batches } => self.handle_push(&core, peer_id, batches),
            Message::Summary { receipts } => self.handle_summary(&core, peer_id, receipts),
            Message::SummaryResponse { receipts } => {
                self.handle_summary_response(&core, peer_id, receipts)
            }
            Message::Fetch { ranges } => self.handle_fetch(&core, peer_id, ranges),
            Message::FetchResponse { batches } => self.handle_push(&core, peer_id, batches),
            Message::Error(_) | Message::Handshake { .. } => Vec::new(),
        }
    }

    // ── Broadcast ────────────────────────────────────────────────────────

    /// Broadcast table batches to every active remote installation, excluding
    /// one peer when forwarding received batches.
    pub(super) fn broadcast(
        &self,
        batches: &[TableBatch],
        exclude: Option<PeerId>,
    ) -> Vec<(PeerId, Message)> {
        if batches.is_empty() {
            return Vec::new();
        }
        self.active_peer_routes()
            .into_iter()
            .map(|(peer_id, _)| peer_id)
            .filter(|peer_id| Some(*peer_id) != exclude)
            .map(|peer_id| {
                (
                    peer_id,
                    Message::Push {
                        batches: batches.to_vec(),
                    },
                )
            })
            .collect()
    }

    // ── Private helpers ──────────────────────────────────────────────────

    fn handle_push(
        &mut self,
        core: &WorkspaceCore,
        peer_id: PeerId,
        batches: Vec<TableBatch>,
    ) -> Vec<(PeerId, Message)> {
        match commit_batches(core, batches) {
            Ok(novel) => {
                // Cache novel events for fast future fetch responses.
                for batch in &novel {
                    for event in &batch.events {
                        self.cache.insert(&batch.table, event);
                    }
                }
                self.broadcast(&novel, Some(peer_id))
            }
            Err(_) => vec![(peer_id, Message::Error(ProtocolError::Internal))],
        }
    }

    fn handle_summary(
        &self,
        core: &WorkspaceCore,
        peer_id: PeerId,
        receipts: Vec<ReceiptSummary>,
    ) -> Vec<(PeerId, Message)> {
        let mut outbound = vec![(
            peer_id,
            Message::SummaryResponse {
                receipts: sync::receipt_summaries(core),
            },
        )];
        let ranges = sync::compute_missing(core, receipts, self.config.max_ranges);
        if !ranges.is_empty() {
            outbound.push((peer_id, Message::Fetch { ranges }));
        }
        outbound
    }

    fn handle_summary_response(
        &self,
        core: &WorkspaceCore,
        peer_id: PeerId,
        receipts: Vec<ReceiptSummary>,
    ) -> Vec<(PeerId, Message)> {
        let ranges = sync::compute_missing(core, receipts, self.config.max_ranges);
        if ranges.is_empty() {
            Vec::new()
        } else {
            vec![(peer_id, Message::Fetch { ranges })]
        }
    }

    fn handle_fetch(
        &mut self,
        core: &WorkspaceCore,
        peer_id: PeerId,
        ranges: Vec<EventRange>,
    ) -> Vec<(PeerId, Message)> {
        // Try serving from the recent cache first.
        let (cached_batches, remaining) = self.cache.fetch(&ranges);
        if remaining.is_empty() && !cached_batches.is_empty() {
            return vec![(
                peer_id,
                Message::FetchResponse {
                    batches: cached_batches,
                },
            )];
        }

        // Fall through to durable topic scan for remaining ranges.
        let scan_ranges = if remaining.is_empty() {
            &ranges
        } else {
            &remaining
        };
        match sync::fetch_ranges(core, scan_ranges) {
            Ok(mut batches) => {
                if !cached_batches.is_empty() {
                    // Merge cached results with topic results.
                    batches.extend(cached_batches);
                    batches
                        .sort_unstable_by(|a, b| table_order(&a.table).cmp(&table_order(&b.table)));
                }
                vec![(peer_id, Message::FetchResponse { batches })]
            }
            Err(_) => vec![(peer_id, Message::Error(ProtocolError::Internal))],
        }
    }
}

// ─── Free functions ──────────────────────────────────────────────────────────

/// Commit received batches to the workspace. Returns only novel events.
fn commit_batches(core: &WorkspaceCore, mut batches: Vec<TableBatch>) -> Result<Vec<TableBatch>> {
    batches.sort_unstable_by(|a, b| table_order(&a.table).cmp(&table_order(&b.table)));
    let mut novel = Vec::new();
    for batch in batches {
        let mut events = Vec::new();
        for event in batch.events {
            match core.commit_replication_event(&batch.table, event.clone()) {
                Ok(true) => events.push(event),
                Ok(false) | Err(Error::TableNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        if !events.is_empty() {
            novel.push(TableBatch {
                table: batch.table,
                events,
            });
        }
    }
    Ok(novel)
}
