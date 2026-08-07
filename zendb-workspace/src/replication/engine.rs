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
//! - Once an installation transitions to Active, the mesh dials it.

use std::sync::Weak;

use libp2p::PeerId;
use zendb_types::{
    Installation, InstallationId, InstallationState, Multiaddr, Op, Path, PublicKey, TypeOp,
};

use super::batcher::table_order;
use super::config::ZeninConfig;
use super::mesh::{Mesh, MeshAction};
use super::sync::{self, RecentCache};
use super::wire::{EventRange, Message, ProtocolError, ReceiptSummary, TableBatch};
use crate::core::WorkspaceCore;
use crate::system::INSTALLATIONS_TABLE_NAME;
use crate::{Error, Result};

// ─── Engine ──────────────────────────────────────────────────────────────────

pub(super) struct Engine {
    core: Weak<WorkspaceCore>,
    config: ZeninConfig,
    pub(super) mesh: Mesh,
    pub(super) cache: RecentCache,
}

impl Engine {
    pub(super) fn new(
        core: Weak<WorkspaceCore>,
        local_peer_id: PeerId,
        config: ZeninConfig,
        mesh_config: super::config::MeshConfig,
    ) -> Self {
        let cache_cap = config.recent_cache_capacity;
        Self {
            core,
            config,
            mesh: Mesh::new(local_peer_id, mesh_config),
            cache: RecentCache::new(cache_cap),
        }
    }

    // ── Startup ──────────────────────────────────────────────────────────

    /// Populate mesh routes from the current membership snapshot.
    /// Called once at startup before the event loop begins.
    pub(super) fn initialize_mesh(&mut self) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        for (id, installation) in core.membership.list() {
            self.mesh.add(id, &installation);
        }
    }

    // ── Mesh maintenance ─────────────────────────────────────────────────

    /// Evaluate mesh topology and return Graft/Prune messages.
    pub(super) fn maintain_mesh(&mut self) -> Vec<(PeerId, Message)> {
        let mut outbound = Vec::new();
        for action in self.mesh.maintain() {
            match action {
                MeshAction::Graft { peer_id } => {
                    self.mesh.graft(peer_id);
                    outbound.push((peer_id, Message::Graft));
                }
                MeshAction::Prune { peer_id } => {
                    self.mesh.prune(peer_id);
                    outbound.push((peer_id, Message::Prune));
                }
            }
        }
        outbound
    }

    // ── Anti-entropy ─────────────────────────────────────────────────────

    /// Send receipt summaries to all mesh neighbours.
    pub(super) fn send_summaries(&self) -> Vec<(PeerId, Message)> {
        let Some(core) = self.core.upgrade() else {
            return Vec::new();
        };
        let receipts = sync::receipt_summaries(&core);
        self.mesh
            .neighbours(None)
            .into_iter()
            .map(|peer_id| (peer_id, Message::Summary { receipts: receipts.clone() }))
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
        if !self.mesh.is_active(peer_id) {
            return vec![(peer_id, Message::Error(ProtocolError::Unauthorized))];
        }

        match msg {
            Message::Push { batches } => self.handle_push(&core, peer_id, batches),
            Message::Graft => {
                if self.mesh.accepts(peer_id) {
                    self.mesh.graft(peer_id);
                }
                Vec::new()
            }
            Message::Prune => {
                self.mesh.prune(peer_id);
                Vec::new()
            }
            Message::Summary { receipts } => self.handle_summary(&core, peer_id, receipts),
            Message::SummaryResponse { receipts } => self.handle_summary_response(&core, peer_id, receipts),
            Message::Fetch { ranges, max_bytes } => self.handle_fetch(&core, peer_id, ranges, max_bytes),
            Message::FetchResponse { batches } => self.handle_push(&core, peer_id, batches),
            Message::Error(_) | Message::Handshake { .. } => Vec::new(),
        }
    }

    // ── Broadcast ────────────────────────────────────────────────────────

    /// Broadcast table batches to mesh neighbours, excluding one peer.
    pub(super) fn broadcast(
        &self,
        batches: &[TableBatch],
        exclude: Option<PeerId>,
    ) -> Vec<(PeerId, Message)> {
        if batches.is_empty() {
            return Vec::new();
        }
        self.mesh
            .neighbours(exclude)
            .into_iter()
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

    // ── Membership events ────────────────────────────────────────────────

    /// An installation was admitted (state became Active).
    pub(super) fn installation_admitted(&mut self, installation_id: InstallationId) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        if let Some(installation) = core.membership.get(&installation_id) {
            self.mesh.add(installation_id, &installation);
        }
    }

    /// An installation was rejected or removed.
    pub(super) fn installation_rejected(&mut self, installation_id: InstallationId) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        if let Some(installation) = core.membership.get(&installation_id) {
            self.mesh.remove(installation_id, &installation.public_key);
        }
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
            outbound.push((
                peer_id,
                Message::Fetch {
                    ranges,
                    max_bytes: self.config.max_sync_bytes.min(u32::MAX as usize) as u32,
                },
            ));
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
            vec![(
                peer_id,
                Message::Fetch {
                    ranges,
                    max_bytes: self.config.max_sync_bytes.min(u32::MAX as usize) as u32,
                },
            )]
        }
    }

    fn handle_fetch(
        &mut self,
        core: &WorkspaceCore,
        peer_id: PeerId,
        ranges: Vec<EventRange>,
        max_bytes: u32,
    ) -> Vec<(PeerId, Message)> {
        let budget = (max_bytes as usize).min(self.config.max_sync_bytes);

        // Try serving from the recent cache first.
        let (cached_batches, remaining) = self.cache.fetch(&ranges);
        if remaining.is_empty() && !cached_batches.is_empty() {
            return vec![(peer_id, Message::FetchResponse { batches: cached_batches })];
        }

        // Fall through to durable topic scan for remaining ranges.
        let scan_ranges = if remaining.is_empty() { &ranges } else { &remaining };
        match sync::fetch_ranges(core, scan_ranges, budget) {
            Ok(mut batches) => {
                if !cached_batches.is_empty() {
                    // Merge cached results with topic results.
                    batches.extend(cached_batches);
                    batches.sort_unstable_by(|a, b| {
                        table_order(&a.table).cmp(&table_order(&b.table))
                    });
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
