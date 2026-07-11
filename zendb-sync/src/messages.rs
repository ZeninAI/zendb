use bincode::{Decode, Encode};
use zendb_identity::{DeviceId, WorkspaceId};

use crate::{journal::ReplicatedEvent, summary::WorkspaceSyncSummary};

/// Replication capabilities advertised by a peer.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SyncCapabilities {
    pub can_serve_snapshot: bool,
    pub can_accept_tail_subscription: bool,
    pub max_batch_events: u32,
}

/// Wire-format summary message.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SyncSummaryMessage {
    pub summary: WorkspaceSyncSummary,
    pub capabilities: SyncCapabilities,
}

/// Request missing events for an origin device sequence range.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RangeRequest {
    pub workspace_id: WorkspaceId,
    pub origin_device_id: DeviceId,
    pub from_exclusive: u64,
    pub to_inclusive: u64,
}

/// Batch of replicated events exchanged between peers.
#[derive(Debug, Clone, Encode, Decode)]
pub struct EventBatch {
    pub workspace_id: WorkspaceId,
    pub events: Vec<ReplicatedEvent>,
}

/// Snapshot metadata exchanged before streaming snapshot chunks.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SyncSnapshotMeta {
    pub workspace_id: WorkspaceId,
    pub chunk_count: u32,
    pub total_bytes: u64,
    pub summary: WorkspaceSyncSummary,
    pub snapshot_hash: [u8; 32],
    pub resource_scope_hash: [u8; 32],
}

/// One chunk of a snapshot stream.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SyncSnapshotChunk {
    pub workspace_id: WorkspaceId,
    pub chunk_index: u32,
    pub chunk_hash: [u8; 32],
    pub bytes: Vec<u8>,
}

/// Request or acknowledge a live tail subscription.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct TailSubscription {
    pub workspace_id: WorkspaceId,
    pub policy_epoch: u64,
    pub resource_scope_hash: [u8; 32],
    pub accepted: bool,
}
