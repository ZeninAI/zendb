use bincode::{Decode, Encode};
use zendb_types::{DepartureNotice, DeviceId, PresenceHeartbeat, ReplicatedEvent, WorkspaceId};

use super::WorkspaceSyncSummary;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RangeRequest {
    pub workspace_id: WorkspaceId,
    pub origin_device_id: DeviceId,
    pub from_exclusive: u64,
    pub to_inclusive: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct EventBatch {
    pub workspace_id: WorkspaceId,
    pub events: Vec<ReplicatedEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SyncSnapshotMeta {
    pub workspace_id: WorkspaceId,
    pub chunk_count: u32,
    pub total_bytes: u64,
    pub summary: WorkspaceSyncSummary,
    pub snapshot_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SyncSnapshotChunk {
    pub workspace_id: WorkspaceId,
    pub chunk_index: u32,
    pub chunk_hash: [u8; 32],
    pub bytes: Vec<u8>,
}

/// One versioned anti-entropy conversation over an authenticated session.
#[derive(Debug, Clone, Encode, Decode)]
pub enum WorkspaceMessage {
    Summary {
        summary: WorkspaceSyncSummary,
        heartbeat: PresenceHeartbeat,
    },
    Pull {
        ranges: Vec<RangeRequest>,
        include_snapshot: bool,
    },
    Events(EventBatch),
    SnapshotMeta(SyncSnapshotMeta),
    SnapshotChunk(SyncSnapshotChunk),
    PullComplete,
    Complete,
    Departure(DepartureNotice),
    Error(String),
}
