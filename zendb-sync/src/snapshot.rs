use bincode::{Decode, Encode};
use zendb_identity::WorkspaceId;

use crate::summary::WorkspaceSyncSummary;

/// Snapshot manifest transferred before bulk snapshot bytes are consumed.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SnapshotManifest {
    pub workspace_id: WorkspaceId,
    pub summary: WorkspaceSyncSummary,
    pub total_bytes: u64,
    pub snapshot_hash: [u8; 32],
    pub policy_epoch: u64,
    pub resource_scope_hash: [u8; 32],
}

/// In-memory snapshot export placeholder. The concrete storage format can evolve later.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SnapshotExport {
    pub manifest: SnapshotManifest,
    pub bytes: Vec<u8>,
}
