use bincode::{Decode, Encode};
use zendb_types::{Hlc, WorkspaceId};

use super::WorkspaceSyncSummary;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SnapshotManifest {
    pub workspace_id: WorkspaceId,
    pub summary: WorkspaceSyncSummary,
    pub total_bytes: u64,
    pub snapshot_hash: [u8; 32],
    pub compacted_through: Option<Hlc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SnapshotExport {
    pub manifest: SnapshotManifest,
    pub bytes: Vec<u8>,
}
