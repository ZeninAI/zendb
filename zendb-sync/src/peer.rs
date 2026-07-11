use bincode::{Decode, Encode};
use zendb_identity::{DeviceId, PeerIdentity, WorkspaceId};

use crate::{messages::SyncCapabilities, summary::WorkspaceSyncSummary};

/// Identifies a local or remote replica for a specific workspace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct ReplicaId {
    pub device_id: DeviceId,
    pub workspace_id: WorkspaceId,
}

/// Authenticated peer plus replication-related metadata.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ReplicationPeer {
    pub peer: PeerIdentity,
    pub workspace_id: WorkspaceId,
    pub capabilities: SyncCapabilities,
    pub summary: WorkspaceSyncSummary,
}
