use bincode::{Decode, Encode};
use zendb_identity::{BootstrapEnvelope, DeviceId, KeyId, WorkspaceId};

/// Sent by a new device before a trusted peer approves bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BootstrapRequest {
    pub workspace_id: WorkspaceId,
    pub candidate_device_id: DeviceId,
    pub candidate_device_key_id: KeyId,
    pub candidate_device_key: Vec<u8>,
}

/// Sent back to complete device bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BootstrapApproval {
    pub envelope: BootstrapEnvelope,
    pub snapshot_hint_bytes: Option<u64>,
}
