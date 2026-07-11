use bincode::{Decode, Encode};

use crate::claims::SignatureBytes;
use zendb_types::{DeviceId, DeviceMembership, KeyId, VersionVector, WorkspaceId};

/// Identifies the snapshot boundary a bootstrap bundle was cut from.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SnapshotAnchor {
    pub workspace_id: WorkspaceId,
    pub created_at_ms: u64,
    pub version_vector: VersionVector,
    pub snapshot_hash: [u8; 32],
    pub policy_epoch: u64,
}

/// Payload delivered to a newly enrolled device before normal replication begins.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BootstrapBundle {
    pub workspace_id: WorkspaceId,
    pub approved_device: DeviceMembership,
    pub encrypted_workspace_bundle: Vec<u8>,
    pub snapshot_anchor: SnapshotAnchor,
    pub bootstrap_peers: Vec<DeviceId>,
}

/// Envelope used when a trusted device vouches for a new device during bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BootstrapEnvelope {
    pub bundle: BootstrapBundle,
    pub approver_device_id: DeviceId,
    /// Identifies the approver key used for the envelope signature. The
    /// transport session normally supplies the corresponding public key, but
    /// retaining the key ID makes audit and later key rotation unambiguous.
    pub approver_key_id: KeyId,
    pub signature: SignatureBytes,
}
