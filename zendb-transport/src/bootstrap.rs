use bincode::{Decode, Encode};
use std::collections::BTreeSet;
use zendb_types::{
    CapabilityId, DeviceId, DevicePublicKey, EnrollmentTicketId, SignatureBytes, WorkspaceId,
};

/// Sent by a new device before a trusted peer approves bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BootstrapRequest {
    pub workspace_id: WorkspaceId,
    pub candidate_device_id: DeviceId,
    pub candidate_public_key: DevicePublicKey,
    pub requested_name: String,
    pub capabilities: BTreeSet<CapabilityId>,
    pub candidate_proof: SignatureBytes,
    pub ticket_admission: Option<BootstrapTicketProof>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BootstrapTicketProof {
    pub ticket_id: EnrollmentTicketId,
    pub signature: SignatureBytes,
}

/// Sent back to complete device bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BootstrapApproval {
    pub snapshot_hint_bytes: Option<u64>,
    pub accepted_device_id: DeviceId,
}
