use bincode::{Decode, Encode};
use zendb_types::{DeviceId, WorkspaceId};

/// The operation for which a rendezvous ticket may be used. Reachability
/// tickets are not interchangeable with device-enrollment tickets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum RendezvousPurpose {
    PairDevice,
    ConnectPeer,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RendezvousRequest {
    pub workspace_id: WorkspaceId,
    pub purpose: RendezvousPurpose,
    pub inviter_device_id: Option<DeviceId>,
    pub candidate_device_id: Option<DeviceId>,
    pub expires_at_ms: u64,
}

/// Opaque, purpose-bound ticket that allows two devices to rendezvous out of
/// band. Possession of this ticket still does not authorize workspace access.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RendezvousTicket {
    pub ticket_id: String,
    pub workspace_id: WorkspaceId,
    pub purpose: RendezvousPurpose,
    pub inviter_device_id: Option<DeviceId>,
    pub candidate_device_id: Option<DeviceId>,
    pub token: Vec<u8>,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub one_time: bool,
    pub signature: Vec<u8>,
}
