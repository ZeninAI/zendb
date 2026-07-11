use std::io;

use bincode::{Decode, Encode};
use zendb_identity::{DeviceId, KeyId, WorkspaceId};

use crate::NetworkEndpoint;

/// The operation for which a rendezvous ticket may be used. Reachability
/// tickets are not interchangeable with device-enrollment tickets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum RendezvousPurpose {
    PairDevice,
    JoinGuest,
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
    pub issuer_key_id: Option<KeyId>,
    pub signature: Vec<u8>,
}

/// Client-side provider for creating and resolving rendezvous tickets.
///
/// A local implementation may use a trusted peer or local QR exchange. A
/// hosted implementation belongs in `zendb-external` and is still only an
/// outbound client adapter from this database's perspective.
pub trait RendezvousProvider: Send + Sync + 'static {
    fn create_ticket(&self, request: &RendezvousRequest) -> io::Result<RendezvousTicket>;
    fn resolve_ticket(&self, ticket: &RendezvousTicket) -> io::Result<Vec<NetworkEndpoint>>;
}
