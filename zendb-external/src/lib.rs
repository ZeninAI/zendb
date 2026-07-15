//! Optional outbound adapters for services outside the embedded database.
//!
//! `zendb-external` contains client-side interfaces only. It does not define
//! server handlers, server storage, authority implementations, or a required
//! central service. An application may omit this crate entirely and use local
//! peer discovery, direct bootstrap, and device-to-device synchronization.

use std::io;

use zendb_transport::{DiscoveredPeer, NetworkEndpoint, RendezvousRequest, RendezvousTicket};
use zendb_types::{DeviceId, WorkspaceId};

/// Outbound rendezvous client. Ticket authorization remains a workspace and
/// peer-handshake concern; this interface only obtains reachability hints.
pub trait HostedRendezvousClient: Send + Sync + 'static {
    fn create_ticket(&self, request: &RendezvousRequest) -> io::Result<RendezvousTicket>;
    fn resolve_ticket(&self, ticket: &RendezvousTicket) -> io::Result<Vec<NetworkEndpoint>>;
}

/// Optional hosted presence/discovery adapter. Returned peers are untrusted
/// candidates and must still pass the normal transport handshake.
pub trait HostedDiscoveryClient: Send + Sync + 'static {
    fn publish_presence(&self, workspace_id: &WorkspaceId, device_id: DeviceId) -> io::Result<()>;
    fn discover(&self, workspace_id: &WorkspaceId) -> io::Result<Vec<DiscoveredPeer>>;
}
