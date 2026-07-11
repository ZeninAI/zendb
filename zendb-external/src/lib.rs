//! Optional outbound adapters for services outside the embedded database.
//!
//! `zendb-external` contains client-side interfaces only. It does not define
//! server handlers, server storage, authority implementations, or a required
//! central service. An application may omit this crate entirely and use local
//! peer discovery, direct bootstrap, and device-to-device synchronization.

use std::io;

use zendb_identity::{CredentialRequest, WorkspaceCredential};
use zendb_transport::{DiscoveredPeer, NetworkEndpoint, RendezvousRequest, RendezvousTicket};
use zendb_types::{DeviceId, JobId, OperatorJob, OperatorJobResult, WorkspaceId};

/// Outbound credential request to an optional hosted authority.
pub trait HostedCredentialClient: Send + Sync + 'static {
    fn request_credential(&self, request: &CredentialRequest) -> io::Result<WorkspaceCredential>;
}

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

/// Optional job relay for a client that wants hosted workers. The embedded
/// database remains the authority for whether a job is claimable and whether
/// a result is admissible; this adapter only transports job records.
pub trait HostedJobClient: Send + Sync + 'static {
    fn submit(&self, job: &OperatorJob) -> io::Result<()>;
    fn claim(&self, device_id: DeviceId) -> io::Result<Option<OperatorJob>>;
    fn publish_result(&self, result: &OperatorJobResult) -> io::Result<()>;
    fn cancel(&self, job_id: &JobId) -> io::Result<()>;
}
