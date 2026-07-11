use std::io;

use bincode::{Decode, Encode};
use zendb_identity::{DeviceId, WorkspaceId};

use crate::NetworkEndpoint;

/// Reachability candidate returned by a discovery backend.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DiscoveredPeer {
    pub workspace_id: WorkspaceId,
    pub device_id: Option<DeviceId>,
    pub endpoint: NetworkEndpoint,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
    /// A local ranking hint only. It is not an authorization claim.
    pub score: u16,
}

/// Finds peers for a workspace through LAN, relay, local pairing, or other means.
pub trait DiscoveryProvider: Send + Sync + 'static {
    fn discover(&self, workspace_id: &WorkspaceId) -> io::Result<Vec<DiscoveredPeer>>;
}
