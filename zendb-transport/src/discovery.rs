use bincode::{Decode, Encode};
use zendb_types::{DeviceId, WorkspaceId};

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
