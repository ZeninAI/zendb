//! Signed, non-replicated device presence messages.

use bincode::{Decode, Encode};

use crate::{DeviceId, Hlc, SignatureBytes};

/// Soft evidence that a device is still reachable. It is sent on an
/// authenticated session and is never written into Workspace CRDT state.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PresenceHeartbeat {
    pub device_id: DeviceId,
    pub presence_seq: u64,
    pub emitted_hlc: Hlc,
    pub advertised_idle_period_ms: u64,
    pub signature: SignatureBytes,
}

/// Best-effort signal emitted before an intentional disconnect. A later
/// heartbeat with a higher sequence supersedes it.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DepartureNotice {
    pub device_id: DeviceId,
    pub presence_seq: u64,
    pub emitted_hlc: Hlc,
    pub signature: SignatureBytes,
}
