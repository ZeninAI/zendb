use bincode::{Decode, Encode};

/// Logical protocol multiplexed over a transport session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum ProtocolChannel {
    Handshake,
    Sync,
    Bootstrap,
    Presence,
    JobClaim,
    Control,
    Diagnostics,
}

/// Best-effort request correlation id carried by a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RequestId(pub u64);

/// Identifies the logical peer session across bearer handoffs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct SessionId(pub u128);

/// Coarse health state of a transport session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum SessionHealth {
    Connecting,
    Authenticating,
    Established,
    Degraded,
    Closing,
    Closed,
}
