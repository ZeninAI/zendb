use bincode::{Decode, Encode};

use crate::{ProtocolChannel, RequestId};

/// Logical stream within one authenticated session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct StreamId(pub u32);

/// Lowest common denominator frame sent over any live session.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct TransportFrame {
    pub channel: ProtocolChannel,
    pub stream_id: StreamId,
    pub request_id: Option<RequestId>,
    /// Monotonic per-stream sequence used for duplicate suppression and
    /// resumption after a bearer handoff.
    pub sequence: u64,
    /// Highest sequence durably consumed by the receiver on this stream.
    pub acknowledgement: u64,
    pub flags: u8,
    pub payload: Vec<u8>,
}
