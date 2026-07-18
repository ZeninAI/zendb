use std::net::SocketAddr;

use bincode::{Decode, Encode};

/// Untrusted address hint carried by discovery or enrollment.
///
/// `Tcp` is the concrete carrier implemented by this crate. `Named` lets an
/// application preserve Bluetooth, WebRTC, relay, or platform-specific hints
/// without pretending that zendb-transport implements those carriers. A
/// connector must still establish a `FramedLink`, and the secure handshake
/// performs identity and Workspace authentication afterward.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub enum ConnectionHint {
    Tcp(SocketAddr),
    Named { carrier: String, address: Vec<u8> },
}
