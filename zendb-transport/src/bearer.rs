//! Bearer-neutral connectivity interfaces.

use std::io;

use crate::{NetworkEndpoint, RawTransport};

/// Transport families are performance and reachability choices, not trust
/// levels. Every bearer still goes through the authenticated session handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BearerKind {
    Lan,
    Direct,
    Bluetooth,
    PeerToPeer,
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BearerCapabilities {
    pub reliable: bool,
    pub ordered: bool,
    pub supports_migration: bool,
    pub max_frame_bytes: u32,
}

/// Creates sessions over one concrete network bearer. The adapter must not
/// perform workspace authorization; it only establishes a raw session that is
/// authenticated by the handshake layer afterward. The returned object is a
/// `RawTransport`, not an authenticated `TransportSession`.
pub trait BearerAdapter: Send + Sync + 'static {
    fn kind(&self) -> BearerKind;
    fn capabilities(&self) -> BearerCapabilities;
    fn connect(&self, endpoint: &NetworkEndpoint) -> io::Result<Box<dyn RawTransport>>;
}
