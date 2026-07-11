use std::io;

use zendb_identity::PeerIdentity;

use crate::{NetworkEndpoint, SessionHealth, SessionId, TransportFrame};

/// Immutable metadata attached to an authenticated session.
#[derive(Debug, Clone)]
pub struct SessionMetadata {
    pub peer: PeerIdentity,
    pub endpoint: NetworkEndpoint,
}

/// Raw bearer connection before the workspace handshake. It has an endpoint,
/// but deliberately has no `PeerIdentity` because the remote has not been
/// authenticated yet.
pub trait RawTransport: Send + 'static {
    fn session_id(&self) -> SessionId;
    fn endpoint(&self) -> &NetworkEndpoint;
    fn send(&mut self, frame: TransportFrame) -> io::Result<()>;
    fn recv(&mut self) -> io::Result<Option<TransportFrame>>;
    fn close(&mut self) -> io::Result<()>;
}

/// Live framed session over any underlying transport.
pub trait TransportSession: Send + 'static {
    /// The logical session ID must survive a successful bearer handoff.
    fn session_id(&self) -> SessionId;
    fn metadata(&self) -> &SessionMetadata;
    fn health(&self) -> SessionHealth;
    fn send(&mut self, frame: TransportFrame) -> io::Result<()>;
    fn recv(&mut self) -> io::Result<Option<TransportFrame>>;
    /// Implementations must authenticate the new path before switching the
    /// logical session. Returning `Unsupported` is valid for a first bearer.
    fn migrate(&mut self, _replacement: Box<dyn RawTransport>) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "transport bearer migration is not supported",
        ))
    }
    fn close(&mut self) -> io::Result<()>;
}
