//! Peer identity, independent workspace identity, and persisted roles.

pub mod peer;
pub mod workspace;

pub use peer::{IdParseError, PeerId, PeerIdentity, Role, Signature, SigningError};
pub use workspace::{WorkspaceId, WorkspaceIdParseError};
