//! Installation identity, workspace identity, device keys, and persisted roles.

#[macro_use]
mod opaque_id;
pub mod installation;
pub mod peer;
pub mod workspace;

pub use installation::{InstallationId, InstallationIdParseError};
pub use peer::{PeerIdentity, PublicKey, Role};
pub use workspace::{WorkspaceId, WorkspaceIdParseError};
