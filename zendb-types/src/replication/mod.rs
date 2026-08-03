//! Replication installation identities, addresses, and wire envelopes.

#[macro_use]
mod _macros;
mod _traits;
mod envelope;
mod ids;
mod installation;
mod multiaddr;
mod permissions;
mod public_key;

pub use _traits::PeerIdentity;
pub use envelope::{CompactEvent, Envelope};
pub use ids::{
    IdFromPrimaryKeyError, InstallationId, InstallationIdParseError, WorkspaceId,
    WorkspaceIdParseError,
};
pub use installation::{Installation, InstallationState};
pub use multiaddr::{Multiaddr, MultiaddrError};
pub use permissions::{Permission, Permissions};
pub use public_key::PublicKey;
