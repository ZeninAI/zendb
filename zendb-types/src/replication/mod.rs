//! Replication installation identities, addresses, and wire envelopes.

#[macro_use]
mod _macros;
mod _traits;
mod envelope;
mod ids;
mod installation;
mod multiaddr;
mod public_key;
mod role;

pub use _traits::PeerIdentity;
pub use envelope::{CompactEvent, Envelope};
pub use ids::{
    IdFromPrimaryKeyError, InstallationId, InstallationIdParseError, WorkspaceId,
    WorkspaceIdParseError,
};
pub use installation::Installation;
pub use multiaddr::{Multiaddr, MultiaddrError};
pub use public_key::PublicKey;
pub use role::Role;
