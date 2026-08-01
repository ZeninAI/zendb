//! Replication identifiers, device identity values, addresses, and wire envelopes.

#[macro_use]
mod _macros;
pub mod _trait;
pub mod envelope;
pub mod ids;
pub mod multiaddr;
pub mod public_key;
pub mod role;

pub use _macros::IdFromPrimaryKeyError;
pub use _trait::PeerIdentity;
pub use envelope::{CompactEvent, Envelope};
pub use ids::{InstallationId, InstallationIdParseError, WorkspaceId, WorkspaceIdParseError};
pub use multiaddr::{Multiaddr, MultiaddrError};
pub use public_key::PublicKey;
pub use role::Role;
