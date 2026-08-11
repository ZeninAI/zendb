//! Portable identifiers, per-operation CRDT time, and metadata-owning values.

extern crate self as zendb_types;

#[macro_use]
pub mod crdt;

mod clock;
pub mod replication;
pub mod utils;

pub use clock::{HybridClock, global_clock};

register_types! {
    key Bool => bool,
    key Int => i64,
    key String => std::string::String,
    key Timestamp => crate::crdt::values::timestamp::Timestamp,
    key Blob => crate::crdt::values::blob::Blob,
    leaf Bool => crate::crdt::values::bool::Bool,
    leaf Int => crate::crdt::values::int::Int,
    leaf String => crate::crdt::values::string::String,
    leaf Timestamp => crate::crdt::values::timestamp::Timestamp,
    leaf Blob => crate::crdt::values::blob::Blob,
    leaf Installation => crate::crdt::values::installation::Installation,
    leaf Float => crate::crdt::values::float::Float,
    leaf Text => crate::crdt::values::text::Text,
    container Record => crate::crdt::values::record::Record,
}

pub(crate) use crdt::TypeMetadata;
pub use crdt::*;
pub use replication::{
    IdFromPrimaryKeyError, InstallationId, InstallationIdParseError, Multiaddr, MultiaddrError,
    PeerIdentity, Permission, Permissions, PublicKey, WorkspaceId, WorkspaceIdParseError,
};
pub use zendb_macros::{zendb_container_type, zendb_type};

#[cfg(test)]
mod tests;
