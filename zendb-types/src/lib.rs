//! Portable identifiers, event stamps, and CRDT values used by ZenDB.

#[macro_use]
pub mod crdt;

pub mod replication;
pub mod utils;

register_types! {
    key Bool => crate::crdt::values::bool::Bool,
    key Int => crate::crdt::values::int::Int,
    key String => crate::crdt::values::string::String,
    key Timestamp => crate::crdt::values::timestamp::Timestamp,
    key Blob => crate::crdt::values::blob::Blob,
    leaf Bool => crate::crdt::values::bool::Bool,
    leaf Int => crate::crdt::values::int::Int,
    leaf String => crate::crdt::values::string::String,
    leaf Timestamp => crate::crdt::values::timestamp::Timestamp,
    leaf Blob => crate::crdt::values::blob::Blob,
    leaf Float32 => crate::crdt::values::float::Float32,
    leaf Float64 => crate::crdt::values::float::Float64,
    leaf Counter => crate::crdt::values::counter::Counter,
    leaf MvRegister => crate::crdt::values::mv_register::MvRegister,
    leaf OrSet => crate::crdt::values::or_set::OrSet,
    leaf PriorityQueue => crate::crdt::values::priority_queue::PriorityQueue,
    leaf Set => crate::crdt::values::set::Set,
    leaf Text => crate::crdt::values::text::Text,
    container Record(crate::crdt::values::record::RecordSegment) => crate::crdt::values::record::Record,
    container List(crate::crdt::values::list::ListSegment) => crate::crdt::values::list::List,
}

pub use crdt::*;
pub use replication::{
    CompactEvent, Envelope, IdFromPrimaryKeyError, InstallationId, InstallationIdParseError,
    Multiaddr, MultiaddrError, PeerIdentity, PublicKey, Role, WorkspaceId, WorkspaceIdParseError,
};
