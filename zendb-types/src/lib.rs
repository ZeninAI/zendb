//! # zendb-types
//!
//! Core type system for ZeninDB — an embedded, local-first, eventually
//! consistent database with first-class collaborative editing.
//!
//! This crate contains portable data and protocol types shared by devices:
//! - **CRDT primitives & values**: HLC, Event, Path, Cell, collaborative types
//! - **Device control**: membership, fixed roles, keys, and admission
//! - **Control plane types**: presence, operator specifications, and leases
//!
//! ## Module Organization
//!
//! - `crdt::*` — Complete CRDT foundation (primitives, traits, and value types)
//! - `identity::*` — Identity types that are part of shared truth
//! - `control::*` — Control plane types for operators, jobs, leases (future)
//!
//! ## Adding a CRDT type
//!
//! 1. Create a module in `src/crdt/values/` with a struct implementing `Type`
//! 2. Add one line to `register_types!` below

// --- hand-written modules ---
pub mod control;

#[macro_use]
pub mod crdt;

#[macro_use]
pub mod identity;

// --- Type registration and dispatch ---
// The register_types! macro is defined in crdt/_macros.rs and generates:
// - TypeTag, PrimaryKey, Value, TypeOp, Segment, TypeError enums
// - Trait implementations for type dispatch

// --- invoke the macro ---
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
    leaf Counter => crate::crdt::values::counter::Counter,
    leaf MvRegister => crate::crdt::values::mv_register::MvRegister,
    leaf OrSet => crate::crdt::values::or_set::OrSet,
    leaf PriorityQueue => crate::crdt::values::priority_queue::PriorityQueue,
    leaf Set => crate::crdt::values::set::Set,
    leaf Text => crate::crdt::values::text::Text,
    container Record(crate::crdt::values::record::RecordSegment) => crate::crdt::values::record::Record,
    container List(crate::crdt::values::list::ListSegment) => crate::crdt::values::list::List,
}

// --- re-exports ---

// CRDT primitives and types (everything from crdt module)
pub use crdt::*;

// Identity types
pub use control::{
    DepartureNotice, DeviceCapabilitySummary, OperatorDesiredState, OperatorEffect, OperatorInput,
    OperatorLease, OperatorOutputFence, OperatorSource, OperatorSpec, PresenceHeartbeat,
};
pub use identity::{
    CapabilityId, DeviceId, DeviceKeyPhase, DeviceKeyRing, DevicePublicKey, DeviceRecord,
    DeviceRecordError, EnrollmentTicket, EnrollmentTicketId, OperatorId, SignatureBytes,
    WorkspaceAction, WorkspaceId, WorkspaceRole,
};

// TypeTag, Value, TypeOp, Segment, TypeError are generated above by register_types!
