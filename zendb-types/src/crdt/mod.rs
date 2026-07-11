//! CRDT primitives and collaborative data structures.
//!
//! This module contains the complete CRDT foundation for ZeninDB:
//! - **Core primitives**: HLC, Event, Path, Cell, Op
//! - **Type traits**: Type, ContainerType, MergeClocks
//! - **CRDT values**: All collaborative data structures
//! - **Type dispatch**: Macro for registering and dispatching CRDT types
//!
//! ## Module Organization
//!
//! - `_traits` — Core trait definitions (Type, ContainerType, MergeClocks)
//! - `_macros` — Type registration macro (register_types!)
//! - `values/*` — CRDT value implementations
//! - Top level — Core primitives (HLC, Event, Path, Cell, Op, Change)

#[macro_use]
pub mod _macros;

pub mod _traits;
pub mod cell;
pub mod change;
pub mod event;
pub mod hlc;
pub mod op;
pub mod path;
pub mod replication;
pub mod values;

// Re-export traits
pub use _traits::{ContainerType, MergeClocks, Type};

// Re-export core primitives
pub use cell::Cell;
pub use change::Change;
pub use event::{Event, Signature, TableId};
pub use hlc::{device_id, init_device_id, Hlc, HlcDeviceId};
pub use op::Op;
pub use path::{Path, PathStep};
pub use replication::{EventIdentity, ReplicatedEvent, SyncEnvelope, VersionVector};

// Re-export all CRDT values
pub use values::*;
