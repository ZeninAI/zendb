//! CRDT primitives and collaborative data structures.
//!
//! This module contains the complete CRDT foundation for ZeninDB:
//! - **Core primitives**: HLC, Event, Path, Cell, Op
//! - **Type traits**: CrdtCodec, CellCodec, Type, ContainerType, MergeClocks
//! - **CRDT values**: All collaborative data structures
//! - **Type dispatch**: Macro for registering and dispatching CRDT types
//!
//! ## Module Organization
//!
//! - `_traits` — Core trait definitions (CrdtCodec, CellCodec, Type, ContainerType)
//! - `_macros` — Type registration and generated `Value` dispatch
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
pub mod sync_policy;
pub mod values;

// Re-export traits
pub use _traits::{
    CellCodec, CellCodecError, CellCodecKey, ContainerType, CrdtCodec, DefaultCrdtCodec,
    MergeClocks, Type,
};

// Re-export core primitives
pub use cell::Cell;
pub use change::Change;
pub use event::{Event, TableId};
pub use hlc::{device_id, init_device_id, Hlc, HlcDeviceId};
pub use op::Op;
pub use path::{Path, PathStep};
pub use replication::{
    compaction_watermark, stable_frontier, ContiguousFrontier, EventIdentity, ReplicatedEvent,
    Signature, SyncEnvelope, TicketAdmissionEvidence, VersionVector,
};
pub use sync_policy::{SyncPolicy, SyncScope};

// Re-export all CRDT values
pub use values::*;
