//! CRDT value types - collaborative data structures.
//!
//! This module contains all the Conflict-free Replicated Data Types (CRDTs)
//! that form the foundation of ZeninDB's collaborative editing capabilities.
//!
//! ## Available Types
//!
//! ### Scalar LWW Types
//! - `Bool`, `Int`, `Float32`, `Float64`, `String`, `Timestamp`, `Blob` — Last-Writer-Wins scalars
//!
//! ### CRDT Types
//! - `Counter` — PN-Counter for concurrent increment/decrement
//! - `Set` — LWW set with per-element add/remove stamps
//! - `OrSet` — Observed-remove set (additive-wins)
//! - `MvRegister` — Multi-value register
//! - `PriorityQueue` — Replicated min-heap
//! - `Text` — Collaborative text with RGA + per-character formatting
//!
//! ### Container Types
//! - `Record` — Named-field map with recursive field-wise merge
//! - `List` — RGA ordered list with stable element IDs

pub mod blob;
pub mod bool;
pub mod counter;
pub mod float;
pub mod int;
pub mod list;
pub mod mv_register;
pub mod or_set;
pub mod priority_queue;
pub mod record;
pub mod set;
pub mod string;
pub mod text;
pub mod timestamp;

pub use blob::{Blob, BlobError, BlobOp};
pub use bool::{Bool, BoolError, BoolOp};
pub use counter::{Counter, CounterError, CounterOp};
pub use float::{Float32, Float32Error, Float32Op, Float64, Float64Error, Float64Op};
pub use int::{Int, IntError, IntOp};
pub use list::{List, ListError, ListId, ListOp, ListSegment};
pub use mv_register::{MvRegister, MvRegisterError, MvRegisterOp};
pub use or_set::{OrSet, OrSetError, OrSetOp};
pub use priority_queue::{PqError, PqOp, PriorityQueue};
pub use record::{Record, RecordError, RecordOp, RecordSegment};
pub use set::{Set, SetError, SetOp};
pub use string::{String, StringError, StringOp};
pub use text::{Text, TextError, TextId, TextOp};
pub use timestamp::{Timestamp, TimestampError, TimestampOp};
