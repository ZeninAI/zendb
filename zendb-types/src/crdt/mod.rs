//! CRDT primitives and collaborative data structures.

#[macro_use]
mod _macros;

pub mod _traits;
pub mod event;
pub mod stamp;
pub mod values;

pub use _traits::{ContainerType, Type, TypeMismatch};
pub use event::{Event, PathOp};
pub use stamp::{EventId, EventTime};
pub use values::*;
