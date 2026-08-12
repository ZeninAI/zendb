//! CRDT primitives and collaborative data structures.

#[macro_use]
mod _macros;

pub mod _traits;
pub mod edit;
pub mod event;
pub mod stamp;
pub mod values;

pub(crate) use _traits::TypeMetadata;
pub use _traits::{ContainerType, OpDispatcher, Type, TypeMismatch};
pub use edit::{Edit, TypedEdit};
pub use event::{Event, PathOp};
pub use stamp::{EventId, EventTime};
pub use values::*;
