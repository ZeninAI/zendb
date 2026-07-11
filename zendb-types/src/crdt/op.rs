//! Cell-level operations.

use bincode::{Decode, Encode};

use crate::{Cell, TypeOp, TypeTag, Value};

/// A cell-level operation.
///
/// [`TypeOp`] values are type-specific. `Op` owns cross-cutting behavior that
/// applies to every cell regardless of its current type.
#[derive(Debug, Clone, Encode, Decode)]
pub enum Op {
    /// Apply a generated type-specific operation to the target cell.
    Type(TypeOp),
    /// Set or clear the target cell's sync metadata.
    SetSync { sync: Option<bool> },
    /// Tombstone the target cell at the event HLC.
    Delete,
    /// Replace the target cell's value at the event HLC.
    Replace { value: Value },
    /// Merge remote cell state into the target cell.
    Merge { cell: Cell },
}

impl Op {
    pub(crate) fn target_type(&self) -> Option<TypeTag> {
        match self {
            Op::Type(op) => Some(op.type_tag()),
            Op::Replace { value } => Some(value.type_tag()),
            Op::Merge { cell } => cell.type_tag(),
            Op::SetSync { .. } | Op::Delete => None,
        }
    }
}
