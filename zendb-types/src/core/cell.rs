//! Cell — the universal addressable value wrapper.
//!
//! Every value in ZeninDB is wrapped in a `Cell`. A cell carries the typed
//! runtime value, an HLC timestamp, and an optional sync policy flag.

use crate::{Hlc, TypeTag, Value};

/// The universal wrapper around every addressable value.
///
/// # Fields
///
/// - `value`: The typed runtime value (`Atom`, `Record`, etc.).
/// - `hlc`: HLC of the last operation that directly targeted this cell.
///   Does NOT cascade from child writes.
/// - `sync`: Explicit replication policy for this subtree.
///   `None` means "inherit from parent."
#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    /// The typed runtime value.
    pub value: Value,
    /// HLC of the last direct write to this cell.
    pub hlc: Hlc,
    /// Replication policy: `Some(true)` = synced, `Some(false)` = local-only,
    /// `None` = inherit from nearest ancestor with an opinion.
    pub sync: Option<bool>,
}

impl Cell {
    /// Create a new cell with the given value, HLC, and sync policy.
    pub fn new(value: Value, hlc: Hlc, sync: Option<bool>) -> Cell {
        Cell { value, hlc, sync }
    }

    /// Create a dummy cell — HLC is `ZERO`, sync is `None`.
    ///
    /// Dummy cells are created during the apply walk when an operation
    /// descends through a path where an intermediate container does not yet
    /// exist. Any real write targeting this cell will beat `ZERO` and replace it.
    pub fn dummy(value: Value) -> Cell {
        Cell {
            value,
            hlc: Hlc::ZERO,
            sync: None,
        }
    }

    /// True if this is a dummy cell (HLC is `ZERO`).
    pub fn is_dummy(&self) -> bool {
        self.hlc == Hlc::ZERO
    }

    /// The `TypeTag` of this cell's value.
    pub fn type_tag(&self) -> TypeTag {
        self.value.type_tag()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::atom::AtomValue;

    #[test]
    fn cell_dummy_has_zero_hlc() {
        let cell = Cell::dummy(Value::Atom(AtomValue::Null));
        assert!(cell.is_dummy());
        assert_eq!(cell.hlc, Hlc::ZERO);
        assert_eq!(cell.sync, None);
    }

    #[test]
    fn cell_new_preserves_fields() {
        let hlc = Hlc::new(100, 0, 1).unwrap();
        let cell = Cell::new(Value::Atom(AtomValue::Int(42)), hlc, Some(true));
        assert_eq!(cell.hlc, hlc);
        assert_eq!(cell.sync, Some(true));
        assert_eq!(cell.type_tag(), TypeTag::Atom);
    }
}
