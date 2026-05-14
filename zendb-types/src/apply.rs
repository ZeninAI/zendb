//! Apply walk — recursive operation application through a `Path`.
//!
//! The apply walk descends through the cell tree following the path steps,
//! materializing missing intermediate containers as dummy cells. At the leaf,
//! it dispatches to the type-specific `Type::apply_op`.
//!
//! `SetSync` is NOT handled here — it is intercepted by the engine before
//! the apply walk and applied directly to `Cell.sync`.

use crate::{Cell, Hlc, Op, PathStep, TypeTag};

/// Apply an operation recursively through a path.
///
/// Returns `true` if the operation was applied, `false` if it was dropped
/// (type conflict, LWW loss, or apply failure).
pub fn apply_recursive(
    cursor: &mut Cell,
    steps: &[PathStep],
    op: Op,
    op_hlc: Hlc,
    op_tag: TypeTag,
) -> bool {
    if let Some((head, tail)) = steps.split_first() {
        // --- Intermediate step: descend through a container ---
        if !ensure_type(cursor, head.container_tag, op_hlc) {
            return false;
        }

        // Determine the expected child type for dummy creation.
        let child_tag = tail.first().map(|s| s.container_tag).unwrap_or(op_tag);

        let child =
            match crate::descend_or_create_dispatch(&mut cursor.value, &head.segment, child_tag) {
                Ok(c) => c,
                Err(_) => return false,
            };

        return apply_recursive(child, tail, op, op_hlc, op_tag);
    }

    // --- Leaf: apply the operation ---
    apply_at_leaf(cursor, op, op_hlc, op_tag)
}

/// Ensure the cursor cell has the expected type.
///
/// If the type matches, returns `true`. If there's a mismatch:
/// - Dummy cells (HLC == ZERO) always allow replacement.
/// - Real cells: LWW — incoming op must beat the cursor's HLC.
fn ensure_type(cursor: &mut Cell, expected: TypeTag, op_hlc: Hlc) -> bool {
    if cursor.type_tag() == expected {
        return true;
    }

    // Type mismatch. Replace if dummy or if incoming beats existing.
    if cursor.is_dummy() || op_hlc.beats(cursor.hlc) {
        let new_value = crate::empty_for_tag(expected);
        *cursor = Cell::new(new_value, cursor.hlc, cursor.sync);
        return true;
    }

    false
}

/// Apply an operation at the leaf cell.
fn apply_at_leaf(cursor: &mut Cell, op: Op, op_hlc: Hlc, op_tag: TypeTag) -> bool {
    // Ensure the cursor has the expected type.
    if !ensure_type(cursor, op_tag, op_hlc) {
        return false;
    }

    // Replacement ops get an LWW gate.
    if op.is_replacement() && cursor.hlc.beats(op_hlc) {
        return false;
    }

    // Dispatch to the type.
    match crate::apply_op_dispatch(cursor.value.clone(), op, op_hlc) {
        Ok(new_value) => {
            cursor.value = new_value;
            cursor.hlc = op_hlc;
            true
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::Type;
    use crate::types::atom::{AtomOp, AtomValue};
    use crate::types::record::{RecordOp, RecordType};
    use crate::Value;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, 1).unwrap()
    }

    #[test]
    fn apply_atom_to_root() {
        let mut cell = Cell::dummy(Value::Atom(AtomValue::Null));
        let applied = apply_recursive(
            &mut cell,
            &[], // empty path = root
            Op::Atom(AtomOp::Set(AtomValue::Int(42))),
            hlc(100),
            TypeTag::Atom,
        );
        assert!(applied);
        assert_eq!(cell.hlc, hlc(100));
        if let Value::Atom(v) = &cell.value {
            assert_eq!(*v, AtomValue::Int(42));
        } else {
            panic!("expected Atom");
        }
    }

    #[test]
    fn apply_set_field_to_record_root() {
        // SetField targets the record cell (empty path = root).
        let mut root = Cell::new(Value::Record(RecordType::empty()), hlc(50), None);

        let applied = apply_recursive(
            &mut root,
            &[], // empty path: SetField is applied to the record directly
            Op::Record(RecordOp::SetField {
                name: "x".into(),
                value: Cell::new(Value::Atom(AtomValue::String("hi".into())), hlc(100), None),
            }),
            hlc(100),
            TypeTag::Record,
        );

        assert!(applied);
        if let Value::Record(r) = &root.value {
            let child = r.fields.get("x").unwrap();
            assert_eq!(child.hlc, hlc(100));
        } else {
            panic!("expected Record");
        }
    }

    #[test]
    fn apply_lww_replacement_drops_older() {
        let mut cell = Cell::new(Value::Atom(AtomValue::Int(1)), hlc(200), None);

        // Try to set with an older HLC — should be dropped.
        let applied = apply_recursive(
            &mut cell,
            &[],
            Op::Atom(AtomOp::Set(AtomValue::Int(2))),
            hlc(100),
            TypeTag::Atom,
        );
        assert!(!applied);
        // Value unchanged.
        if let Value::Atom(v) = &cell.value {
            assert_eq!(*v, AtomValue::Int(1));
        }
    }

    #[test]
    fn ensure_type_replaces_dummy() {
        let mut cell = Cell::dummy(Value::Atom(AtomValue::Null));
        // Dummy has Atom, we expect Record — should replace with empty Record.
        let ok = ensure_type(&mut cell, TypeTag::Record, hlc(100));
        assert!(ok);
        assert_eq!(cell.type_tag(), TypeTag::Record);
    }

    #[test]
    fn ensure_type_drops_older_mismatch() {
        let mut cell = Cell::new(Value::Atom(AtomValue::Int(1)), hlc(200), None);
        // Existing is Atom with HLC 200, incoming wants Record with HLC 100.
        let ok = ensure_type(&mut cell, TypeTag::Record, hlc(100));
        assert!(!ok); // dropped
        assert_eq!(cell.type_tag(), TypeTag::Atom); // unchanged
    }
}
