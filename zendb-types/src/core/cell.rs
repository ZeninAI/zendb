//! Cell — the universal addressable value wrapper.

use bincode::{Decode, Encode};

use crate::{Hlc, Op, PathStep, TypeTag, Value};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct Cell {
    pub value: Value,
    pub hlc: Hlc,
    pub sync: Option<bool>,
}

impl Cell {
    pub fn new(value: Value, hlc: Hlc, sync: Option<bool>) -> Cell {
        Cell { value, hlc, sync }
    }

    pub fn dummy(value: Value) -> Cell {
        Cell {
            value,
            hlc: Hlc::ZERO,
            sync: None,
        }
    }

    pub fn is_dummy(&self) -> bool {
        self.hlc == Hlc::ZERO
    }
    pub fn type_tag(&self) -> TypeTag {
        self.value.type_tag()
    }

    // --- apply ---

    /// Apply a delta to this cell. Returns true if state was modified.
    pub fn apply_delta(&mut self, delta: &crate::Delta) -> bool {
        apply_walk(self, &delta.path.steps, &delta.op, delta.hlc)
    }

    /// Merge a remote cell into this one.
    pub fn merge(&mut self, remote: &Cell) {
        if self.type_tag() != remote.type_tag() {
            if remote.hlc.beats(self.hlc) {
                *self = remote.clone();
            }
            return;
        }
        if remote.hlc.beats(self.hlc) {
            self.hlc = remote.hlc;
        }
        self.sync = match (self.sync, remote.sync) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), _) | (_, Some(false)) => Some(false),
            (None, None) => None,
        };
        let _ = crate::merge_dispatch(&mut self.value, self.hlc, &remote.value, remote.hlc);
    }
}

// ---------------------------------------------------------------------------
// Apply walk (the one helper)
// ---------------------------------------------------------------------------

fn apply_walk(cursor: &mut Cell, steps: &[PathStep], op: &Op, op_hlc: Hlc) -> bool {
    if let Some((head, tail)) = steps.split_first() {
        if !ensure_type(cursor, head.container_tag, op_hlc) {
            return false;
        }
        let child_tag = tail
            .first()
            .map(|s| s.container_tag)
            .unwrap_or(op.type_tag());
        let child =
            match crate::descend_or_create_dispatch(&mut cursor.value, &head.segment, child_tag) {
                Ok(c) => c,
                Err(_) => return false,
            };
        return apply_walk(child, tail, op, op_hlc);
    }
    // Leaf
    if !ensure_type(cursor, op.type_tag(), op_hlc) {
        return false;
    }
    match crate::apply_op_dispatch(&mut cursor.value, op, cursor.hlc, op_hlc) {
        Ok(true) => {
            if op_hlc.beats(cursor.hlc) {
                cursor.hlc = op_hlc;
            }
            true
        }
        Ok(false) => false,
        Err(_) => false,
    }
}

fn ensure_type(cursor: &mut Cell, expected: TypeTag, op_hlc: Hlc) -> bool {
    if cursor.type_tag() == expected {
        return true;
    }
    if cursor.is_dummy() || op_hlc.beats(cursor.hlc) {
        *cursor = Cell::new(expected.empty_value(), cursor.hlc, cursor.sync);
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::Type;
    use crate::types::atom::{AtomOp, AtomValue};
    use crate::types::record::{RecordOp, RecordType};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, 1).unwrap()
    }

    #[test]
    fn cell_dummy() {
        let cell = Cell::dummy(Value::Atom(AtomValue::Null));
        assert!(cell.is_dummy());
        assert_eq!(cell.sync, None);
    }

    #[test]
    fn bincode_roundtrip() {
        let cell = Cell::new(
            Value::Atom(AtomValue::String("hi".into())),
            hlc(100),
            Some(true),
        );
        let buf = encode_to_vec(&cell, config::standard()).unwrap();
        let (decoded, n): (Cell, usize) = decode_from_slice(&buf, config::standard()).unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(decoded.hlc, cell.hlc);
    }

    #[test]
    fn apply_atom() {
        let mut cell = Cell::dummy(Value::Atom(AtomValue::Null));
        assert!(apply_walk(
            &mut cell,
            &[],
            &Op::Atom(AtomOp::Set(AtomValue::Int(42))),
            hlc(100)
        ));
        assert_eq!(cell.hlc, hlc(100));
    }

    #[test]
    fn apply_lww_older_no_change() {
        let mut cell = Cell::new(Value::Atom(AtomValue::Int(1)), hlc(200), None);
        let changed = apply_walk(
            &mut cell,
            &[],
            &Op::Atom(AtomOp::Set(AtomValue::Int(2))),
            hlc(100),
        );
        assert!(!changed);
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn apply_set_field() {
        let mut root = Cell::new(Value::Record(RecordType::empty()), hlc(50), None);
        assert!(apply_walk(
            &mut root,
            &[],
            &Op::Record(RecordOp::SetField {
                name: "x".into(),
                value: Cell::new(Value::Atom(AtomValue::String("hi".into())), hlc(100), None),
            }),
            hlc(100)
        ));
    }

    #[test]
    fn ensure_type_replaces_dummy() {
        let mut cell = Cell::dummy(Value::Atom(AtomValue::Null));
        assert!(ensure_type(&mut cell, TypeTag::Record, hlc(100)));
        assert_eq!(cell.type_tag(), TypeTag::Record);
    }

    #[test]
    fn op_set_sync_bincode_roundtrip() {
        let op = Op::SetSync { sync: Some(false) };
        let buf = encode_to_vec(&op, config::standard()).unwrap();
        let (decoded, _): (Op, usize) = decode_from_slice(&buf, config::standard()).unwrap();
        assert!(matches!(decoded, Op::SetSync { sync: Some(false) }));
    }

    #[test]
    fn record_replace_lww() {
        let mut cell = Cell::new(Value::Record(RecordType::empty()), hlc(200), None);
        let mut new_val = RecordType::empty();
        new_val.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None),
        );
        let changed = apply_walk(
            &mut cell,
            &[],
            &Op::Record(RecordOp::Replace { value: new_val }),
            hlc(100),
        );
        assert!(!changed);
        assert_eq!(cell.hlc, hlc(200));
    }
}
