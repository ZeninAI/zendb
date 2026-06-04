//! Cell - the universal addressable value wrapper.

use bincode::{Decode, Encode};

use crate::{Hlc, Op, TypeTag, Value};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct Cell {
    /// `Some` means the cell is live. `None` means the cell is a tombstone.
    pub value: Option<Value>,
    /// Structural clock for this cell: creation, deletion, type replacement,
    /// or a direct operation against this cell. Descendant-only updates do not
    /// bump ancestor HLCs.
    pub hlc: Hlc,
    pub sync: Option<bool>,
}

impl Cell {
    pub fn new(value: Value, hlc: Hlc, sync: Option<bool>) -> Cell {
        Cell {
            value: Some(value),
            hlc,
            sync,
        }
    }

    pub fn tombstone(hlc: Hlc, sync: Option<bool>) -> Cell {
        Cell {
            value: None,
            hlc,
            sync,
        }
    }

    pub fn dummy(value: Option<Value>) -> Cell {
        Cell {
            value,
            hlc: Hlc::ZERO,
            sync: None,
        }
    }

    pub fn is_dummy(&self) -> bool {
        self.hlc == Hlc::ZERO
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    pub fn type_tag(&self) -> Option<TypeTag> {
        self.value.as_ref().map(Value::type_tag)
    }

    /// Apply a delta/event to this cell. Returns true if state was modified.
    pub fn apply_delta(&mut self, delta: &crate::Delta) -> bool {
        let mut cursor = self;

        for (index, step) in delta.path.steps.iter().enumerate() {
            if !ensure_ancestor_container(cursor, step.container_tag, delta.hlc) {
                return false;
            }

            let child_tag = delta
                .path
                .steps
                .get(index + 1)
                .map(|s| s.container_tag)
                .or_else(|| delta.op.target_type());

            let value = cursor
                .value
                .as_mut()
                .expect("ensure_ancestor_container must leave cursor live");
            cursor = match crate::descend_or_create_dispatch(value, &step.segment, child_tag) {
                Ok(c) => c,
                Err(_) => return false,
            };
        }

        apply_at_target(cursor, &delta.op, delta.hlc)
    }

    /// Merge a remote cell into this one.
    ///
    /// Tombstone/live and type conflicts are resolved by this cell's structural
    /// HLC. When both cells are live with the same type, merge is delegated to
    /// the type implementation so containers can recursively merge children.
    pub fn merge(&mut self, remote: &Cell) -> bool {
        let mut changed = false;

        match (&mut self.value, &remote.value) {
            (None, None) => {
                if remote.hlc.beats(self.hlc) {
                    self.hlc = remote.hlc;
                    changed = true;
                }
            }
            (None, Some(_)) | (Some(_), None) => {
                if remote.hlc.beats(self.hlc) {
                    *self = remote.clone();
                    return true;
                }
            }
            (Some(local), Some(remote_value)) => {
                if local.type_tag() != remote_value.type_tag() {
                    if remote.hlc.beats(self.hlc) {
                        *self = remote.clone();
                        return true;
                    }
                    return false;
                }

                let local_hlc = self.hlc;
                if crate::merge_dispatch(local, local_hlc, remote_value, remote.hlc)
                    .unwrap_or(false)
                {
                    changed = true;
                }
                if remote.hlc.beats(self.hlc) {
                    self.hlc = remote.hlc;
                    changed = true;
                }
            }
        }

        if merge_sync(&mut self.sync, remote.sync) {
            changed = true;
        }

        changed
    }
}

fn apply_at_target(cursor: &mut Cell, op: &Op, op_hlc: Hlc) -> bool {
    match op {
        Op::Type(type_op) => {
            let expected = type_op.type_tag();
            if !ensure_target_type(cursor, expected, op_hlc) {
                return false;
            }
            let value = cursor
                .value
                .as_mut()
                .expect("ensure_target_type must leave cursor live");
            match crate::apply_op_dispatch(value, type_op, cursor.hlc, op_hlc) {
                Ok(true) => {
                    if op_hlc.beats(cursor.hlc) {
                        cursor.hlc = op_hlc;
                    }
                    true
                }
                Ok(false) | Err(_) => false,
            }
        }
        Op::SetSync { sync } => {
            if cursor.hlc.beats(op_hlc) {
                return false;
            }
            cursor.sync = *sync;
            cursor.hlc = op_hlc;
            true
        }
        Op::Delete => {
            if cursor.hlc.beats(op_hlc) {
                return false;
            }
            cursor.value = None;
            cursor.hlc = op_hlc;
            true
        }
        Op::Replace { value } => {
            if cursor.hlc.beats(op_hlc) {
                return false;
            }
            cursor.value = Some(value.clone());
            cursor.hlc = op_hlc;
            true
        }
        Op::Merge { cell } => cursor.merge(cell),
    }
}

fn ensure_ancestor_container(cursor: &mut Cell, expected: TypeTag, op_hlc: Hlc) -> bool {
    if cursor.type_tag() == Some(expected) {
        if cursor.is_dummy() {
            // Dummy created container because it didn't exist before
            cursor.hlc = op_hlc;
        }
        return true;
    }

    retype_cell(cursor, expected, op_hlc)
}

fn ensure_target_type(cursor: &mut Cell, expected: TypeTag, op_hlc: Hlc) -> bool {
    if cursor.type_tag() == Some(expected) {
        return true;
    }

    retype_cell(cursor, expected, op_hlc)
}

fn retype_cell(cursor: &mut Cell, expected: TypeTag, op_hlc: Hlc) -> bool {
    if op_hlc.beats(cursor.hlc) {
        cursor.value = Some(expected.empty_value());
        cursor.hlc = op_hlc;
        return true;
    }

    false
}

fn merge_sync(local: &mut Option<bool>, remote: Option<bool>) -> bool {
    let merged = match (*local, remote) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), _) | (_, Some(false)) => Some(false),
        (None, None) => None,
    };
    if *local == merged {
        false
    } else {
        *local = merged;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::Type;
    use crate::types::atom::{AtomOp, AtomValue};
    use crate::types::record::{RecordOp, RecordType};
    use crate::{Path, Segment, TypeOp};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, [1u8; 8]).unwrap()
    }

    fn delta(path: Path, op: Op, hlc: Hlc) -> crate::Delta {
        crate::Delta {
            table_id: "test".into(),
            primary_key: AtomValue::String("pk".into()),
            path,
            op,
            hlc,
            sync: false,
            signature: Vec::new(),
        }
    }

    #[test]
    fn cell_dummy() {
        let cell = Cell::dummy(Some(Value::Atom(AtomValue::Null)));
        assert!(cell.is_dummy());
        assert!(!cell.is_tombstone());
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
        assert_eq!(decoded.type_tag(), Some(TypeTag::Atom));
    }

    #[test]
    fn apply_atom() {
        let mut cell = Cell::dummy(Some(Value::Atom(AtomValue::Null)));
        assert!(cell.apply_delta(&delta(
            Path::new(),
            Op::Type(TypeOp::Atom(AtomOp::Set(AtomValue::Int(42)))),
            hlc(100),
        )));
        assert_eq!(cell.hlc, hlc(100));
    }

    #[test]
    fn apply_lww_older_no_change() {
        let mut cell = Cell::new(Value::Atom(AtomValue::Int(1)), hlc(200), None);
        let changed = cell.apply_delta(&delta(
            Path::new(),
            Op::Type(TypeOp::Atom(AtomOp::Set(AtomValue::Int(2)))),
            hlc(100),
        ));
        assert!(!changed);
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn delete_tombstones_cell() {
        let mut cell = Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None);
        assert!(cell.apply_delta(&delta(Path::new(), Op::Delete, hlc(200))));
        assert!(cell.is_tombstone());
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn older_write_does_not_resurrect_tombstone() {
        let mut cell = Cell::tombstone(hlc(200), None);
        let changed = cell.apply_delta(&delta(
            Path::new(),
            Op::Type(TypeOp::Atom(AtomOp::Set(AtomValue::Int(2)))),
            hlc(100),
        ));
        assert!(!changed);
        assert!(cell.is_tombstone());
    }

    #[test]
    fn apply_set_field() {
        let mut root = Cell::new(Value::Record(RecordType::empty()), hlc(50), None);
        assert!(root.apply_delta(&delta(
            Path::new(),
            Op::Type(TypeOp::Record(RecordOp::SetField {
                name: "x".into(),
                value: Cell::new(Value::Atom(AtomValue::String("hi".into())), hlc(100), None),
            })),
            hlc(100),
        )));
    }

    #[test]
    fn nested_update_does_not_bump_existing_parent_hlc() {
        let mut root = Cell::new(Value::Record(RecordType::empty()), hlc(50), None);
        let path = Path::new().step(
            TypeTag::Record,
            Segment::Record(crate::RecordSegment {
                field_name: "x".into(),
            }),
        );
        assert!(root.apply_delta(&delta(
            path,
            Op::Type(TypeOp::Atom(AtomOp::Set(AtomValue::Int(1)))),
            hlc(100),
        )));
        assert_eq!(root.hlc, hlc(50));
    }

    #[test]
    fn recreated_parent_gets_event_hlc() {
        let mut root = Cell::tombstone(hlc(50), None);
        let path = Path::new().step(
            TypeTag::Record,
            Segment::Record(crate::RecordSegment {
                field_name: "x".into(),
            }),
        );
        assert!(root.apply_delta(&delta(
            path,
            Op::Type(TypeOp::Atom(AtomOp::Set(AtomValue::Int(1)))),
            hlc(100),
        )));
        assert_eq!(root.hlc, hlc(100));
        assert_eq!(root.type_tag(), Some(TypeTag::Record));
    }

    #[test]
    fn merge_same_record_recurses() {
        let mut local = Cell::new(Value::Record(RecordType::empty()), hlc(50), None);
        let mut remote = Cell::new(Value::Record(RecordType::empty()), hlc(50), None);
        local.apply_delta(&delta(
            Path::new(),
            Op::Type(TypeOp::Record(RecordOp::SetField {
                name: "a".into(),
                value: Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None),
            })),
            hlc(100),
        ));
        remote.apply_delta(&delta(
            Path::new(),
            Op::Type(TypeOp::Record(RecordOp::SetField {
                name: "b".into(),
                value: Cell::new(Value::Atom(AtomValue::Int(2)), hlc(110), None),
            })),
            hlc(110),
        ));
        assert!(local.merge(&remote));
        let Some(Value::Record(record)) = &local.value else {
            panic!("expected record");
        };
        assert!(record.is_field_visible("a"));
        assert!(record.is_field_visible("b"));
    }

    #[test]
    fn merge_atom_uses_original_local_hlc() {
        let mut local = Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None);
        let remote = Cell::new(Value::Atom(AtomValue::Int(2)), hlc(200), None);
        assert!(local.merge(&remote));
        assert_eq!(local.value, Some(Value::Atom(AtomValue::Int(2))));
        assert_eq!(local.hlc, hlc(200));
    }
}
