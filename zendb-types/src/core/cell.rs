//! Cell - the universal addressable value wrapper.

use bincode::{Decode, Encode};

use crate::core::traits::{ContainerType, Type};
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
    pub fn new(value: Option<Value>, hlc: Hlc, sync: Option<bool>) -> Cell {
        Cell { value, hlc, sync }
    }

    pub fn dummy(value: Value) -> Cell {
        Cell {
            value: Some(value),
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
    pub fn apply(&mut self, delta: &crate::Delta) -> bool {
        let mut cursor = self;

        for (index, step) in delta.path.steps.iter().enumerate() {
            if !ensure_target_type(cursor, step.container_tag, delta.hlc) {
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
            cursor = match value.child_or_default(&step.segment, child_tag) {
                Ok(c) => c,
                Err(_) => return false,
            };
        }

        match &delta.op {
            Op::Type(type_op) => {
                let expected = type_op.type_tag();
                if !ensure_target_type(cursor, expected, delta.hlc) {
                    return false;
                }
                let value = cursor
                    .value
                    .as_mut()
                    .expect("ensure_target_type must leave cursor live");
                match value.apply(type_op, cursor.hlc, delta.hlc) {
                    Ok(true) => {
                        if delta.hlc.beats(cursor.hlc) {
                            cursor.hlc = delta.hlc;
                        }
                        true
                    }
                    Ok(false) | Err(_) => false,
                }
            }
            Op::SetSync { sync } => {
                if cursor.hlc.beats(delta.hlc) {
                    return false;
                }
                cursor.sync = *sync;
                cursor.hlc = delta.hlc;
                true
            }
            Op::Delete => {
                if cursor.hlc.beats(delta.hlc) {
                    return false;
                }
                cursor.value = None;
                cursor.hlc = delta.hlc;
                true
            }
            Op::Replace { value } => {
                if cursor.hlc.beats(delta.hlc) {
                    return false;
                }
                cursor.value = Some(value.clone());
                cursor.hlc = delta.hlc;
                true
            }
            Op::Merge { cell } => cursor.merge(cell),
        }
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
                    let sync = self.sync;
                    *self = remote.clone();
                    self.sync = sync;
                    return true;
                }
            }
            (Some(local), Some(remote_value)) => {
                if local.type_tag() != remote_value.type_tag() {
                    if remote.hlc.beats(self.hlc) {
                        let sync = self.sync;
                        *self = remote.clone();
                        self.sync = sync;
                        return true;
                    }
                    return false;
                }

                let local_hlc = self.hlc;
                if local
                    .merge(remote_value, local_hlc, remote.hlc)
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

        changed
    }

    pub fn max_hlc(&self) -> Hlc {
        match &self.value {
            Some(value) => std::cmp::max(self.hlc, value.max_hlc()),
            None => self.hlc,
        }
    }
}

fn ensure_target_type(cursor: &mut Cell, expected: TypeTag, op_hlc: Hlc) -> bool {
    if cursor.type_tag() == Some(expected) {
        return true;
    }

    if op_hlc.beats(cursor.max_hlc()) {
        // If types are wrong but the edit is beyond the max_hlc point
        // Rare occurance
        cursor.value = Some(expected.empty_value());
        cursor.hlc = op_hlc;
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::record::Record;
    use crate::{Path, PrimaryKey, Segment};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, [1u8; 8]).unwrap()
    }

    fn delta(path: Path, op: Op, hlc: Hlc) -> crate::Delta {
        crate::Delta {
            table_id: "test".into(),
            primary_key: PrimaryKey::String("pk".into()),
            path,
            op,
            hlc,
            sync: false,
            signature: Vec::new(),
        }
    }

    #[test]
    fn cell_dummy() {
        let cell = Cell::dummy(Value::String(String::new()));
        assert!(cell.is_dummy());
        assert!(!cell.is_tombstone());
        assert_eq!(cell.sync, None);
    }

    #[test]
    fn bincode_roundtrip() {
        let cell = Cell::new(Some(Value::String("hi".into())), hlc(100), Some(true));
        let buf = encode_to_vec(&cell, config::standard()).unwrap();
        let (decoded, n): (Cell, usize) = decode_from_slice(&buf, config::standard()).unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(decoded.hlc, cell.hlc);
        assert_eq!(decoded.type_tag(), Some(TypeTag::String));
    }

    #[test]
    fn replace_scalar() {
        let mut cell = Cell::dummy(Value::String(String::new()));
        assert!(cell.apply(&delta(
            Path::new(),
            Op::Replace {
                value: Value::Int(42),
            },
            hlc(100),
        )));
        assert_eq!(cell.hlc, hlc(100));
    }

    #[test]
    fn apply_lww_older_no_change() {
        let mut cell = Cell::new(Some(Value::Int(1)), hlc(200), None);
        let changed = cell.apply(&delta(
            Path::new(),
            Op::Replace {
                value: Value::Int(2),
            },
            hlc(100),
        ));
        assert!(!changed);
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn delete_tombstones_cell() {
        let mut cell = Cell::new(Some(Value::Int(1)), hlc(100), None);
        assert!(cell.apply(&delta(Path::new(), Op::Delete, hlc(200))));
        assert!(cell.is_tombstone());
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn older_write_does_not_resurrect_tombstone() {
        let mut cell = Cell::new(None, hlc(200), None);
        let changed = cell.apply(&delta(
            Path::new(),
            Op::Replace {
                value: Value::Int(2),
            },
            hlc(100),
        ));
        assert!(!changed);
        assert!(cell.is_tombstone());
    }

    #[test]
    fn apply_set_field() {
        let mut root = Cell::new(Some(Value::Record(Record::new())), hlc(50), None);
        let path = Path::new().step(TypeTag::Record, Segment::Record("x".into()));
        assert!(root.apply(&delta(
            path,
            Op::Replace {
                value: Value::String("hi".into()),
            },
            hlc(100),
        )));
    }

    #[test]
    fn nested_update_does_not_bump_existing_parent_hlc() {
        let mut root = Cell::new(Some(Value::Record(Record::new())), hlc(50), None);
        let path = Path::new().step(TypeTag::Record, Segment::Record("x".into()));
        assert!(root.apply(&delta(
            path,
            Op::Replace {
                value: Value::Int(1),
            },
            hlc(100),
        )));
        assert_eq!(root.hlc, hlc(50));
    }

    #[test]
    fn recreated_parent_gets_event_hlc() {
        let mut root = Cell::new(None, hlc(50), None);
        let path = Path::new().step(TypeTag::Record, Segment::Record("x".into()));
        assert!(root.apply(&delta(
            path,
            Op::Replace {
                value: Value::Int(1),
            },
            hlc(100),
        )));
        assert_eq!(root.hlc, hlc(100));
        assert_eq!(root.type_tag(), Some(TypeTag::Record));
    }

    #[test]
    fn merge_same_record_recurses() {
        let mut local = Cell::new(Some(Value::Record(Record::new())), hlc(50), None);
        let mut remote = Cell::new(Some(Value::Record(Record::new())), hlc(50), None);
        let local_path = Path::new().step(TypeTag::Record, Segment::Record("a".into()));
        let remote_path = Path::new().step(TypeTag::Record, Segment::Record("b".into()));
        local.apply(&delta(
            local_path,
            Op::Replace {
                value: Value::Int(1),
            },
            hlc(100),
        ));
        remote.apply(&delta(
            remote_path,
            Op::Replace {
                value: Value::Int(2),
            },
            hlc(110),
        ));
        assert!(local.merge(&remote));
        let Some(Value::Record(record)) = &local.value else {
            panic!("expected record");
        };
        assert!(record.get("a").is_some_and(|cell| !cell.is_tombstone()));
        assert!(record.get("b").is_some_and(|cell| !cell.is_tombstone()));
    }

    #[test]
    fn merge_scalar_uses_original_local_hlc() {
        let mut local = Cell::new(Some(Value::Int(1)), hlc(100), None);
        let remote = Cell::new(Some(Value::Int(2)), hlc(200), None);
        assert!(local.merge(&remote));
        assert_eq!(local.value, Some(Value::Int(2)));
        assert_eq!(local.hlc, hlc(200));
    }

    #[test]
    fn merge_keeps_sync_local() {
        let mut local = Cell::new(Some(Value::Int(1)), hlc(100), Some(false));
        let remote = Cell::new(Some(Value::Record(Record::new())), hlc(200), Some(true));
        assert!(local.merge(&remote));
        assert_eq!(local.type_tag(), Some(TypeTag::Record));
        assert_eq!(local.sync, Some(false));
    }
}
