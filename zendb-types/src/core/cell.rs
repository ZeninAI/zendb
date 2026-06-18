//! Cell - the universal addressable value wrapper.

use bincode::{Decode, Encode};

use crate::core::traits::{ContainerType, MergeClocks, Type};
use crate::{Hlc, Op, PathStep, TypeError, TypeTag, Value};

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

    /// Ensure this cell contains `expected`, replacing stale state when the
    /// incoming operation is newer than all state currently below this cell.
    pub(crate) fn ensure_type(&mut self, expected: TypeTag, op_hlc: Hlc) -> bool {
        if self.type_tag() == Some(expected) {
            return true;
        }

        if op_hlc.beats(self.max_hlc()) {
            self.value = Some(expected.empty_value());
            self.hlc = op_hlc;
            return true;
        }

        false
    }

    /// Apply an event to this cell. Returns true if state was modified.
    ///
    /// `sync` is the mandatory sync policy inherited from the owning table.
    /// The nearest explicit cell sync flag on the target path overrides it.
    /// Remote-device events cannot mutate local-only values, except that
    /// `SetSync` is always allowed to update the policy itself.
    pub fn apply_event(&mut self, event: &crate::Event, sync: bool) -> Result<bool, TypeError> {
        if !matches!(&event.op, Op::SetSync { .. })
            && !self.is_synced(sync, &event.path)
            && event.hlc.device_id() != crate::device_id()
        {
            return Ok(false);
        }

        ContainerType::apply_walk(self, &event.op, event.hlc, &event.path)
    }
}

impl Type for Cell {
    type Op = Op;
    type Error = TypeError;

    fn apply(&mut self, op: &Op, op_hlc: Hlc) -> Result<bool, TypeError> {
        let changed = match op {
            Op::Type(type_op) => {
                let expected = type_op.type_tag();
                if !self.ensure_type(expected, op_hlc) {
                    return Ok(false);
                }
                let value = self
                    .value
                    .as_mut()
                    .expect("ensure_type must leave cursor live");
                match value.apply(type_op, op_hlc)? {
                    true => {
                        if op_hlc.beats(self.hlc) {
                            self.hlc = op_hlc;
                        }
                        true
                    }
                    false => false,
                }
            }
            Op::SetSync { sync } => {
                if !op_hlc.beats(self.hlc) {
                    return Ok(false);
                }
                self.sync = *sync;
                self.hlc = op_hlc;
                true
            }
            Op::Delete => {
                if !op_hlc.beats(self.hlc) {
                    return Ok(false);
                }
                self.value = None;
                self.hlc = op_hlc;
                true
            }
            Op::Replace { value } => {
                if !op_hlc.beats(self.hlc) {
                    return Ok(false);
                }
                self.value = Some(value.clone());
                self.hlc = op_hlc;
                true
            }
            Op::Merge { cell } => Type::merge(self, cell, MergeClocks::ZERO)?,
        };
        Ok(changed)
    }

    fn merge(&mut self, remote: &Cell, _clocks: MergeClocks) -> Result<bool, TypeError> {
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
                    return Ok(true);
                }
            }
            (Some(local), Some(remote_value)) => {
                if local.type_tag() != remote_value.type_tag() {
                    if remote.hlc.beats(self.hlc) {
                        let sync = self.sync;
                        *self = remote.clone();
                        self.sync = sync;
                        return Ok(true);
                    }
                    return Ok(false);
                }

                if local.merge(remote_value, MergeClocks::new(self.hlc, remote.hlc))? {
                    changed = true;
                }
                if remote.hlc.beats(self.hlc) {
                    self.hlc = remote.hlc;
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn is_synced(&self, inherited: bool, path: &[PathStep]) -> bool {
        let effective = self.sync.unwrap_or(inherited);
        if path.is_empty() {
            return effective;
        }
        if self.type_tag() != Some(path[0].container_tag) {
            return effective;
        }
        self.value
            .as_ref()
            .map(|value| value.is_synced(effective, path))
            .unwrap_or(effective)
    }

    fn compact(&mut self, watermark: Hlc) -> Result<bool, TypeError> {
        let Some(value) = self.value.as_mut() else {
            return Ok(false);
        };
        value.compact(watermark)
    }

    fn max_hlc(&self) -> Hlc {
        match &self.value {
            Some(value) => self.hlc.max(value.max_hlc()),
            None => self.hlc,
        }
    }
}

impl ContainerType for Cell {
    fn apply_walk(&mut self, op: &Op, op_hlc: Hlc, path: &[PathStep]) -> Result<bool, TypeError> {
        let Some((step, _)) = path.split_first() else {
            return self.apply(op, op_hlc);
        };

        if !self.ensure_type(step.container_tag, op_hlc) {
            return Ok(false);
        }
        self.value
            .as_mut()
            .expect("ensure_type must leave the cell live")
            .apply_walk(op, op_hlc, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::record::Record;
    use crate::{Path, PathStep, PrimaryKey, Segment};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, [1u8; 8]).unwrap()
    }

    fn cell(value: Option<Value>, hlc: Hlc, sync: Option<bool>) -> Cell {
        Cell { value, hlc, sync }
    }

    fn local_hlc(ms: u64) -> Hlc {
        crate::init_device_id();
        Hlc::with_device_id(ms, 0, crate::device_id()).unwrap()
    }

    fn remote_hlc(ms: u64) -> Hlc {
        crate::init_device_id();
        let mut remote = crate::device_id();
        remote[0] ^= u8::MAX;
        Hlc::with_device_id(ms, 0, remote).unwrap()
    }

    fn event(path: Path, op: Op, hlc: Hlc) -> crate::Event {
        crate::Event {
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
        let cell = Cell::dummy(Some(Value::String(String::new())));
        assert!(cell.is_dummy());
        assert!(!cell.is_tombstone());
        assert_eq!(cell.sync, None);
    }

    #[test]
    fn cell_dummy_tombstone() {
        let cell = Cell::dummy(None);
        assert!(cell.is_dummy());
        assert!(cell.is_tombstone());
        assert_eq!(cell.sync, None);
    }

    #[test]
    fn bincode_roundtrip() {
        let cell = cell(Some(Value::String("hi".into())), hlc(100), Some(true));
        let buf = encode_to_vec(&cell, config::standard()).unwrap();
        let (decoded, n): (Cell, usize) = decode_from_slice(&buf, config::standard()).unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(decoded.hlc, cell.hlc);
        assert_eq!(decoded.type_tag(), Some(TypeTag::String));
    }

    #[test]
    fn replace_scalar() {
        let mut cell = Cell::dummy(Some(Value::String(String::new())));
        assert!(cell
            .apply_event(
                &event(
                    Path::new(),
                    Op::Replace {
                        value: Value::Int(42),
                    },
                    hlc(100),
                ),
                true
            )
            .unwrap());
        assert_eq!(cell.hlc, hlc(100));
    }

    #[test]
    fn apply_lww_older_no_change() {
        let mut cell = cell(Some(Value::Int(1)), hlc(200), None);
        let changed = cell
            .apply_event(
                &event(
                    Path::new(),
                    Op::Replace {
                        value: Value::Int(2),
                    },
                    hlc(100),
                ),
                true,
            )
            .unwrap();
        assert!(!changed);
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn delete_tombstones_cell() {
        let mut cell = cell(Some(Value::Int(1)), hlc(100), None);
        assert!(cell
            .apply_event(&event(Path::new(), Op::Delete, hlc(200)), true)
            .unwrap());
        assert!(cell.is_tombstone());
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn older_write_does_not_resurrect_tombstone() {
        let mut cell = cell(None, hlc(200), None);
        let changed = cell
            .apply_event(
                &event(
                    Path::new(),
                    Op::Replace {
                        value: Value::Int(2),
                    },
                    hlc(100),
                ),
                true,
            )
            .unwrap();
        assert!(!changed);
        assert!(cell.is_tombstone());
    }

    #[test]
    fn apply_set_field() {
        let mut root = cell(Some(Value::Record(Record::default())), hlc(50), None);
        let path = vec![PathStep::new(TypeTag::Record, Segment::Record("x".into()))];
        assert!(root
            .apply_event(
                &event(
                    path,
                    Op::Replace {
                        value: Value::String("hi".into()),
                    },
                    hlc(100),
                ),
                true
            )
            .unwrap());
    }

    #[test]
    fn remote_event_is_rejected_by_nearest_local_only_ancestor() {
        let mut nested = Record::default();
        nested.insert(
            "field".into(),
            cell(Some(Value::Int(1)), local_hlc(100), None),
        );
        let mut root_record = Record::default();
        root_record.insert(
            "nested".into(),
            cell(Some(Value::Record(nested)), local_hlc(100), Some(false)),
        );
        let mut root = cell(Some(Value::Record(root_record)), local_hlc(100), Some(true));
        let path = vec![
            PathStep::new(TypeTag::Record, Segment::Record("nested".into())),
            PathStep::new(TypeTag::Record, Segment::Record("field".into())),
        ];

        assert!(!root
            .apply_event(
                &event(
                    path,
                    Op::Replace {
                        value: Value::Int(2),
                    },
                    remote_hlc(200),
                ),
                true,
            )
            .unwrap());

        let Some(Value::Record(root_record)) = &root.value else {
            panic!("expected root record");
        };
        let Some(Value::Record(nested)) = &root_record.get("nested").unwrap().value else {
            panic!("expected nested record");
        };
        assert_eq!(nested.get("field").unwrap().value, Some(Value::Int(1)));
    }

    #[test]
    fn rejected_remote_event_does_not_create_missing_children() {
        let mut root = cell(
            Some(Value::Record(Record::default())),
            local_hlc(100),
            Some(false),
        );
        let path = vec![PathStep::new(
            TypeTag::Record,
            Segment::Record("missing".into()),
        )];

        assert!(!root
            .apply_event(
                &event(
                    path,
                    Op::Replace {
                        value: Value::Int(1),
                    },
                    remote_hlc(200),
                ),
                true,
            )
            .unwrap());

        let Some(Value::Record(record)) = &root.value else {
            panic!("expected record");
        };
        assert!(!record.contains("missing"));
    }

    #[test]
    fn remote_set_sync_is_allowed_on_local_only_target() {
        let mut root = cell(Some(Value::Int(1)), local_hlc(100), Some(false));

        assert!(root
            .apply_event(
                &event(
                    Path::new(),
                    Op::SetSync { sync: Some(true) },
                    remote_hlc(200),
                ),
                false,
            )
            .unwrap());
        assert_eq!(root.sync, Some(true));
    }

    #[test]
    fn set_sync_uses_hlc_for_clock_check() {
        let mut local = cell(Some(Value::Int(1)), hlc(100), None);
        // SetSync at 300 bumps hlc; older remote value is rejected.
        Type::apply(&mut local, &Op::SetSync { sync: Some(false) }, hlc(300)).unwrap();
        let remote = cell(Some(Value::Int(2)), hlc(200), None);

        let changed = Type::merge(&mut local, &remote, MergeClocks::ZERO).unwrap();
        // Remote at 200 doesn't beat the SetSync-bumped hlc of 300.
        assert!(!changed);
        assert_eq!(local.value, Some(Value::Int(1)));
        assert_eq!(local.hlc, hlc(300));
        assert_eq!(local.sync, Some(false));
    }

    #[test]
    fn nested_update_does_not_bump_existing_parent_hlc() {
        let mut root = cell(Some(Value::Record(Record::default())), hlc(50), None);
        let path = vec![PathStep::new(TypeTag::Record, Segment::Record("x".into()))];
        assert!(root
            .apply_event(
                &event(
                    path,
                    Op::Replace {
                        value: Value::Int(1),
                    },
                    hlc(100),
                ),
                true
            )
            .unwrap());
        assert_eq!(root.hlc, hlc(50));
    }

    #[test]
    fn recreated_parent_gets_event_hlc() {
        let mut root = cell(None, hlc(50), None);
        let path = vec![PathStep::new(TypeTag::Record, Segment::Record("x".into()))];
        assert!(root
            .apply_event(
                &event(
                    path,
                    Op::Replace {
                        value: Value::Int(1),
                    },
                    hlc(100),
                ),
                true
            )
            .unwrap());
        assert_eq!(root.hlc, hlc(100));
        assert_eq!(root.type_tag(), Some(TypeTag::Record));
    }

    #[test]
    fn merge_same_record_recurses() {
        let mut local = cell(Some(Value::Record(Record::default())), hlc(50), None);
        let mut remote = cell(Some(Value::Record(Record::default())), hlc(50), None);
        let local_path = vec![PathStep::new(TypeTag::Record, Segment::Record("a".into()))];
        let remote_path = vec![PathStep::new(TypeTag::Record, Segment::Record("b".into()))];
        local
            .apply_event(
                &event(
                    local_path,
                    Op::Replace {
                        value: Value::Int(1),
                    },
                    hlc(100),
                ),
                true,
            )
            .unwrap();
        remote
            .apply_event(
                &event(
                    remote_path,
                    Op::Replace {
                        value: Value::Int(2),
                    },
                    hlc(110),
                ),
                true,
            )
            .unwrap();
        assert!(Type::merge(&mut local, &remote, MergeClocks::ZERO).unwrap());
        let Some(Value::Record(record)) = &local.value else {
            panic!("expected record");
        };
        assert!(record.get("a").is_some_and(|cell| !cell.is_tombstone()));
        assert!(record.get("b").is_some_and(|cell| !cell.is_tombstone()));
    }

    #[test]
    fn merge_scalar_uses_original_local_hlc() {
        let mut local = cell(Some(Value::Int(1)), hlc(100), None);
        let remote = cell(Some(Value::Int(2)), hlc(200), None);
        assert!(Type::merge(&mut local, &remote, MergeClocks::ZERO).unwrap());
        assert_eq!(local.value, Some(Value::Int(2)));
        assert_eq!(local.hlc, hlc(200));
    }

    #[test]
    fn merge_keeps_sync_local() {
        let mut local = cell(Some(Value::Int(1)), hlc(100), Some(false));
        let remote = cell(Some(Value::Record(Record::default())), hlc(200), Some(true));
        assert!(Type::merge(&mut local, &remote, MergeClocks::ZERO).unwrap());
        assert_eq!(local.type_tag(), Some(TypeTag::Record));
        assert_eq!(local.sync, Some(false));
    }
}
