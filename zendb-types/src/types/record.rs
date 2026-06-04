//! Record - the named-field container type.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{
    core::traits::{ContainerType, Type},
    Cell, Hlc, TypeTag,
};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RecordValue {
    pub fields: BTreeMap<String, Cell>,
}

impl RecordValue {
    pub fn is_field_visible(&self, name: &str) -> bool {
        self.fields.get(name).is_some_and(|cell| !cell.is_tombstone())
    }

    pub fn visible_fields(&self) -> impl Iterator<Item = (&String, &Cell)> {
        self.fields
            .iter()
            .filter(|(_, cell)| !cell.is_tombstone())
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum RecordOp {
    SetField { name: String, value: Cell },
    Replace { value: RecordValue },
    RemoveField { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RecordSegment {
    pub field_name: String,
}

#[derive(Debug)]
pub enum RecordError {}

impl std::fmt::Display for RecordError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}
impl std::error::Error for RecordError {}

pub struct RecordType;

impl Type for RecordType {
    const TAG: TypeTag = TypeTag::Record;
    const NAME: &'static str = "Record";
    const KEYABLE: bool = false;
    const IS_CONTAINER: bool = true;
    type Value = RecordValue;
    type Op = RecordOp;
    type Error = RecordError;

    fn empty() -> RecordValue {
        RecordValue {
            fields: BTreeMap::new(),
        }
    }

    fn apply_op(
        value: &mut RecordValue,
        op: &RecordOp,
        local_hlc: Hlc,
        op_hlc: Hlc,
    ) -> Result<bool, RecordError> {
        match op {
            RecordOp::SetField { name, value: cell } => {
                if value
                    .fields
                    .get(name)
                    .is_some_and(|existing| existing.hlc.beats(cell.hlc))
                {
                    return Ok(false);
                }
                value.fields.insert(name.clone(), cell.clone());
                Ok(true)
            }
            RecordOp::Replace { value: new_val } => {
                if local_hlc.beats(op_hlc) {
                    return Ok(false);
                }
                *value = new_val.clone();
                Ok(true)
            }
            RecordOp::RemoveField { name } => {
                if value
                    .fields
                    .get(name)
                    .is_some_and(|existing| existing.hlc.beats(op_hlc))
                {
                    return Ok(false);
                }
                value
                    .fields
                    .insert(name.clone(), Cell::tombstone(op_hlc, None));
                Ok(true)
            }
        }
    }

    fn merge(
        local: &mut RecordValue,
        _local_hlc: Hlc,
        remote: &RecordValue,
        _remote_hlc: Hlc,
    ) -> Result<bool, RecordError> {
        let mut changed = false;

        for (field_name, remote_cell) in &remote.fields {
            match local.fields.get_mut(field_name) {
                Some(local_cell) => {
                    if local_cell.merge(remote_cell) {
                        changed = true;
                    }
                }
                None => {
                    local.fields.insert(field_name.clone(), remote_cell.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }
}

impl ContainerType for RecordType {
    type Segment = RecordSegment;

    fn descend_or_create<'a>(
        value: &'a mut RecordValue,
        segment: &RecordSegment,
        child_tag: Option<TypeTag>,
    ) -> Result<&'a mut Cell, RecordError> {
        if !value.fields.contains_key(&segment.field_name) {
            let cell = child_tag
                .map(|tag| Cell::dummy(Some(tag.empty_value())))
                .unwrap_or_else(|| Cell::dummy(None));
            value.fields.insert(segment.field_name.clone(), cell);
        }
        Ok(value.fields.get_mut(&segment.field_name).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{types::atom::AtomValue, Value};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, [1u8; 8]).unwrap()
    }

    #[test]
    fn record_apply_set_field() {
        let mut state = RecordType::empty();
        let op = RecordOp::SetField {
            name: "x".into(),
            value: Cell::new(Value::Atom(AtomValue::Int(42)), hlc(100), None),
        };
        let changed = RecordType::apply_op(&mut state, &op, Hlc::ZERO, hlc(100)).unwrap();
        assert!(changed);
        assert!(state.is_field_visible("x"));
    }

    #[test]
    fn record_apply_remove_field() {
        let mut state = RecordType::empty();
        state.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(42)), hlc(100), None),
        );
        let op = RecordOp::RemoveField { name: "x".into() };
        let changed = RecordType::apply_op(&mut state, &op, Hlc::ZERO, hlc(200)).unwrap();
        assert!(changed);
        assert!(!state.is_field_visible("x"));
        assert!(state.fields.get("x").unwrap().is_tombstone());
    }

    #[test]
    fn record_apply_set_beats_tombstone() {
        let mut state = RecordType::empty();
        state
            .fields
            .insert("x".into(), Cell::tombstone(hlc(200), None));
        let op = RecordOp::SetField {
            name: "x".into(),
            value: Cell::new(Value::Atom(AtomValue::Int(2)), hlc(300), None),
        };
        let changed = RecordType::apply_op(&mut state, &op, Hlc::ZERO, hlc(300)).unwrap();
        assert!(changed);
        assert!(state.is_field_visible("x"));
    }

    #[test]
    fn older_set_does_not_beat_tombstone() {
        let mut state = RecordType::empty();
        state
            .fields
            .insert("x".into(), Cell::tombstone(hlc(200), None));
        let op = RecordOp::SetField {
            name: "x".into(),
            value: Cell::new(Value::Atom(AtomValue::Int(2)), hlc(100), None),
        };
        let changed = RecordType::apply_op(&mut state, &op, Hlc::ZERO, hlc(100)).unwrap();
        assert!(!changed);
        assert!(!state.is_field_visible("x"));
    }

    #[test]
    fn record_merge_both_visible() {
        let mut local = RecordType::empty();
        local.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None),
        );
        let mut remote = RecordType::empty();
        remote.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(2)), hlc(200), None),
        );
        let changed = RecordType::merge(&mut local, hlc(100), &remote, hlc(200)).unwrap();
        assert!(changed);
        assert!(local.is_field_visible("x"));
    }

    #[test]
    fn record_merge_tombstone_wins() {
        let mut local = RecordType::empty();
        local.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None),
        );
        let mut remote = RecordType::empty();
        remote
            .fields
            .insert("x".into(), Cell::tombstone(hlc(200), None));
        let changed = RecordType::merge(&mut local, hlc(100), &remote, hlc(200)).unwrap();
        assert!(changed);
        assert!(!local.is_field_visible("x"));
    }

    #[test]
    fn record_value_bincode_roundtrip() {
        let mut val = RecordType::empty();
        val.fields.insert(
            "name".into(),
            Cell::new(
                Value::Atom(AtomValue::String("Alice".into())),
                hlc(100),
                None,
            ),
        );
        val.fields.insert(
            "deleted".into(),
            Cell::tombstone(hlc(300), None),
        );
        let buf = encode_to_vec(&val, config::standard()).unwrap();
        let (decoded, consumed): (RecordValue, usize) =
            decode_from_slice(&buf, config::standard()).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.fields.len(), 2);
        assert!(decoded.fields.get("deleted").unwrap().is_tombstone());
    }
}
