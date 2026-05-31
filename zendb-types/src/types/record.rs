//! Record — the named-field container type.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{
    core::traits::{ContainerType, Type},
    Cell, Hlc, TypeTag,
};

// ---------------------------------------------------------------------------
// RecordValue
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RecordValue {
    pub fields: BTreeMap<String, Cell>,
    pub tombstones: BTreeMap<String, Hlc>,
}

impl RecordValue {
    pub fn is_field_visible(&self, name: &str) -> bool {
        if let Some(cell) = self.fields.get(name) {
            if let Some(&tomb_hlc) = self.tombstones.get(name) {
                return cell.hlc.beats(tomb_hlc);
            }
            return true;
        }
        false
    }

    pub fn visible_fields(&self) -> impl Iterator<Item = (&String, &Cell)> {
        self.fields.iter().filter(move |(name, cell)| {
            if let Some(&tomb_hlc) = self.tombstones.get(*name) {
                cell.hlc.beats(tomb_hlc)
            } else {
                true
            }
        })
    }
}

// ---------------------------------------------------------------------------
// RecordOp
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Encode, Decode)]
pub enum RecordOp {
    SetField { name: String, value: Cell },
    Replace { value: RecordValue },
    RemoveField { name: String },
}

// ---------------------------------------------------------------------------
// RecordSegment
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RecordSegment {
    pub field_name: String,
}

// ---------------------------------------------------------------------------
// RecordError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum RecordError {
    UnknownChildTag(u8),
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordError::UnknownChildTag(tag) => write!(f, "unknown child type tag: {}", tag),
        }
    }
}
impl std::error::Error for RecordError {}

// ---------------------------------------------------------------------------
// RecordType
// ---------------------------------------------------------------------------

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
            tombstones: BTreeMap::new(),
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
                if let Some(&tomb_hlc) = value.tombstones.get(name) {
                    if !cell.hlc.beats(tomb_hlc) {
                        return Ok(false);
                    }
                    value.tombstones.remove(name);
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
                let existing = value.tombstones.get(name).copied().unwrap_or(Hlc::ZERO);
                let effective = if op_hlc.beats(existing) {
                    op_hlc
                } else {
                    existing
                };
                value.tombstones.insert(name.clone(), effective);
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

        // Collect all field names from both sides
        let mut all_names: Vec<String> = local
            .fields
            .keys()
            .chain(remote.fields.keys())
            .cloned()
            .collect();
        all_names.sort();
        all_names.dedup();

        for field_name in all_names {
            let local_cell = local.fields.get(&field_name);
            let remote_cell = remote.fields.get(&field_name);
            let local_tomb = local.tombstones.get(&field_name);
            let remote_tomb = remote.tombstones.get(&field_name);

            let local_vis =
                local_cell.is_some_and(|c| local_tomb.map_or(true, |t| c.hlc.beats(*t)));
            let remote_vis =
                remote_cell.is_some_and(|c| remote_tomb.map_or(true, |t| c.hlc.beats(*t)));

            match (local_vis, remote_vis) {
                (true, true) => {
                    let mut merged_cell = local_cell.unwrap().clone();
                    merged_cell.merge(remote_cell.unwrap());
                    local.fields.insert(field_name.clone(), merged_cell);
                    changed = true;
                }
                (true, false) => {}
                (false, true) => {
                    local
                        .fields
                        .insert(field_name.clone(), remote_cell.unwrap().clone());
                    changed = true;
                }
                (false, false) => {}
            }

            let max_tomb = match (local_tomb, remote_tomb) {
                (Some(&lt), Some(&rt)) => {
                    if rt.beats(lt) {
                        rt
                    } else {
                        lt
                    }
                }
                (Some(&t), None) | (None, Some(&t)) => t,
                (None, None) => continue,
            };
            if !local
                .fields
                .get(&field_name)
                .is_some_and(|c| c.hlc.beats(max_tomb))
            {
                local.tombstones.insert(field_name, max_tomb);
                changed = true;
            }
        }
        Ok(changed)
    }
}

// ---------------------------------------------------------------------------
// ContainerType
// ---------------------------------------------------------------------------

impl ContainerType for RecordType {
    type Segment = RecordSegment;

    fn descend_or_create<'a>(
        value: &'a mut RecordValue,
        segment: &RecordSegment,
        child_tag: TypeTag,
    ) -> Result<&'a mut Cell, RecordError> {
        if let Some(&tomb_hlc) = value.tombstones.get(&segment.field_name) {
            if let Some(cell) = value.fields.get(&segment.field_name) {
                if !cell.hlc.beats(tomb_hlc) {
                    value.fields.remove(&segment.field_name);
                }
            }
        }
        if !value.fields.contains_key(&segment.field_name) {
            value.fields.insert(
                segment.field_name.clone(),
                Cell::dummy(child_tag.empty_value()),
            );
        }
        Ok(value.fields.get_mut(&segment.field_name).unwrap())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{types::atom::AtomValue, Value};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, 1).unwrap()
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
    }

    #[test]
    fn record_apply_set_beats_tombstone() {
        let mut state = RecordType::empty();
        state.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None),
        );
        state.tombstones.insert("x".into(), hlc(200));
        let op = RecordOp::SetField {
            name: "x".into(),
            value: Cell::new(Value::Atom(AtomValue::Int(2)), hlc(300), None),
        };
        let changed = RecordType::apply_op(&mut state, &op, Hlc::ZERO, hlc(300)).unwrap();
        assert!(changed);
        assert!(state.is_field_visible("x"));
        assert!(!state.tombstones.contains_key("x"));
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
            "age".into(),
            Cell::new(Value::Atom(AtomValue::Int(30)), hlc(200), None),
        );
        val.tombstones.insert("deleted".into(), hlc(300));
        let buf = encode_to_vec(&val, config::standard()).unwrap();
        let (decoded, consumed): (RecordValue, usize) =
            decode_from_slice(&buf, config::standard()).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.fields.len(), 2);
        assert_eq!(decoded.tombstones.len(), 1);
    }
}
