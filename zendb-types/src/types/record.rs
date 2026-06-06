//! Record - the named-field container type.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{
    core::traits::{ContainerType, Type},
    Cell, Hlc, TypeTag,
};

pub type Record = BTreeMap<String, Cell>;
pub type RecordSegment = String;

#[derive(Debug, Clone, Encode, Decode)]
pub enum RecordOp {}

#[derive(Debug)]
pub enum RecordError {}

impl std::fmt::Display for RecordError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}
impl std::error::Error for RecordError {}

impl Type for Record {
    type Op = RecordOp;
    type Error = RecordError;

    fn apply(&mut self, op: &RecordOp, _local_hlc: Hlc, _op_hlc: Hlc) -> Result<bool, RecordError> {
        match *op {}
    }

    fn merge(
        &mut self,
        remote: &Record,
        _local_hlc: Hlc,
        _remote_hlc: Hlc,
    ) -> Result<bool, RecordError> {
        let mut changed = false;

        for (field_name, remote_cell) in remote {
            match self.get_mut(field_name) {
                Some(local_cell) => {
                    if local_cell.merge(remote_cell) {
                        changed = true;
                    }
                }
                None => {
                    self.insert(field_name.clone(), remote_cell.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.values()
            .fold(Hlc::ZERO, |max, cell| std::cmp::max(max, cell.max_hlc()))
    }
}

impl ContainerType for Record {
    type Segment = RecordSegment;

    fn child_or_default<'a>(
        &'a mut self,
        segment: &RecordSegment,
        child_tag: Option<TypeTag>,
    ) -> Result<&'a mut Cell, RecordError> {
        if !self.contains_key(segment) {
            let cell = child_tag
                .map(|tag| Cell::dummy(tag.empty_value()))
                .unwrap_or_else(|| Cell::new(None, Hlc::ZERO, None));
            self.insert(segment.clone(), cell);
        }
        Ok(self.get_mut(segment).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, [1u8; 8]).unwrap()
    }

    #[test]
    fn record_child_or_default_creates_field() {
        let mut state = Record::new();
        let segment: RecordSegment = "x".into();
        let child = state
            .child_or_default(&segment, Some(TypeTag::Int))
            .unwrap();
        assert_eq!(child.type_tag(), Some(TypeTag::Int));
        assert!(state.get("x").is_some_and(|cell| !cell.is_tombstone()));
    }

    #[test]
    fn record_merge_both_visible() {
        let mut local = Record::new();
        local.insert("x".into(), Cell::new(Some(Value::Int(1)), hlc(100), None));
        let mut remote = Record::new();
        remote.insert("x".into(), Cell::new(Some(Value::Int(2)), hlc(200), None));
        let changed = Type::merge(&mut local, &remote, hlc(100), hlc(200)).unwrap();
        assert!(changed);
        assert!(local.get("x").is_some_and(|cell| !cell.is_tombstone()));
    }

    #[test]
    fn record_merge_tombstone_wins() {
        let mut local = Record::new();
        local.insert("x".into(), Cell::new(Some(Value::Int(1)), hlc(100), None));
        let mut remote = Record::new();
        remote.insert("x".into(), Cell::new(None, hlc(200), None));
        let changed = Type::merge(&mut local, &remote, hlc(100), hlc(200)).unwrap();
        assert!(changed);
        assert!(!local.get("x").is_some_and(|cell| !cell.is_tombstone()));
    }

    #[test]
    fn record_max_hlc_recurses_through_fields() {
        let mut nested = Record::new();
        nested.insert(
            "leaf".into(),
            Cell::new(Some(Value::Int(1)), hlc(300), None),
        );

        let mut record = Record::new();
        record.insert(
            "nested".into(),
            Cell::new(Some(Value::Record(nested)), hlc(100), None),
        );
        record.insert("deleted".into(), Cell::new(None, hlc(200), None));

        assert_eq!(record.max_hlc(), hlc(300));
    }

    #[test]
    fn record_value_bincode_roundtrip() {
        let mut val = Record::new();
        val.insert(
            "name".into(),
            Cell::new(Some(Value::String("Alice".into())), hlc(100), None),
        );
        val.insert("deleted".into(), Cell::new(None, hlc(300), None));
        let buf = encode_to_vec(&val, config::standard()).unwrap();
        let (decoded, consumed): (Record, usize) =
            decode_from_slice(&buf, config::standard()).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.len(), 2);
        assert!(decoded.get("deleted").unwrap().is_tombstone());
    }
}
