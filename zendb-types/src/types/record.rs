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
                .map(|tag| Cell::dummy(Some(tag.empty_value())))
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

    fn clock(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn record(entries: impl IntoIterator<Item = (&'static str, Cell)>) -> Record {
        entries
            .into_iter()
            .map(|(name, cell)| (name.into(), cell))
            .collect()
    }

    fn merge_order(records: &[Record; 3], order: [usize; 3]) -> Record {
        let mut merged = records[order[0]].clone();
        Type::merge(&mut merged, &records[order[1]], Hlc::ZERO, Hlc::ZERO).unwrap();
        Type::merge(&mut merged, &records[order[2]], Hlc::ZERO, Hlc::ZERO).unwrap();
        merged
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
    fn record_child_or_default_preserves_existing_field() {
        let mut state = record([(
            "x",
            Cell::new(Some(Value::String("existing".into())), hlc(100), Some(true)),
        )]);
        let original = state.get("x").unwrap().clone();

        let child = state
            .child_or_default(&"x".into(), Some(TypeTag::Int))
            .unwrap();

        assert_eq!(child, &original);
    }

    #[test]
    fn record_child_or_default_can_create_tombstone_placeholder() {
        let mut state = Record::new();
        let child = state.child_or_default(&"missing".into(), None).unwrap();
        assert!(child.is_tombstone());
        assert!(child.is_dummy());
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
        assert!(local.get("x").is_none_or(Cell::is_tombstone));
    }

    #[test]
    fn record_merge_is_idempotent() {
        let mut local = record([
            ("live", Cell::new(Some(Value::Int(1)), clock(100, 1), None)),
            ("deleted", Cell::new(None, clock(200, 1), None)),
        ]);
        let snapshot = local.clone();

        assert!(!Type::merge(&mut local, &snapshot, Hlc::ZERO, Hlc::ZERO).unwrap());
        assert_eq!(local, snapshot);
    }

    #[test]
    fn record_merge_is_commutative_for_independent_and_conflicting_fields() {
        let left = record([
            (
                "left",
                Cell::new(Some(Value::Bool(true)), clock(100, 1), None),
            ),
            (
                "shared",
                Cell::new(Some(Value::Int(1)), clock(100, 1), None),
            ),
        ]);
        let right = record([
            (
                "right",
                Cell::new(Some(Value::String("right".into())), clock(100, 2), None),
            ),
            (
                "shared",
                Cell::new(Some(Value::Int(2)), clock(100, 2), None),
            ),
        ]);

        let mut left_first = left.clone();
        Type::merge(&mut left_first, &right, Hlc::ZERO, Hlc::ZERO).unwrap();
        let mut right_first = right;
        Type::merge(&mut right_first, &left, Hlc::ZERO, Hlc::ZERO).unwrap();

        assert_eq!(left_first, right_first);
        assert_eq!(left_first.get("shared").unwrap().value, Some(Value::Int(2)));
    }

    #[test]
    fn record_merge_converges_for_every_replica_order() {
        let records = [
            record([("field", Cell::new(Some(Value::Int(1)), clock(100, 1), None))]),
            record([("field", Cell::new(None, clock(200, 2), None))]),
            record([
                ("field", Cell::new(Some(Value::Int(3)), clock(300, 3), None)),
                (
                    "extra",
                    Cell::new(Some(Value::Bool(true)), clock(150, 3), None),
                ),
            ]),
        ];
        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let expected = merge_order(&records, orders[0]);

        for order in orders.into_iter().skip(1) {
            assert_eq!(merge_order(&records, order), expected);
        }
    }

    #[test]
    fn record_merge_recursively_combines_nested_records() {
        let left_nested = record([("left", Cell::new(Some(Value::Int(1)), clock(100, 1), None))]);
        let right_nested = record([("right", Cell::new(Some(Value::Int(2)), clock(100, 2), None))]);
        let mut left = record([(
            "nested",
            Cell::new(Some(Value::Record(left_nested)), clock(50, 1), None),
        )]);
        let right = record([(
            "nested",
            Cell::new(Some(Value::Record(right_nested)), clock(50, 2), None),
        )]);

        assert!(Type::merge(&mut left, &right, Hlc::ZERO, Hlc::ZERO).unwrap());
        let Some(Value::Record(nested)) = &left.get("nested").unwrap().value else {
            panic!("expected nested record");
        };
        assert!(nested.contains_key("left"));
        assert!(nested.contains_key("right"));
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
