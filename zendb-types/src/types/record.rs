//! Record - the named-field container type.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{Cell, ContainerType, Hlc, MergeClocks, Op, PathStep, Segment, Type, TypeError};

pub type RecordSegment = String;

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct Record {
    fields: BTreeMap<String, Cell>,
}

impl Record {
    pub fn get(&self, field: &str) -> Option<&Cell> {
        self.fields.get(field)
    }

    pub fn contains(&self, field: &str) -> bool {
        self.fields.contains_key(field)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn insert(&mut self, field: String, cell: Cell) -> Option<Cell> {
        self.fields.insert(field, cell)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum RecordOp {}

#[derive(Debug)]
pub enum RecordError {
    Child(Box<TypeError>),
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordError::Child(error) => write!(f, "child operation failed: {error}"),
        }
    }
}
impl std::error::Error for RecordError {}

impl Type for Record {
    type Op = RecordOp;
    type Error = RecordError;

    fn apply(&mut self, op: &RecordOp, _op_hlc: Hlc) -> Result<bool, RecordError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Record, _clocks: MergeClocks) -> Result<bool, RecordError> {
        let mut changed = false;

        for (field_name, remote_cell) in &remote.fields {
            match self.fields.get_mut(field_name) {
                Some(local_cell) => {
                    if Type::merge(local_cell, remote_cell, MergeClocks::ZERO)
                        .map_err(|error| RecordError::Child(Box::new(error)))?
                    {
                        changed = true;
                    }
                }
                None => {
                    self.fields.insert(field_name.clone(), remote_cell.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn is_synced(&self, inherited: bool, path: &[PathStep]) -> bool {
        let Some((step, remaining)) = path.split_first() else {
            return inherited;
        };
        let Segment::Record(field) = &step.segment else {
            return inherited;
        };
        self.fields
            .get(field)
            .map(|child| child.is_synced(inherited, remaining))
            .unwrap_or(inherited)
    }

    fn compact(&mut self, watermark: Hlc) -> Result<bool, RecordError> {
        let mut changed = false;
        let stable_tombstones: Vec<String> = self
            .fields
            .iter()
            .filter(|(_, child)| child.is_tombstone() && child.hlc <= watermark)
            .map(|(field, _)| field.clone())
            .collect();
        for field in stable_tombstones {
            self.fields.remove(&field);
            changed = true;
        }
        for child in self.fields.values_mut() {
            if Type::compact(child, watermark)
                .map_err(|error| RecordError::Child(Box::new(error)))?
            {
                changed = true;
            }
        }
        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.fields.values().fold(Hlc::ZERO, |max, cell| {
            std::cmp::max(max, Type::max_hlc(cell))
        })
    }
}

impl ContainerType for Record {
    fn apply_walk(&mut self, op: &Op, op_hlc: Hlc, path: &[PathStep]) -> Result<bool, RecordError> {
        let Some((step, remaining)) = path.split_first() else {
            return Ok(false);
        };
        let Segment::Record(field) = &step.segment else {
            return Ok(false);
        };
        let child_tag = remaining
            .first()
            .map(|step| step.container_tag)
            .or_else(|| op.target_type());
        let child = self.fields.entry(field.clone()).or_insert_with(|| {
            child_tag
                .map(|tag| Cell::dummy(Some(tag.empty_value())))
                .unwrap_or(Cell {
                    value: None,
                    hlc: Hlc::ZERO,
                    sync: None,
                })
        });
        if child_tag.is_some_and(|tag| !child.ensure_type(tag, op_hlc)) {
            return Ok(false);
        }
        ContainerType::apply_walk(child, op, op_hlc, remaining)
            .map_err(|error| RecordError::Child(Box::new(error)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PathStep, Segment, TypeTag, Value};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, [1u8; 8]).unwrap()
    }

    fn cell(value: Option<Value>, hlc: Hlc, sync: Option<bool>) -> Cell {
        Cell { value, hlc, sync }
    }

    fn clock(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn record(entries: impl IntoIterator<Item = (&'static str, Cell)>) -> Record {
        let mut record = Record::default();
        for (name, cell) in entries {
            record.insert(name.into(), cell);
        }
        record
    }

    fn merge_order(records: &[Record; 3], order: [usize; 3]) -> Record {
        let mut merged = records[order[0]].clone();
        Type::merge(&mut merged, &records[order[1]], crate::MergeClocks::ZERO).unwrap();
        Type::merge(&mut merged, &records[order[2]], crate::MergeClocks::ZERO).unwrap();
        merged
    }

    #[test]
    fn record_merge_both_visible() {
        let mut local = Record::default();
        local.insert("x".into(), cell(Some(Value::Int(1)), hlc(100), None));
        let mut remote = Record::default();
        remote.insert("x".into(), cell(Some(Value::Int(2)), hlc(200), None));
        let changed =
            Type::merge(&mut local, &remote, MergeClocks::new(hlc(100), hlc(200))).unwrap();
        assert!(changed);
        assert!(local.get("x").is_some_and(|cell| !cell.is_tombstone()));
    }

    #[test]
    fn record_apply_heals_stale_child_type_before_descending() {
        let mut state = record([(
            "nested",
            cell(Some(Value::String("stale".into())), hlc(100), None),
        )]);
        let path = vec![
            PathStep::new(TypeTag::Record, Segment::Record("nested".into())),
            PathStep::new(TypeTag::Record, Segment::Record("leaf".into())),
        ];

        assert!(ContainerType::apply_walk(
            &mut state,
            &Op::Replace {
                value: Value::Int(1),
            },
            hlc(200),
            &path,
        )
        .unwrap());

        let Some(Value::Record(nested)) = &state.get("nested").unwrap().value else {
            panic!("expected healed record");
        };
        assert_eq!(nested.get("leaf").unwrap().value, Some(Value::Int(1)));
    }

    #[test]
    fn record_apply_does_not_heal_newer_child_type() {
        let mut state = record([(
            "nested",
            cell(Some(Value::String("newer".into())), hlc(200), None),
        )]);
        let path = vec![
            PathStep::new(TypeTag::Record, Segment::Record("nested".into())),
            PathStep::new(TypeTag::Record, Segment::Record("leaf".into())),
        ];

        assert!(!ContainerType::apply_walk(
            &mut state,
            &Op::Replace {
                value: Value::Int(1),
            },
            hlc(100),
            &path,
        )
        .unwrap());
        assert_eq!(
            state.get("nested").unwrap().value,
            Some(Value::String("newer".into()))
        );
    }

    #[test]
    fn record_merge_tombstone_wins() {
        let mut local = Record::default();
        local.insert("x".into(), cell(Some(Value::Int(1)), hlc(100), None));
        let mut remote = Record::default();
        remote.insert("x".into(), cell(None, hlc(200), None));
        let changed =
            Type::merge(&mut local, &remote, MergeClocks::new(hlc(100), hlc(200))).unwrap();
        assert!(changed);
        assert!(local.get("x").is_none_or(Cell::is_tombstone));
    }

    #[test]
    fn record_merge_is_idempotent() {
        let mut local = record([
            ("live", cell(Some(Value::Int(1)), clock(100, 1), None)),
            ("deleted", cell(None, clock(200, 1), None)),
        ]);
        let snapshot = local.clone();

        assert!(!Type::merge(&mut local, &snapshot, crate::MergeClocks::ZERO).unwrap());
        assert_eq!(local, snapshot);
    }

    #[test]
    fn record_merge_is_commutative_for_independent_and_conflicting_fields() {
        let left = record([
            ("left", cell(Some(Value::Bool(true)), clock(100, 1), None)),
            ("shared", cell(Some(Value::Int(1)), clock(100, 1), None)),
        ]);
        let right = record([
            (
                "right",
                cell(Some(Value::String("right".into())), clock(100, 2), None),
            ),
            ("shared", cell(Some(Value::Int(2)), clock(100, 2), None)),
        ]);

        let mut left_first = left.clone();
        Type::merge(&mut left_first, &right, crate::MergeClocks::ZERO).unwrap();
        let mut right_first = right;
        Type::merge(&mut right_first, &left, crate::MergeClocks::ZERO).unwrap();

        assert_eq!(left_first, right_first);
        assert_eq!(left_first.get("shared").unwrap().value, Some(Value::Int(2)));
    }

    #[test]
    fn record_merge_converges_for_every_replica_order() {
        let records = [
            record([("field", cell(Some(Value::Int(1)), clock(100, 1), None))]),
            record([("field", cell(None, clock(200, 2), None))]),
            record([
                ("field", cell(Some(Value::Int(3)), clock(300, 3), None)),
                ("extra", cell(Some(Value::Bool(true)), clock(150, 3), None)),
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
        let left_nested = record([("left", cell(Some(Value::Int(1)), clock(100, 1), None))]);
        let right_nested = record([("right", cell(Some(Value::Int(2)), clock(100, 2), None))]);
        let mut left = record([(
            "nested",
            cell(Some(Value::Record(left_nested)), clock(50, 1), None),
        )]);
        let right = record([(
            "nested",
            cell(Some(Value::Record(right_nested)), clock(50, 2), None),
        )]);

        assert!(Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap());
        let Some(Value::Record(nested)) = &left.get("nested").unwrap().value else {
            panic!("expected nested record");
        };
        assert!(nested.contains("left"));
        assert!(nested.contains("right"));
    }

    #[test]
    fn record_max_hlc_recurses_through_fields() {
        let mut nested = Record::default();
        nested.insert("leaf".into(), cell(Some(Value::Int(1)), hlc(300), None));

        let mut record = Record::default();
        record.insert(
            "nested".into(),
            cell(Some(Value::Record(nested)), hlc(100), None),
        );
        record.insert("deleted".into(), cell(None, hlc(200), None));

        assert_eq!(record.max_hlc(), hlc(300));
    }

    #[test]
    fn record_compact_removes_stable_tombstones_and_recurses_into_live_values() {
        let mut nested = Record::default();
        nested.insert("dead".into(), cell(None, hlc(100), None));
        let mut record = record([
            ("dead", cell(None, hlc(100), None)),
            ("future", cell(None, hlc(300), None)),
            ("nested", cell(Some(Value::Record(nested)), hlc(50), None)),
        ]);

        assert!(Type::compact(&mut record, hlc(200)).unwrap());
        assert!(!record.contains("dead"));
        assert!(record.contains("future"));
        let Some(Value::Record(nested)) = &record.get("nested").unwrap().value else {
            panic!("expected nested record");
        };
        assert!(!nested.contains("dead"));
    }

    #[test]
    fn record_value_bincode_roundtrip() {
        let mut val = Record::default();
        val.insert(
            "name".into(),
            cell(Some(Value::String("Alice".into())), hlc(100), None),
        );
        val.insert("deleted".into(), cell(None, hlc(300), None));
        let buf = encode_to_vec(&val, config::standard()).unwrap();
        let (decoded, consumed): (Record, usize) =
            decode_from_slice(&buf, config::standard()).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.len(), 2);
        assert!(decoded.get("deleted").unwrap().is_tombstone());
    }
}
