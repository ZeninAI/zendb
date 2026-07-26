//! Record - the named-field container type.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{Cell, ContainerType, EventStamp, MergeStamps, Op, Segment, Type, TypeError};

pub type RecordSegment = String;

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct Record {
    fields: BTreeMap<String, Cell>,
}

impl Record {
    pub fn from_fields(fields: impl IntoIterator<Item = (String, Cell)>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
        }
    }

    pub fn get(&self, field: &str) -> Option<&Cell> {
        self.fields.get(field)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Iterate over all fields and their cells.
    pub fn fields(&self) -> impl Iterator<Item = (&str, &Cell)> {
        self.fields.iter().map(|(k, v)| (k.as_str(), v))
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

    fn apply(&mut self, op: &RecordOp, _stamps: crate::MergeStamps) -> Result<bool, RecordError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Record, _stamps: MergeStamps) -> Result<bool, RecordError> {
        let mut changed = false;

        for (field_name, remote_cell) in &remote.fields {
            match self.fields.get_mut(field_name) {
                Some(local_cell) => {
                    let stamps = MergeStamps::new(local_cell.stamp, remote_cell.stamp);
                    if local_cell
                        .merge(remote_cell, stamps)
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

    fn max_stamp(&self) -> EventStamp {
        self.fields
            .values()
            .fold(EventStamp::default(), |max, cell| {
                std::cmp::max(max, cell.max_stamp())
            })
    }
}

impl ContainerType for Record {
    fn child(&self, segment: &Segment) -> Option<&Cell> {
        let Segment::Record(field) = segment else {
            return None;
        };
        self.fields.get(field)
    }

    fn child_mut(&mut self, segment: &Segment) -> Option<&mut Cell> {
        let Segment::Record(field) = segment else {
            return None;
        };
        self.fields.get_mut(field)
    }

    fn apply_walk(
        &mut self,
        op: &Op,
        stamps: crate::MergeStamps,
        path: &[Segment],
    ) -> Result<bool, RecordError> {
        let incoming = stamps.incoming;
        let Some((segment, remaining)) = path.split_first() else {
            return Ok(false);
        };
        let Segment::Record(field) = segment else {
            return Ok(false);
        };
        let child_tag = remaining
            .first()
            .map(Segment::type_tag)
            .or_else(|| op.type_tag());
        let child = self.fields.entry(field.clone()).or_insert_with(|| {
            child_tag
                .map(|tag| Cell::dummy(Some(tag.empty_value())))
                .unwrap_or(Cell {
                    value: None,
                    stamp: EventStamp::default(),
                })
        });
        if child_tag.is_some_and(|tag| !child.ensure_type(tag, incoming)) {
            return Ok(false);
        }
        let child_stamps = MergeStamps::new(child.stamp, incoming);
        child
            .apply_walk(op, child_stamps, remaining)
            .map_err(|error| RecordError::Child(Box::new(error)))
    }
}
