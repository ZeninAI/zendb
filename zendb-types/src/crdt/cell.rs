//! The universal addressable value wrapper.

use bincode::{Decode, Encode};

use crate::crdt::_traits::{ContainerType, MergeStamps, Type};
use crate::{EventStamp, Op, Segment, TypeError, TypeTag, Value};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct Cell {
    /// `Some` is live data; `None` is a tombstone.
    pub value: Option<Value>,
    pub stamp: EventStamp,
}

impl Cell {
    pub fn dummy(value: Option<Value>) -> Self {
        Self {
            value,
            stamp: EventStamp::default(),
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    pub fn type_tag(&self) -> Option<TypeTag> {
        self.value.as_ref().map(Value::type_tag)
    }

    pub(crate) fn ensure_type(&mut self, expected: TypeTag, incoming: EventStamp) -> bool {
        if self.type_tag() == Some(expected) {
            return true;
        }
        if incoming > self.max_stamp() {
            self.value = Some(expected.empty_value());
            self.stamp = incoming;
            return true;
        }
        false
    }
}

impl Type for Cell {
    type Op = Op;
    type Error = TypeError;

    fn apply(&mut self, op: &Op, stamps: MergeStamps) -> Result<bool, TypeError> {
        let incoming = stamps.incoming;
        match op {
            Op::Type(type_op) => {
                let expected = type_op.type_tag();
                if self.type_tag() != Some(expected) {
                    if incoming <= self.max_stamp() {
                        return Ok(false);
                    }
                    self.value = Some(expected.empty_value());
                }
                let value = self
                    .value
                    .as_mut()
                    .expect("typed operations must leave the cell live");
                let merge_stamps = MergeStamps::new(self.stamp, incoming);
                let changed = value.apply(type_op, merge_stamps)?;
                if let Some(stamp) = value.apply_stamp(merge_stamps, changed) {
                    self.stamp = stamp;
                }
                Ok(changed)
            }
            Op::Delete => {
                if incoming <= self.stamp {
                    return Ok(false);
                }
                self.value = None;
                self.stamp = incoming;
                Ok(true)
            }
            Op::Upsert { value } => {
                if incoming <= self.stamp {
                    return Ok(false);
                }
                self.value = Some(value.clone());
                self.stamp = incoming;
                Ok(true)
            }
            Op::Merge { cell } => self.merge(cell, MergeStamps::new(self.stamp, cell.stamp)),
        }
    }

    fn merge(&mut self, incoming: &Cell, stamps: MergeStamps) -> Result<bool, TypeError> {
        match (&mut self.value, &incoming.value) {
            (None, None) => {
                if stamps.incoming > stamps.current {
                    self.stamp = incoming.stamp;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            (None, Some(_)) | (Some(_), None) => {
                if stamps.incoming > stamps.current {
                    *self = incoming.clone();
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            (Some(current), Some(incoming_value)) => {
                if current.type_tag() != incoming_value.type_tag() {
                    if stamps.incoming > stamps.current {
                        *self = incoming.clone();
                        return Ok(true);
                    }
                    return Ok(false);
                }
                let mut changed = current.merge(incoming_value, stamps)?;
                if let Some(stamp) = current.merge_stamp(stamps, changed) {
                    if stamp != self.stamp {
                        self.stamp = stamp;
                        changed = true;
                    }
                }
                Ok(changed)
            }
        }
    }

    fn max_stamp(&self) -> EventStamp {
        self.value
            .as_ref()
            .map_or(self.stamp, |value| self.stamp.max(value.max_stamp()))
    }
}

impl ContainerType for Cell {
    fn child(&self, segment: &crate::Segment) -> Option<&Cell> {
        self.value.as_ref().and_then(|value| value.child(segment))
    }

    fn child_mut(&mut self, segment: &crate::Segment) -> Option<&mut Cell> {
        self.value
            .as_mut()
            .and_then(|value| value.child_mut(segment))
    }

    fn cell_at_path(&self, path: &[Segment]) -> Option<&Cell> {
        if path.is_empty() {
            return Some(self);
        }
        self.value.as_ref()?.cell_at_path(path)
    }

    fn cell_at_path_mut(&mut self, path: &[Segment]) -> Option<&mut Cell> {
        if path.is_empty() {
            return Some(self);
        }
        self.value.as_mut()?.cell_at_path_mut(path)
    }

    fn apply_walk(
        &mut self,
        op: &Op,
        stamps: MergeStamps,
        path: &[Segment],
    ) -> Result<bool, TypeError> {
        let Some((segment, _)) = path.split_first() else {
            return self.apply(op, MergeStamps::new(self.stamp, stamps.incoming));
        };
        if !self.ensure_type(segment.type_tag(), stamps.incoming) {
            return Ok(false);
        }
        self.value
            .as_mut()
            .expect("ensure_type must leave the cell live")
            .apply_walk(op, MergeStamps::new(self.stamp, stamps.incoming), path)
    }
}
