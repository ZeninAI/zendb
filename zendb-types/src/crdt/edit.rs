//! Transactional edit sessions for materialized CRDT values.

use std::{marker::PhantomData, mem};

use crate::{EventTime, OpDispatcher, PathOp, Segment, TypeError, TypeOp, Value};

/// A local materialized value and the path operations produced while editing it.
#[derive(Debug, Clone, Default)]
pub struct Edit {
    value: Option<Value>,
    changes: Vec<PathOp>,
}

impl Edit {
    pub fn new(value: impl Into<Value>) -> Self {
        Self {
            value: Some(value.into()),
            changes: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_option(value: Option<Value>) -> Self {
        Self {
            value,
            changes: Vec::new(),
        }
    }

    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    pub fn changes(&self) -> &[PathOp] {
        &self.changes
    }

    pub fn take_changes(&mut self) -> Vec<PathOp> {
        mem::take(&mut self.changes)
    }

    pub fn into_parts(self) -> (Option<Value>, Vec<PathOp>) {
        (self.value, self.changes)
    }

    pub fn at(&mut self, segment: Segment) -> EditCursor<'_> {
        EditCursor {
            edit: self,
            path: vec![segment],
        }
    }

    pub fn typed<T>(&mut self) -> TypedEdit<'_, T> {
        TypedEdit {
            edit: self,
            path: Vec::new(),
            marker: PhantomData,
        }
    }

    fn apply_operation(
        &mut self,
        remote: EventTime,
        path: &[Segment],
        op: TypeOp,
    ) -> Result<bool, TypeError> {
        let changed = self.value.apply_path(remote, path, &op)?;
        if changed {
            self.changes.push(PathOp {
                path: path.to_vec(),
                time: remote,
                op,
            });
        }
        Ok(changed)
    }
}

impl From<Value> for Edit {
    fn from(value: Value) -> Self {
        Self::new(value)
    }
}

/// A path-building cursor into an edit session.
pub struct EditCursor<'a> {
    edit: &'a mut Edit,
    path: Vec<Segment>,
}

impl<'a> EditCursor<'a> {
    pub fn at(mut self, segment: Segment) -> Self {
        self.path.push(segment);
        self
    }

    pub fn typed<T>(self) -> TypedEdit<'a, T> {
        TypedEdit {
            edit: self.edit,
            path: self.path,
            marker: PhantomData,
        }
    }
}

/// A type-specific edit facade generated with the operations of `T`.
pub struct TypedEdit<'a, T> {
    edit: &'a mut Edit,
    path: Vec<Segment>,
    marker: PhantomData<fn() -> T>,
}

impl<'a, T> TypedEdit<'a, T> {
    pub fn at(mut self, segment: Segment) -> Self {
        self.path.push(segment);
        self
    }

    pub fn typed<U>(self) -> TypedEdit<'a, U> {
        TypedEdit {
            edit: self.edit,
            path: self.path,
            marker: PhantomData,
        }
    }

    pub(crate) fn apply_operation(
        &mut self,
        remote: EventTime,
        op: TypeOp,
    ) -> Result<bool, TypeError> {
        self.edit.apply_operation(remote, &self.path, op)
    }
}
