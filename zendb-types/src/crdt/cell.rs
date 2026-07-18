//! Cell - the universal addressable value wrapper.

use bincode::{Decode, Encode};

use crate::crdt::_traits::{ContainerType, MergeClocks, Type};
use crate::{Hlc, Op, PathStep, SyncPolicy, SyncScope, TypeError, TypeTag, Value};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct Cell {
    /// `Some` means the cell is live. `None` means the cell is a tombstone.
    pub value: Option<Value>,
    /// Structural clock for this cell: creation, deletion, type replacement,
    /// or a direct operation against this cell. Descendant-only updates do not
    /// bump ancestor HLCs.
    pub hlc: Hlc,
    /// Replica-local sync boundary. It is never changed by a CRDT operation.
    pub sync: SyncPolicy,
}

impl Cell {
    pub fn dummy(value: Option<Value>) -> Cell {
        Cell {
            value,
            hlc: Hlc::ZERO,
            sync: SyncPolicy::Inherit,
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

    /// Change local replication policy without changing CRDT state or clocks.
    ///
    /// Returns `false` when the path does not exist or already has `policy`.
    pub fn set_sync_policy(&mut self, path: &[PathStep], policy: SyncPolicy) -> bool {
        let Some(target) = self.cell_at_path_mut(path) else {
            return false;
        };
        if target.sync == policy {
            return false;
        }
        target.sync = policy;
        true
    }

    /// Apply an already-routed operation. This bypasses sync classification and
    /// is used after the owning table has classified a mutation as local.
    pub fn apply_routed(
        &mut self,
        op: &Op,
        op_hlc: Hlc,
        path: &[PathStep],
    ) -> Result<bool, TypeError> {
        self.apply_walk(op, op_hlc, path)
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
    /// `parent_scope` is the effective routing scope inherited from the
    /// owning table.
    /// The nearest explicit Cell flag on the target path overrides it.
    ///
    /// This direct-apply path filters remote writes to local targets and
    /// conservatively rejects an ancestor replacement that would erase local
    /// descendants in the single physical Cell tree.
    pub fn apply_event(
        &mut self,
        event: &crate::Event,
        parent_scope: SyncScope,
    ) -> Result<bool, TypeError> {
        self.apply_event_from(event, parent_scope, crate::device_id())
    }

    /// Apply an event using the actual local replica identity. The two-argument
    /// method above remains a convenience fallback for low-level callers, but
    /// database-backed tables should always use this explicit form.
    pub fn apply_event_from(
        &mut self,
        event: &crate::Event,
        parent_scope: SyncScope,
        local_device_id: crate::DeviceId,
    ) -> Result<bool, TypeError> {
        if event.hlc.device_id() != local_device_id {
            if !self
                .effective_scope_at(parent_scope, &event.path)
                .is_shared()
            {
                return Ok(false);
            }
            if let Some(target) = self.cell_at_path(&event.path) {
                let replaces_subtree = match &event.op {
                    Op::Delete | Op::Replace { .. } | Op::Merge { .. } => true,
                    Op::Type(op) => target.type_tag() != Some(op.type_tag()),
                };
                // With one physical tree, accepting an ancestor replacement
                // would erase local descendants. Keep the replicated event in
                // history and defer this branch to state reconciliation.
                if replaces_subtree && target.contains_local_boundary(SyncScope::Shared) {
                    return Ok(false);
                }
            }
        }

        self.apply_walk(&event.op, event.hlc, &event.path)
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
    fn child(&self, segment: &crate::Segment) -> Option<&Cell> {
        self.value.as_ref().and_then(|value| value.child(segment))
    }

    fn child_mut(&mut self, segment: &crate::Segment) -> Option<&mut Cell> {
        self.value
            .as_mut()
            .and_then(|value| value.child_mut(segment))
    }

    fn any_child(&self, predicate: &mut dyn FnMut(&Cell) -> bool) -> bool {
        self.value
            .as_ref()
            .is_some_and(|value| value.any_child(predicate))
    }

    fn cell_at_path(&self, path: &[PathStep]) -> Option<&Cell> {
        if path.is_empty() {
            return Some(self);
        }
        self.value.as_ref()?.cell_at_path(path)
    }

    fn cell_at_path_mut(&mut self, path: &[PathStep]) -> Option<&mut Cell> {
        if path.is_empty() {
            return Some(self);
        }
        self.value.as_mut()?.cell_at_path_mut(path)
    }

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

    fn effective_scope_at(&self, parent_scope: SyncScope, path: &[PathStep]) -> SyncScope {
        let effective_scope = self.sync.resolve(parent_scope);
        if path.is_empty() || self.type_tag() != Some(path[0].container_tag) {
            return effective_scope;
        }
        self.value
            .as_ref()
            .map(|value| value.effective_scope_at(effective_scope, path))
            .unwrap_or(effective_scope)
    }

    fn contains_local_boundary(&self, parent_scope: SyncScope) -> bool {
        let effective_scope = self.sync.resolve(parent_scope);
        if !effective_scope.is_shared() {
            return true;
        }
        self.value
            .as_ref()
            .is_some_and(|value| value.contains_local_boundary(effective_scope))
    }

    fn merge_shared(
        &mut self,
        remote: &Self,
        _clocks: MergeClocks,
        parent_scope: SyncScope,
    ) -> Result<bool, TypeError> {
        if !self.sync.resolve(parent_scope).is_shared() {
            return Ok(false);
        }

        if let (Some(local), Some(remote_value)) = (&mut self.value, &remote.value) {
            if local.type_tag() == remote_value.type_tag() {
                let mut changed = local.merge_shared(
                    remote_value,
                    MergeClocks::new(self.hlc, remote.hlc),
                    parent_scope,
                )?;
                if remote.hlc.beats(self.hlc) {
                    self.hlc = remote.hlc;
                    changed = true;
                }
                return Ok(changed);
            }
        }

        if self.contains_local_boundary(parent_scope) {
            return Ok(false);
        }
        Type::merge(self, remote, MergeClocks::ZERO)
    }

    fn shared_clone(&self, parent_scope: SyncScope) -> Option<Self> {
        let effective_scope = self.sync.resolve(parent_scope);
        if !effective_scope.is_shared() {
            return None;
        }
        let value = match self.value.as_ref() {
            Some(value) => Some(value.shared_clone(effective_scope)?),
            None => None,
        };
        Some(Self {
            value,
            hlc: self.hlc,
            sync: SyncPolicy::Inherit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::values::record::Record;
    use crate::{Path, PathStep, PrimaryKey, Segment};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, crate::DeviceId::from_bytes([1u8; 16])).unwrap()
    }

    fn cell(value: Option<Value>, hlc: Hlc, sync: SyncPolicy) -> Cell {
        Cell { value, hlc, sync }
    }

    fn local_hlc(ms: u64) -> Hlc {
        crate::init_device_id();
        Hlc::with_device_id(ms, 0, crate::device_id()).unwrap()
    }

    fn remote_hlc(ms: u64) -> Hlc {
        crate::init_device_id();
        let mut remote = crate::device_id();
        remote.0[0] ^= u8::MAX;
        Hlc::with_device_id(ms, 0, remote).unwrap()
    }

    fn event(path: Path, op: Op, hlc: Hlc) -> crate::Event {
        crate::Event {
            table_id: "test".into(),
            primary_key: PrimaryKey::String("pk".into()),
            path,
            op,
            hlc,
        }
    }

    #[test]
    fn cell_dummy() {
        let cell = Cell::dummy(Some(Value::String(String::new())));
        assert!(cell.is_dummy());
        assert!(!cell.is_tombstone());
        assert_eq!(cell.sync, SyncPolicy::Inherit);
    }

    #[test]
    fn cell_dummy_tombstone() {
        let cell = Cell::dummy(None);
        assert!(cell.is_dummy());
        assert!(cell.is_tombstone());
        assert_eq!(cell.sync, SyncPolicy::Inherit);
    }

    #[test]
    fn bincode_roundtrip() {
        let cell = cell(
            Some(Value::String("hi".into())),
            hlc(100),
            SyncPolicy::Inherit,
        );
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
                SyncScope::Shared
            )
            .unwrap());
        assert_eq!(cell.hlc, hlc(100));
    }

    #[test]
    fn apply_lww_older_no_change() {
        let mut cell = cell(Some(Value::Int(1)), hlc(200), SyncPolicy::Inherit);
        let changed = cell
            .apply_event(
                &event(
                    Path::new(),
                    Op::Replace {
                        value: Value::Int(2),
                    },
                    hlc(100),
                ),
                SyncScope::Shared,
            )
            .unwrap();
        assert!(!changed);
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn delete_tombstones_cell() {
        let mut cell = cell(Some(Value::Int(1)), hlc(100), SyncPolicy::Inherit);
        assert!(cell
            .apply_event(&event(Path::new(), Op::Delete, hlc(200)), SyncScope::Shared,)
            .unwrap());
        assert!(cell.is_tombstone());
        assert_eq!(cell.hlc, hlc(200));
    }

    #[test]
    fn older_write_does_not_resurrect_tombstone() {
        let mut cell = cell(None, hlc(200), SyncPolicy::Inherit);
        let changed = cell
            .apply_event(
                &event(
                    Path::new(),
                    Op::Replace {
                        value: Value::Int(2),
                    },
                    hlc(100),
                ),
                SyncScope::Shared,
            )
            .unwrap();
        assert!(!changed);
        assert!(cell.is_tombstone());
    }

    #[test]
    fn apply_set_field() {
        let mut root = cell(
            Some(Value::Record(Record::default())),
            hlc(50),
            SyncPolicy::Inherit,
        );
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
                SyncScope::Shared
            )
            .unwrap());
    }

    #[test]
    fn remote_event_is_rejected_by_nearest_local_only_ancestor() {
        let mut nested = Record::default();
        nested.insert(
            "field".into(),
            cell(Some(Value::Int(1)), local_hlc(100), SyncPolicy::Inherit),
        );
        let mut root_record = Record::default();
        root_record.insert(
            "nested".into(),
            cell(
                Some(Value::Record(nested)),
                local_hlc(100),
                SyncPolicy::Local,
            ),
        );
        let mut root = cell(
            Some(Value::Record(root_record)),
            local_hlc(100),
            SyncPolicy::Inherit,
        );
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
                SyncScope::Shared,
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
    fn effective_scope_stays_local_below_a_local_ancestor() {
        let mut nested = Record::default();
        nested.insert(
            "child".into(),
            cell(Some(Value::Int(1)), hlc(100), SyncPolicy::Inherit),
        );
        let mut root_record = Record::default();
        root_record.insert(
            "local".into(),
            cell(Some(Value::Record(nested)), hlc(100), SyncPolicy::Local),
        );
        let root = cell(
            Some(Value::Record(root_record)),
            hlc(100),
            SyncPolicy::Inherit,
        );
        let path = vec![
            PathStep::new(TypeTag::Record, Segment::Record("local".into())),
            PathStep::new(TypeTag::Record, Segment::Record("child".into())),
        ];

        assert_eq!(
            root.effective_scope_at(SyncScope::Shared, &path),
            SyncScope::Local
        );
    }

    #[test]
    fn rejected_remote_event_does_not_create_missing_children() {
        let mut root = cell(
            Some(Value::Record(Record::default())),
            local_hlc(100),
            SyncPolicy::Local,
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
                SyncScope::Shared,
            )
            .unwrap());

        let Some(Value::Record(record)) = &root.value else {
            panic!("expected record");
        };
        assert!(!record.contains("missing"));
    }

    #[test]
    fn sync_policy_is_local_and_hlc_neutral() {
        let mut root = cell(Some(Value::Int(1)), local_hlc(100), SyncPolicy::Local);
        let original_hlc = root.hlc;

        assert!(root.set_sync_policy(&[], SyncPolicy::Inherit));
        assert_eq!(root.sync, SyncPolicy::Inherit);
        assert_eq!(root.hlc, original_hlc);
    }

    #[test]
    fn sync_policy_does_not_affect_merge_order() {
        let mut local = cell(Some(Value::Int(1)), hlc(100), SyncPolicy::Inherit);
        assert!(local.set_sync_policy(&[], SyncPolicy::Local));
        let remote = cell(Some(Value::Int(2)), hlc(200), SyncPolicy::Inherit);

        let changed = Type::merge(&mut local, &remote, MergeClocks::ZERO).unwrap();
        assert!(changed);
        assert_eq!(local.value, Some(Value::Int(2)));
        assert_eq!(local.hlc, hlc(200));
        assert_eq!(local.sync, SyncPolicy::Local);
    }

    #[test]
    fn shared_merge_updates_shared_siblings_and_preserves_local_children() {
        let mut local = cell(
            Some(Value::Record(Record::from_fields([
                (
                    "shared".into(),
                    cell(
                        Some(Value::String("old".into())),
                        hlc(10),
                        SyncPolicy::Inherit,
                    ),
                ),
                (
                    "private".into(),
                    cell(
                        Some(Value::String("mine".into())),
                        hlc(30),
                        SyncPolicy::Local,
                    ),
                ),
            ]))),
            hlc(10),
            SyncPolicy::Inherit,
        );
        let remote = cell(
            Some(Value::Record(Record::from_fields([
                (
                    "shared".into(),
                    cell(
                        Some(Value::String("new".into())),
                        hlc(20),
                        SyncPolicy::Inherit,
                    ),
                ),
                (
                    "private".into(),
                    cell(
                        Some(Value::String("theirs".into())),
                        hlc(40),
                        SyncPolicy::Inherit,
                    ),
                ),
            ]))),
            hlc(20),
            SyncPolicy::Inherit,
        );

        assert!(ContainerType::merge_shared(
            &mut local,
            &remote,
            MergeClocks::ZERO,
            SyncScope::Shared,
        )
        .unwrap());
        let Some(Value::Record(record)) = local.value else {
            panic!("expected record");
        };
        assert_eq!(
            record.get("shared").unwrap().value,
            Some(Value::String("new".into()))
        );
        assert_eq!(
            record.get("private").unwrap().value,
            Some(Value::String("mine".into()))
        );
    }

    #[test]
    fn shared_clone_omits_local_policy() {
        let local = cell(
            Some(Value::String("private".into())),
            hlc(10),
            SyncPolicy::Local,
        );
        assert!(local.shared_clone(SyncScope::Shared).is_none());

        let shared = cell(
            Some(Value::String("shared".into())),
            hlc(10),
            SyncPolicy::Inherit,
        );
        let projected = shared.shared_clone(SyncScope::Shared).unwrap();
        assert_eq!(projected.sync, SyncPolicy::Inherit);
        let bytes = encode_to_vec(&projected, config::standard()).unwrap();
        let (decoded, consumed): (Cell, usize) =
            decode_from_slice(&bytes, config::standard()).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, projected);
    }

    #[test]
    fn shared_clone_omits_nested_local_cells() {
        let root = cell(
            Some(Value::Record(Record::from_fields([
                (
                    "shared".into(),
                    cell(
                        Some(Value::String("visible".into())),
                        hlc(10),
                        SyncPolicy::Inherit,
                    ),
                ),
                (
                    "local".into(),
                    cell(
                        Some(Value::String("private".into())),
                        hlc(10),
                        SyncPolicy::Local,
                    ),
                ),
            ]))),
            hlc(10),
            SyncPolicy::Inherit,
        );

        let projected = root.shared_clone(SyncScope::Shared).unwrap();
        let Value::Record(record) = projected.value.unwrap() else {
            panic!("the projected root must remain a Record");
        };
        assert!(record.contains("shared"));
        assert!(!record.contains("local"));
    }

    #[test]
    fn nested_update_does_not_bump_existing_parent_hlc() {
        let mut root = cell(
            Some(Value::Record(Record::default())),
            hlc(50),
            SyncPolicy::Inherit,
        );
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
                SyncScope::Shared
            )
            .unwrap());
        assert_eq!(root.hlc, hlc(50));
    }

    #[test]
    fn recreated_parent_gets_event_hlc() {
        let mut root = cell(None, hlc(50), SyncPolicy::Inherit);
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
                SyncScope::Shared
            )
            .unwrap());
        assert_eq!(root.hlc, hlc(100));
        assert_eq!(root.type_tag(), Some(TypeTag::Record));
    }

    #[test]
    fn merge_same_record_recurses() {
        let mut local = cell(
            Some(Value::Record(Record::default())),
            hlc(50),
            SyncPolicy::Inherit,
        );
        let mut remote = cell(
            Some(Value::Record(Record::default())),
            hlc(50),
            SyncPolicy::Inherit,
        );
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
                SyncScope::Shared,
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
                SyncScope::Shared,
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
        let mut local = cell(Some(Value::Int(1)), hlc(100), SyncPolicy::Inherit);
        let remote = cell(Some(Value::Int(2)), hlc(200), SyncPolicy::Inherit);
        assert!(Type::merge(&mut local, &remote, MergeClocks::ZERO).unwrap());
        assert_eq!(local.value, Some(Value::Int(2)));
        assert_eq!(local.hlc, hlc(200));
    }

    #[test]
    fn merge_keeps_sync_local() {
        let mut local = cell(Some(Value::Int(1)), hlc(100), SyncPolicy::Local);
        let remote = cell(
            Some(Value::Record(Record::default())),
            hlc(200),
            SyncPolicy::Inherit,
        );
        assert!(Type::merge(&mut local, &remote, MergeClocks::ZERO).unwrap());
        assert_eq!(local.type_tag(), Some(TypeTag::Record));
        assert_eq!(local.sync, SyncPolicy::Local);
    }
}
