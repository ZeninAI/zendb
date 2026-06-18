//! List - an RGA-style ordered container with stable element identities.
//!
//! Elements are addressed by the HLC of their insert operation. Placement is
//! immutable: an element records the element it was inserted after, or `None`
//! for the list head. Concurrent siblings are ordered by descending ID.

use std::collections::{BTreeMap, BTreeSet};

use bincode::{Decode, Encode};

use crate::{Cell, ContainerType, Hlc, MergeClocks, Op, PathStep, Segment, Type, TypeError, Value};

pub type ListId = Hlc;
pub type ListSegment = ListId;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct ListEntry {
    /// The element this entry was inserted after. `None` means list head.
    after: Option<ListId>,
    /// False for placeholders created by an out-of-order path operation or
    /// delete. A later insert supplies the immutable placement.
    after_known: bool,
    cell: Cell,
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct List {
    entries: BTreeMap<ListId, ListEntry>,
}

impl List {
    pub fn visible_ids(&self) -> Vec<ListId> {
        let mut children: BTreeMap<Option<ListId>, Vec<ListId>> = BTreeMap::new();
        for (id, entry) in &self.entries {
            if entry.after_known {
                children.entry(entry.after).or_default().push(*id);
            }
        }
        for siblings in children.values_mut() {
            siblings.sort_unstable_by(|a, b| b.cmp(a));
        }

        let mut ids = Vec::new();
        let mut visited = BTreeSet::new();
        walk_visible(None, self, &children, &mut visited, &mut ids);
        ids
    }

    pub fn id_at(&self, index: usize) -> Option<ListId> {
        self.visible_ids().get(index).copied()
    }

    pub fn cell_at(&self, index: usize) -> Option<&Cell> {
        let id = self.id_at(index)?;
        self.entries.get(&id).map(|entry| &entry.cell)
    }

    #[cfg(test)]
    fn insert(&mut self, id: ListId, entry: ListEntry) -> Option<ListEntry> {
        self.entries.insert(id, entry)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl ListEntry {
    fn inserted(after: Option<ListId>, cell: Cell) -> ListEntry {
        ListEntry {
            after,
            after_known: true,
            cell,
        }
    }

    fn placeholder(cell: Cell) -> ListEntry {
        ListEntry {
            after: None,
            after_known: false,
            cell,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ListOp {
    Insert {
        /// Insert after this stable element ID, or at the list head.
        after: Option<ListId>,
        value: Value,
    },
    Delete {
        id: ListId,
    },
}

#[derive(Debug)]
pub enum ListError {
    ZeroId,
    Child(Box<TypeError>),
    PositionConflict {
        id: ListId,
        local_after: Option<ListId>,
        remote_after: Option<ListId>,
    },
}

impl std::fmt::Display for ListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListError::ZeroId => f.write_str("list element ID cannot be Hlc::ZERO"),
            ListError::Child(error) => write!(f, "list child operation failed: {error}"),
            ListError::PositionConflict {
                id,
                local_after,
                remote_after,
            } => write!(
                f,
                "list element {id} has conflicting positions: {local_after:?} vs {remote_after:?}"
            ),
        }
    }
}

impl std::error::Error for ListError {}

impl Type for List {
    type Op = ListOp;
    type Error = ListError;

    fn apply(&mut self, op: &ListOp, op_hlc: Hlc) -> Result<bool, ListError> {
        match op {
            ListOp::Insert { after, value } => {
                if op_hlc == Hlc::ZERO {
                    return Err(ListError::ZeroId);
                }

                let incoming = Cell {
                    value: Some(value.clone()),
                    hlc: op_hlc,
                    sync: None,
                };
                match self.entries.get_mut(&op_hlc) {
                    Some(entry) => {
                        let positioned = resolve_position(entry, op_hlc, *after)?;
                        Ok(Type::merge(&mut entry.cell, &incoming, MergeClocks::ZERO)
                            .map_err(|error| ListError::Child(Box::new(error)))?
                            || positioned)
                    }
                    None => {
                        self.entries
                            .insert(op_hlc, ListEntry::inserted(*after, incoming));
                        Ok(true)
                    }
                }
            }
            ListOp::Delete { id } => {
                if *id == Hlc::ZERO {
                    return Err(ListError::ZeroId);
                }

                let tombstone = Cell {
                    value: None,
                    hlc: op_hlc,
                    sync: None,
                };
                match self.entries.get_mut(id) {
                    Some(entry) => Type::merge(&mut entry.cell, &tombstone, MergeClocks::ZERO)
                        .map_err(|error| ListError::Child(Box::new(error))),
                    None => {
                        self.entries.insert(*id, ListEntry::placeholder(tombstone));
                        Ok(true)
                    }
                }
            }
        }
    }

    fn merge(&mut self, remote: &List, _clocks: MergeClocks) -> Result<bool, ListError> {
        let mut changed = false;

        for (id, remote_entry) in &remote.entries {
            match self.entries.get_mut(id) {
                Some(local_entry) => {
                    if remote_entry.after_known
                        && resolve_position(local_entry, *id, remote_entry.after)?
                    {
                        changed = true;
                    }
                    if Type::merge(&mut local_entry.cell, &remote_entry.cell, MergeClocks::ZERO)
                        .map_err(|error| ListError::Child(Box::new(error)))?
                    {
                        changed = true;
                    }
                }
                None => {
                    self.entries.insert(*id, remote_entry.clone());
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
        let Segment::List(id) = step.segment else {
            return inherited;
        };
        self.entries
            .get(&id)
            .map(|entry| entry.cell.is_synced(inherited, remaining))
            .unwrap_or(inherited)
    }

    fn compact(&mut self, watermark: Hlc) -> Result<bool, ListError> {
        let mut changed = false;
        for entry in self.entries.values_mut() {
            changed |= Type::compact(&mut entry.cell, watermark)
                .map_err(|error| ListError::Child(Box::new(error)))?;
        }
        loop {
            let referenced: BTreeSet<ListId> = self
                .entries
                .values()
                .filter_map(|entry| entry.after)
                .collect();
            let before = self.entries.len();
            self.entries.retain(|id, entry| {
                !(entry.cell.is_tombstone()
                    && entry.cell.hlc <= watermark
                    && !referenced.contains(id))
            });
            if self.entries.len() == before {
                break;
            }
            changed = true;
        }
        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.entries.values().fold(Hlc::ZERO, |max, entry| {
            std::cmp::max(max, Type::max_hlc(&entry.cell))
        })
    }
}

impl ContainerType for List {
    fn apply_walk(&mut self, op: &Op, op_hlc: Hlc, path: &[PathStep]) -> Result<bool, ListError> {
        let Some((step, remaining)) = path.split_first() else {
            return Ok(false);
        };
        let Segment::List(id) = &step.segment else {
            return Ok(false);
        };
        let id = *id;
        if id == Hlc::ZERO {
            return Err(ListError::ZeroId);
        }
        let child_tag = remaining
            .first()
            .map(|step| step.container_tag)
            .or_else(|| op.target_type());
        let entry = self.entries.entry(id).or_insert_with(|| {
            let cell = child_tag
                .map(|tag| Cell::dummy(Some(tag.empty_value())))
                .unwrap_or(Cell {
                    value: None,
                    hlc: Hlc::ZERO,
                    sync: None,
                });
            ListEntry::placeholder(cell)
        });
        if child_tag.is_some_and(|tag| !entry.cell.ensure_type(tag, op_hlc)) {
            return Ok(false);
        }
        ContainerType::apply_walk(&mut entry.cell, op, op_hlc, remaining)
            .map_err(|error| ListError::Child(Box::new(error)))
    }
}

fn resolve_position(
    entry: &mut ListEntry,
    id: ListId,
    after: Option<ListId>,
) -> Result<bool, ListError> {
    if !entry.after_known {
        entry.after = after;
        entry.after_known = true;
        return Ok(true);
    }
    if entry.after != after {
        return Err(ListError::PositionConflict {
            id,
            local_after: entry.after,
            remote_after: after,
        });
    }
    Ok(false)
}

fn walk_visible(
    after: Option<ListId>,
    list: &List,
    children: &BTreeMap<Option<ListId>, Vec<ListId>>,
    visited: &mut BTreeSet<ListId>,
    visible: &mut Vec<ListId>,
) {
    let Some(siblings) = children.get(&after) else {
        return;
    };

    for id in siblings {
        if !visited.insert(*id) {
            continue;
        }
        let Some(entry) = list.entries.get(id) else {
            continue;
        };
        if !entry.cell.is_tombstone() {
            visible.push(*id);
        }
        walk_visible(Some(*id), list, children, visited, visible);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Event, Op, Path, PathStep, PrimaryKey, Segment, TypeOp, TypeTag};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn cell(value: Option<Value>, hlc: Hlc, sync: Option<bool>) -> Cell {
        Cell { value, hlc, sync }
    }

    fn apply(list: &mut List, op: ListOp, at: Hlc) -> bool {
        Type::apply(list, &op, at).unwrap()
    }

    fn merge_order(lists: &[List; 3], order: [usize; 3]) -> List {
        let mut merged = lists[order[0]].clone();
        Type::merge(&mut merged, &lists[order[1]], crate::MergeClocks::ZERO).unwrap();
        Type::merge(&mut merged, &lists[order[2]], crate::MergeClocks::ZERO).unwrap();
        merged
    }

    fn event(path: Path, op: Op, at: Hlc) -> Event {
        Event {
            table_id: "test".into(),
            primary_key: PrimaryKey::String("pk".into()),
            path,
            op,
            hlc: at,
            sync: false,
            signature: Vec::new(),
        }
    }

    #[test]
    fn sequential_inserts_follow_anchor() {
        let mut list = List::default();
        let a = hlc(100, 1);
        let b = hlc(200, 1);
        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::String("a".into()),
            },
            a,
        );
        apply(
            &mut list,
            ListOp::Insert {
                after: Some(a),
                value: Value::String("b".into()),
            },
            b,
        );

        assert_eq!(list.visible_ids(), vec![a, b]);
    }

    #[test]
    fn orphan_is_hidden_until_its_anchor_arrives() {
        let mut list = List::default();
        let anchor = hlc(100, 1);
        let child = hlc(200, 1);
        apply(
            &mut list,
            ListOp::Insert {
                after: Some(anchor),
                value: Value::String("child".into()),
            },
            child,
        );
        assert!(list.visible_ids().is_empty());

        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::String("anchor".into()),
            },
            anchor,
        );
        assert_eq!(list.visible_ids(), vec![anchor, child]);
    }

    #[test]
    fn deleting_anchor_keeps_its_visible_descendants() {
        let mut list = List::default();
        let anchor = hlc(100, 1);
        let child = hlc(200, 1);
        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::String("anchor".into()),
            },
            anchor,
        );
        apply(
            &mut list,
            ListOp::Insert {
                after: Some(anchor),
                value: Value::String("child".into()),
            },
            child,
        );
        apply(&mut list, ListOp::Delete { id: anchor }, hlc(300, 1));

        assert_eq!(list.visible_ids(), vec![child]);
        assert_eq!(list.id_at(0), Some(child));
        assert_eq!(list.id_at(1), None);
    }

    #[test]
    fn compact_removes_only_stable_unreferenced_tombstones() {
        let mut list = List::default();
        let anchor = hlc(100, 1);
        let child = hlc(200, 1);
        let orphan = hlc(300, 1);
        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::Int(1),
            },
            anchor,
        );
        apply(
            &mut list,
            ListOp::Insert {
                after: Some(anchor),
                value: Value::Int(2),
            },
            child,
        );
        apply(&mut list, ListOp::Delete { id: anchor }, hlc(400, 1));
        apply(&mut list, ListOp::Delete { id: orphan }, hlc(400, 1));

        assert!(Type::compact(&mut list, hlc(500, 1)).unwrap());
        assert!(list.entries.contains_key(&anchor));
        assert!(!list.entries.contains_key(&orphan));
        assert_eq!(list.visible_ids(), vec![child]);
    }

    #[test]
    fn later_head_insert_appears_first() {
        let mut list = List::default();
        let old = hlc(100, 1);
        let new = hlc(200, 1);
        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::Int(1),
            },
            old,
        );
        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::Int(2),
            },
            new,
        );

        assert_eq!(list.visible_ids(), vec![new, old]);
    }

    #[test]
    fn concurrent_inserts_merge_deterministically() {
        let mut left = List::default();
        let mut right = List::default();
        let a = hlc(100, 1);
        let b = hlc(100, 2);
        apply(
            &mut left,
            ListOp::Insert {
                after: None,
                value: Value::String("a".into()),
            },
            a,
        );
        apply(
            &mut right,
            ListOp::Insert {
                after: None,
                value: Value::String("b".into()),
            },
            b,
        );

        let mut left_first = left.clone();
        let mut right_first = right.clone();
        Type::merge(&mut left_first, &right, crate::MergeClocks::ZERO).unwrap();
        Type::merge(&mut right_first, &left, crate::MergeClocks::ZERO).unwrap();

        assert_eq!(left_first, right_first);
        assert_eq!(left_first.visible_ids(), vec![b, a]);
    }

    #[test]
    fn delete_before_insert_resolves_position_and_stays_deleted() {
        let mut list = List::default();
        let id = hlc(100, 1);
        apply(&mut list, ListOp::Delete { id }, hlc(200, 2));
        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::String("late".into()),
            },
            id,
        );

        let entry = list.entries.get(&id).unwrap();
        assert!(entry.after_known);
        assert!(entry.cell.is_tombstone());
        assert!(list.visible_ids().is_empty());
    }

    #[test]
    fn duplicate_operations_and_merges_are_idempotent() {
        let mut list = List::default();
        let id = hlc(100, 1);
        let insert = ListOp::Insert {
            after: None,
            value: Value::Int(1),
        };
        assert!(apply(&mut list, insert.clone(), id));
        assert!(!apply(&mut list, insert, id));

        let snapshot = list.clone();
        assert!(!Type::merge(&mut list, &snapshot, crate::MergeClocks::ZERO).unwrap());
        assert_eq!(list, snapshot);
    }

    #[test]
    fn zero_id_is_rejected_for_insert_delete_and_path_segments() {
        let mut list = List::default();
        assert!(matches!(
            Type::apply(
                &mut list,
                &ListOp::Insert {
                    after: None,
                    value: Value::Int(1),
                },
                Hlc::ZERO,
            ),
            Err(ListError::ZeroId)
        ));
        assert!(matches!(
            Type::apply(&mut list, &ListOp::Delete { id: Hlc::ZERO }, hlc(100, 1),),
            Err(ListError::ZeroId)
        ));
        assert!(list.is_empty());
    }

    #[test]
    fn element_position_is_immutable() {
        let mut list = List::default();
        let first_anchor = hlc(100, 1);
        let second_anchor = hlc(100, 2);
        let id = hlc(200, 1);
        apply(
            &mut list,
            ListOp::Insert {
                after: Some(first_anchor),
                value: Value::Int(1),
            },
            id,
        );

        assert!(matches!(
            Type::apply(
                &mut list,
                &ListOp::Insert {
                    after: Some(second_anchor),
                    value: Value::Int(2),
                },
                id,
            ),
            Err(ListError::PositionConflict { .. })
        ));
        assert_eq!(list.entries.get(&id).unwrap().after, Some(first_anchor));
        assert_eq!(
            list.entries.get(&id).unwrap().cell.value,
            Some(Value::Int(1))
        );
    }

    #[test]
    fn merge_is_associative_for_independent_inserts() {
        let mut a = List::default();
        let mut b = List::default();
        let mut c = List::default();
        for (list, id, value) in [
            (&mut a, hlc(100, 1), 1),
            (&mut b, hlc(100, 2), 2),
            (&mut c, hlc(100, 3), 3),
        ] {
            apply(
                list,
                ListOp::Insert {
                    after: None,
                    value: Value::Int(value),
                },
                id,
            );
        }

        let mut left = a.clone();
        Type::merge(&mut left, &b, crate::MergeClocks::ZERO).unwrap();
        Type::merge(&mut left, &c, crate::MergeClocks::ZERO).unwrap();

        let mut right_branch = b;
        Type::merge(&mut right_branch, &c, crate::MergeClocks::ZERO).unwrap();
        let mut right = a;
        Type::merge(&mut right, &right_branch, crate::MergeClocks::ZERO).unwrap();

        assert_eq!(left, right);
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let anchor = hlc(100, 1);
        let left = hlc(200, 1);
        let right = hlc(200, 2);
        let mut lists = [List::default(), List::default(), List::default()];
        for list in &mut lists {
            apply(
                list,
                ListOp::Insert {
                    after: None,
                    value: Value::String("anchor".into()),
                },
                anchor,
            );
        }
        apply(
            &mut lists[0],
            ListOp::Insert {
                after: Some(anchor),
                value: Value::String("left".into()),
            },
            left,
        );
        apply(
            &mut lists[1],
            ListOp::Insert {
                after: Some(anchor),
                value: Value::String("right".into()),
            },
            right,
        );
        apply(&mut lists[2], ListOp::Delete { id: anchor }, hlc(300, 3));

        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let expected = merge_order(&lists, orders[0]);
        for order in orders.into_iter().skip(1) {
            assert_eq!(merge_order(&lists, order), expected);
        }
        assert_eq!(expected.visible_ids(), vec![right, left]);
    }

    #[test]
    fn list_path_targets_stable_element_id() {
        let id = hlc(100, 1);
        let mut root = cell(Some(Value::List(List::default())), hlc(50, 1), None);
        assert!(root
            .apply_event(
                &event(
                    Path::new(),
                    Op::Type(TypeOp::List(ListOp::Insert {
                        after: None,
                        value: Value::Int(1),
                    })),
                    id,
                ),
                true
            )
            .unwrap());
        assert!(root
            .apply_event(
                &event(
                    vec![PathStep::new(TypeTag::List, Segment::List(id))],
                    Op::Replace {
                        value: Value::Int(2),
                    },
                    hlc(200, 1),
                ),
                true
            )
            .unwrap());

        let Some(Value::List(list)) = &root.value else {
            panic!("expected list");
        };
        assert_eq!(
            list.entries.get(&id).unwrap().cell.value,
            Some(Value::Int(2))
        );
        assert_eq!(list.cell_at(0).unwrap().value, Some(Value::Int(2)));
    }

    #[test]
    fn list_apply_heals_stale_element_type_before_descending() {
        let id = hlc(100, 1);
        let mut list = List::default();
        list.insert(
            id,
            ListEntry::inserted(
                None,
                cell(Some(Value::String("stale".into())), hlc(100, 1), None),
            ),
        );
        let path = vec![
            PathStep::new(TypeTag::List, Segment::List(id)),
            PathStep::new(TypeTag::Record, Segment::Record("leaf".into())),
        ];

        assert!(ContainerType::apply_walk(
            &mut list,
            &Op::Replace {
                value: Value::Int(1),
            },
            hlc(200, 1),
            &path,
        )
        .unwrap());

        let Some(Value::Record(record)) = &list.entries.get(&id).unwrap().cell.value else {
            panic!("expected healed record");
        };
        assert_eq!(record.get("leaf").unwrap().value, Some(Value::Int(1)));
    }

    #[test]
    fn matching_elements_recursively_merge_nested_cells() {
        let id = hlc(100, 1);
        let mut base = cell(Some(Value::List(List::default())), hlc(50, 1), None);
        assert!(base
            .apply_event(
                &event(
                    Path::new(),
                    Op::Type(TypeOp::List(ListOp::Insert {
                        after: None,
                        value: Value::Record(Default::default()),
                    })),
                    id,
                ),
                true
            )
            .unwrap());

        let mut left = base.clone();
        let mut right = base;
        let element = vec![PathStep::new(TypeTag::List, Segment::List(id))];
        assert!(left
            .apply_event(
                &event(
                    [
                        element.clone(),
                        vec![PathStep::new(
                            TypeTag::Record,
                            Segment::Record("left".into()),
                        )],
                    ]
                    .concat(),
                    Op::Replace {
                        value: Value::Bool(true),
                    },
                    hlc(200, 1),
                ),
                true
            )
            .unwrap());
        assert!(right
            .apply_event(
                &event(
                    [
                        element,
                        vec![PathStep::new(
                            TypeTag::Record,
                            Segment::Record("right".into()),
                        )],
                    ]
                    .concat(),
                    Op::Replace {
                        value: Value::Bool(true),
                    },
                    hlc(200, 2),
                ),
                true
            )
            .unwrap());

        assert!(Type::merge(&mut left, &right, MergeClocks::ZERO).unwrap());
        let Some(Value::List(list)) = &left.value else {
            panic!("expected list");
        };
        let Some(Value::Record(record)) = &list.entries.get(&id).unwrap().cell.value else {
            panic!("expected record element");
        };
        assert!(record.contains("left"));
        assert!(record.contains("right"));
    }

    #[test]
    fn list_bincode_roundtrip() {
        let mut list = List::default();
        let id = hlc(100, 1);
        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::String("value".into()),
            },
            id,
        );

        let buf = encode_to_vec(&list, config::standard()).unwrap();
        let (decoded, consumed): (List, usize) =
            decode_from_slice(&buf, config::standard()).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded, list);
    }
}
