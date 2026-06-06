//! List - an RGA-style ordered container with stable element identities.
//!
//! Elements are addressed by the HLC of their insert operation. Placement is
//! immutable: an element records the element it was inserted after, or `None`
//! for the list head. Concurrent siblings are ordered by descending ID.

use std::collections::{BTreeMap, BTreeSet};

use bincode::{Decode, Encode};

use crate::{
    core::traits::{ContainerType, Type},
    Cell, Hlc, TypeTag, Value,
};

pub type ListId = Hlc;
pub type ListSegment = ListId;
pub type List = BTreeMap<ListId, ListEntry>;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct ListEntry {
    /// The element this entry was inserted after. `None` means list head.
    pub after: Option<ListId>,
    /// False for placeholders created by an out-of-order path operation or
    /// delete. A later insert supplies the immutable placement.
    pub after_known: bool,
    pub cell: Cell,
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

    fn apply(&mut self, op: &ListOp, _local_hlc: Hlc, op_hlc: Hlc) -> Result<bool, ListError> {
        match op {
            ListOp::Insert { after, value } => {
                if op_hlc == Hlc::ZERO {
                    return Err(ListError::ZeroId);
                }

                let incoming = Cell::new(Some(value.clone()), op_hlc, None);
                match self.get_mut(&op_hlc) {
                    Some(entry) => {
                        let positioned = resolve_position(entry, op_hlc, *after)?;
                        Ok(entry.cell.merge(&incoming) || positioned)
                    }
                    None => {
                        self.insert(op_hlc, ListEntry::inserted(*after, incoming));
                        Ok(true)
                    }
                }
            }
            ListOp::Delete { id } => {
                if *id == Hlc::ZERO {
                    return Err(ListError::ZeroId);
                }

                let tombstone = Cell::new(None, op_hlc, None);
                match self.get_mut(id) {
                    Some(entry) => Ok(entry.cell.merge(&tombstone)),
                    None => {
                        self.insert(*id, ListEntry::placeholder(tombstone));
                        Ok(true)
                    }
                }
            }
        }
    }

    fn merge(
        &mut self,
        remote: &List,
        _local_hlc: Hlc,
        _remote_hlc: Hlc,
    ) -> Result<bool, ListError> {
        let mut changed = false;

        for (id, remote_entry) in remote {
            match self.get_mut(id) {
                Some(local_entry) => {
                    if remote_entry.after_known
                        && resolve_position(local_entry, *id, remote_entry.after)?
                    {
                        changed = true;
                    }
                    if local_entry.cell.merge(&remote_entry.cell) {
                        changed = true;
                    }
                }
                None => {
                    self.insert(*id, remote_entry.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.values().fold(Hlc::ZERO, |max, entry| {
            std::cmp::max(max, entry.cell.max_hlc())
        })
    }
}

impl ContainerType for List {
    type Segment = ListSegment;

    fn child_or_default<'a>(
        &'a mut self,
        segment: &ListSegment,
        child_tag: Option<TypeTag>,
    ) -> Result<&'a mut Cell, ListError> {
        if *segment == Hlc::ZERO {
            return Err(ListError::ZeroId);
        }

        if !self.contains_key(segment) {
            let cell = child_tag
                .map(|tag| Cell::dummy(tag.empty_value()))
                .unwrap_or_else(|| Cell::new(None, Hlc::ZERO, None));
            self.insert(*segment, ListEntry::placeholder(cell));
        }
        Ok(&mut self.get_mut(segment).expect("entry was inserted").cell)
    }
}

/// Return visible element IDs in their deterministic list order.
pub fn list_visible_ids(list: &List) -> Vec<ListId> {
    let mut children: BTreeMap<Option<ListId>, Vec<ListId>> = BTreeMap::new();
    for (id, entry) in list {
        if entry.after_known {
            children.entry(entry.after).or_default().push(*id);
        }
    }
    for siblings in children.values_mut() {
        siblings.sort_unstable_by(|a, b| b.cmp(a));
    }

    let mut ids = Vec::new();
    let mut visited = BTreeSet::new();
    walk_visible(None, list, &children, &mut visited, &mut ids);
    ids
}

/// Resolve a visible zero-based index to its stable element ID.
pub fn list_id_at(list: &List, index: usize) -> Option<ListId> {
    list_visible_ids(list).get(index).copied()
}

/// Resolve a visible zero-based index to its cell.
pub fn list_cell_at(list: &List, index: usize) -> Option<&Cell> {
    let id = list_id_at(list, index)?;
    list.get(&id).map(|entry| &entry.cell)
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
        let Some(entry) = list.get(id) else {
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
    use crate::{Delta, Op, Path, PrimaryKey, Segment, TypeOp};
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn apply(list: &mut List, op: ListOp, at: Hlc) -> bool {
        Type::apply(list, &op, Hlc::ZERO, at).unwrap()
    }

    fn merge_order(lists: &[List; 3], order: [usize; 3]) -> List {
        let mut merged = lists[order[0]].clone();
        Type::merge(&mut merged, &lists[order[1]], Hlc::ZERO, Hlc::ZERO).unwrap();
        Type::merge(&mut merged, &lists[order[2]], Hlc::ZERO, Hlc::ZERO).unwrap();
        merged
    }

    fn delta(path: Path, op: Op, at: Hlc) -> Delta {
        Delta {
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
        let mut list = List::new();
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

        assert_eq!(list_visible_ids(&list), vec![a, b]);
    }

    #[test]
    fn orphan_is_hidden_until_its_anchor_arrives() {
        let mut list = List::new();
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
        assert!(list_visible_ids(&list).is_empty());

        apply(
            &mut list,
            ListOp::Insert {
                after: None,
                value: Value::String("anchor".into()),
            },
            anchor,
        );
        assert_eq!(list_visible_ids(&list), vec![anchor, child]);
    }

    #[test]
    fn deleting_anchor_keeps_its_visible_descendants() {
        let mut list = List::new();
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

        assert_eq!(list_visible_ids(&list), vec![child]);
        assert_eq!(list_id_at(&list, 0), Some(child));
        assert_eq!(list_id_at(&list, 1), None);
    }

    #[test]
    fn later_head_insert_appears_first() {
        let mut list = List::new();
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

        assert_eq!(list_visible_ids(&list), vec![new, old]);
    }

    #[test]
    fn concurrent_inserts_merge_deterministically() {
        let mut left = List::new();
        let mut right = List::new();
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
        Type::merge(&mut left_first, &right, Hlc::ZERO, Hlc::ZERO).unwrap();
        Type::merge(&mut right_first, &left, Hlc::ZERO, Hlc::ZERO).unwrap();

        assert_eq!(left_first, right_first);
        assert_eq!(list_visible_ids(&left_first), vec![b, a]);
    }

    #[test]
    fn delete_before_insert_resolves_position_and_stays_deleted() {
        let mut list = List::new();
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

        let entry = list.get(&id).unwrap();
        assert!(entry.after_known);
        assert!(entry.cell.is_tombstone());
        assert!(list_visible_ids(&list).is_empty());
    }

    #[test]
    fn duplicate_operations_and_merges_are_idempotent() {
        let mut list = List::new();
        let id = hlc(100, 1);
        let insert = ListOp::Insert {
            after: None,
            value: Value::Int(1),
        };
        assert!(apply(&mut list, insert.clone(), id));
        assert!(!apply(&mut list, insert, id));

        let snapshot = list.clone();
        assert!(!Type::merge(&mut list, &snapshot, Hlc::ZERO, Hlc::ZERO).unwrap());
        assert_eq!(list, snapshot);
    }

    #[test]
    fn zero_id_is_rejected_for_insert_delete_and_path_segments() {
        let mut list = List::new();
        assert!(matches!(
            Type::apply(
                &mut list,
                &ListOp::Insert {
                    after: None,
                    value: Value::Int(1),
                },
                Hlc::ZERO,
                Hlc::ZERO,
            ),
            Err(ListError::ZeroId)
        ));
        assert!(matches!(
            Type::apply(
                &mut list,
                &ListOp::Delete { id: Hlc::ZERO },
                Hlc::ZERO,
                hlc(100, 1),
            ),
            Err(ListError::ZeroId)
        ));
        assert!(matches!(
            list.child_or_default(&Hlc::ZERO, Some(TypeTag::Int)),
            Err(ListError::ZeroId)
        ));
        assert!(list.is_empty());
    }

    #[test]
    fn element_position_is_immutable() {
        let mut list = List::new();
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
                Hlc::ZERO,
                id,
            ),
            Err(ListError::PositionConflict { .. })
        ));
        assert_eq!(list.get(&id).unwrap().after, Some(first_anchor));
        assert_eq!(list.get(&id).unwrap().cell.value, Some(Value::Int(1)));
    }

    #[test]
    fn merge_is_associative_for_independent_inserts() {
        let mut a = List::new();
        let mut b = List::new();
        let mut c = List::new();
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
        Type::merge(&mut left, &b, Hlc::ZERO, Hlc::ZERO).unwrap();
        Type::merge(&mut left, &c, Hlc::ZERO, Hlc::ZERO).unwrap();

        let mut right_branch = b;
        Type::merge(&mut right_branch, &c, Hlc::ZERO, Hlc::ZERO).unwrap();
        let mut right = a;
        Type::merge(&mut right, &right_branch, Hlc::ZERO, Hlc::ZERO).unwrap();

        assert_eq!(left, right);
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let anchor = hlc(100, 1);
        let left = hlc(200, 1);
        let right = hlc(200, 2);
        let mut lists = [List::new(), List::new(), List::new()];
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
        assert_eq!(list_visible_ids(&expected), vec![right, left]);
    }

    #[test]
    fn list_path_targets_stable_element_id() {
        let id = hlc(100, 1);
        let mut root = Cell::new(Some(Value::List(List::new())), hlc(50, 1), None);
        assert!(root.apply(&delta(
            Path::new(),
            Op::Type(TypeOp::List(ListOp::Insert {
                after: None,
                value: Value::Int(1),
            })),
            id,
        )));
        assert!(root.apply(&delta(
            Path::new().step(TypeTag::List, Segment::List(id)),
            Op::Replace {
                value: Value::Int(2),
            },
            hlc(200, 1),
        )));

        let Some(Value::List(list)) = &root.value else {
            panic!("expected list");
        };
        assert_eq!(list.get(&id).unwrap().cell.value, Some(Value::Int(2)));
        assert_eq!(list_cell_at(list, 0).unwrap().value, Some(Value::Int(2)));
    }

    #[test]
    fn matching_elements_recursively_merge_nested_cells() {
        let id = hlc(100, 1);
        let mut base = Cell::new(Some(Value::List(List::new())), hlc(50, 1), None);
        assert!(base.apply(&delta(
            Path::new(),
            Op::Type(TypeOp::List(ListOp::Insert {
                after: None,
                value: Value::Record(Default::default()),
            })),
            id,
        )));

        let mut left = base.clone();
        let mut right = base;
        let element = Path::new().step(TypeTag::List, Segment::List(id));
        assert!(left.apply(&delta(
            element
                .clone()
                .step(TypeTag::Record, Segment::Record("left".into())),
            Op::Replace {
                value: Value::Bool(true),
            },
            hlc(200, 1),
        )));
        assert!(right.apply(&delta(
            element.step(TypeTag::Record, Segment::Record("right".into())),
            Op::Replace {
                value: Value::Bool(true),
            },
            hlc(200, 2),
        )));

        assert!(left.merge(&right));
        let Some(Value::List(list)) = &left.value else {
            panic!("expected list");
        };
        let Some(Value::Record(record)) = &list.get(&id).unwrap().cell.value else {
            panic!("expected record element");
        };
        assert!(record.contains_key("left"));
        assert!(record.contains_key("right"));
    }

    #[test]
    fn list_bincode_roundtrip() {
        let mut list = List::new();
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
