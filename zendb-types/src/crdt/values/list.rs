//! List - an RGA-style ordered container with stable element identities.
//!
//! Elements are addressed by the HLC of their insert operation. Placement is
//! immutable: an element records the element it was inserted after, or `None`
//! for the list head. Concurrent siblings are ordered by descending ID.

use std::collections::{BTreeMap, BTreeSet};

use bincode::{Decode, Encode};

use crate::{Cell, ContainerType, EventStamp, MergeStamps, Op, Segment, Type, TypeError, Value};

pub type ListId = EventStamp;
pub type ListSegment = ListId;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct ListEntry {
    position: ListPosition,
    cell: Cell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
enum ListPosition {
    Unknown,
    Head,
    After(ListId),
}

impl ListPosition {
    fn known(after: Option<ListId>) -> Self {
        after.map_or(Self::Head, Self::After)
    }

    fn after(self) -> Option<Option<ListId>> {
        match self {
            Self::Unknown => None,
            Self::Head => Some(None),
            Self::After(id) => Some(Some(id)),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct List {
    entries: BTreeMap<ListId, ListEntry>,
}

impl List {
    pub fn visible_ids(&self) -> Vec<ListId> {
        let mut children: BTreeMap<Option<ListId>, Vec<ListId>> = BTreeMap::new();
        for (id, entry) in &self.entries {
            if let Some(after) = entry.position.after() {
                children.entry(after).or_default().push(*id);
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

    pub fn cell_by_id(&self, id: ListId) -> Option<&Cell> {
        self.entries.get(&id).map(|entry| &entry.cell)
    }

    pub fn cell_by_id_mut(&mut self, id: ListId) -> Option<&mut Cell> {
        self.entries.get_mut(&id).map(|entry| &mut entry.cell)
    }

    /// Iterate over all visible cells in list order.
    pub fn cells(&self) -> impl Iterator<Item = &Cell> {
        self.visible_ids()
            .into_iter()
            .filter_map(move |id| self.entries.get(&id).map(|entry| &entry.cell))
    }
}

impl ListEntry {
    fn inserted(after: Option<ListId>, cell: Cell) -> ListEntry {
        ListEntry {
            position: ListPosition::known(after),
            cell,
        }
    }

    fn placeholder(cell: Cell) -> ListEntry {
        ListEntry {
            position: ListPosition::Unknown,
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
        local_after: Box<Option<ListId>>,
        remote_after: Box<Option<ListId>>,
    },
}

impl std::fmt::Display for ListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListError::ZeroId => f.write_str("list element ID cannot use the zero event stamp"),
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

    fn apply(&mut self, op: &ListOp, stamps: crate::MergeStamps) -> Result<bool, ListError> {
        let stamps = stamps.incoming;
        match op {
            ListOp::Insert { after, value } => {
                if stamps == EventStamp::default() {
                    return Err(ListError::ZeroId);
                }

                let incoming = Cell {
                    value: Some(value.clone()),
                    stamp: stamps,
                };
                match self.entries.get_mut(&stamps) {
                    Some(entry) => {
                        let positioned = resolve_position(entry, stamps, *after)?;
                        let cell_stamps = MergeStamps::new(entry.cell.stamp, incoming.stamp);
                        Ok(entry
                            .cell
                            .merge(&incoming, cell_stamps)
                            .map_err(|error| ListError::Child(Box::new(error)))?
                            || positioned)
                    }
                    None => {
                        self.entries
                            .insert(stamps, ListEntry::inserted(*after, incoming));
                        Ok(true)
                    }
                }
            }
            ListOp::Delete { id } => {
                if *id == EventStamp::default() {
                    return Err(ListError::ZeroId);
                }

                let tombstone = Cell {
                    value: None,
                    stamp: stamps,
                };
                match self.entries.get_mut(id) {
                    Some(entry) => {
                        let cell_stamps = MergeStamps::new(entry.cell.stamp, tombstone.stamp);
                        entry
                            .cell
                            .merge(&tombstone, cell_stamps)
                            .map_err(|error| ListError::Child(Box::new(error)))
                    }
                    None => {
                        self.entries.insert(*id, ListEntry::placeholder(tombstone));
                        Ok(true)
                    }
                }
            }
        }
    }

    fn merge(&mut self, remote: &List, _stamps: MergeStamps) -> Result<bool, ListError> {
        let mut changed = false;

        for (id, remote_entry) in &remote.entries {
            match self.entries.get_mut(id) {
                Some(local_entry) => {
                    if let Some(after) = remote_entry.position.after() {
                        changed |= resolve_position(local_entry, *id, after)?;
                    }
                    let cell_stamps =
                        MergeStamps::new(local_entry.cell.stamp, remote_entry.cell.stamp);
                    if local_entry
                        .cell
                        .merge(&remote_entry.cell, cell_stamps)
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

    fn max_stamp(&self) -> EventStamp {
        self.entries
            .values()
            .fold(EventStamp::default(), |max, entry| {
                std::cmp::max(max, entry.cell.max_stamp())
            })
    }
}

impl ContainerType for List {
    fn child(&self, segment: &Segment) -> Option<&Cell> {
        let Segment::List(id) = segment else {
            return None;
        };
        self.entries.get(id).map(|entry| &entry.cell)
    }

    fn child_mut(&mut self, segment: &Segment) -> Option<&mut Cell> {
        let Segment::List(id) = segment else {
            return None;
        };
        self.entries.get_mut(id).map(|entry| &mut entry.cell)
    }

    fn apply_walk(
        &mut self,
        op: &Op,
        stamps: crate::MergeStamps,
        path: &[Segment],
    ) -> Result<bool, ListError> {
        let incoming = stamps.incoming;
        let Some((segment, remaining)) = path.split_first() else {
            return Ok(false);
        };
        let Segment::List(id) = segment else {
            return Ok(false);
        };
        let id = *id;
        if id == EventStamp::default() {
            return Err(ListError::ZeroId);
        }
        let child_tag = remaining
            .first()
            .map(Segment::type_tag)
            .or_else(|| op.type_tag());
        let entry = self.entries.entry(id).or_insert_with(|| {
            let cell = child_tag
                .map(|tag| Cell::dummy(Some(tag.empty_value())))
                .unwrap_or(Cell {
                    value: None,
                    stamp: EventStamp::default(),
                });
            ListEntry::placeholder(cell)
        });
        if child_tag.is_some_and(|tag| !entry.cell.ensure_type(tag, incoming)) {
            return Ok(false);
        }
        let child_stamps = MergeStamps::new(entry.cell.stamp, incoming);
        entry
            .cell
            .apply_walk(op, child_stamps, remaining)
            .map_err(|error| ListError::Child(Box::new(error)))
    }
}

fn resolve_position(
    entry: &mut ListEntry,
    id: ListId,
    after: Option<ListId>,
) -> Result<bool, ListError> {
    let position = ListPosition::known(after);
    if entry.position == ListPosition::Unknown {
        entry.position = position;
        return Ok(true);
    }
    if entry.position != position {
        return Err(ListError::PositionConflict {
            id,
            local_after: Box::new(entry.position.after().flatten()),
            remote_after: Box::new(after),
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
