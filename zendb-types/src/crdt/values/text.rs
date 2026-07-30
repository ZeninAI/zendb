//! Text - an RGA-style collaborative Unicode text sequence.

use std::collections::{BTreeMap, BTreeSet};

use bincode::{Decode, Encode};

use crate::{EventStamp, Type, Value};

/// Stable character identity: insert operation HLC plus character offset.
pub type TextId = (EventStamp, u32);

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct TextContent {
    after: Option<TextId>,
    character: char,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct TextEntry {
    content: Option<TextContent>,
    deleted_at: Option<EventStamp>,
    /// Per-character formatting attributes with per-key LWW stamps.
    /// Each entry is (format_value, operation_stamp). Merge picks the value
    /// with the higher HLC for each key, so concurrent format operations
    /// targeting the same key converge deterministically.
    ///
    /// Reference: Litt, Lim, Kleppmann & van Hardenberg. "Peritext: A CRDT
    /// for collaborative rich text editing." CSCW 2022.
    attrs: std::collections::BTreeMap<String, (Option<Value>, EventStamp)>,
}

impl TextEntry {
    fn inserted(after: Option<TextId>, character: char) -> TextEntry {
        TextEntry {
            content: Some(TextContent { after, character }),
            deleted_at: None,
            attrs: BTreeMap::new(),
        }
    }

    fn placeholder(deleted_at: Option<EventStamp>) -> TextEntry {
        TextEntry {
            content: None,
            deleted_at,
            attrs: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct Text {
    entries: BTreeMap<TextId, TextEntry>,
}

impl Text {
    pub fn visible_ids(&self) -> Vec<TextId> {
        let mut children: BTreeMap<Option<TextId>, Vec<TextId>> = BTreeMap::new();
        for (id, entry) in &self.entries {
            if let Some(content) = &entry.content {
                children.entry(content.after).or_default().push(*id);
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

    pub fn id_at(&self, index: usize) -> Option<TextId> {
        self.visible_ids().get(index).copied()
    }

    pub fn string(&self) -> String {
        self.visible_ids()
            .into_iter()
            .filter_map(|id| {
                self.entries
                    .get(&id)
                    .and_then(|entry| entry.content.as_ref())
                    .map(|content| content.character)
            })
            .collect()
    }

    /// Return a snapshot of active formatting attributes at a character index.
    pub fn format_at(&self, index: usize) -> Option<BTreeMap<String, Value>> {
        let id = self.id_at(index)?;
        let entry = self.entries.get(&id)?;
        Some(
            entry
                .attrs
                .iter()
                .filter_map(|(key, (value, _))| {
                    value.as_ref().map(|value| (key.clone(), value.clone()))
                })
                .collect(),
        )
    }

    /// Build a deterministic formatting operation over the currently visible
    /// character interval.
    pub fn format(
        &self,
        start: Option<TextId>,
        end: Option<TextId>,
        key: String,
        value: Option<Value>,
    ) -> Result<TextOp, TextError> {
        let visible = self.visible_ids();
        let start_index = match start {
            Some(id) => visible
                .iter()
                .position(|candidate| *candidate == id)
                .ok_or(TextError::FormatTargetUnknown { id })?,
            None => 0,
        };
        let end_index = match end {
            Some(id) => visible
                .iter()
                .position(|candidate| *candidate == id)
                .ok_or(TextError::FormatTargetUnknown { id })?,
            None => visible.len(),
        };
        Ok(TextOp::Format {
            ids: visible[start_index..end_index.max(start_index)].to_vec(),
            key,
            value,
        })
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum TextOp {
    Insert {
        after: Option<TextId>,
        text: String,
    },
    Delete {
        ids: Vec<TextId>,
    },
    /// Apply or remove formatting on explicit stable character IDs.
    /// `value = None` removes the key from affected characters.
    Format {
        ids: Vec<TextId>,
        key: String,
        value: Option<Value>,
    },
}

#[derive(Debug)]
pub enum TextError {
    ZeroClock,
    ZeroId,
    SelfAnchor,
    TooLong,
    InsertConflict { id: TextId },
    FormatTargetUnknown { id: TextId },
}

impl std::fmt::Display for TextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TextError::ZeroClock => f.write_str("text operation cannot use the zero event stamp"),
            TextError::ZeroId => f.write_str("text character ID cannot contain a zero event stamp"),
            TextError::SelfAnchor => f.write_str("text insert cannot anchor to its own operation"),
            TextError::TooLong => f.write_str("text insert exceeds u32::MAX characters"),
            TextError::InsertConflict { id } => {
                write!(f, "text character {id:?} has conflicting insert content")
            }
            TextError::FormatTargetUnknown { id } => {
                write!(f, "format target character {id:?} does not exist")
            }
        }
    }
}

impl std::error::Error for TextError {}

impl Type for Text {
    type Op = TextOp;
    type Error = TextError;

    fn apply(&mut self, op: &TextOp, stamps: crate::MergeStamps) -> Result<bool, TextError> {
        let stamps = stamps.incoming;
        if stamps == EventStamp::default() {
            return Err(TextError::ZeroClock);
        }

        match op {
            TextOp::Insert { after, text } => {
                if after.is_some_and(|id| id.0 == EventStamp::default()) {
                    return Err(TextError::ZeroId);
                }
                if after.is_some_and(|id| id.0 == stamps) {
                    return Err(TextError::SelfAnchor);
                }
                let characters: Vec<char> = text.chars().collect();
                let count = u32::try_from(characters.len()).map_err(|_| TextError::TooLong)?;
                for (id, entry) in self.entries.iter().filter(|(id, _)| id.0 == stamps) {
                    let Some(content) = &entry.content else {
                        continue;
                    };
                    let offset = id.1;
                    let expected_after = if offset == 0 {
                        *after
                    } else {
                        Some((stamps, offset - 1))
                    };
                    let expected_character = characters.get(offset as usize).copied();
                    if offset >= count
                        || content.after != expected_after
                        || Some(content.character) != expected_character
                    {
                        return Err(TextError::InsertConflict { id: *id });
                    }
                }

                let mut changed = false;
                let mut previous = *after;
                for (offset, character) in characters.into_iter().enumerate() {
                    let id = (stamps, offset as u32);
                    match self.entries.get_mut(&id) {
                        Some(entry) => {
                            if entry.content.is_none() {
                                entry.content = Some(TextContent {
                                    after: previous,
                                    character,
                                });
                                changed = true;
                            }
                        }
                        None => {
                            self.entries
                                .insert(id, TextEntry::inserted(previous, character));
                            changed = true;
                        }
                    }
                    previous = Some(id);
                }
                Ok(changed)
            }
            TextOp::Delete { ids } => {
                if ids.iter().any(|id| id.0 == EventStamp::default()) {
                    return Err(TextError::ZeroId);
                }
                let mut changed = false;
                for id in ids {
                    if stamps <= id.0 {
                        continue;
                    }
                    match self.entries.get_mut(id) {
                        Some(entry) => {
                            if merge_clock(&mut entry.deleted_at, Some(stamps)) {
                                changed = true;
                            }
                        }
                        None => {
                            self.entries
                                .insert(*id, TextEntry::placeholder(Some(stamps)));
                            changed = true;
                        }
                    }
                }
                Ok(changed)
            }
            TextOp::Format { ids, key, value } => {
                if ids.iter().any(|id| id.0 == EventStamp::default()) {
                    return Err(TextError::ZeroId);
                }
                let mut changed = false;
                for id in ids {
                    let entry = self
                        .entries
                        .entry(*id)
                        .or_insert_with(|| TextEntry::placeholder(None));
                    match value {
                        Some(v) => {
                            let should_update = match entry.attrs.get(key) {
                                Some((_, existing_stamp)) => stamps > *existing_stamp,
                                None => true,
                            };
                            if should_update {
                                entry.attrs.insert(key.clone(), (Some(v.clone()), stamps));
                                changed = true;
                            }
                        }
                        None => {
                            // Remove only if this op's HLC beats the existing attr's HLC.
                            let should_remove = match entry.attrs.get(key) {
                                Some((_, existing_stamp)) => stamps > *existing_stamp,
                                None => true,
                            };
                            if should_remove {
                                entry.attrs.insert(key.clone(), (None, stamps));
                                changed = true;
                            }
                        }
                    }
                }
                Ok(changed)
            }
        }
    }

    fn merge(&mut self, remote: &Text, _stamps: crate::MergeStamps) -> Result<bool, TextError> {
        for (id, remote_entry) in &remote.entries {
            let Some(local_entry) = self.entries.get(id) else {
                continue;
            };
            if let (Some(local), Some(remote)) = (&local_entry.content, &remote_entry.content) {
                if local != remote {
                    return Err(TextError::InsertConflict { id: *id });
                }
            }
        }
        let mut changed = false;

        for (id, remote_entry) in &remote.entries {
            match self.entries.get_mut(id) {
                Some(local_entry) => {
                    if local_entry.content.is_none() && remote_entry.content.is_some() {
                        local_entry.content = remote_entry.content.clone();
                        changed = true;
                    }
                    if merge_clock(&mut local_entry.deleted_at, remote_entry.deleted_at) {
                        changed = true;
                    }
                    for (key, (remote_value, remote_stamp)) in &remote_entry.attrs {
                        match local_entry.attrs.get(key) {
                            Some((_, local_stamp)) if remote_stamp <= local_stamp => {}
                            _ => {
                                local_entry
                                    .attrs
                                    .insert(key.clone(), (remote_value.clone(), *remote_stamp));
                                changed = true;
                            }
                        }
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
            .iter()
            .fold(EventStamp::default(), |max, (id, entry)| {
                let entry_max = entry
                    .attrs
                    .values()
                    .map(|(_, h)| *h)
                    .fold(EventStamp::default(), EventStamp::max);
                std::cmp::max(
                    max,
                    std::cmp::max(
                        std::cmp::max(id.0, entry.deleted_at.unwrap_or_else(EventStamp::default)),
                        entry_max,
                    ),
                )
            })
    }
}

fn merge_clock(local: &mut Option<EventStamp>, remote: Option<EventStamp>) -> bool {
    let Some(remote) = remote else {
        return false;
    };
    if local.is_none_or(|current| remote > current) {
        *local = Some(remote);
        true
    } else {
        false
    }
}

fn walk_visible(
    after: Option<TextId>,
    text: &Text,
    children: &BTreeMap<Option<TextId>, Vec<TextId>>,
    visited: &mut BTreeSet<TextId>,
    visible: &mut Vec<TextId>,
) {
    let Some(siblings) = children.get(&after) else {
        return;
    };

    for id in siblings {
        if !visited.insert(*id) {
            continue;
        }
        let Some(entry) = text.entries.get(id) else {
            continue;
        };
        if entry.content.is_some() && entry.deleted_at.is_none_or(|deleted| id.0 > deleted) {
            visible.push(*id);
        }
        walk_visible(Some(*id), text, children, visited, visible);
    }
}
