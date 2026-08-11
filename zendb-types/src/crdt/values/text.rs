//! RGA-style collaborative text with explicit insert, delete, and format operations.

use crate::{EventTime, Value, zendb_type};
use bincode::{Decode, Encode};
use std::collections::{BTreeMap, BTreeSet};

pub type TextId = (EventTime, u32);

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct TextContent {
    after: Option<TextId>,
    character: char,
}
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct TextEntry {
    content: Option<TextContent>,
    deleted_at: Option<EventTime>,
    attrs: BTreeMap<std::string::String, (Option<Value>, EventTime)>,
}

#[derive(Debug)]
pub enum TextError {
    ZeroId,
    SelfAnchor,
    TooLong,
}
impl std::fmt::Display for TextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroId => f.write_str("text character ID cannot be zero"),
            Self::SelfAnchor => f.write_str("text insert cannot anchor to itself"),
            Self::TooLong => f.write_str("text insert exceeds u32::MAX characters"),
        }
    }
}
impl std::error::Error for TextError {}

zendb_type! {
    #[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
    pub struct Text { entries: BTreeMap<TextId, TextEntry> }

    impl Text {
        pub fn op_insert(&mut self, remote: EventTime, after: Option<TextId>, text: std::string::String) -> Result<bool, TextError> {
            if remote == EventTime::ZERO { return Err(TextError::ZeroId); }
            if after.is_some_and(|id| id.0 == remote) { return Err(TextError::SelfAnchor); }
            let chars: Vec<char> = text.chars().collect();
            let _: u32 = chars.len().try_into().map_err(|_| TextError::TooLong)?;
            let mut previous = after;
            let mut changed = false;
            for (offset, character) in chars.into_iter().enumerate() {
                let id = (remote, offset as u32);
                if let Some(entry) = self.entries.get_mut(&id) {
                    if entry.content.is_none() { entry.content = Some(TextContent { after: previous, character }); changed = true; }
                } else {
                    self.entries.insert(id, TextEntry { content: Some(TextContent { after: previous, character }), deleted_at: None, attrs: BTreeMap::new() });
                    changed = true;
                }
                previous = Some(id);
            }
            if changed { self.__event_time = self.__event_time.max(remote); }
            Ok(changed)
        }

        pub fn op_delete(&mut self, remote: EventTime, ids: Vec<TextId>) -> Result<bool, TextError> {
            if ids.iter().any(|id| id.0 == EventTime::ZERO) { return Err(TextError::ZeroId); }
            let mut changed = false;
            for id in ids {
                let entry = self.entries.entry(id).or_insert(TextEntry { content: None, deleted_at: None, attrs: BTreeMap::new() });
                if entry.deleted_at.is_none_or(|current| remote > current) { entry.deleted_at = Some(remote); changed = true; }
            }
            if changed { self.__event_time = self.__event_time.max(remote); }
            Ok(changed)
        }

        pub fn op_format(&mut self, remote: EventTime, ids: Vec<TextId>, key: std::string::String, value: Option<Value>) -> Result<bool, TextError> {
            if ids.iter().any(|id| id.0 == EventTime::ZERO) { return Err(TextError::ZeroId); }
            let mut changed = false;
            for id in ids {
                let entry = self.entries.entry(id).or_insert(TextEntry { content: None, deleted_at: None, attrs: BTreeMap::new() });
                if entry.attrs.get(&key).is_none_or(|(_, current)| remote > *current) { entry.attrs.insert(key.clone(), (value.clone(), remote)); changed = true; }
            }
            if changed { self.__event_time = self.__event_time.max(remote); }
            Ok(changed)
        }
    }
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
    pub fn string(&self) -> std::string::String {
        self.visible_ids()
            .into_iter()
            .filter_map(|id| {
                self.entries
                    .get(&id)?
                    .content
                    .as_ref()
                    .map(|content| content.character)
            })
            .collect()
    }
    pub fn format_at(&self, index: usize) -> Option<BTreeMap<std::string::String, Value>> {
        let id = self.id_at(index)?;
        let entry = self.entries.get(&id)?;
        Some(
            entry
                .attrs
                .iter()
                .filter_map(|(key, (value, _))| value.clone().map(|value| (key.clone(), value)))
                .collect(),
        )
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
        if let Some(entry) = text.entries.get(id) {
            if entry.content.is_some() && entry.deleted_at.is_none() {
                visible.push(*id);
            }
            walk_visible(Some(*id), text, children, visited, visible);
        }
    }
}
