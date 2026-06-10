//! Text - an RGA-style collaborative Unicode text sequence.

use std::collections::{BTreeMap, BTreeSet};

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc, Value};

/// Stable character identity: insert operation HLC plus character offset.
pub type TextId = (Hlc, u32);

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct TextEntry {
    after: Option<TextId>,
    character: Option<char>,
    content_known: bool,
    deleted_at: Option<Hlc>,
    /// Per-character formatting attributes with per-key LWW clocks.
    /// Each entry is (format_value, operation_hlc). Merge picks the value
    /// with the higher HLC for each key, so concurrent format operations
    /// targeting the same key converge deterministically.
    ///
    /// Reference: Litt, Lim, Kleppmann & van Hardenberg. "Peritext: A CRDT
    /// for collaborative rich text editing." CSCW 2022.
    attrs: std::collections::BTreeMap<String, (Option<Value>, Hlc)>,
}

impl TextEntry {
    fn inserted(after: Option<TextId>, character: char) -> TextEntry {
        TextEntry {
            after,
            character: Some(character),
            content_known: true,
            deleted_at: None,
            attrs: BTreeMap::new(),
        }
    }

    fn placeholder(deleted_at: Option<Hlc>) -> TextEntry {
        TextEntry {
            after: None,
            character: None,
            content_known: false,
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
            if entry.content_known {
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

    pub fn id_at(&self, index: usize) -> Option<TextId> {
        self.visible_ids().get(index).copied()
    }

    pub fn string(&self) -> String {
        self.visible_ids()
            .into_iter()
            .filter_map(|id| self.entries.get(&id).and_then(|entry| entry.character))
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

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
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
            TextError::ZeroClock => f.write_str("text operation HLC cannot be Hlc::ZERO"),
            TextError::ZeroId => f.write_str("text character ID cannot contain Hlc::ZERO"),
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

    fn apply(&mut self, op: &TextOp, op_hlc: Hlc) -> Result<bool, TextError> {
        if op_hlc == Hlc::ZERO {
            return Err(TextError::ZeroClock);
        }

        match op {
            TextOp::Insert { after, text } => {
                if after.is_some_and(|id| id.0 == Hlc::ZERO) {
                    return Err(TextError::ZeroId);
                }
                if after.is_some_and(|id| id.0 == op_hlc) {
                    return Err(TextError::SelfAnchor);
                }
                let characters: Vec<char> = text.chars().collect();
                let count = u32::try_from(characters.len()).map_err(|_| TextError::TooLong)?;
                for (id, entry) in self.entries.iter().filter(|(id, _)| id.0 == op_hlc) {
                    if !entry.content_known {
                        continue;
                    }
                    let offset = id.1;
                    let expected_after = if offset == 0 {
                        *after
                    } else {
                        Some((op_hlc, offset - 1))
                    };
                    let expected_character = characters.get(offset as usize).copied();
                    if offset >= count
                        || entry.after != expected_after
                        || entry.character != expected_character
                    {
                        return Err(TextError::InsertConflict { id: *id });
                    }
                }

                let mut changed = false;
                let mut previous = *after;
                for (offset, character) in characters.into_iter().enumerate() {
                    let id = (op_hlc, offset as u32);
                    match self.entries.get_mut(&id) {
                        Some(entry) => {
                            if !entry.content_known {
                                entry.after = previous;
                                entry.character = Some(character);
                                entry.content_known = true;
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
                if ids.iter().any(|id| id.0 == Hlc::ZERO) {
                    return Err(TextError::ZeroId);
                }
                let mut changed = false;
                for id in ids {
                    match self.entries.get_mut(id) {
                        Some(entry) => {
                            if merge_clock(&mut entry.deleted_at, Some(op_hlc)) {
                                changed = true;
                            }
                        }
                        None => {
                            self.entries
                                .insert(*id, TextEntry::placeholder(Some(op_hlc)));
                            changed = true;
                        }
                    }
                }
                Ok(changed)
            }
            TextOp::Format { ids, key, value } => {
                if ids.iter().any(|id| id.0 == Hlc::ZERO) {
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
                                Some((_, existing_hlc)) => op_hlc.beats(*existing_hlc),
                                None => true,
                            };
                            if should_update {
                                entry.attrs.insert(key.clone(), (Some(v.clone()), op_hlc));
                                changed = true;
                            }
                        }
                        None => {
                            // Remove only if this op's HLC beats the existing attr's HLC.
                            let should_remove = match entry.attrs.get(key) {
                                Some((_, existing_hlc)) => op_hlc.beats(*existing_hlc),
                                None => true,
                            };
                            if should_remove {
                                entry.attrs.insert(key.clone(), (None, op_hlc));
                                changed = true;
                            }
                        }
                    }
                }
                Ok(changed)
            }
        }
    }

    fn merge(&mut self, remote: &Text, _clocks: crate::MergeClocks) -> Result<bool, TextError> {
        for (id, remote_entry) in &remote.entries {
            let Some(local_entry) = self.entries.get(id) else {
                continue;
            };
            if local_entry.content_known
                && remote_entry.content_known
                && (local_entry.after != remote_entry.after
                    || local_entry.character != remote_entry.character)
            {
                return Err(TextError::InsertConflict { id: *id });
            }
        }
        let mut changed = false;

        for (id, remote_entry) in &remote.entries {
            match self.entries.get_mut(id) {
                Some(local_entry) => {
                    if remote_entry.content_known && !local_entry.content_known {
                        local_entry.after = remote_entry.after;
                        local_entry.character = remote_entry.character;
                        local_entry.content_known = true;
                        changed = true;
                    }
                    if merge_clock(&mut local_entry.deleted_at, remote_entry.deleted_at) {
                        changed = true;
                    }
                    for (key, (remote_value, remote_hlc)) in &remote_entry.attrs {
                        match local_entry.attrs.get(key) {
                            Some((_, local_hlc)) if !remote_hlc.beats(*local_hlc) => {}
                            _ => {
                                local_entry
                                    .attrs
                                    .insert(key.clone(), (remote_value.clone(), *remote_hlc));
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

    fn compact(&mut self, watermark: Hlc) -> Result<bool, TextError> {
        let mut changed = false;
        loop {
            let referenced: BTreeSet<TextId> = self
                .entries
                .values()
                .filter_map(|entry| entry.after)
                .collect();
            let before = self.entries.len();
            self.entries.retain(|id, entry| {
                entry.deleted_at.is_none_or(|deleted| deleted > watermark)
                    || referenced.contains(id)
            });
            if self.entries.len() == before {
                break;
            }
            changed = true;
        }
        for entry in self.entries.values_mut() {
            let before = entry.attrs.len();
            entry
                .attrs
                .retain(|_, (value, at)| value.is_some() || *at > watermark);
            changed |= entry.attrs.len() != before;
        }
        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.entries.iter().fold(Hlc::ZERO, |max, (id, entry)| {
            let entry_max = entry
                .attrs
                .values()
                .map(|(_, h)| *h)
                .fold(Hlc::ZERO, Hlc::max);
            std::cmp::max(
                max,
                std::cmp::max(
                    std::cmp::max(id.0, entry.deleted_at.unwrap_or(Hlc::ZERO)),
                    entry_max,
                ),
            )
        })
    }
}

fn merge_clock(local: &mut Option<Hlc>, remote: Option<Hlc>) -> bool {
    let Some(remote) = remote else {
        return false;
    };
    if local.is_none_or(|current| remote.beats(current)) {
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
        if entry.content_known && entry.deleted_at.is_none_or(|deleted| id.0.beats(deleted)) {
            visible.push(*id);
        }
        walk_visible(Some(*id), text, children, visited, visible);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn apply(text: &mut Text, op: TextOp, at: Hlc) -> bool {
        Type::apply(text, &op, at).unwrap()
    }

    fn merge_order(texts: &[Text; 3], order: [usize; 3]) -> Text {
        let mut merged = texts[order[0]].clone();
        Type::merge(&mut merged, &texts[order[1]], crate::MergeClocks::ZERO).unwrap();
        Type::merge(&mut merged, &texts[order[2]], crate::MergeClocks::ZERO).unwrap();
        merged
    }

    #[test]
    fn insert_creates_stable_id_for_each_unicode_scalar() {
        let mut text = Text::default();
        let at = hlc(100, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "a\u{1f642}b".into(),
            },
            at,
        );

        assert_eq!(text.string(), "a\u{1f642}b");
        assert_eq!(text.visible_ids(), vec![(at, 0), (at, 1), (at, 2)]);
    }

    #[test]
    fn sequential_insert_can_target_character_cursor() {
        let mut text = Text::default();
        let first = hlc(100, 1);
        let second = hlc(200, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "ac".into(),
            },
            first,
        );
        apply(
            &mut text,
            TextOp::Insert {
                after: Some((first, 0)),
                text: "b".into(),
            },
            second,
        );

        assert_eq!(text.string(), "abc");
    }

    #[test]
    fn concurrent_inserts_merge_deterministically() {
        let mut left = Text::default();
        let mut right = Text::default();
        apply(
            &mut left,
            TextOp::Insert {
                after: None,
                text: "left".into(),
            },
            hlc(100, 1),
        );
        apply(
            &mut right,
            TextOp::Insert {
                after: None,
                text: "right".into(),
            },
            hlc(100, 2),
        );

        let mut left_first = left.clone();
        let mut right_first = right.clone();
        Type::merge(&mut left_first, &right, crate::MergeClocks::ZERO).unwrap();
        Type::merge(&mut right_first, &left, crate::MergeClocks::ZERO).unwrap();
        assert_eq!(left_first, right_first);
        assert_eq!(left_first.string(), "rightleft");
    }

    #[test]
    fn deleted_character_remains_an_anchor() {
        let mut text = Text::default();
        let insert = hlc(100, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "ab".into(),
            },
            insert,
        );
        apply(
            &mut text,
            TextOp::Delete {
                ids: vec![(insert, 0)],
            },
            hlc(200, 1),
        );
        assert_eq!(text.string(), "b");
    }

    #[test]
    fn compact_removes_only_stable_unreferenced_deleted_characters() {
        let mut text = Text::default();
        let anchor = hlc(100, 1);
        let orphan = hlc(200, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "a".into(),
            },
            anchor,
        );
        apply(
            &mut text,
            TextOp::Insert {
                after: Some((anchor, 0)),
                text: "b".into(),
            },
            hlc(150, 1),
        );
        apply(
            &mut text,
            TextOp::Delete {
                ids: vec![(anchor, 0), (orphan, 0)],
            },
            hlc(300, 1),
        );

        assert!(Type::compact(&mut text, hlc(300, 1)).unwrap());
        assert!(text.entries.contains_key(&(anchor, 0)));
        assert!(!text.entries.contains_key(&(orphan, 0)));
        assert_eq!(text.string(), "b");
    }

    #[test]
    fn delete_before_insert_converges() {
        let insert = hlc(100, 1);
        let delete = hlc(200, 2);
        let mut before = Text::default();
        apply(
            &mut before,
            TextOp::Delete {
                ids: vec![(insert, 0)],
            },
            delete,
        );
        apply(
            &mut before,
            TextOp::Insert {
                after: None,
                text: "a".into(),
            },
            insert,
        );

        let mut after = Text::default();
        apply(
            &mut after,
            TextOp::Insert {
                after: None,
                text: "a".into(),
            },
            insert,
        );
        apply(
            &mut after,
            TextOp::Delete {
                ids: vec![(insert, 0)],
            },
            delete,
        );

        assert_eq!(before, after);
        assert_eq!(before.string(), "");
    }

    #[test]
    fn older_delete_does_not_hide_newer_insert() {
        let insert = hlc(200, 1);
        let mut text = Text::default();
        apply(
            &mut text,
            TextOp::Delete {
                ids: vec![(insert, 0)],
            },
            hlc(100, 2),
        );
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "a".into(),
            },
            insert,
        );
        assert_eq!(text.string(), "a");
    }

    #[test]
    fn conflicting_payload_for_same_insert_id_is_rejected_without_mutation() {
        let at = hlc(100, 1);
        let mut text = Text::default();
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "abc".into(),
            },
            at,
        );
        let snapshot = text.clone();

        assert!(matches!(
            Type::apply(
                &mut text,
                &TextOp::Insert {
                    after: None,
                    text: "ax".into(),
                },
                at,
            ),
            Err(TextError::InsertConflict { .. })
        ));
        assert_eq!(text, snapshot);
    }

    #[test]
    fn empty_and_duplicate_operations_are_idempotent() {
        let mut text = Text::default();
        let at = hlc(100, 1);
        assert!(!apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: String::new(),
            },
            at,
        ));
        let insert = TextOp::Insert {
            after: None,
            text: "a".into(),
        };
        assert!(apply(&mut text, insert.clone(), at));
        assert!(!apply(&mut text, insert, at));
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let anchor = hlc(100, 1);
        let mut texts = [Text::default(), Text::default(), Text::default()];
        for text in &mut texts {
            apply(
                text,
                TextOp::Insert {
                    after: None,
                    text: "a".into(),
                },
                anchor,
            );
        }
        apply(
            &mut texts[0],
            TextOp::Insert {
                after: Some((anchor, 0)),
                text: "x".into(),
            },
            hlc(200, 1),
        );
        apply(
            &mut texts[1],
            TextOp::Insert {
                after: Some((anchor, 0)),
                text: "y".into(),
            },
            hlc(200, 2),
        );
        apply(
            &mut texts[2],
            TextOp::Delete {
                ids: vec![(anchor, 0)],
            },
            hlc(300, 3),
        );

        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let expected = merge_order(&texts, orders[0]);
        for order in orders.into_iter().skip(1) {
            assert_eq!(merge_order(&texts, order), expected);
        }
        assert_eq!(expected.string(), "yx");
    }

    #[test]
    fn zero_clocks_and_ids_are_rejected() {
        let mut text = Text::default();
        assert!(matches!(
            Type::apply(
                &mut text,
                &TextOp::Insert {
                    after: None,
                    text: "a".into(),
                },
                Hlc::ZERO,
            ),
            Err(TextError::ZeroClock)
        ));
        assert!(matches!(
            Type::apply(
                &mut text,
                &TextOp::Delete {
                    ids: vec![(Hlc::ZERO, 0)],
                },
                hlc(100, 1),
            ),
            Err(TextError::ZeroId)
        ));
        assert!(matches!(
            Type::apply(
                &mut text,
                &TextOp::Insert {
                    after: Some((Hlc::ZERO, 0)),
                    text: "a".into(),
                },
                hlc(100, 1),
            ),
            Err(TextError::ZeroId)
        ));
    }

    #[test]
    fn insert_cannot_anchor_to_its_own_character_ids() {
        let at = hlc(100, 1);
        let mut text = Text::default();
        assert!(matches!(
            Type::apply(
                &mut text,
                &TextOp::Insert {
                    after: Some((at, 0)),
                    text: "cycle".into(),
                },
                at,
            ),
            Err(TextError::SelfAnchor)
        ));
        assert!(text.is_empty());
    }

    #[test]
    fn invalid_batch_delete_does_not_partially_mutate_text() {
        let insert = hlc(100, 1);
        let mut text = Text::default();
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "ab".into(),
            },
            insert,
        );
        let snapshot = text.clone();

        assert!(matches!(
            Type::apply(
                &mut text,
                &TextOp::Delete {
                    ids: vec![(insert, 0), (Hlc::ZERO, 0)],
                },
                hlc(200, 1),
            ),
            Err(TextError::ZeroId)
        ));
        assert_eq!(text, snapshot);
    }

    #[test]
    fn text_bincode_roundtrip() {
        let mut text = Text::default();
        let insert = hlc(100, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "hello \u{1f642}".into(),
            },
            insert,
        );
        apply(
            &mut text,
            TextOp::Delete {
                ids: vec![(insert, 1)],
            },
            hlc(200, 2),
        );

        let encoded = encode_to_vec(&text, config::standard()).unwrap();
        let (decoded, consumed): (Text, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, text);
    }

    // --- Rich text formatting tests ---

    #[test]
    fn format_applies_to_visible_characters_in_range() {
        let mut text = Text::default();
        let a = hlc(100, 1);
        let b = hlc(200, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "abc".into(),
            },
            a,
        );
        apply(
            &mut text,
            TextOp::Insert {
                after: Some((a, 2)),
                text: "d".into(),
            },
            b,
        );
        // text = "abcd"

        let ids = text.visible_ids();
        let format = text
            .format(
                Some(ids[1]),
                Some(ids[3]),
                "bold".into(),
                Some(Value::Bool(true)),
            )
            .unwrap();
        apply(&mut text, format, hlc(300, 1));

        // b and c should be bold (ids[1] and ids[2]); a and d should not.
        assert_eq!(text.format_at(0).unwrap().get("bold"), None);
        assert_eq!(
            text.format_at(1).unwrap().get("bold"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            text.format_at(2).unwrap().get("bold"),
            Some(&Value::Bool(true))
        );
        assert_eq!(text.format_at(3).unwrap().get("bold"), None);
    }

    #[test]
    fn format_remove_clears_attribute() {
        let mut text = Text::default();
        let a = hlc(100, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "x".into(),
            },
            a,
        );
        let ids = text.visible_ids();

        let add = text
            .format(Some(ids[0]), None, "bold".into(), Some(Value::Bool(true)))
            .unwrap();
        apply(&mut text, add, hlc(200, 1));
        assert!(text.format_at(0).unwrap().contains_key("bold"));

        let remove = text
            .format(Some(ids[0]), None, "bold".into(), None)
            .unwrap();
        apply(&mut text, remove, hlc(300, 1));
        assert!(!text.format_at(0).unwrap().contains_key("bold"));
    }

    #[test]
    fn format_removal_survives_merge_with_older_attribute() {
        let mut old = Text::default();
        let insert = hlc(100, 1);
        apply(
            &mut old,
            TextOp::Insert {
                after: None,
                text: "x".into(),
            },
            insert,
        );
        let id = old.visible_ids()[0];
        let add = old
            .format(Some(id), None, "bold".into(), Some(Value::Bool(true)))
            .unwrap();
        apply(&mut old, add, hlc(200, 1));

        let mut removed = old.clone();
        let remove = removed.format(Some(id), None, "bold".into(), None).unwrap();
        apply(&mut removed, remove, hlc(300, 1));
        Type::merge(&mut removed, &old, crate::MergeClocks::ZERO).unwrap();

        assert!(!removed.format_at(0).unwrap().contains_key("bold"));
    }

    #[test]
    fn explicit_format_targets_converge_when_format_arrives_before_insert() {
        let insert = hlc(100, 1);
        let format = TextOp::Format {
            ids: vec![(insert, 0)],
            key: "bold".into(),
            value: Some(Value::Bool(true)),
        };
        let insert_op = TextOp::Insert {
            after: None,
            text: "x".into(),
        };

        let mut insert_first = Text::default();
        apply(&mut insert_first, insert_op.clone(), insert);
        apply(&mut insert_first, format.clone(), hlc(200, 2));

        let mut format_first = Text::default();
        apply(&mut format_first, format, hlc(200, 2));
        apply(&mut format_first, insert_op, insert);

        assert_eq!(insert_first, format_first);
        assert!(insert_first.format_at(0).unwrap().contains_key("bold"));
    }

    #[test]
    fn stale_format_does_not_overwrite_newer_format() {
        let mut text = Text::default();
        let a = hlc(100, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "x".into(),
            },
            a,
        );
        let ids = text.visible_ids();

        let red = text
            .format(
                Some(ids[0]),
                None,
                "color".into(),
                Some(Value::String("red".into())),
            )
            .unwrap();
        apply(&mut text, red, hlc(300, 1));
        // Stale format with older HLC should not replace.
        let blue = text
            .format(
                Some(ids[0]),
                None,
                "color".into(),
                Some(Value::String("blue".into())),
            )
            .unwrap();
        apply(&mut text, blue, hlc(200, 2));
        assert_eq!(
            text.format_at(0).unwrap().get("color"),
            Some(&Value::String("red".into()))
        );
    }

    #[test]
    fn format_merged_across_replicas() {
        let mut left = Text::default();
        let mut right = Text::default();
        let a = hlc(100, 1);
        apply(
            &mut left,
            TextOp::Insert {
                after: None,
                text: "hi".into(),
            },
            a,
        );

        let ids = left.visible_ids();
        let bold = left
            .format(
                Some(ids[0]),
                Some(ids[1]),
                "bold".into(),
                Some(Value::Bool(true)),
            )
            .unwrap();
        apply(&mut left, bold, hlc(200, 1));

        // Right has same text but format on second character.
        apply(
            &mut right,
            TextOp::Insert {
                after: None,
                text: "hi".into(),
            },
            a,
        );
        let italic = right
            .format(Some(ids[1]), None, "italic".into(), Some(Value::Bool(true)))
            .unwrap();
        apply(&mut right, italic, hlc(200, 2));

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        assert!(left.format_at(0).unwrap().contains_key("bold"));
        assert!(!left.format_at(0).unwrap().contains_key("italic"));
        assert!(!left.format_at(1).unwrap().contains_key("bold"));
        assert!(left.format_at(1).unwrap().contains_key("italic"));
    }

    #[test]
    fn format_survives_concurrent_insert_inside_range() {
        // User A: formats "a..z" as bold across the whole range.
        // User B: inserts "x" inside the range.
        // The inserted "x" does NOT automatically inherit bold — formatting
        // must be explicitly applied. This test verifies that existing
        // formatting on surrounding characters is preserved.
        let mut left = Text::default();
        let a = hlc(100, 1);
        apply(
            &mut left,
            TextOp::Insert {
                after: None,
                text: "az".into(),
            },
            a,
        );
        let left_ids = left.visible_ids();

        // Format the full range
        let bold = left
            .format(
                Some(left_ids[0]),
                None,
                "bold".into(),
                Some(Value::Bool(true)),
            )
            .unwrap();
        apply(&mut left, bold, hlc(200, 1));

        // Replica B: insert 'b' between 'a' and 'z'
        let mut right = Text::default();
        apply(
            &mut right,
            TextOp::Insert {
                after: None,
                text: "az".into(),
            },
            a,
        );
        apply(
            &mut right,
            TextOp::Insert {
                after: Some((a, 0)),
                text: "b".into(),
            },
            hlc(150, 2),
        );

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        assert_eq!(left.string(), "abz");
        // 'a' and 'z' still bold; 'b' has no formatting (insert didn't carry it).
        assert!(left.format_at(0).unwrap().contains_key("bold"));
        assert!(!left.format_at(1).unwrap().contains_key("bold"));
        assert!(left.format_at(2).unwrap().contains_key("bold"));
    }

    #[test]
    fn text_bincode_roundtrip_with_formatting() {
        let mut text = Text::default();
        let a = hlc(100, 1);
        apply(
            &mut text,
            TextOp::Insert {
                after: None,
                text: "hi".into(),
            },
            a,
        );
        let ids = text.visible_ids();
        let bold = text
            .format(Some(ids[0]), None, "bold".into(), Some(Value::Bool(true)))
            .unwrap();
        apply(&mut text, bold, hlc(200, 1));

        let encoded = encode_to_vec(&text, config::standard()).unwrap();
        let (decoded, consumed): (Text, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, text);
        assert!(decoded
            .entries
            .get(&(a, 0))
            .unwrap()
            .attrs
            .contains_key("bold"));
    }
}
