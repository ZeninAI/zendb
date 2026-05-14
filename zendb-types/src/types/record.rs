//! Record — the named-field container type.
//!
//! Record maps field names to child `Cell` values. It is the primary
//! mechanism for structuring data in ZeninDB. Supports set, replace,
//! and delete (tombstone) operations on individual fields.
//!
//! ## Type registration
//!
//! `RecordType` implements both `Type` and `ContainerType`. It is registered
//! as a `container` type in `register_types!`.

use indexmap::IndexMap;

use crate::{
    codec::{decode_varint, encode_varint},
    types::atom::AtomValue,
    Cell, ContainerType, Hlc, Type, TypeTag, Value,
};

// ---------------------------------------------------------------------------
// RecordValue
// ---------------------------------------------------------------------------

/// A named-field container.
///
/// Fields are stored in an `IndexMap` to preserve insertion order for
/// deterministic encoding and hashing. Tombstones track deleted fields
/// so that older peers cannot accidentally resurrect them.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordValue {
    /// Named child cells. Insertion order is preserved.
    pub fields: IndexMap<String, Cell>,

    /// Deletion tombstones. Maps field name → HLC at which it was deleted.
    /// A field is visible iff `fields[name].hlc > tombstones[name]`.
    pub tombstones: IndexMap<String, Hlc>,
}

impl RecordValue {
    /// Check whether a field is visible.
    ///
    /// A field is visible if it exists in `fields` AND its cell's HLC
    /// strictly beats any tombstone for that field name.
    pub fn is_field_visible(&self, name: &str) -> bool {
        if let Some(cell) = self.fields.get(name) {
            if let Some(&tomb_hlc) = self.tombstones.get(name) {
                return cell.hlc.beats(tomb_hlc);
            }
            return true;
        }
        false
    }

    /// Iterator over visible fields (name, cell) pairs.
    pub fn visible_fields(&self) -> impl Iterator<Item = (&String, &Cell)> {
        self.fields.iter().filter(move |(name, cell)| {
            if let Some(&tomb_hlc) = self.tombstones.get(*name) {
                cell.hlc.beats(tomb_hlc)
            } else {
                true
            }
        })
    }
}

// ---------------------------------------------------------------------------
// RecordOp
// ---------------------------------------------------------------------------

/// Operations on Record values.
#[derive(Debug, Clone)]
pub enum RecordOp {
    /// Set or update a single named field.
    SetField {
        /// Field name.
        name: String,
        /// The cell to set (wraps the value, carries its own HLC).
        value: Cell,
    },
    /// Replace the entire record contents.
    Replace {
        /// The new record value.
        value: RecordValue,
    },
    /// Delete a field by recording a tombstone.
    RemoveField {
        /// Field name to remove.
        name: String,
    },
}

// ---------------------------------------------------------------------------
// RecordSegment
// ---------------------------------------------------------------------------

/// A segment for descending into a Record by field name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordSegment {
    /// The field name to select.
    pub field_name: String,
}

// ---------------------------------------------------------------------------
// RecordError
// ---------------------------------------------------------------------------

/// Errors specific to Record operations.
#[derive(Debug)]
pub enum RecordError {
    /// A `descend_or_create` call asked for a child type we don't know.
    UnknownChildTag(u8),
    /// Decoding failed.
    Decode(String),
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordError::UnknownChildTag(tag) => write!(f, "unknown child type tag: {}", tag),
            RecordError::Decode(msg) => write!(f, "Record decode error: {}", msg),
        }
    }
}

// ---------------------------------------------------------------------------
// RecordType — unit struct implementing Type + ContainerType
// ---------------------------------------------------------------------------

/// The registered type for Record.
///
/// Implements both `Type` and `ContainerType`. All behaviour lives in these
/// trait implementations.
pub struct RecordType;

impl Type for RecordType {
    const TAG: TypeTag = TypeTag::Record;
    const NAME: &'static str = "Record";
    const KEYABLE: bool = false;
    const IS_CONTAINER: bool = true;

    type Value = RecordValue;
    type Op = RecordOp;
    type Error = RecordError;

    fn empty() -> RecordValue {
        RecordValue {
            fields: IndexMap::new(),
            tombstones: IndexMap::new(),
        }
    }

    fn apply_op(state: RecordValue, op: RecordOp, hlc: Hlc) -> Result<RecordValue, RecordError> {
        let mut state = state;
        match op {
            // --- SetField ---
            RecordOp::SetField { name, value } => {
                // If there's a tombstone that beats (or equals) the new
                // value's HLC, the delete happened later — drop the set.
                if let Some(&tomb_hlc) = state.tombstones.get(&name) {
                    if !value.hlc.beats(tomb_hlc) {
                        return Ok(state);
                    }
                    // New value beats the tombstone. Remove it — the field
                    // is alive again.
                    state.tombstones.swap_remove(&name);
                }
                state.fields.insert(name, value);
                Ok(state)
            }

            // --- Replace ---
            // The LWW check was already performed by apply_at_leaf against
            // the cell's HLC. We just accept the new value.
            RecordOp::Replace { value } => Ok(value),

            // --- RemoveField ---
            RecordOp::RemoveField { name } => {
                let existing = state.tombstones.get(&name).copied().unwrap_or(Hlc::ZERO);
                // Take the max tombstone HLC to handle out-of-order delivery.
                let effective = if hlc.beats(existing) { hlc } else { existing };
                state.tombstones.insert(name, effective);
                Ok(state)
            }
        }
    }

    fn is_replacement(op: &RecordOp) -> bool {
        match op {
            RecordOp::SetField { .. } => false,
            RecordOp::Replace { .. } => true,
            RecordOp::RemoveField { .. } => false,
        }
    }

    fn merge(
        local: RecordValue,
        _local_hlc: Hlc,
        remote: RecordValue,
        _remote_hlc: Hlc,
    ) -> Result<RecordValue, RecordError> {
        let mut merged = RecordType::empty();

        // Collect all field names from both sides.
        let mut all_names: Vec<String> = local
            .fields
            .keys()
            .chain(remote.fields.keys())
            .cloned()
            .collect();
        all_names.sort();
        all_names.dedup();

        for field_name in all_names {
            let local_cell = local.fields.get(&field_name);
            let remote_cell = remote.fields.get(&field_name);
            let local_tomb = local.tombstones.get(&field_name);
            let remote_tomb = remote.tombstones.get(&field_name);

            let local_vis =
                local_cell.is_some_and(|c| local_tomb.map_or(true, |t| c.hlc.beats(*t)));
            let remote_vis =
                remote_cell.is_some_and(|c| remote_tomb.map_or(true, |t| c.hlc.beats(*t)));

            match (local_vis, remote_vis) {
                (true, true) => {
                    let merged_cell = crate::merge::merge_cells(
                        local_cell.unwrap().clone(),
                        remote_cell.unwrap().clone(),
                    );
                    merged.fields.insert(field_name.clone(), merged_cell);
                }
                (true, false) => {
                    merged
                        .fields
                        .insert(field_name.clone(), local_cell.unwrap().clone());
                }
                (false, true) => {
                    merged
                        .fields
                        .insert(field_name.clone(), remote_cell.unwrap().clone());
                }
                (false, false) => { /* neither visible */ }
            }

            // Merge tombstones: take the max HLC.
            let max_tomb = match (local_tomb, remote_tomb) {
                (Some(&lt), Some(&rt)) => {
                    if rt.beats(lt) {
                        rt
                    } else {
                        lt
                    }
                }
                (Some(&t), None) | (None, Some(&t)) => t,
                (None, None) => continue,
            };

            // Only store tombstone if the field is not visible AND the
            // tombstone beats any existing cell HLC.
            if !merged
                .fields
                .get(&field_name)
                .is_some_and(|c| c.hlc.beats(max_tomb))
            {
                merged.tombstones.insert(field_name, max_tomb);
            }
        }

        Ok(merged)
    }

    // --- encoding ---

    fn encode_value(value: &RecordValue, out: &mut Vec<u8>) -> Result<(), RecordError> {
        // Field count + fields
        encode_varint(out, value.fields.len() as u64);
        for (name, cell) in &value.fields {
            encode_string(out, name);
            crate::encode::encode_cell(cell, out)
                .map_err(|e| RecordError::Decode(e.to_string()))?;
        }
        // Tombstone count + tombstones
        encode_varint(out, value.tombstones.len() as u64);
        for (name, hlc) in &value.tombstones {
            encode_string(out, name);
            out.extend_from_slice(hlc.as_bytes());
        }
        Ok(())
    }

    fn decode_value(bytes: &[u8]) -> Result<(RecordValue, usize), RecordError> {
        let mut consumed = 0;
        let mut value = RecordType::empty();

        // Fields
        let (field_count, n) = decode_varint(&bytes[consumed..])
            .ok_or_else(|| RecordError::Decode("truncated field count".into()))?;
        consumed += n;
        for _ in 0..field_count {
            let (name, n) = decode_string(&bytes[consumed..])
                .ok_or_else(|| RecordError::Decode("truncated field name".into()))?;
            consumed += n;
            let (cell, n) = crate::encode::decode_cell(&bytes[consumed..])
                .map_err(|e| RecordError::Decode(e.to_string()))?;
            consumed += n;
            value.fields.insert(name, cell);
        }

        // Tombstones
        let (tomb_count, n) = decode_varint(&bytes[consumed..])
            .ok_or_else(|| RecordError::Decode("truncated tombstone count".into()))?;
        consumed += n;
        for _ in 0..tomb_count {
            let (name, n) = decode_string(&bytes[consumed..])
                .ok_or_else(|| RecordError::Decode("truncated tombstone name".into()))?;
            consumed += n;
            if bytes.len() < consumed + 12 {
                return Err(RecordError::Decode("truncated tombstone HLC".into()));
            }
            let mut hlc_bytes = [0u8; 12];
            hlc_bytes.copy_from_slice(&bytes[consumed..consumed + 12]);
            value.tombstones.insert(name, Hlc::from_bytes(hlc_bytes));
            consumed += 12;
        }

        Ok((value, consumed))
    }

    fn encode_op(op: &RecordOp, out: &mut Vec<u8>) -> Result<(), RecordError> {
        match op {
            RecordOp::SetField { name, value } => {
                out.push(0x00);
                encode_string(out, name);
                crate::encode::encode_cell(value, out)
                    .map_err(|e| RecordError::Decode(e.to_string()))?;
            }
            RecordOp::Replace { value } => {
                out.push(0x01);
                Self::encode_value(value, out)?;
            }
            RecordOp::RemoveField { name } => {
                out.push(0x02);
                encode_string(out, name);
            }
        }
        Ok(())
    }

    fn decode_op(bytes: &[u8]) -> Result<(RecordOp, usize), RecordError> {
        if bytes.is_empty() {
            return Err(RecordError::Decode("empty input".into()));
        }
        match bytes[0] {
            0x00 => {
                let (name, n) = decode_string(&bytes[1..])
                    .ok_or_else(|| RecordError::Decode("truncated SetField name".into()))?;
                let (cell, m) = crate::encode::decode_cell(&bytes[1 + n..])
                    .map_err(|e| RecordError::Decode(e.to_string()))?;
                Ok((RecordOp::SetField { name, value: cell }, 1 + n + m))
            }
            0x01 => {
                let (value, n) = Self::decode_value(&bytes[1..])?;
                Ok((RecordOp::Replace { value }, 1 + n))
            }
            0x02 => {
                let (name, n) = decode_string(&bytes[1..])
                    .ok_or_else(|| RecordError::Decode("truncated RemoveField name".into()))?;
                Ok((RecordOp::RemoveField { name }, 1 + n))
            }
            tag => Err(RecordError::Decode(format!(
                "unknown RecordOp tag: {}",
                tag
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// ContainerType impl
// ---------------------------------------------------------------------------

impl ContainerType for RecordType {
    type Segment = RecordSegment;

    fn descend<'a>(
        value: &'a RecordValue,
        segment: &RecordSegment,
    ) -> Result<Option<&'a Cell>, RecordError> {
        // Check tombstone first — if the field is tombstoned with an HLC
        // that beats (or equals) the cell's HLC, treat it as absent.
        if let Some(&tomb_hlc) = value.tombstones.get(&segment.field_name) {
            if let Some(cell) = value.fields.get(&segment.field_name) {
                if !cell.hlc.beats(tomb_hlc) {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }
        Ok(value.fields.get(&segment.field_name))
    }

    fn descend_or_create<'a>(
        value: &'a mut RecordValue,
        segment: &RecordSegment,
        child_tag: TypeTag,
    ) -> Result<&'a mut Cell, RecordError> {
        // If the field is tombstoned but the incoming op beats the tombstone,
        // the field can be re-created. We just remove any existing entry so
        // the dummy creation path below triggers.
        if let Some(&tomb_hlc) = value.tombstones.get(&segment.field_name) {
            if let Some(cell) = value.fields.get(&segment.field_name) {
                if !cell.hlc.beats(tomb_hlc) {
                    value.fields.swap_remove(&segment.field_name);
                }
            }
        }

        if !value.fields.contains_key(&segment.field_name) {
            // Create a dummy cell of the expected child type.
            let dummy_val = match child_tag {
                TypeTag::Atom => Value::Atom(AtomValue::Null),
                TypeTag::Record => Value::Record(RecordType::empty()),
                // Future types will add variants to TypeTag, making this reachable:
                #[allow(unreachable_patterns)]
                other => return Err(RecordError::UnknownChildTag(other as u8)),
            };
            value
                .fields
                .insert(segment.field_name.clone(), Cell::dummy(dummy_val));
        }

        // Safety: we just ensured the key exists.
        Ok(value.fields.get_mut(&segment.field_name).unwrap())
    }

    fn encode_segment(segment: &RecordSegment, out: &mut Vec<u8>) -> Result<(), RecordError> {
        encode_string(out, &segment.field_name);
        Ok(())
    }

    fn decode_segment(bytes: &[u8]) -> Result<(RecordSegment, usize), RecordError> {
        let (name, n) =
            decode_string(bytes).ok_or_else(|| RecordError::Decode("truncated segment".into()))?;
        Ok((RecordSegment { field_name: name }, n))
    }
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn encode_string(out: &mut Vec<u8>, s: &str) {
    encode_varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

fn decode_string(bytes: &[u8]) -> Option<(String, usize)> {
    let (len, n) = decode_varint(bytes)?;
    let end = n + len as usize;
    if bytes.len() < end {
        return None;
    }
    let s = String::from_utf8(bytes[n..end].to_vec()).ok()?;
    Some((s, end))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, 1).unwrap()
    }

    #[test]
    fn record_apply_set_field() {
        let state = RecordType::empty();
        let op = RecordOp::SetField {
            name: "x".into(),
            value: Cell::new(Value::Atom(AtomValue::Int(42)), hlc(100), None),
        };
        let result = RecordType::apply_op(state, op, hlc(100)).unwrap();
        assert!(result.is_field_visible("x"));
        assert_eq!(
            result.fields.get("x").unwrap().value.type_tag(),
            TypeTag::Atom
        );
    }

    #[test]
    fn record_apply_remove_field() {
        let mut state = RecordType::empty();
        state.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(42)), hlc(100), None),
        );

        let op = RecordOp::RemoveField { name: "x".into() };
        let result = RecordType::apply_op(state, op, hlc(200)).unwrap();

        // The field data is still there but a tombstone exists.
        assert!(result.fields.contains_key("x"));
        assert!(result.tombstones.contains_key("x"));
        // It should not be visible — tombstone (200) beats cell HLC (100).
        assert!(!result.is_field_visible("x"));
    }

    #[test]
    fn record_apply_set_beats_tombstone() {
        let mut state = RecordType::empty();
        state.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None),
        );
        state.tombstones.insert("x".into(), hlc(200)); // tombstone wins

        // Now set with a newer HLC — should resurrect.
        let op = RecordOp::SetField {
            name: "x".into(),
            value: Cell::new(Value::Atom(AtomValue::Int(2)), hlc(300), None),
        };
        let result = RecordType::apply_op(state, op, hlc(300)).unwrap();
        assert!(result.is_field_visible("x"));
        assert!(!result.tombstones.contains_key("x")); // tombstone removed
    }

    #[test]
    fn record_merge_both_visible() {
        let mut local = RecordType::empty();
        local.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None),
        );
        let mut remote = RecordType::empty();
        remote.fields.insert(
            "x".into(),
            Cell::new(Value::Atom(AtomValue::Int(2)), hlc(200), None),
        );

        let merged = RecordType::merge(local, hlc(100), remote, hlc(200)).unwrap();
        // Remote wins by HLC.
        assert!(merged.is_field_visible("x"));
    }

    #[test]
    fn record_is_replacement() {
        assert!(!RecordType::is_replacement(&RecordOp::SetField {
            name: "x".into(),
            value: Cell::dummy(Value::Atom(AtomValue::Null)),
        }));
        assert!(RecordType::is_replacement(&RecordOp::Replace {
            value: RecordType::empty(),
        }));
        assert!(!RecordType::is_replacement(&RecordOp::RemoveField {
            name: "x".into(),
        }));
    }

    #[test]
    fn record_encode_decode_roundtrip() {
        let mut val = RecordType::empty();
        val.fields.insert(
            "name".into(),
            Cell::new(
                Value::Atom(AtomValue::String("Alice".into())),
                hlc(100),
                None,
            ),
        );
        val.fields.insert(
            "age".into(),
            Cell::new(Value::Atom(AtomValue::Int(30)), hlc(200), None),
        );
        val.tombstones.insert("deleted".into(), hlc(300));

        let mut buf = Vec::new();
        RecordType::encode_value(&val, &mut buf).unwrap();
        let (decoded, consumed) = RecordType::decode_value(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.fields.len(), 2);
        assert_eq!(decoded.tombstones.len(), 1);
    }
}
