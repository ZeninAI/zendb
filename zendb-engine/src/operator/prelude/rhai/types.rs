//! Rhai-compatible type wrappers for zendb types.
//!
//! These types wrap zendb types and expose them to Rhai scripts with
//! appropriate accessor methods.

use rhai::{Array, Dynamic, EvalAltResult, Map, Position};
use zendb_types::{Cell, PrimaryKey, Value};

use crate::Change;

/// Wrapper around [`Change`] for Rhai scripts.
#[derive(Debug, Clone)]
pub struct ScriptChange {
    pub(crate) inner: Change,
}

impl ScriptChange {
    pub fn new(change: Change) -> Self {
        Self { inner: change }
    }

    /// Get the source table name.
    pub fn table(&mut self) -> String {
        self.inner.event.table_id.clone()
    }

    /// Get the primary key as a Rhai value.
    pub fn key(&mut self) -> Dynamic {
        primary_key_to_dynamic(&self.inner.event.primary_key)
    }

    /// Get the primary key as a string representation.
    pub fn key_string(&mut self) -> String {
        format!("{:?}", self.inner.event.primary_key)
    }

    /// Get the previous cell value (before the change).
    pub fn previous(&mut self) -> Dynamic {
        match &self.inner.previous {
            Some(cell) => Dynamic::from(ScriptCell::new(cell.clone())),
            None => Dynamic::UNIT,
        }
    }

    /// Get the current cell value (after the change).
    pub fn current(&mut self) -> Dynamic {
        match &self.inner.current {
            Some(cell) => Dynamic::from(ScriptCell::new(cell.clone())),
            None => Dynamic::UNIT,
        }
    }

    /// Check if this is an insert (no previous value, has current value).
    pub fn is_insert(&mut self) -> bool {
        self.inner.previous.is_none()
            && self
                .inner
                .current
                .as_ref()
                .is_some_and(|c| !c.is_tombstone())
    }

    /// Check if this is an update (has both previous and current values).
    pub fn is_update(&mut self) -> bool {
        self.inner
            .previous
            .as_ref()
            .is_some_and(|c| !c.is_tombstone())
            && self
                .inner
                .current
                .as_ref()
                .is_some_and(|c| !c.is_tombstone())
    }

    /// Check if this is a delete (has previous value, current is tombstone or none).
    pub fn is_delete(&mut self) -> bool {
        self.inner
            .previous
            .as_ref()
            .is_some_and(|c| !c.is_tombstone())
            && self
                .inner
                .current
                .as_ref()
                .map_or(true, |c| c.is_tombstone())
    }

    /// Get the HLC timestamp of the event.
    pub fn timestamp(&mut self) -> i64 {
        self.inner.event.hlc.physical_ms() as i64
    }
}

/// Wrapper around [`Cell`] for Rhai scripts.
#[derive(Debug, Clone)]
pub struct ScriptCell {
    pub(crate) inner: Cell,
}

impl ScriptCell {
    pub fn new(cell: Cell) -> Self {
        Self { inner: cell }
    }

    /// Get the value contained in this cell.
    pub fn value(&mut self) -> Dynamic {
        match &self.inner.value {
            Some(v) => ScriptValue::new(v.clone()).to_dynamic(),
            None => Dynamic::UNIT,
        }
    }

    /// Check if this cell is a tombstone (deleted).
    pub fn is_tombstone(&mut self) -> bool {
        self.inner.is_tombstone()
    }

    /// Get the type name of the contained value.
    pub fn type_name(&mut self) -> String {
        match self.inner.type_tag() {
            Some(tag) => tag.name().to_owned(),
            None => "tombstone".to_owned(),
        }
    }

    /// Get the HLC timestamp.
    pub fn timestamp(&mut self) -> i64 {
        self.inner.hlc.physical_ms() as i64
    }

    /// Check if this cell is synced.
    pub fn is_synced(&mut self) -> bool {
        self.inner.sync.unwrap_or(true)
    }
}

/// Wrapper around [`Value`] for Rhai scripts.
#[derive(Debug, Clone)]
pub struct ScriptValue {
    pub(crate) inner: Value,
}

impl ScriptValue {
    pub fn new(value: Value) -> Self {
        Self { inner: value }
    }

    /// Convert this value to a Rhai Dynamic.
    pub fn to_dynamic(&self) -> Dynamic {
        value_to_dynamic(&self.inner)
    }

    /// Get the type name.
    pub fn type_name(&mut self) -> String {
        self.inner.type_tag().name().to_owned()
    }

    /// Try to get as a string.
    pub fn as_string(&mut self) -> Result<String, Box<EvalAltResult>> {
        match &self.inner {
            Value::String(s) => Ok(s.to_string()),
            _ => Err(type_error("String", &self.inner)),
        }
    }

    /// Try to get as an integer.
    pub fn as_int(&mut self) -> Result<i64, Box<EvalAltResult>> {
        match &self.inner {
            Value::Int(i) => Ok(*i),
            _ => Err(type_error("Int", &self.inner)),
        }
    }

    /// Try to get as a boolean.
    pub fn as_bool(&mut self) -> Result<bool, Box<EvalAltResult>> {
        match &self.inner {
            Value::Bool(b) => Ok(*b),
            _ => Err(type_error("Bool", &self.inner)),
        }
    }

    /// Try to get as a counter value.
    pub fn as_counter(&mut self) -> Result<i64, Box<EvalAltResult>> {
        match &self.inner {
            Value::Counter(c) => Ok(c.value() as i64),
            _ => Err(type_error("Counter", &self.inner)),
        }
    }

    /// Check if value is a record.
    pub fn is_record(&mut self) -> bool {
        matches!(self.inner, Value::Record(_))
    }

    /// Check if value is a list.
    pub fn is_list(&mut self) -> bool {
        matches!(self.inner, Value::List(_))
    }

    /// Get a field from a record.
    pub fn get(&mut self, field: &str) -> Dynamic {
        match &self.inner {
            Value::Record(r) => match r.get(field) {
                Some(cell) => {
                    if cell.is_tombstone() {
                        Dynamic::UNIT
                    } else {
                        match &cell.value {
                            Some(v) => ScriptValue::new(v.clone()).to_dynamic(),
                            None => Dynamic::UNIT,
                        }
                    }
                }
                None => Dynamic::UNIT,
            },
            _ => Dynamic::UNIT,
        }
    }

    /// Get all field names from a record.
    pub fn fields(&mut self) -> Array {
        match &self.inner {
            Value::Record(r) => r
                .fields()
                .filter(|(_, cell)| !cell.is_tombstone())
                .map(|(k, _)| Dynamic::from(k.to_string()))
                .collect(),
            _ => Array::new(),
        }
    }
}

/// Convert a [`PrimaryKey`] to a Rhai [`Dynamic`].
pub fn primary_key_to_dynamic(pk: &PrimaryKey) -> Dynamic {
    match pk {
        PrimaryKey::String(s) => Dynamic::from(s.clone()),
        PrimaryKey::Int(i) => Dynamic::from(*i),
        PrimaryKey::Bool(b) => Dynamic::from(*b),
        PrimaryKey::Timestamp(t) => Dynamic::from(*t as i64),
        PrimaryKey::Blob(b) => {
            let bytes: Vec<Dynamic> = b.as_slice().iter().map(|&byte| Dynamic::from(byte as i64)).collect();
            Dynamic::from(bytes)
        }
    }
}

/// Convert a Rhai [`Dynamic`] to a [`PrimaryKey`].
pub fn dynamic_to_primary_key(d: &Dynamic) -> Result<PrimaryKey, Box<EvalAltResult>> {
    if let Some(s) = d.clone().try_cast::<String>() {
        Ok(PrimaryKey::String(s))
    } else if let Some(i) = d.clone().try_cast::<i64>() {
        Ok(PrimaryKey::Int(i))
    } else {
        Err(Box::new(EvalAltResult::ErrorMismatchDataType(
            "String or Int".into(),
            d.type_name().into(),
            Position::NONE,
        )))
    }
}

/// Convert a [`Value`] to a Rhai [`Dynamic`].
pub fn value_to_dynamic(value: &Value) -> Dynamic {
    match value {
        Value::String(s) => Dynamic::from(s.to_string()),
        Value::Int(i) => Dynamic::from(*i),
        Value::Bool(b) => Dynamic::from(*b),
        Value::Counter(c) => Dynamic::from(c.value() as i64),
        Value::Timestamp(t) => Dynamic::from(*t as i64),
        Value::Blob(b) => {
            let bytes: Vec<Dynamic> = b.as_slice().iter().map(|&byte| Dynamic::from(byte as i64)).collect();
            Dynamic::from(bytes)
        }
        Value::Record(r) => {
            let mut map = Map::new();
            for (key, cell) in r.fields() {
                if !cell.is_tombstone() {
                    if let Some(v) = &cell.value {
                        map.insert(key.into(), value_to_dynamic(v));
                    }
                }
            }
            Dynamic::from(map)
        }
        Value::List(l) => {
            let arr: Array = l
                .cells()
                .filter(|cell| !cell.is_tombstone())
                .filter_map(|cell| cell.value.as_ref())
                .map(value_to_dynamic)
                .collect();
            Dynamic::from(arr)
        }
        Value::Set(s) => {
            let arr: Array = s.keys().map(primary_key_to_dynamic).collect();
            Dynamic::from(arr)
        }
        Value::OrSet(s) => {
            let arr: Array = s.keys().map(primary_key_to_dynamic).collect();
            Dynamic::from(arr)
        }
        Value::MvRegister(r) => {
            let values: Array = r.values().into_iter().map(value_to_dynamic).collect();
            if values.len() == 1 {
                values.into_iter().next().unwrap_or(Dynamic::UNIT)
            } else {
                Dynamic::from(values)
            }
        }
        Value::Text(t) => Dynamic::from(t.string()),
        Value::PriorityQueue(_pq) => {
            // Priority queue is complex - return type name for now
            Dynamic::from("PriorityQueue")
        }
    }
}

/// Convert a Rhai [`Dynamic`] to a [`Value`].
pub fn dynamic_to_value(d: &Dynamic) -> Result<Value, Box<EvalAltResult>> {
    if d.is_string() {
        Ok(Value::String(d.clone_cast::<String>().into()))
    } else if d.is_int() {
        Ok(Value::Int(d.clone_cast::<i64>()))
    } else if d.is_bool() {
        Ok(Value::Bool(d.clone_cast::<bool>()))
    } else if d.is_unit() {
        // Unit maps to empty string as a placeholder
        Ok(Value::String(zendb_types::String::default()))
    } else if d.is_map() {
        // For maps, we need to create events for each field rather than a Record
        // For simplicity, convert to a string representation
        let map = d.clone_cast::<Map>();
        let json = format!("{:?}", map);
        Ok(Value::String(json.into()))
    } else if d.is_array() {
        // For arrays, convert to string representation
        let arr = d.clone_cast::<Array>();
        let json = format!("{:?}", arr);
        Ok(Value::String(json.into()))
    } else {
        Err(Box::new(EvalAltResult::ErrorMismatchDataType(
            "convertible value".into(),
            d.type_name().into(),
            Position::NONE,
        )))
    }
}

fn type_error(expected: &str, actual: &Value) -> Box<EvalAltResult> {
    Box::new(EvalAltResult::ErrorMismatchDataType(
        expected.into(),
        actual.type_tag().name().into(),
        Position::NONE,
    ))
}
