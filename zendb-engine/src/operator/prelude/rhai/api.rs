//! Workspace API module for Rhai scripts.
//!
//! This module provides the `db` namespace with functions to interact with
//! the local workspace from within Rhai scripts.

use std::sync::Arc;

use parking_lot::RwLock;
use rhai::{Array, Dynamic, EvalAltResult, FuncRegistration, Module, Position};
use zendb_types::PrimaryKey;

use super::config::RhaiWriteMode;
use super::types::{
    dynamic_to_primary_key, dynamic_to_value, primary_key_to_dynamic, value_to_dynamic,
};
use zendb_types::{Cell, Value};

/// Pending effect requests collected during script execution. A host-side
/// effect gate must authorize them before durable application.
#[derive(Debug, Clone, Default)]
pub struct PendingWrites {
    /// Events to emit after script completes.
    pub events: Vec<PendingEvent>,
    /// Timers to schedule after script completes.
    pub timers: Vec<PendingTimer>,
}

/// A pending event to be emitted.
#[derive(Debug, Clone)]
pub struct PendingEvent {
    pub table: String,
    pub key: PrimaryKey,
    pub value: Option<Value>,
    /// Whether the script requested a shared event. The host still applies
    /// the operator's effective output policy before accepting it.
    pub sync: bool,
}

/// A pending timer to be scheduled.
#[derive(Debug, Clone)]
pub struct PendingTimer {
    pub delay_ms: u64,
    pub payload: Vec<u8>,
}

/// Shared context for workspace operations during script execution.
#[derive(Clone)]
pub struct ScriptContext {
    /// Pending writes to apply after script execution.
    pub pending: Arc<RwLock<PendingWrites>>,
    /// The operator name for generating HLCs.
    pub operator_name: String,
    pub write_mode: RhaiWriteMode,
}

impl ScriptContext {
    pub fn new(operator_name: String, write_mode: RhaiWriteMode) -> Self {
        Self {
            pending: Arc::new(RwLock::new(PendingWrites::default())),
            operator_name,
            write_mode,
        }
    }

    /// Take pending writes, resetting the buffer.
    pub fn take_pending(&self) -> PendingWrites {
        std::mem::take(&mut *self.pending.write())
    }

    /// Emit an event to a table.
    pub fn emit(&self, table: String, key: PrimaryKey, value: Value) {
        self.pending.write().events.push(PendingEvent {
            table,
            key,
            value: Some(value),
            sync: matches!(self.write_mode, RhaiWriteMode::SharedAllowed),
        });
    }

    /// Delete a key from a table.
    pub fn delete(&self, table: String, key: PrimaryKey) {
        self.pending.write().events.push(PendingEvent {
            table,
            key,
            value: None,
            sync: matches!(self.write_mode, RhaiWriteMode::SharedAllowed),
        });
    }

    /// Schedule a timer.
    pub fn set_timer(&self, delay_ms: u64, payload: String) {
        self.pending.write().timers.push(PendingTimer {
            delay_ms,
            payload: payload.into_bytes(),
        });
    }
}

/// Create the `db` module with workspace access functions.
///
/// NOTE: These functions need access to a ScriptContext stored in the scope
/// as `__db_ctx`. The operator must set this before calling any handlers.
pub fn create_db_module() -> Module {
    let mut module = Module::new();

    // db::emit(table, key, value) - Queue an event to emit
    // Note: We use raw function registration to access the scope
    FuncRegistration::new("emit")
        .in_internal_namespace()
        .set_into_module(&mut module, emit_fn);

    // db::delete(table, key) - Queue a delete event
    FuncRegistration::new("delete")
        .in_internal_namespace()
        .set_into_module(&mut module, delete_fn);

    // db::set_timer(delay_ms, payload) - Schedule a timer
    FuncRegistration::new("set_timer")
        .in_internal_namespace()
        .set_into_module(&mut module, set_timer_fn);

    // db::log(message) - Log a message for debugging
    FuncRegistration::new("log")
        .in_internal_namespace()
        .set_into_module(&mut module, log_fn);

    module
}

/// Emit an event to a table.
fn emit_fn(
    ctx: rhai::NativeCallContext,
    table: String,
    key: Dynamic,
    value: Dynamic,
) -> Result<(), Box<EvalAltResult>> {
    let script_ctx = get_script_context(&ctx)?;
    let pk = dynamic_to_primary_key(&key)?;
    let val = dynamic_to_value(&value)?;
    script_ctx.emit(table, pk, val);
    Ok(())
}

/// Delete a key from a table.
fn delete_fn(
    ctx: rhai::NativeCallContext,
    table: String,
    key: Dynamic,
) -> Result<(), Box<EvalAltResult>> {
    let script_ctx = get_script_context(&ctx)?;
    let pk = dynamic_to_primary_key(&key)?;
    script_ctx.delete(table, pk);
    Ok(())
}

/// Schedule a timer.
fn set_timer_fn(
    ctx: rhai::NativeCallContext,
    delay_ms: i64,
    payload: String,
) -> Result<(), Box<EvalAltResult>> {
    let script_ctx = get_script_context(&ctx)?;

    if delay_ms < 0 {
        return Err(Box::new(EvalAltResult::ErrorArithmetic(
            "Timer delay cannot be negative".into(),
            Position::NONE,
        )));
    }

    script_ctx.set_timer(delay_ms as u64, payload);
    Ok(())
}

/// Log a debug message.
fn log_fn(_ctx: rhai::NativeCallContext, message: String) -> Result<(), Box<EvalAltResult>> {
    log::info!("[Rhai] {}", message);
    Ok(())
}

/// Extract the script context from the native call context.
fn get_script_context(ctx: &rhai::NativeCallContext) -> Result<ScriptContext, Box<EvalAltResult>> {
    ctx.tag()
        .and_then(|tag| tag.clone().try_cast::<ScriptContext>())
        .ok_or_else(|| {
            Box::new(EvalAltResult::ErrorSystem(
                "Script context not available".into(),
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "No script context - db:: functions must be called from within a handler",
                )),
            ))
        })
}

/// Table accessor for read operations within scripts.
#[derive(Clone)]
pub struct ScriptTable {
    name: String,
    // We store the table data as a snapshot for read operations
    entries: Arc<RwLock<Vec<(PrimaryKey, Cell)>>>,
}

impl ScriptTable {
    /// Create a new empty script table.
    pub fn new(name: String) -> Self {
        Self {
            name,
            entries: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a script table with pre-loaded entries.
    pub fn with_entries(name: String, entries: Vec<(PrimaryKey, Cell)>) -> Self {
        Self {
            name,
            entries: Arc::new(RwLock::new(entries)),
        }
    }

    /// Get the table name.
    pub fn name(&mut self) -> String {
        self.name.clone()
    }

    /// Get a value by key.
    pub fn get(&mut self, key: Dynamic) -> Result<Dynamic, Box<EvalAltResult>> {
        let pk = dynamic_to_primary_key(&key)?;
        let entries = self.entries.read();

        for (k, cell) in entries.iter() {
            if k == &pk {
                if cell.is_tombstone() {
                    return Ok(Dynamic::UNIT);
                }
                return Ok(match &cell.value {
                    Some(v) => value_to_dynamic(v),
                    None => Dynamic::UNIT,
                });
            }
        }

        Ok(Dynamic::UNIT)
    }

    /// Check if a key exists.
    pub fn has(&mut self, key: Dynamic) -> Result<bool, Box<EvalAltResult>> {
        let pk = dynamic_to_primary_key(&key)?;
        let entries = self.entries.read();

        for (k, cell) in entries.iter() {
            if k == &pk && !cell.is_tombstone() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get all keys.
    pub fn keys(&mut self) -> Array {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|(_, cell)| !cell.is_tombstone())
            .map(|(k, _)| primary_key_to_dynamic(k))
            .collect()
    }

    /// Get the number of entries.
    pub fn len(&mut self) -> i64 {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|(_, cell)| !cell.is_tombstone())
            .count() as i64
    }
}

/// Register table accessor methods.
pub fn register_table_type(engine: &mut rhai::Engine) {
    engine
        .register_type_with_name::<ScriptTable>("Table")
        .register_fn("name", ScriptTable::name)
        .register_fn("get", ScriptTable::get)
        .register_fn("has", ScriptTable::has)
        .register_fn("keys", ScriptTable::keys)
        .register_fn("len", ScriptTable::len);
}
