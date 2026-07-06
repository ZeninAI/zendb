//! Rhai engine setup with registered types and functions.

use rhai::Engine;

use super::api::{create_db_module, register_table_type, ScriptContext};
use super::types::{ScriptCell, ScriptChange, ScriptValue};

/// Create a new Rhai engine configured for zendb operator scripts.
pub fn create_engine() -> Engine {
    let mut engine = Engine::new();

    // Disable potentially dangerous features for sandboxed execution
    engine.set_max_expr_depths(64, 32);
    engine.set_max_string_size(1024 * 1024); // 1MB max string
    engine.set_max_array_size(10000);
    engine.set_max_map_size(10000);
    engine.set_max_operations(1_000_000);

    // Register custom types
    register_types(&mut engine);

    // Register the db module
    let db_module = create_db_module();
    engine.register_static_module("db", db_module.into());

    // Register helper functions
    register_helpers(&mut engine);

    engine
}

/// Register all custom types with the engine.
fn register_types(engine: &mut Engine) {
    // Register ScriptChange type
    engine
        .register_type_with_name::<ScriptChange>("Change")
        .register_fn("table", ScriptChange::table)
        .register_fn("key", ScriptChange::key)
        .register_fn("key_string", ScriptChange::key_string)
        .register_fn("previous", ScriptChange::previous)
        .register_fn("current", ScriptChange::current)
        .register_fn("is_insert", ScriptChange::is_insert)
        .register_fn("is_update", ScriptChange::is_update)
        .register_fn("is_delete", ScriptChange::is_delete)
        .register_fn("timestamp", ScriptChange::timestamp);

    // Register ScriptCell type
    engine
        .register_type_with_name::<ScriptCell>("Cell")
        .register_fn("value", ScriptCell::value)
        .register_fn("is_tombstone", ScriptCell::is_tombstone)
        .register_fn("type_name", ScriptCell::type_name)
        .register_fn("timestamp", ScriptCell::timestamp)
        .register_fn("is_synced", ScriptCell::is_synced);

    // Register ScriptValue type
    engine
        .register_type_with_name::<ScriptValue>("Value")
        .register_fn("type_name", ScriptValue::type_name)
        .register_fn("as_string", ScriptValue::as_string)
        .register_fn("as_int", ScriptValue::as_int)
        .register_fn("as_bool", ScriptValue::as_bool)
        .register_fn("as_counter", ScriptValue::as_counter)
        .register_fn("is_record", ScriptValue::is_record)
        .register_fn("is_list", ScriptValue::is_list)
        .register_fn("get", ScriptValue::get)
        .register_fn("fields", ScriptValue::fields);

    // Register ScriptContext type (internal use)
    engine.register_type_with_name::<ScriptContext>("ScriptContext");

    // Register table type
    register_table_type(engine);
}

/// Register helper functions.
fn register_helpers(engine: &mut Engine) {
    // String helpers
    engine.register_fn("to_string", |x: i64| x.to_string());
    engine.register_fn("to_string", |x: bool| x.to_string());
    engine.register_fn("to_string", |x: f64| x.to_string());

    // Type checking helpers
    engine.register_fn("is_string", |x: rhai::Dynamic| x.is_string());
    engine.register_fn("is_int", |x: rhai::Dynamic| x.is_int());
    engine.register_fn("is_bool", |x: rhai::Dynamic| x.is_bool());
    engine.register_fn("is_array", |x: rhai::Dynamic| x.is_array());
    engine.register_fn("is_map", |x: rhai::Dynamic| x.is_map());
    engine.register_fn("is_unit", |x: rhai::Dynamic| x.is_unit());

    // Print for debugging (redirects to log)
    engine.register_fn("print", |s: &str| {
        log::debug!("[Rhai print] {}", s);
    });
    engine.register_fn("print", |x: i64| {
        log::debug!("[Rhai print] {}", x);
    });
    engine.register_fn("print", |x: bool| {
        log::debug!("[Rhai print] {}", x);
    });
    engine.register_fn("print", |x: rhai::Dynamic| {
        log::debug!("[Rhai print] {:?}", x);
    });

    // Debug function
    engine.register_fn("debug", |x: rhai::Dynamic| -> String {
        format!("{:?}", x)
    });
}

/// Names of lifecycle handler functions that scripts can define.
pub mod handlers {
    /// Called when the operator is created.
    pub const ON_CREATE: &str = "on_create";
    /// Called for each batch of changes.
    pub const ON_PROCESS: &str = "on_process";
    /// Called when an input table is opened.
    pub const ON_INPUT_OPENED: &str = "on_input_opened";
    /// Called when an input table is closed.
    pub const ON_INPUT_CLOSED: &str = "on_input_closed";
    /// Called when a timer fires.
    pub const ON_TIMER: &str = "on_timer";
    /// Called when the operator is tearing down.
    pub const ON_TEARDOWN: &str = "on_teardown";
}
