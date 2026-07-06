//! Rhai scripting operator for ZenDB.
//!
//! This module provides a runtime-configurable operator powered by the Rhai
//! scripting language. Users can write scripts that respond to database
//! changes without needing to compile Rust code.
//!
//! # Overview
//!
//! The Rhai operator executes user-provided scripts in response to operator
//! lifecycle events. Scripts can:
//!
//! - Process incoming changes from subscribed tables
//! - Emit events to output tables
//! - Schedule timers for delayed processing
//! - Maintain state between invocations
//!
//! # Script API
//!
//! ## Lifecycle Handlers
//!
//! Scripts define handler functions that are called at appropriate times:
//!
//! - `fn on_create()` - Called once when the operator starts
//! - `fn on_process(changes)` - Called for each batch of changes
//! - `fn on_input_opened(table)` - Called when a subscribed table appears
//! - `fn on_input_closed(table)` - Called when a subscribed table disappears
//! - `fn on_timer(payload)` - Called when a scheduled timer fires
//! - `fn on_teardown(reason)` - Called when the operator stops
//!
//! All handlers are optional. If not defined, default behavior (continue) is used.
//!
//! ## Database Module (`db::`)
//!
//! The `db` module provides database operations:
//!
//! - `db::emit(table, key, value)` - Emit an event to a table
//! - `db::delete(table, key)` - Delete a key from a table
//! - `db::set_timer(delay_ms, payload)` - Schedule a timer
//! - `db::log(message)` - Log a debug message
//!
//! ## Change Type
//!
//! The `Change` type represents a database change:
//!
//! - `change.table()` - Source table name
//! - `change.key()` - Primary key
//! - `change.previous()` - Previous cell value (if any)
//! - `change.current()` - Current cell value (if any)
//! - `change.is_insert()` - True if this is a new entry
//! - `change.is_update()` - True if this modifies an existing entry
//! - `change.is_delete()` - True if this removes an entry
//! - `change.timestamp()` - Event timestamp (milliseconds)
//!
//! ## Cell Type
//!
//! The `Cell` type wraps a database value:
//!
//! - `cell.value()` - Get the contained value
//! - `cell.is_tombstone()` - Check if deleted
//! - `cell.type_name()` - Get the type name
//! - `cell.timestamp()` - Get the HLC timestamp
//!
//! ## Value Conversion
//!
//! ZenDB values are automatically converted to Rhai types:
//!
//! - `String` → Rhai string
//! - `Int` → Rhai integer
//! - `Bool` → Rhai boolean
//! - `Record` → Rhai map
//! - `List` → Rhai array
//! - `Set`/`OrSet` → Rhai array
//!
//! # Example
//!
//! ```rhai
//! // Count processed entries and forward to output table
//! fn on_create() {
//!     this.count = 0;
//! }
//!
//! fn on_process(changes) {
//!     for change in changes {
//!         if change.is_insert() || change.is_update() {
//!             this.count += 1;
//!             
//!             // Get the value and emit to output
//!             let cell = change.current();
//!             if !cell.is_tombstone() {
//!                 db::emit("output", change.key(), cell.value());
//!             }
//!         }
//!     }
//!     
//!     // Log progress
//!     db::log(`Processed ${this.count} entries`);
//! }
//!
//! fn on_timer(payload) {
//!     db::log(`Timer fired: ${payload}`);
//! }
//! ```
//!
//! # Safety
//!
//! The Rhai engine is configured with safety limits:
//!
//! - Maximum expression depth: 64
//! - Maximum string size: 1MB
//! - Maximum array size: 10,000 elements
//! - Maximum map size: 10,000 entries
//! - Maximum operations: 1,000,000 per handler call

mod api;
mod config;
mod engine;
mod operator;
mod types;

pub use config::RhaiOperatorConfig;
pub use operator::{RhaiFacet, RhaiOperator};

// Re-export types for advanced usage
pub use api::{PendingEvent, PendingTimer, PendingWrites, ScriptContext, ScriptTable};
pub use types::{ScriptCell, ScriptChange, ScriptValue};
