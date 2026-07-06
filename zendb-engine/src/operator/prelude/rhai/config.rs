//! Configuration for the Rhai scripting operator.

use bincode::{Decode, Encode};

/// Configuration for the Rhai scripting operator.
///
/// The script should define handler functions that match the operator lifecycle:
///
/// - `fn on_create()` - Called once when the operator is created
/// - `fn on_process(changes)` - Called for each batch of changes
/// - `fn on_input_opened(table)` - Called when a subscribed table becomes available
/// - `fn on_input_closed(table)` - Called when a subscribed table is removed
/// - `fn on_timer(payload)` - Called when a timer fires
/// - `fn on_teardown(reason)` - Called when the operator is stopping
///
/// All functions are optional. If a handler is not defined, the default
/// behavior (continue) is used.
///
/// # Example Script
///
/// ```rhai
/// // Initialize operator state
/// fn on_create() {
///     this.processed_count = 0;
/// }
///
/// // Process incoming changes
/// fn on_process(changes) {
///     for change in changes {
///         if change.is_insert() || change.is_update() {
///             this.processed_count += 1;
///             // Emit transformed data to output table
///             db::emit("output", change.key, change.current.value);
///         }
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RhaiOperatorConfig {
    /// The Rhai script source code.
    pub script: String,

    /// Optional state path for persisting operator state between restarts.
    /// If not set, state is kept in memory only.
    pub state_path: Option<String>,
}

impl Default for RhaiOperatorConfig {
    fn default() -> Self {
        Self {
            script: String::new(),
            state_path: None,
        }
    }
}

impl RhaiOperatorConfig {
    /// Create a new config with the given script.
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            script: script.into(),
            state_path: None,
        }
    }

    /// Set the state persistence path.
    pub fn with_state_path(mut self, path: impl Into<String>) -> Self {
        self.state_path = Some(path.into());
        self
    }
}
