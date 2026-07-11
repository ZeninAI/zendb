//! Configuration for the Rhai scripting operator.

use bincode::{Decode, Encode};
use zendb_types::CapabilityId;

/// Whether a script may request shared writes. The operator spec and policy
/// evaluator remain authoritative; this is only the script's declared mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum RhaiWriteMode {
    LocalOnly,
    SharedAllowed,
}

/// Resource limits applied to one script invocation.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RhaiExecutionPolicy {
    pub max_expr_depth: u32,
    pub max_call_depth: u32,
    pub max_string_bytes: u32,
    pub max_array_len: u32,
    pub max_map_len: u32,
    pub max_operations: u64,
    pub requested_capabilities: Vec<CapabilityId>,
    pub write_mode: RhaiWriteMode,
}

impl Default for RhaiExecutionPolicy {
    fn default() -> Self {
        Self {
            max_expr_depth: 64,
            max_call_depth: 32,
            max_string_bytes: 1024 * 1024,
            max_array_len: 10_000,
            max_map_len: 10_000,
            max_operations: 1_000_000,
            requested_capabilities: Vec::new(),
            write_mode: RhaiWriteMode::LocalOnly,
        }
    }
}

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
    /// Execution limits and declared host capability/write requirements.
    pub policy: RhaiExecutionPolicy,
}

impl Default for RhaiOperatorConfig {
    fn default() -> Self {
        Self {
            script: String::new(),
            policy: RhaiExecutionPolicy::default(),
        }
    }
}

impl RhaiOperatorConfig {
    /// Create a new config with the given script.
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            script: script.into(),
            policy: RhaiExecutionPolicy::default(),
        }
    }

    /// Set the execution policy declared by this script.
    pub fn with_policy(mut self, policy: RhaiExecutionPolicy) -> Self {
        self.policy = policy;
        self
    }
}
