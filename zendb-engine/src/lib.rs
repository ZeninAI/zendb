//! ZeninDB table engine.

pub mod database;
pub mod table;

pub use database::{Database, TableHandle};
pub use table::{FlushConfig, Table, TableConfig, TableStats, DEFAULT_MAX_EVENTS};
pub use zendb_storage::core::state::{State, StateConfig, StateStats};
