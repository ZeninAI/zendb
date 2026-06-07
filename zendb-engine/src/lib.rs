//! ZeninDB table engine.

pub mod database;
pub mod table;

pub use database::Database;
pub use table::{
    FlushConfig, StateConfig, StateStats, Table, TableConfig, TableStats, DEFAULT_MAX_EVENTS,
};
