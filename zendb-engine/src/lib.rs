//! ZeninDB table engine.

pub mod table;

pub use table::{
    EventKey, EventPosition, FlushConfig, IndexConfig, InsertResult, MaterializeResult,
    StateConfig, StateStats, Table, TableConfig, TableStats,
};
