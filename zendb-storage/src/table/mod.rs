//! Table storage facade and lazy read-merge support.

mod change;
mod iter;
#[expect(
    clippy::module_inception,
    reason = "the requested table folder keeps its implementation in table.rs"
)]
mod table;

pub use change::Change;
pub use table::{InsertOutcome, Table, TableConfig, TableStats, DEFAULT_MAX_BUFFERED_RECORDS};
