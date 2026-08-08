//! Table storage facade over materialized state and its durable change topic.

mod change;
mod receipt;
#[expect(
    clippy::module_inception,
    reason = "the requested table folder keeps its implementation in table.rs"
)]
mod table;

pub use change::Change;
pub use receipt::ReceiptWindow;
pub use table::{InsertOutcome, Table, TableConfig, TableStats};
