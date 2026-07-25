//! Persisted catalog declarations and public table metadata.

use bincode::{Decode, Encode};
use zendb_storage::TableConfig;

pub const TABLE_CATALOG_NAME: &str = "_table_catalog";
pub const STATE_CATALOG_NAME: &str = "_state_catalog";
pub const DEVICES_NAME: &str = "_devices";
pub const PEER_STATE_NAME: &str = "_peer_state";

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct CatalogEntry {
    pub config: TableConfig,
}

impl CatalogEntry {
    pub(crate) fn new(config: TableConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    pub name: String,
    pub config: TableConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    Unchanged,
    Updated,
}

pub(crate) fn is_system_table(name: &str) -> bool {
    matches!(name, TABLE_CATALOG_NAME | DEVICES_NAME)
}

pub(crate) fn is_system_state(name: &str) -> bool {
    matches!(name, STATE_CATALOG_NAME | PEER_STATE_NAME)
}
