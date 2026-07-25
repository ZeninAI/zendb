//! Persisted table catalog declarations and public table metadata.

use zendb_storage::TableConfig;

pub const TABLE_CATALOG_NAME: &str = "_table_catalog";
pub const DEVICES_NAME: &str = "_devices";

#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    pub name: String,
    pub config: TableConfig,
}

pub(crate) fn is_system_table(name: &str) -> bool {
    matches!(name, TABLE_CATALOG_NAME | DEVICES_NAME)
}
