//! Workspace directory names, system resource names, and their classifiers.

pub(crate) const IDENTITY_FILE: &str = "_identity";
pub(crate) const LOCK_FILE: &str = "_lock";
pub(crate) const STATES_DIR: &str = "states";
pub(crate) const TABLES_DIR: &str = "tables";

pub(crate) const STATE_CATALOG_NAME: &str = "_catalog";
pub(crate) const PEER_STATE_NAME: &str = "_peers";
pub(crate) const TABLE_CATALOG_NAME: &str = "_catalog";
pub(crate) const DEVICES_TABLE_NAME: &str = "_devices";

pub(crate) fn is_system_state(name: &str) -> bool {
    matches!(name, STATE_CATALOG_NAME | PEER_STATE_NAME)
}

pub(crate) fn is_system_table(name: &str) -> bool {
    matches!(name, TABLE_CATALOG_NAME | DEVICES_TABLE_NAME)
}
