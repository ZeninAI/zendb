//! Workspace layout, key-derivation domains, and reserved system resources.

use std::sync::LazyLock;

use zendb_storage::{KeyDirConfig, StateConfig, TableConfig, TopicConfig};

pub(crate) const IDENTITY_FILE: &str = "_identity";
pub(crate) const LOCK_FILE: &str = "_lock";
pub(crate) const WORKSPACE_KEY_DOMAIN: &[u8] = b"zendb/v1";
pub(crate) const STATES_DIR: &str = "states";
pub(crate) const TABLES_DIR: &str = "tables";

pub(crate) const STATE_CATALOG_NAME: &str = "_catalog";
pub(crate) const TABLE_CATALOG_NAME: &str = "_catalog";
pub(crate) const INSTALLATIONS_TABLE_NAME: &str = "_installations";

/// Shared configuration for the catalog and system tables.
pub(crate) static SYSTEM_STATE_CONFIG: LazyLock<StateConfig> =
    LazyLock::new(|| StateConfig::Unordered(KeyDirConfig::default()));

/// Shared configuration for the catalog and installations tables.
pub(crate) static SYSTEM_TABLE_CONFIG: LazyLock<TableConfig> = LazyLock::new(|| TableConfig {
    state: SYSTEM_STATE_CONFIG.clone(),
    causal: SYSTEM_STATE_CONFIG.clone(),
    topic: TopicConfig::default(),
});

pub(crate) fn is_system_state(name: &str) -> bool {
    name == STATE_CATALOG_NAME
}

pub(crate) fn is_system_table(name: &str) -> bool {
    matches!(name, TABLE_CATALOG_NAME | INSTALLATIONS_TABLE_NAME)
}
