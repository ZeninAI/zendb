//! Workspace paths, system resource names, classifiers, and bootstrap configs.

use std::sync::LazyLock;

use zendb_storage::{
    DEFAULT_MAX_BUFFERED_RECORDS, KeyDirConfig, StateConfig, TableConfig, TopicConfig,
};

pub(crate) const IDENTITY_FILE: &str = "_identity";
pub(crate) const LOCK_FILE: &str = "_lock";
pub(crate) const STATES_DIR: &str = "states";
pub(crate) const TABLES_DIR: &str = "tables";

pub(crate) const STATE_CATALOG_NAME: &str = "_catalog";
pub(crate) const PEERS_STATE_NAME: &str = "_peers";
pub(crate) const TABLE_CATALOG_NAME: &str = "_catalog";
pub(crate) const INSTALLATIONS_TABLE_NAME: &str = "_installations";

/// Shared configuration for the catalog and peer states.
pub(crate) static SYSTEM_STATE_CONFIG: LazyLock<StateConfig> =
    LazyLock::new(|| StateConfig::Unordered(KeyDirConfig::default()));

/// Shared configuration for the catalog and installations tables.
pub(crate) static SYSTEM_TABLE_CONFIG: LazyLock<TableConfig> = LazyLock::new(|| TableConfig {
    state: SYSTEM_STATE_CONFIG.clone(),
    max_buffered_records: DEFAULT_MAX_BUFFERED_RECORDS,
    topic: TopicConfig::default(),
});

pub(crate) fn is_system_state(name: &str) -> bool {
    matches!(name, STATE_CATALOG_NAME | PEERS_STATE_NAME)
}

pub(crate) fn is_system_table(name: &str) -> bool {
    matches!(name, TABLE_CATALOG_NAME | INSTALLATIONS_TABLE_NAME)
}
