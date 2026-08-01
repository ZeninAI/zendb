//! Table catalog listener: opens/closes tables in response to catalog events.

use std::{
    fs,
    sync::{Arc, Weak},
};

use zendb_storage::{Change, DurableStorage, Table, TableConfig};
use zendb_types::{Op, PrimaryKey, Value};

use super::super::runtime::{ChangeListener, TableHandle};
use crate::tables::Tables;

/// Reacts to `_catalog` events: opens tables on `Upsert`, closes +
/// removes the physical directory on `Delete`. This is the single owner of
/// the in-memory table-handle map after bootstrap.
///
/// Holds a shared receipt listener to register on newly opened application
/// tables, so every table, whether bootstrap-opened or callback-opened, has
/// the receipt listener.
pub(in crate::tables) struct CatalogListener {
    tables: Weak<Tables>,
    receipt_listener: Arc<dyn ChangeListener>,
}

impl CatalogListener {
    pub(in crate::tables) fn build(
        tables: Weak<Tables>,
        receipt_listener: Arc<dyn ChangeListener>,
    ) -> Arc<dyn ChangeListener> {
        Arc::new(Self {
            tables,
            receipt_listener,
        })
    }
}

impl ChangeListener for CatalogListener {
    fn on_change(&self, change: &Change) {
        let Some(tables) = self.tables.upgrade() else {
            return;
        };
        let PrimaryKey::String(name) = &change.event.primary_key else {
            return;
        };
        if !change.event.path.is_empty() {
            return;
        }
        match &change.event.op {
            Op::Upsert {
                value: Value::Blob(blob),
            } => {
                // Already open (update case or bootstrap no-op). A future
                // iteration may migrate on config change; for now, no-op.
                if tables.tables.read().contains_key(name) {
                    return;
                }
                let config: TableConfig = match blob.decode() {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let path = tables.root.join(name);
                // Create the physical table if the directory doesn't exist yet
                // (local create or remote create); otherwise open it (workspace
                // reopen after restart, or remote update of an existing table).
                let table = if path.exists() {
                    match Table::open(&path, config) {
                        Ok(t) => t,
                        Err(_) => return,
                    }
                } else {
                    match Table::create(&path, config) {
                        Ok(t) => t,
                        Err(_) => return,
                    }
                };
                let handle = TableHandle::new(
                    name.clone(),
                    table,
                    Arc::downgrade(&tables.installations),
                    false,
                );
                handle.add_internal_listener(self.receipt_listener.clone());
                tables.tables.write().insert(name.clone(), handle);
            }
            Op::Delete => {
                let mut tables_map = tables.tables.write();
                if let Some(entry) = tables_map.remove(name) {
                    drop(entry);
                    let path = tables.root.join(name);
                    if path.exists() {
                        let _ = fs::remove_dir_all(path);
                    }
                }
            }
            _ => {}
        }
    }
}
