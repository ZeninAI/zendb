//! Internal workspace listeners for receipts, the table catalog, and devices.

use std::{
    fs,
    sync::{Arc, Weak},
};

use zendb_storage::{DurableStorage, Table, TableConfig};
use zendb_types::{Op, PrimaryKey, Value};

use super::runtime::{ChangeListener, TableHandle};
use crate::{devices::Devices, tables::Tables};

/// Observes every successful insert on every table. Replaces the one-shot
/// `replay_receipts` consumer: receipts are now live-observed via callback
/// and written to `_peers` at the next durability barrier.
pub(crate) struct ReceiptListener {
    devices: Weak<Devices>,
}

impl ReceiptListener {
    pub(crate) fn build(devices: Weak<Devices>) -> Arc<dyn ChangeListener> {
        Arc::new(Self { devices })
    }
}

impl ChangeListener for ReceiptListener {
    fn on_change(&self, change: &zendb_storage::Change) {
        let Some(devices) = self.devices.upgrade() else {
            return;
        };
        let _ = devices.observe(change.event.stamp);
    }
}

/// Reacts to `_catalog` events: opens tables on `Upsert`, closes +
/// removes the physical directory on `Delete`. This is the single owner of
/// the in-memory table-handle map after bootstrap.
///
/// Holds a shared receipt listener to register on newly opened application
/// tables, so every table — bootstrap-opened or callback-opened — has the
/// receipt listener.
pub(crate) struct TableCatalogListener {
    tables: Weak<Tables>,
    receipt_listener: Arc<dyn ChangeListener>,
}

impl TableCatalogListener {
    pub(crate) fn build(
        tables: Weak<Tables>,
        receipt_listener: Arc<dyn ChangeListener>,
    ) -> Arc<dyn ChangeListener> {
        Arc::new(Self {
            tables,
            receipt_listener,
        })
    }
}

impl ChangeListener for TableCatalogListener {
    fn on_change(&self, change: &zendb_storage::Change) {
        let Some(tables) = self.tables.upgrade() else {
            return;
        };
        let PrimaryKey::String(name) = &change.event.primary_key else {
            return;
        };
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
                let handle =
                    TableHandle::new(name.clone(), table, Arc::downgrade(&tables.devices), false);
                handle.add_listener(self.receipt_listener.clone());
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

/// Reacts to `_devices` events: upserts/removes in the device registry cache.
/// This is the single owner of in-memory device-record mutation
/// after the initial load performed by `Devices::open`.
pub(crate) struct DeviceRegistryListener {
    devices: Weak<Devices>,
}

impl DeviceRegistryListener {
    pub(crate) fn build(devices: Weak<Devices>) -> Arc<dyn ChangeListener> {
        Arc::new(Self { devices })
    }
}

impl ChangeListener for DeviceRegistryListener {
    fn on_change(&self, change: &zendb_storage::Change) {
        use crate::devices::DeviceRecord;
        let Some(devices) = self.devices.upgrade() else {
            return;
        };
        let PrimaryKey::PeerId(peer_id) = &change.event.primary_key else {
            return;
        };
        let mut cache = devices.registry_cache.write();
        match &change.event.op {
            Op::Upsert {
                value: Value::Blob(blob),
            } => {
                if let Ok(record) = blob.decode::<DeviceRecord>() {
                    if *peer_id == devices.local_peer_id() {
                        cache.local_role = record.role;
                    }
                    cache.entries.insert(*peer_id, record);
                }
            }
            Op::Delete => {
                if *peer_id == devices.local_peer_id() {
                    cache.local_role = None;
                }
                cache.entries.remove(peer_id);
            }
            _ => {}
        }
    }
}
