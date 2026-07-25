//! Internal workspace listeners: receipts, catalog sync, and device sync.

use std::{
    fs,
    sync::{Arc, Weak},
};

use zendb_storage::{DurableStorage, Table, TableConfig};
use zendb_types::{Op, PrimaryKey, Value};

use super::runtime::{ChangeListener, TableEntry};
use crate::{devices::Devices, tables::TablesCore};

/// Observes every successful insert on every table. Replaces the one-shot
/// `replay_receipts` consumer: receipts are now live-observed via callback
/// and persisted in `_peer_state`.
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

/// Reacts to `_table_catalog` events: opens tables on `Upsert`, closes +
/// removes the physical directory on `Delete`. This is the single owner of
/// the in-memory table-handle map after bootstrap.
///
/// Holds a shared receipt listener to register on newly opened application
/// tables, so every table — bootstrap-opened or callback-opened — has the
/// receipt listener.
pub(crate) struct CatalogSyncListener {
    core: Weak<TablesCore>,
    receipt_listener: Arc<dyn ChangeListener>,
}

impl CatalogSyncListener {
    pub(crate) fn build(
        core: Weak<TablesCore>,
        receipt_listener: Arc<dyn ChangeListener>,
    ) -> Arc<dyn ChangeListener> {
        Arc::new(Self {
            core,
            receipt_listener,
        })
    }
}

impl ChangeListener for CatalogSyncListener {
    fn on_change(&self, change: &zendb_storage::Change) {
        let Some(core) = self.core.upgrade() else {
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
                if core.tables.read().contains_key(name) {
                    return;
                }
                let config: TableConfig = match blob.decode() {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let path = core.root.join("tables").join(name);
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
                let entry = match TableEntry::build(table) {
                    Ok(e) => e,
                    Err(_) => return,
                };
                entry.add_listener(self.receipt_listener.clone());
                core.tables.write().insert(name.clone(), entry);
            }
            Op::Delete => {
                let mut tables = core.tables.write();
                if let Some(entry) = tables.remove(name) {
                    drop(entry);
                    let path = core.root.join("tables").join(name);
                    if path.exists() {
                        let _ = fs::remove_dir_all(path);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Reacts to `_devices` events: upserts/removes in the in-memory device
/// records map. This is the single owner of in-memory device-record mutation
/// after `reload`.
pub(crate) struct DeviceSyncListener {
    devices: Weak<Devices>,
}

impl DeviceSyncListener {
    pub(crate) fn build(devices: Weak<Devices>) -> Arc<dyn ChangeListener> {
        Arc::new(Self { devices })
    }
}

impl ChangeListener for DeviceSyncListener {
    fn on_change(&self, change: &zendb_storage::Change) {
        use crate::devices::DeviceRecord;
        let Some(devices) = self.devices.upgrade() else {
            return;
        };
        let PrimaryKey::PeerId(peer) = &change.event.primary_key else {
            return;
        };
        match &change.event.op {
            Op::Upsert {
                value: Value::Blob(blob),
            } => {
                if let Ok(record) = blob.decode::<DeviceRecord>() {
                    devices.inner().records.write().insert(*peer, record);
                }
            }
            Op::Delete => {
                devices.inner().records.write().remove(peer);
            }
            _ => {}
        }
    }
}
