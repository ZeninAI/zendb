//! Device registry listener: mirrors `_devices` events into the registry cache.

use std::sync::{Arc, Weak};

use zendb_storage::Change;
use zendb_types::{Op, PrimaryKey, Value};

use super::super::runtime::ChangeListener;
use crate::devices::{DeviceRecord, Devices};

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
    fn on_change(&self, change: &Change) {
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
                    if peer_id == devices.local_peer_id() {
                        cache.local_role = record.role;
                    }
                    cache.entries.insert(*peer_id, record);
                }
            }
            Op::Delete => {
                if peer_id == devices.local_peer_id() {
                    cache.local_role = None;
                }
                cache.entries.remove(peer_id);
            }
            _ => {}
        }
    }
}
