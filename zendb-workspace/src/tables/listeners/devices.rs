//! Device registry listener: mirrors `_devices` events into the registry cache.

use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use zendb_storage::Change;
use zendb_types::{EventId, InstallationId};
use zendb_types::{Op, Value};

use super::super::runtime::ChangeListener;
use crate::devices::{DeviceRecord, Devices, installation_id_from_key};

pub(crate) trait DeviceChangeObserver: Send + Sync {
    fn device_upserted(
        &self,
        installation_id: InstallationId,
        previous: Option<DeviceRecord>,
        current: DeviceRecord,
        event_id: EventId,
    );

    fn device_removed(
        &self,
        installation_id: InstallationId,
        removed: DeviceRecord,
        event_id: EventId,
    );
}

/// Reacts to `_devices` events: upserts/removes in the device registry cache.
/// This is the single owner of in-memory device-record mutation
/// after the initial load performed by `Devices::open`.
pub(crate) struct DeviceRegistryListener {
    devices: Weak<Devices>,
    observer: RwLock<Option<Weak<dyn DeviceChangeObserver>>>,
}

impl DeviceRegistryListener {
    pub(crate) fn build(devices: Weak<Devices>) -> Arc<Self> {
        Arc::new(Self {
            devices,
            observer: RwLock::new(None),
        })
    }

    pub(crate) fn set_observer(&self, observer: Weak<dyn DeviceChangeObserver>) {
        *self.observer.write() = Some(observer);
    }
}

impl ChangeListener for DeviceRegistryListener {
    fn on_change(&self, change: &Change) {
        let Some(devices) = self.devices.upgrade() else {
            return;
        };
        let Some(installation_id) = installation_id_from_key(&change.event.primary_key) else {
            return;
        };
        match &change.event.op {
            Op::Upsert {
                value: Value::Blob(blob),
            } => {
                if let Ok(record) = blob.decode::<DeviceRecord>() {
                    let previous = {
                        let mut cache = devices.registry_cache.write();
                        if installation_id == devices.local_installation_id() {
                            cache.local_role = record.role;
                        }
                        cache.entries.insert(installation_id, record.clone())
                    };
                    if let Some(observer) = self.observer.read().as_ref().and_then(Weak::upgrade) {
                        observer.device_upserted(
                            installation_id,
                            previous,
                            record,
                            change.event.stamp.id,
                        );
                    }
                }
            }
            Op::Delete => {
                let removed = {
                    let mut cache = devices.registry_cache.write();
                    if installation_id == devices.local_installation_id() {
                        cache.local_role = None;
                    }
                    cache.entries.remove(&installation_id)
                };
                if let (Some(removed), Some(observer)) = (
                    removed,
                    self.observer.read().as_ref().and_then(Weak::upgrade),
                ) {
                    observer.device_removed(installation_id, removed, change.event.stamp.id);
                }
            }
            _ => {}
        }
    }
}
