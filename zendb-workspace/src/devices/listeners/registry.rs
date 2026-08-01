//! Device-table listener that mirrors persisted records into the registry cache.

use std::sync::{Arc, Weak};

use zendb_storage::Change;
use zendb_types::{InstallationId, Op, Value};

use crate::{
    devices::{DeviceRecord, Devices},
    tables::ChangeListener,
};

pub(crate) struct DeviceRegistryListener {
    devices: Weak<Devices>,
}

impl DeviceRegistryListener {
    pub(crate) fn build(devices: Weak<Devices>) -> Arc<Self> {
        Arc::new(Self { devices })
    }
}

impl ChangeListener for DeviceRegistryListener {
    fn on_change(&self, change: &Change) {
        let Some(devices) = self.devices.upgrade() else {
            return;
        };
        let Ok(installation_id) = InstallationId::try_from(&change.event.primary_key) else {
            return;
        };
        match &change.event.op {
            Op::Upsert {
                value: Value::Blob(blob),
            } => {
                if let Ok(record) = blob.decode::<DeviceRecord>() {
                    let mut cache = devices.registry_cache.write();
                    if installation_id == devices.local_installation_id() {
                        cache.local_role = record.role;
                    }
                    cache.entries.insert(installation_id, record);
                }
            }
            Op::Delete => {
                let mut cache = devices.registry_cache.write();
                if installation_id == devices.local_installation_id() {
                    cache.local_role = None;
                }
                cache.entries.remove(&installation_id);
            }
            _ => {}
        }
    }
}
