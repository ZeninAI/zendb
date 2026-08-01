//! Installation-table listener that mirrors persisted values into the registry cache.

use std::sync::{Arc, Weak};

use zendb_storage::Change;
use zendb_types::{Installation, InstallationId, Op, Value};

use crate::{installations::Installations, tables::ChangeListener};

pub(crate) struct InstallationRegistryListener {
    installations: Weak<Installations>,
}

impl InstallationRegistryListener {
    pub(crate) fn build(installations: Weak<Installations>) -> Arc<Self> {
        Arc::new(Self { installations })
    }
}

impl ChangeListener for InstallationRegistryListener {
    fn on_change(&self, change: &Change) {
        let Some(installations) = self.installations.upgrade() else {
            return;
        };
        let Ok(installation_id) = InstallationId::try_from(&change.event.primary_key) else {
            return;
        };
        if !change.event.path.is_empty() {
            return;
        }
        match &change.event.op {
            Op::Upsert {
                value: Value::Blob(blob),
            } => {
                if let Ok(installation) = blob.decode::<Installation>() {
                    let mut cache = installations.registry_cache.write();
                    if installation_id == installations.local_installation_id() {
                        cache.local_role = installation.role;
                    }
                    cache.entries.insert(installation_id, installation);
                }
            }
            Op::Delete => {
                let mut cache = installations.registry_cache.write();
                if installation_id == installations.local_installation_id() {
                    cache.local_role = None;
                }
                cache.entries.remove(&installation_id);
            }
            _ => {}
        }
    }
}
