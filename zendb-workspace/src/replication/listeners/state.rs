//! Device-table listener that controls replication membership and lifecycle.

use std::sync::{Arc, Weak};

use zendb_storage::Change;
use zendb_types::{Cell, InstallationId, Value};

use super::super::ReplicationController;
use crate::{devices::DeviceRecord, tables::ChangeListener};

pub(in crate::replication) struct ReplicationStateListener {
    controller: Weak<ReplicationController>,
}

impl ReplicationStateListener {
    pub(in crate::replication) fn build(
        controller: Weak<ReplicationController>,
    ) -> Arc<dyn ChangeListener> {
        Arc::new(Self { controller })
    }
}

impl ChangeListener for ReplicationStateListener {
    fn on_change(&self, change: &Change) {
        let Some(controller) = self.controller.upgrade() else {
            return;
        };
        let Ok(installation_id) = InstallationId::try_from(&change.event.primary_key) else {
            return;
        };
        let previous = change.previous.as_ref().and_then(device_record);
        let current = change.current.as_ref().and_then(device_record);
        controller.device_changed(installation_id, previous, current, change);
    }
}

fn device_record(cell: &Cell) -> Option<DeviceRecord> {
    let Value::Blob(blob) = cell.value.as_ref()? else {
        return None;
    };
    blob.decode().ok()
}
