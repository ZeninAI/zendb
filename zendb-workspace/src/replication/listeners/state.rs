//! Device-table listener that controls replication membership and lifecycle.

use std::sync::{Arc, Weak};

use zendb_storage::Change;

use super::super::ReplicationController;
use crate::tables::ChangeListener;

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
        controller.device_changed(change);
    }
}
