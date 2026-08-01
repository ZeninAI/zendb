//! Per-table listener that forwards locally authored events to replication.

use std::sync::{Arc, Weak};

use zendb_storage::Change;

use super::super::ReplicationController;
use crate::tables::ChangeListener;

pub(in crate::replication) struct ReplicationListener {
    controller: Weak<ReplicationController>,
    table: String,
}

impl ReplicationListener {
    pub(in crate::replication) fn build(
        controller: Weak<ReplicationController>,
        table: String,
    ) -> Arc<dyn ChangeListener> {
        Arc::new(Self { controller, table })
    }
}

impl ChangeListener for ReplicationListener {
    fn on_change(&self, change: &Change) {
        let Some(controller) = self.controller.upgrade() else {
            return;
        };
        if change.event.stamp.id.author == controller.local_installation_id {
            controller.submit(self.table.clone(), change.event.clone());
        }
        controller.finish_pending_stop(change.event.stamp.id);
    }
}
