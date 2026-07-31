//! Table listener that forwards only locally authored events to replication.

use std::sync::{Arc, Weak};

use zendb_storage::Change;

use super::ReplicationController;
use crate::tables::{ChangeListener, ChangeListenerFactory};

pub(crate) struct ReplicationListener {
    controller: Weak<ReplicationController>,
    table: String,
}

pub(crate) struct ReplicationListenerFactory {
    controller: Weak<ReplicationController>,
}

impl ReplicationListenerFactory {
    pub(crate) fn new(controller: Weak<ReplicationController>) -> Arc<Self> {
        Arc::new(Self { controller })
    }
}

impl ChangeListenerFactory for ReplicationListenerFactory {
    fn build(&self, table: &str) -> Arc<dyn ChangeListener> {
        Arc::new(ReplicationListener {
            controller: self.controller.clone(),
            table: table.to_owned(),
        })
    }
}

impl ChangeListener for ReplicationListener {
    fn on_change(&self, change: &Change) {
        let Some(controller) = self.controller.upgrade() else {
            return;
        };
        if change.event.stamp.id.author == controller.local_installation_id() {
            controller.submit(self.table.clone(), change.event.clone());
        }
        controller.finish_pending_stop(change.event.stamp.id);
    }
}
