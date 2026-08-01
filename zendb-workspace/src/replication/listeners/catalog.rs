//! Replication catalog listener that instruments newly created table handles.

use std::sync::{Arc, Weak};

use zendb_storage::Change;
use zendb_types::{Op, PrimaryKey};

use super::super::ReplicationController;
use crate::tables::ChangeListener;

pub(in crate::replication) struct CatalogListener {
    controller: Weak<ReplicationController>,
}

impl CatalogListener {
    pub(in crate::replication) fn build(
        controller: Weak<ReplicationController>,
    ) -> Arc<dyn ChangeListener> {
        Arc::new(Self { controller })
    }
}

impl ChangeListener for CatalogListener {
    fn on_change(&self, change: &Change) {
        if !matches!(change.event.op, Op::Upsert { .. }) {
            return;
        }
        if change
            .previous
            .as_ref()
            .is_some_and(|cell| cell.value.is_some())
        {
            return;
        }
        let PrimaryKey::String(name) = &change.event.primary_key else {
            return;
        };
        if let Some(controller) = self.controller.upgrade() {
            controller.attach_catalog_table(name);
        }
    }
}
