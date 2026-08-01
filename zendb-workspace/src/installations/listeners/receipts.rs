//! Receipt listener that observes applied events and advances the local clock.

use std::sync::{Arc, Weak};

use zendb_storage::Change;

use crate::{installations::Installations, tables::ChangeListener};

pub(crate) struct ReceiptListener {
    installations: Weak<Installations>,
}

impl ReceiptListener {
    pub(crate) fn build(installations: Weak<Installations>) -> Arc<dyn ChangeListener> {
        Arc::new(Self { installations })
    }
}

impl ChangeListener for ReceiptListener {
    fn on_change(&self, change: &Change) {
        let Some(installations) = self.installations.upgrade() else {
            return;
        };
        let _ = installations.observe(change.event.stamp);
    }
}
