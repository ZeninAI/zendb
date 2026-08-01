//! Receipt listener that observes applied events and advances the local clock.

use std::sync::{Arc, Weak};

use zendb_storage::Change;

use crate::{devices::Devices, tables::ChangeListener};

pub(crate) struct ReceiptListener {
    devices: Weak<Devices>,
}

impl ReceiptListener {
    pub(crate) fn build(devices: Weak<Devices>) -> Arc<dyn ChangeListener> {
        Arc::new(Self { devices })
    }
}

impl ChangeListener for ReceiptListener {
    fn on_change(&self, change: &Change) {
        let Some(devices) = self.devices.upgrade() else {
            return;
        };
        let _ = devices.observe(change.event.stamp);
    }
}
