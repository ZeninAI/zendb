//! Receipt listener: live-observes every insert and feeds the device clock.

use std::sync::{Arc, Weak};

use zendb_storage::Change;

use super::super::runtime::ChangeListener;
use crate::devices::Devices;

/// Observes every successful insert on every table. Replaces the one-shot
/// `replay_receipts` consumer: receipts are now live-observed via callback
/// and written to `_peers` at the next durability barrier.
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
