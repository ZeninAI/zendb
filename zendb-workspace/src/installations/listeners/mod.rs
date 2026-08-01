//! Internal listeners that maintain installation registry and receipt state.

mod receipts;
mod registry;

pub(crate) use receipts::ReceiptListener;
pub(crate) use registry::InstallationRegistryListener;
