//! Device registry, cached installation bookkeeping, and receipt tracking.

mod receipts;
mod runtime;

pub(crate) use runtime::PeerState;
pub use runtime::{DeviceRecord, Devices};

use zendb_types::{InstallationId, PrimaryKey};

pub(crate) fn device_primary_key(installation_id: InstallationId) -> PrimaryKey {
    PrimaryKey::Blob(installation_id.to_bytes().to_vec().into())
}

pub(crate) fn installation_id_from_key(key: &PrimaryKey) -> Option<InstallationId> {
    let PrimaryKey::Blob(bytes) = key else {
        return None;
    };
    bytes
        .as_slice()
        .try_into()
        .ok()
        .map(InstallationId::from_bytes)
}
