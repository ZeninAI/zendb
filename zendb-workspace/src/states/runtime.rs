//! Typed runtime handle and guarded access for an open local State.

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use zendb_storage::State;

use crate::{Error, Result};

/// A typed handle to an open [`State`]. System states are readable through
/// public handles, but [`StateHandle::write`] refuses them.
pub struct StateHandle<K: Ord, V> {
    pub(super) name: String,
    pub(super) state: RwLock<State<K, V>>,
    pub(super) is_system: bool,
}

impl<K: Ord, V> StateHandle<K, V> {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if this handle refers to a system state.
    pub fn is_system(&self) -> bool {
        self.is_system
    }

    pub fn read(&self) -> RwLockReadGuard<'_, State<K, V>> {
        self.state.read()
    }

    pub fn write(&self) -> Result<RwLockWriteGuard<'_, State<K, V>>> {
        if self.is_system {
            return Err(Error::SystemStateReadOnly(self.name.clone()));
        }
        Ok(self.state.write())
    }

    /// Workspace-managed mutation path for system state.
    pub(crate) fn write_internal(&self) -> RwLockWriteGuard<'_, State<K, V>> {
        self.state.write()
    }
}
