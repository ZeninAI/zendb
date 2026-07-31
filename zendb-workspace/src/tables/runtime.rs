//! Shared Table ownership, guarded mutation, and change listeners.

use std::sync::{Arc, Weak};

use parking_lot::{RwLock, RwLockReadGuard};
use zendb_storage::{Change, InsertOutcome, Table, TopicConsumer};
use zendb_types::{Event, Op, Path, PrimaryKey, Role};

use crate::{Error, Result, devices::Devices};

/// Reacts to a successful insert on a [`TableHandle`].
///
/// `on_change` is fire-and-forget: the insert that triggered it has already
/// succeeded. A failing internal callback may leave derived in-memory state
/// stale; on restart it is rebuilt from the stored Table. Callback errors are
/// swallowed now; when logging is added they will be logged.
///
/// The trait is `pub` so applications can implement custom listeners and
/// register them via [`TableHandle::add_listener`].
pub trait ChangeListener: Send + Sync {
    fn on_change(&self, change: &Change);
}

pub(crate) trait ChangeListenerFactory: Send + Sync {
    fn build(&self, table: &str) -> Arc<dyn ChangeListener>;
}

/// A shared handle to an open [`Table`].
///
/// System tables are readable and consumable through public handles, but
/// [`TableHandle::insert`] refuses them.
pub struct TableHandle {
    name: String,
    pub(super) table: RwLock<Table>,
    listeners: RwLock<Vec<Arc<dyn ChangeListener>>>,
    devices: Weak<Devices>,
    is_system: bool,
}

impl TableHandle {
    pub(crate) fn new(
        name: String,
        table: Table,
        devices: Weak<Devices>,
        is_system: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            table: RwLock::new(table),
            listeners: RwLock::new(Vec::new()),
            devices,
            is_system,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if this handle refers to a system table.
    ///
    /// System tables are openable for reads but cannot be written through
    /// this handle; the workspace maintains them internally.
    pub fn is_system(&self) -> bool {
        self.is_system
    }

    pub fn insert(&self, primary_key: PrimaryKey, path: Path, op: Op) -> Result<InsertOutcome> {
        if self.is_system {
            return Err(Error::SystemTableReadOnly(self.name.clone()));
        }
        let devices = self.devices.upgrade().ok_or(Error::WorkspaceClosed)?;
        devices.require_access(&devices.local_installation_id(), Role::Contributor)?;
        let stamp = devices.mint()?;
        self.insert_internal(Event {
            primary_key,
            path,
            op,
            stamp,
        })
    }

    pub fn read(&self) -> RwLockReadGuard<'_, Table> {
        self.table.read()
    }

    pub fn consumer(&self, name: &str) -> Result<TopicConsumer<Change>> {
        Ok(self.table.read().consumer(name)?)
    }

    /// Register a custom [`ChangeListener`] on this table.
    ///
    /// The listener fires on every subsequent insert through any handle to
    /// this table. Application listeners and internal workspace listeners
    /// share the same listener list.
    pub fn add_listener(&self, listener: Arc<dyn ChangeListener>) {
        self.listeners.write().push(listener);
    }

    /// Authorized workspace mutation path for internal and admitted events.
    pub(crate) fn insert_internal(&self, event: Event) -> Result<InsertOutcome> {
        let devices = self.devices.upgrade().ok_or(Error::WorkspaceClosed)?;
        let required = if self.is_system {
            Role::Admin
        } else {
            Role::Contributor
        };
        devices.require_access(&event.stamp.id.author, required)?;
        self.insert_unchecked(event)
    }

    fn insert_unchecked(&self, event: Event) -> Result<InsertOutcome> {
        let outcome = { self.table.write().insert(event)? };
        if let InsertOutcome::Applied(ref change) = outcome {
            for listener in self.listeners.read().iter() {
                listener.on_change(change);
            }
        }
        Ok(outcome)
    }
}
