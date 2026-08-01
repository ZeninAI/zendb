//! Shared Table ownership, guarded mutation, and change listeners.

use std::sync::{Arc, Weak};

use parking_lot::{RwLock, RwLockReadGuard};
use zendb_storage::{Change, InsertOutcome, Table, TopicConsumer};
use zendb_types::{Event, Op, Path, PrimaryKey, Role};

use crate::{Error, Result, installations::Installations};

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

struct ListenerSet {
    internal: Vec<Arc<dyn ChangeListener>>,
    application: Vec<Arc<dyn ChangeListener>>,
}

/// A shared handle to an open [`Table`].
///
/// System tables are readable and consumable through public handles, but
/// [`TableHandle::insert`] refuses them.
pub struct TableHandle {
    name: String,
    pub(super) table: RwLock<Table>,
    listeners: RwLock<ListenerSet>,
    installations: Weak<Installations>,
    is_system: bool,
}

impl TableHandle {
    pub(crate) fn new(
        name: String,
        table: Table,
        installations: Weak<Installations>,
        is_system: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            table: RwLock::new(table),
            listeners: RwLock::new(ListenerSet {
                internal: Vec::new(),
                application: Vec::new(),
            }),
            installations,
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
        let installations = self.installations.upgrade().ok_or(Error::WorkspaceClosed)?;
        installations.require_access(&installations.local_installation_id(), Role::Contributor)?;
        let stamp = installations.mint()?;
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
    /// The listener fires after the workspace's internal listeners on every
    /// subsequent insert through any handle to this table.
    pub fn add_listener(&self, listener: Arc<dyn ChangeListener>) {
        self.listeners.write().application.push(listener);
    }

    /// Remove and return the most recently registered custom listener.
    pub fn pop_listener(&self) -> Option<Arc<dyn ChangeListener>> {
        self.listeners.write().application.pop()
    }

    pub(crate) fn add_internal_listener(&self, listener: Arc<dyn ChangeListener>) {
        self.listeners.write().internal.push(listener);
    }

    /// Authorized workspace mutation path for internal and admitted events.
    pub(crate) fn insert_internal(&self, event: Event) -> Result<InsertOutcome> {
        let installations = self.installations.upgrade().ok_or(Error::WorkspaceClosed)?;
        let required = if self.is_system {
            Role::Admin
        } else {
            Role::Contributor
        };
        installations.require_access(&event.stamp.id.author, required)?;
        self.insert_unchecked(event)
    }

    fn insert_unchecked(&self, event: Event) -> Result<InsertOutcome> {
        let outcome = { self.table.write().insert(event)? };
        if let InsertOutcome::Applied(ref change) = outcome {
            let listeners = self.listeners.read();
            for listener in listeners.internal.iter().chain(&listeners.application) {
                listener.on_change(change);
            }
        }
        Ok(outcome)
    }
}
