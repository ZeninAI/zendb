//! Shared Table ownership, change listeners, and streaming consumers.

use std::{io, ops::Deref, sync::Arc};

use parking_lot::{RwLock, RwLockReadGuard};
use zendb_storage::{Change, InsertOutcome, Table, TopicConsumer};
use zendb_types::{Event, Op, Path, PrimaryKey};

use crate::{devices::Devices, Error, Result};

/// Reacts to a successful insert on a [`TableEntry`].
///
/// `on_change` is fire-and-forget: the insert that triggered it has already
/// succeeded and is durable. A failing callback leaves in-memory state stale
/// but never corrupts the durable state; on restart the in-memory state is
/// rebuilt from durable state. Callback errors are swallowed now; when logging
/// is added they will be logged.
///
/// The trait is `pub` so applications can implement custom listeners and
/// register them via [`TableHandle::add_listener`].
pub trait ChangeListener: Send + Sync {
    fn on_change(&self, change: &Change);
}

pub(crate) struct TableEntry {
    pub(crate) table: Arc<RwLock<Table>>,
    listeners: RwLock<Vec<Arc<dyn ChangeListener>>>,
}

impl TableEntry {
    pub(crate) fn build(table: Table) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            table: Arc::new(RwLock::new(table)),
            listeners: RwLock::new(Vec::new()),
        }))
    }

    /// Insert an event and synchronously dispatch the resulting `Change` to
    /// all registered listeners. This is the single insert path for both
    /// local and (eventually) remote mutations; both converge here so that
    /// listeners always fire.
    pub(crate) fn insert(&self, event: Event) -> Result<InsertOutcome> {
        let outcome = self.table.write().insert(event)?;
        if let InsertOutcome::Applied(ref change) = outcome {
            for listener in self.listeners.read().iter() {
                listener.on_change(change);
            }
        }
        Ok(outcome)
    }

    pub(crate) fn add_listener(&self, listener: Arc<dyn ChangeListener>) {
        self.listeners.write().push(listener);
    }
}

#[derive(Clone)]
pub struct TableHandle {
    name: String,
    entry: Arc<TableEntry>,
    devices: Arc<Devices>,
    is_system: bool,
}

impl TableHandle {
    pub(crate) fn new(
        name: String,
        entry: Arc<TableEntry>,
        devices: Arc<Devices>,
        is_system: bool,
    ) -> Self {
        Self {
            name,
            entry,
            devices,
            is_system,
        }
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
            return Err(Error::PermissionDenied);
        }
        self.devices.authorize(zendb_types::Roles::Contributor)?;
        let stamp = self.devices.mint()?;
        self.entry.insert(Event {
            primary_key,
            path,
            op,
            stamp,
        })
    }

    pub fn read(&self) -> TableReadGuard<'_> {
        TableReadGuard {
            table: self.entry.table.read(),
        }
    }

    pub fn consumer(&self, name: &str) -> Result<TableConsumer> {
        let changes = self.entry.table.read().consumer(name)?;
        Ok(TableConsumer {
            table: self.entry.table.clone(),
            changes,
        })
    }

    /// Register a custom [`ChangeListener`] on this table.
    ///
    /// The listener fires on every subsequent insert through any handle to
    /// this table. Application listeners and internal workspace listeners
    /// share the same listener list.
    pub fn add_listener(&self, listener: Arc<dyn ChangeListener>) {
        self.entry.add_listener(listener);
    }
}

pub struct TableReadGuard<'a> {
    table: RwLockReadGuard<'a, Table>,
}

impl Deref for TableReadGuard<'_> {
    type Target = Table;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

pub struct TableConsumer {
    table: Arc<RwLock<Table>>,
    changes: TopicConsumer<Change>,
}

impl TableConsumer {
    pub fn next_change(&mut self) -> io::Result<Option<Change>> {
        self.changes.next().transpose()
    }

    pub fn read(&self) -> TableReadGuard<'_> {
        TableReadGuard {
            table: self.table.read(),
        }
    }

    pub fn commit(&self) -> io::Result<()> {
        self.changes.commit()
    }
}
