//! Shared open tables and their public workspace-bound handles.

use std::sync::{Arc, Weak};

use parking_lot::{RwLock, RwLockReadGuard};
use zendb_storage::{Change, InsertOutcome, Table};
use zendb_types::{Event, Op, Path, Permission, PrimaryKey};

use crate::{Error, Result, core::WorkspaceCore};

/// Reacts to a successful insert on a [`TableHandle`].
pub trait ChangeListener: Send + Sync {
    fn on_change(&self, change: &Change);
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableKind {
    Catalog,
    Installations,
    Application,
}

pub(crate) struct OpenTable {
    name: String,
    pub(super) table: RwLock<Table>,
    listeners: RwLock<Arc<Vec<Arc<dyn ChangeListener>>>>,
    kind: TableKind,
}

impl OpenTable {
    pub(crate) fn new(name: String, table: Table, kind: TableKind) -> Arc<Self> {
        Arc::new(Self {
            name,
            table: RwLock::new(table),
            listeners: RwLock::new(Arc::new(Vec::new())),
            kind,
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) const fn kind(&self) -> TableKind {
        self.kind
    }

    pub(crate) fn insert_event(&self, event: Event) -> Result<InsertOutcome> {
        Ok(self.table.write().insert(event)?)
    }

    pub(crate) fn observe_event(&self, event: Event) -> Result<InsertOutcome> {
        Ok(self.table.write().observe(event)?)
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<'_, Table> {
        self.table.read()
    }

    pub(crate) fn notify_listeners(&self, change: &Change) {
        // Release the listener lock before callbacks so a listener can add,
        // remove, or re-enter workspace operations without deadlocking.
        let listeners = Arc::clone(&self.listeners.read());
        for listener in listeners.iter() {
            listener.on_change(change);
        }
    }
}

/// A shared handle to an open [`Table`].
///
/// System tables are readable and consumable through public handles, but
/// [`TableHandle::insert`] refuses them.
pub struct TableHandle {
    core: Weak<WorkspaceCore>,
    table: Arc<OpenTable>,
}

impl TableHandle {
    pub(crate) fn new(core: Weak<WorkspaceCore>, table: Arc<OpenTable>) -> Arc<Self> {
        Arc::new(Self { core, table })
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }

    pub fn is_system(&self) -> bool {
        self.table.kind() != TableKind::Application
    }

    pub fn insert(&self, primary_key: PrimaryKey, path: Path, op: Op) -> Result<InsertOutcome> {
        if self.is_system() {
            return Err(Error::SystemTableReadOnly(self.table.name().to_owned()));
        }
        let core = self.core.upgrade().ok_or(Error::WorkspaceClosed)?;
        core.membership.require_permission(
            &core.membership.local_installation_id(),
            Permission::WriteData,
        )?;
        core.commit_change(&self.table, primary_key, path, op)
    }

    pub fn read(&self) -> RwLockReadGuard<'_, Table> {
        self.table.table.read()
    }

    pub fn add_listener(&self, listener: Arc<dyn ChangeListener>) {
        Arc::make_mut(&mut self.table.listeners.write()).push(listener);
    }

    pub fn pop_listener(&self) -> Option<Arc<dyn ChangeListener>> {
        Arc::make_mut(&mut self.table.listeners.write()).pop()
    }
}
