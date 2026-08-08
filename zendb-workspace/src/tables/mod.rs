//! Public table lifecycle facade and workspace-bound table handles.

mod handle;
mod store;

use std::sync::Arc;

use zendb_storage::{Storage, TableConfig};
use zendb_types::{Blob, Op, Path, Permission, PrimaryKey, Value};

pub use handle::{ChangeListener, TableHandle};
pub(crate) use handle::{OpenTable, TableKind};
pub(crate) use store::TableStore;

use crate::{Error, Result, core::WorkspaceCore, system::is_system_table};

/// Table lifecycle operations for one open workspace.
pub struct Tables {
    core: Arc<WorkspaceCore>,
    catalog: Arc<OpenTable>,
}

impl Tables {
    pub(crate) fn new(core: Arc<WorkspaceCore>, catalog: Arc<OpenTable>) -> Self {
        Self { core, catalog }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.core.table_store.contains(name)
    }

    pub fn list(&self) -> Vec<String> {
        self.core.table_store.list()
    }

    pub fn upsert(&self, name: &str, config: TableConfig) -> Result<bool> {
        if is_system_table(name) {
            return Err(Error::SystemTableReadOnly(name.to_owned()));
        }
        self.core.membership.require_permission(
            &self.core.membership.local_installation_id(),
            Permission::ManageTables,
        )?;
        if self
            .core
            .table_store
            .get(name)
            .is_ok_and(|table| table.read().config() == config)
        {
            return Ok(false);
        }
        // The catalog event is authoritative; WorkspaceCore materializes the
        // physical table only after this event has applied successfully.
        self.core.commit_change(
            &self.catalog,
            PrimaryKey::String(name.to_owned()),
            Path::new(),
            Op::Upsert {
                value: Value::Blob(Blob::encode(&config)?),
            },
        )?;
        Ok(true)
    }

    pub fn get(&self, name: &str) -> Result<Arc<TableHandle>> {
        Ok(TableHandle::new(
            Arc::downgrade(&self.core),
            self.core.table_store.get(name)?,
        ))
    }

    pub fn delete(&self, name: &str) -> Result<bool> {
        if is_system_table(name) {
            return Err(Error::SystemTableReadOnly(name.to_owned()));
        }
        self.core.membership.require_permission(
            &self.core.membership.local_installation_id(),
            Permission::ManageTables,
        )?;
        let table = match self.core.table_store.get(name) {
            Ok(table) => table,
            Err(Error::TableNotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        // The store and this temporary lookup are the only expected owners. A
        // remaining Arc means a caller still holds a TableHandle.
        if Arc::strong_count(&table) != 2 {
            return Err(Error::TableInUse(name.to_owned()));
        }
        drop(table);
        self.core.commit_change(
            &self.catalog,
            PrimaryKey::String(name.to_owned()),
            Path::new(),
            Op::Delete,
        )?;
        Ok(true)
    }
}
