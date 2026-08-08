//! Physical table ownership, catalog decoding, and catalog change application.
//! A lifecycle mutex serializes flush, sync, and catalog mutations against each
//! other, mirroring the pattern in `states/mod.rs`.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::{Mutex, RwLock};
use zendb_storage::{Change, DurableStorage, ReadBackend, Table, TableConfig};
use zendb_types::{
    Blob, Cell, Event, EventId, EventStamp, EventTime, InstallationId, Op, Path as CrdtPath,
    PrimaryKey, Value, utils::time::physical_ms,
};

use super::{OpenTable, TableKind};
use crate::{
    Error, Result,
    system::{
        INSTALLATIONS_TABLE_NAME, SYSTEM_TABLE_CONFIG, TABLE_CATALOG_NAME, TABLES_DIR,
        is_system_table,
    },
};

/// Physical table ownership, catalog decoding, and catalog change
/// application. All tables are eagerly opened at construction (unlike
/// [`States`] which lazily opens on first access). A `lifecycle` mutex
/// serializes cold-path operations (flush, sync, catalog changes) so that a
/// concurrent delete cannot remove a backing directory while a flush is
/// writing to it.
pub(crate) struct TableStore {
    root: PathBuf,
    lifecycle: Mutex<()>,
    tables: RwLock<HashMap<String, Arc<OpenTable>>>,
}

impl TableStore {
    /// Create the system tables and their initial catalog declarations.
    pub(crate) fn create(root: &Path, local_installation_id: InstallationId) -> Result<Self> {
        let root = root.join(TABLES_DIR);
        fs::create_dir_all(&root)?;
        let mut catalog =
            Table::create(&root.join(TABLE_CATALOG_NAME), SYSTEM_TABLE_CONFIG.clone())?;
        let installations = Table::create(
            &root.join(INSTALLATIONS_TABLE_NAME),
            SYSTEM_TABLE_CONFIG.clone(),
        )?;
        // These declarations are authored directly by the store. Each table
        // owns its own local sequence stream, so the first declaration starts
        // at sequence 1 on the catalog table.
        catalog.insert(Event {
            primary_key: PrimaryKey::String(TABLE_CATALOG_NAME.to_owned()),
            path: CrdtPath::new(),
            op: Op::Upsert {
                value: Value::Blob(Blob::encode(&*SYSTEM_TABLE_CONFIG)?),
            },
            stamp: EventStamp {
                id: EventId {
                    author: local_installation_id,
                    sequence: 0,
                },
                time: EventTime {
                    physical_ms: physical_ms().ok_or(Error::ClockExhausted)?,
                    logical: 0,
                },
            },
        })?;
        catalog.insert(Event {
            primary_key: PrimaryKey::String(INSTALLATIONS_TABLE_NAME.to_owned()),
            path: CrdtPath::new(),
            op: Op::Upsert {
                value: Value::Blob(Blob::encode(&*SYSTEM_TABLE_CONFIG)?),
            },
            stamp: EventStamp {
                id: EventId {
                    author: local_installation_id,
                    sequence: 0,
                },
                time: EventTime {
                    physical_ms: physical_ms().ok_or(Error::ClockExhausted)?,
                    logical: 0,
                },
            },
        })?;
        Self::from_system_tables(root, catalog, installations)
    }

    pub(crate) fn open(root: &Path) -> Result<Self> {
        let root = root.join(TABLES_DIR);
        let catalog = Table::open(&root.join(TABLE_CATALOG_NAME), SYSTEM_TABLE_CONFIG.clone())?;
        let installations = Table::open(
            &root.join(INSTALLATIONS_TABLE_NAME),
            SYSTEM_TABLE_CONFIG.clone(),
        )?;
        Self::from_system_tables(root, catalog, installations)
    }

    fn from_system_tables(
        root: PathBuf,
        catalog_table: Table,
        installations_table: Table,
    ) -> Result<Self> {
        let catalog = OpenTable::new(
            TABLE_CATALOG_NAME.to_owned(),
            catalog_table,
            TableKind::Catalog,
        );
        let installations = OpenTable::new(
            INSTALLATIONS_TABLE_NAME.to_owned(),
            installations_table,
            TableKind::Installations,
        );
        // System handles are seeded first, then the catalog is replayed to open
        // every declared application table before the workspace is exposed.
        let tables = catalog.table.read().entries().try_fold(
            HashMap::from([
                (TABLE_CATALOG_NAME.to_owned(), catalog.clone()),
                (INSTALLATIONS_TABLE_NAME.to_owned(), installations),
            ]),
            |mut tables, (key, cell)| {
                if let Some((name, config)) =
                    decode_catalog_row(key.into_owned(), cell.into_owned())?
                    && !is_system_table(&name)
                {
                    let path = root.join(&name);
                    tables.insert(
                        name.clone(),
                        OpenTable::new(name, Table::open(&path, config)?, TableKind::Application),
                    );
                }
                Ok::<_, Error>(tables)
            },
        )?;
        Ok(Self {
            root,
            lifecycle: Mutex::new(()),
            tables: RwLock::new(tables),
        })
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    pub(crate) fn list(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    pub(crate) fn for_each(&self, mut f: impl FnMut(&str, &Arc<OpenTable>)) {
        let tables = self.tables.read();
        for (name, table) in tables.iter() {
            f(name, table);
        }
    }

    pub(crate) fn persist(&self, barrier: zendb_types::Barrier) -> Result<()> {
        let _lifecycle = self.lifecycle.lock();
        let tables: Vec<_> = self.tables.read().values().cloned().collect();
        for table in tables {
            table.table.write().persist(barrier)?;
        }
        Ok(())
    }

    pub(crate) fn get(&self, name: &str) -> Result<Arc<OpenTable>> {
        self.tables
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.to_owned()))
    }

    pub(crate) fn apply_catalog_change(&self, change: &Change) {
        let PrimaryKey::String(name) = &change.event.primary_key else {
            return;
        };
        if !change.event.path.is_empty() {
            return;
        }
        let _lifecycle = self.lifecycle.lock();
        match &change.event.op {
            Op::Upsert {
                value: Value::Blob(blob),
            } => {
                if self.tables.read().contains_key(name) {
                    return;
                }
                let Ok(config) = blob.decode::<TableConfig>() else {
                    return;
                };
                let path = self.root.join(name);
                let table = if path.exists() {
                    Table::open(&path, config)
                } else {
                    Table::create(&path, config)
                };
                // Catalog projection is post-commit and best effort; the event
                // remains durable even when its physical table cannot be opened.
                if let Ok(table) = table {
                    let _inserted = self.tables.write().insert(
                        name.clone(),
                        OpenTable::new(name.clone(), table, TableKind::Application),
                    );
                }
            }
            Op::Delete => {
                if let Some(table) = self.tables.write().remove(name) {
                    // Drop the shared handle before removing its directory so
                    // the storage file is no longer owned by the workspace.
                    drop(table);
                    let path = self.root.join(name);
                    if path.exists() {
                        let _ = fs::remove_dir_all(path);
                    }
                }
            }
            _ => {}
        }
    }
}

fn decode_catalog_row(key: PrimaryKey, cell: Cell) -> Result<Option<(String, TableConfig)>> {
    let PrimaryKey::String(name) = key else {
        return Err(Error::CorruptTableCatalog(
            "table catalog key is not a String".to_owned(),
        ));
    };
    let blob = match cell.value {
        Some(Value::Blob(blob)) => blob,
        None => return Ok(None),
        Some(_) => {
            return Err(Error::CorruptTableCatalog(format!(
                "table catalog row {name:?} is not a Blob"
            )));
        }
    };
    let config = blob.decode().map_err(|error| {
        Error::CorruptTableCatalog(format!(
            "table catalog row {name:?} cannot be decoded: {error}"
        ))
    })?;
    Ok(Some((name, config)))
}
