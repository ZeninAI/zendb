//! Table lookup and operator subscription maintenance.

use std::{fs, io, sync::Arc};

use parking_lot::RwLock;
use zendb_storage::core::traits::{Backend, DurableStorage};
use zendb_storage::frontend::table::{Table, TableConfig};

use crate::operator::worker::{OperatorInput, OperatorWorker};
use crate::{DispatchConfig, DispatchOperator, OperatorPhase};

use super::{ConcurrentTable, Database, TableHandle, TABLES_DIR};

impl<D> Database<D>
where
    D: DispatchOperator,
{
    /// Return `true` if a table exists in the durable table catalog.
    pub fn contains_table(&self, name: &str) -> bool {
        self.table_catalog.lock().contains(&name.to_owned())
    }

    /// Return `true` if a table is currently loaded in memory.
    pub fn is_table_open(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    /// List every table known to the durable table catalog.
    pub fn list_tables(&self) -> Vec<String> {
        self.table_catalog
            .lock()
            .keys()
            .map(|name| name.into_owned())
            .collect()
    }

    /// List every table currently loaded in memory.
    pub fn list_open_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    /// Return the persisted config for a table, if the catalog contains one.
    pub fn table_config(&self, name: &str) -> Option<TableConfig> {
        self.table_catalog
            .lock()
            .get(&name.to_owned())
            .map(|config| config.into_owned())
    }

    /// Remove an open table from the in-memory cache and notify live operators
    /// that the input closed. The durable table remains in the catalog and can
    /// be reopened later with [`Database::table`].
    pub fn close_table(&self, name: &str) -> bool {
        let removed = self.tables.write().remove(name).is_some();
        if !removed {
            return false;
        }

        let workers: Vec<_> = self.operators.read().values().cloned().collect();
        for worker in workers {
            worker.detach_input(name);
        }
        true
    }

    /// Delete a table entirely: evict from memory, notify operators, remove
    /// from the catalog, and delete its on-disk directory.
    ///
    /// Running operators that subscribe to this table receive an `InputClosed`
    /// event. If that was their only input, they will suspend (remain active in
    /// the catalog but not running). The operator's consumer offsets for this
    /// table are destroyed along with the table files.
    ///
    /// Returns `Ok(true)` if the table existed, `Ok(false)` if it was not in
    /// the catalog.
    pub fn delete_table(&self, name: &str) -> io::Result<bool> {
        // 1. Evict from memory and notify running operators.
        let was_open = self.tables.write().remove(name).is_some();
        if was_open {
            let workers: Vec<_> = self.operators.read().values().cloned().collect();
            for worker in workers {
                worker.detach_input(name);
            }
        }

        // 2. Remove from catalog.
        if !self.table_catalog.lock().delete(&name.to_owned())? {
            return Ok(false);
        }

        // 3. Delete on-disk files (includes all consumer offsets).
        let path = self.path.join(TABLES_DIR).join(name);
        if path.exists() {
            fs::remove_dir_all(&path)?;
        }
        Ok(true)
    }

    /// Return an open table, opening it lazily from the catalog or creating it
    /// with `config`. If the table is in the catalog and a different `config` is
    /// supplied, the catalog is updated before opening. Automatically starts
    /// catalog operators that subscribe to this table.
    pub fn table(
        self: &Arc<Self>,
        name: &str,
        config: Option<TableConfig>,
    ) -> io::Result<TableHandle> {
        // Fast path: already open
        if let Some(table) = self.tables.read().get(name).cloned() {
            return Ok(TableHandle::new(name, &table));
        }

        // Slow path: open/create under table catalog lock, then spawn operators outside it.
        let (table, workers_to_spawn) = {
            let mut table_catalog = self.table_catalog.lock();
            // Double-check under catalog lock
            if let Some(table) = self.tables.read().get(name).cloned() {
                return Ok(TableHandle::new(name, &table));
            }

            let table = match table_catalog.get(&name.to_owned()) {
                Some(saved_config) => {
                    let saved_config = saved_config.as_ref();
                    let effective_config = match &config {
                        Some(new_config) if new_config != saved_config => {
                            table_catalog.put(name.to_owned(), new_config.clone())?;
                            new_config.clone()
                        }
                        _ => saved_config.clone(),
                    };
                    let path = self.path.join(TABLES_DIR).join(name);
                    Arc::new(RwLock::new(Table::open(&path, effective_config)?))
                }
                None => {
                    let config = config.unwrap_or_default();
                    let path = self.path.join(TABLES_DIR).join(name);
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    let raw = Table::create(&path, config.clone())?;
                    table_catalog.put(name.to_owned(), config)?;
                    Arc::new(RwLock::new(raw))
                }
            };
            // Insert table BEFORE building workers so build_worker can find it.
            self.tables
                .write()
                .insert(name.to_owned(), Arc::clone(&table));
            let workers = self.activate_table_subscribers(name, &table)?;
            (table, workers)
        };
        // Catalog locks released; safe to spawn (workers may call retire() immediately).

        for worker in workers_to_spawn {
            worker.spawn(self);
        }
        Ok(TableHandle::new(name, &table))
    }

    /// Wire a newly opened table into matching operators. For operators already
    /// running, creates a consumer and attaches. For catalog-only active operators,
    /// builds them and returns them for spawning after catalog locks are released.
    fn activate_table_subscribers(
        self: &Arc<Self>,
        name: &str,
        table: &ConcurrentTable,
    ) -> io::Result<Vec<Arc<OperatorWorker<D>>>> {
        let operator_catalog = self.operator_catalog.lock(); // Hold this long for the duration of fn avoid race conditions
        let matching_operators: Vec<(String, D::Config)> = operator_catalog
            .entries()
            .filter_map(|(op_name, entry)| {
                let entry = entry.as_ref();
                if entry.phase == OperatorPhase::Active
                    && entry
                        .config
                        .runtime_config()
                        .subscriptions
                        .iter()
                        .any(|s| s.matches(name))
                {
                    Some((op_name.into_owned(), entry.config.clone()))
                } else {
                    None
                }
            })
            .collect();

        let mut to_spawn = Vec::new();
        for (op_name, op_config) in matching_operators {
            // Hold the write lock to serialize with suspend_operator — prevents
            // attaching to a worker that is concurrently being removed.
            let mut operators = self.operators.write();
            if let Some(worker) = operators.get(&op_name).cloned() {
                let reader = table.read().consumer(worker.name())?;
                worker.attach_input(OperatorInput::new(name.to_owned(), reader));
            } else {
                let worker = self.build_worker(op_name.clone(), op_config)?;
                operators.insert(op_name, Arc::clone(&worker));
                to_spawn.push(worker);
            }
        }
        Ok(to_spawn)
    }
}
