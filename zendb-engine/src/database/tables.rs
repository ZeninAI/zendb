//! Table lookup and operator subscription maintenance.

use std::{fs, io, sync::Arc};

use parking_lot::RwLock;
use zendb_storage::core::traits::{Backend, DurableStorage};
use zendb_storage::frontend::table::{Table, TableConfig};

use crate::operator::worker::{OperatorInput, OperatorWorker};
use crate::{DispatchOperator, DispatchOperatorConfig, OperatorPhase, Subscription};

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
        let matching_operators: Vec<(String, D::DispatchConfig)> = operator_catalog
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
            let existing = self.operators.read().get(&op_name).cloned();
            if let Some(worker) = existing {
                let reader = table.read().consumer(worker.name())?;
                worker.attach_input(OperatorInput::new(name.to_owned(), reader));
            } else {
                let worker = self.build_worker(op_name.clone(), op_config)?;
                self.operators.write().insert(op_name, Arc::clone(&worker));
                to_spawn.push(worker);
            }
        }
        Ok(to_spawn)
    }

    /// Delete an operator's topic consumer from every cataloged table.
    ///
    /// Live readers owned by the worker must be deleted before this sweep; a
    /// topic permits only one active reader for a consumer name.
    pub(super) fn delete_operator_consumers(
        &self,
        operator: &str,
        subscriptions: &Vec<Subscription>,
    ) {
        let tables: Vec<(String, TableConfig)> = self
            .table_catalog
            .lock()
            .entries()
            .filter(|(name, _)| subscriptions.iter().any(|sub| sub.matches(name.as_ref())))
            .map(|(name, config)| (name.into_owned(), config.into_owned()))
            .collect();

        for (table_name, config) in tables {
            let result = if let Some(table) = self.tables.read().get(&table_name).cloned() {
                table
                    .read()
                    .consumer(operator)
                    .and_then(|consumer| consumer.delete())
            } else {
                let path = self.path.join(TABLES_DIR).join(&table_name);
                Table::open(&path, config).and_then(|table| {
                    table
                        .consumer(operator)
                        .and_then(|consumer| consumer.delete())
                })
            };

            if let Err(error) = result {
                log::error!(
                    "failed deleting consumer {:?} from table {:?}: {error}",
                    operator,
                    table_name
                );
            }
        }
    }
}
