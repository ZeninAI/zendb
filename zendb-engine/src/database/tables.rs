//! Table lookup and operator subscription maintenance.

use std::{fs, io, sync::Arc};

use parking_lot::RwLock;
use zendb_storage::core::traits::{Backend, DurableStorage};
use zendb_storage::frontend::table::{Table, TableConfig};

use crate::operator::worker::{OperatorInput, OperatorWorker};
use crate::OperatorConfig;

use super::{
    already_exists, not_found, CatalogEntry, ConcurrentTable, Database, TableHandle, TABLES_DIR,
};

impl Database {
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

        // Slow path: open/create under lifecycle lock, then spawn operators outside it.
        let (table, workers_to_spawn) = {
            let _lifecycle = self.lifecycle.lock();
            // Double-check under lifecycle lock
            if let Some(table) = self.tables.read().get(name).cloned() {
                return Ok(TableHandle::new(name, &table));
            }

            let mut catalog = self.catalog.lock();
            let (table, matching_operators) = match catalog.get(&name.to_owned()) {
                Some(entry) => match entry.as_ref() {
                    CatalogEntry::Table(saved_config) => {
                        let effective_config = match &config {
                            Some(new_config) if new_config != saved_config => {
                                catalog.put(
                                    name.to_owned(),
                                    CatalogEntry::Table(new_config.clone()),
                                )?;
                                new_config.clone()
                            }
                            _ => saved_config.clone(),
                        };
                        let path = self.path.join(TABLES_DIR).join(name);
                        let table = Arc::new(RwLock::new(Table::open(&path, effective_config)?));
                        let ops = Self::collect_matching_operators(&catalog, name);
                        (table, ops)
                    }
                    _ => return Err(already_exists("catalog resource", name)),
                },
                None => {
                    let Some(config) = config else {
                        return Err(not_found("table", name));
                    };
                    let path = self.path.join(TABLES_DIR).join(name);
                    let raw = Table::create(&path, config.clone())?;
                    catalog.put(name.to_owned(), CatalogEntry::Table(config))?;
                    let table = Arc::new(RwLock::new(raw));
                    let ops = Self::collect_matching_operators(&catalog, name);
                    (table, ops)
                }
            };
            drop(catalog);

            // Insert table BEFORE building workers so build_worker can find it.
            self.tables
                .write()
                .insert(name.to_owned(), Arc::clone(&table));
            let workers = self.activate_table_subscribers(name, &table, matching_operators)?;
            (table, workers)
        };
        // lifecycle lock released — safe to spawn (workers may call retire() immediately)

        for worker in workers_to_spawn {
            OperatorWorker::spawn(self, worker);
        }
        Ok(TableHandle::new(name, &table))
    }

    /// Scan the catalog for operator entries whose subscriptions match `table_name`.
    fn collect_matching_operators(
        catalog: &super::Catalog,
        table_name: &str,
    ) -> Vec<(String, OperatorConfig)> {
        let mut result = Vec::new();
        for (op_name, entry) in catalog.entries() {
            if let CatalogEntry::Operator(config) = entry.as_ref() {
                if config.subscriptions.iter().any(|s| s.matches(table_name)) {
                    result.push((op_name.into_owned(), config.clone()));
                }
            }
        }
        result
    }

    /// Wire a newly opened table into matching operators. For operators already
    /// running, creates a consumer and attaches. For catalog-only operators,
    /// builds them (but does NOT spawn — caller spawns after releasing the
    /// lifecycle lock to avoid deadlock with `retire()`).
    fn activate_table_subscribers(
        self: &Arc<Self>,
        name: &str,
        table: &ConcurrentTable,
        matching_operators: Vec<(String, OperatorConfig)>,
    ) -> io::Result<Vec<Arc<OperatorWorker>>> {
        let mut to_spawn = Vec::new();
        for (op_name, op_config) in matching_operators {
            let existing = self.operators.read().get(&op_name).cloned();
            if let Some(worker) = existing {
                // Already running — just attach this table
                let reader = table.read().consumer(&worker.name)?;
                worker.inputs.lock().push(OperatorInput {
                    table_name: name.to_owned(),
                    reader,
                });
            } else {
                // Not running — build and register, defer spawn
                let worker = self.build_worker(op_name.clone(), op_config)?;
                self.operators.write().insert(op_name, Arc::clone(&worker));
                to_spawn.push(worker);
            }
        }
        Ok(to_spawn)
    }

    fn prepare_drop_table(&self, name: &str) -> io::Result<Vec<Arc<OperatorWorker>>> {
        if !self.tables.read().contains_key(name) {
            // Table exists in catalog but has never been opened; no operators to detach
            if !self.catalog.lock().contains(&name.to_owned()) {
                return Err(not_found("table", name));
            }
            return Ok(Vec::new());
        }

        let workers: Vec<_> = self.operators.read().values().cloned().collect();
        let mut orphaned = Vec::new();
        for worker in &workers {
            let removed = self.detach_table_from_worker(name, worker);
            // An operator is "orphaned" (nothing left to subscribe to) only if
            // none of its subscriptions are wildcards and all its inputs are gone.
            if removed
                && !worker.config.subscriptions.iter().any(|s| s.is_wildcard())
                && worker.inputs.lock().is_empty()
            {
                worker.stop();
                orphaned.push(Arc::clone(worker));
            }
        }
        Ok(orphaned)
    }

    /// Guard against live handles (strong-count check), then remove the table
    /// from the in-memory map, catalog, and disk.
    fn finish_drop_table(&self, name: &str) -> io::Result<()> {
        let mut tables = self.tables.write();
        if let Some(table) = tables.get(name) {
            if Arc::strong_count(table) != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("table {name:?} still has active handles"),
                ));
            }
            tables.remove(name);
        }
        drop(tables);

        self.catalog.lock().delete(&name.to_owned())?;
        let _ = fs::remove_dir_all(self.path.join(TABLES_DIR).join(name));
        Ok(())
    }

    /// Two-phase drop: collect orphaned workers under the lifecycle lock, wait
    /// for them to finish outside it, then physically remove the table.
    pub fn drop_table(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let orphaned = {
            let _lifecycle = self.lifecycle.lock();
            self.prepare_drop_table(name)?
        };
        for worker in orphaned {
            worker.wait_finished();
        }

        let _lifecycle = self.lifecycle.lock();
        self.finish_drop_table(name)
    }

    /// Evict a table from memory without removing it from the catalog or disk.
    /// Orphaned operators (those that lose all inputs) are evicted with their
    /// catalog entries intact so they can be re-opened later. Blocks until all
    /// evicted workers finish.
    pub fn close_table(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let orphaned = {
            let _lifecycle = self.lifecycle.lock();
            if !self.tables.read().contains_key(name) {
                if !self.catalog.lock().contains(&name.to_owned()) {
                    return Err(not_found("table", name));
                }
                return Ok(()); // Table not open; nothing to evict
            }

            let workers: Vec<_> = self.operators.read().values().cloned().collect();
            let mut evicted = Vec::new();
            for worker in &workers {
                let removed = self.detach_table_from_worker(name, worker);
                if removed
                    && !worker.config.subscriptions.iter().any(|s| s.is_wildcard())
                    && worker.inputs.lock().is_empty()
                {
                    if let Some(w) = self.evict_operator_locked(&worker.name) {
                        evicted.push(w);
                    }
                }
            }
            evicted
        };
        for worker in orphaned {
            worker.wait_finished();
        }

        let _lifecycle = self.lifecycle.lock();
        let mut tables = self.tables.write();
        let table = tables.get(name).ok_or_else(|| not_found("table", name))?;
        if Arc::strong_count(table) != 1 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("table {name:?} still has active handles"),
            ));
        }
        tables.remove(name);
        Ok(())
    }

    /// Remove the input for `name` from the worker's list, delete its consumer,
    /// and return whether one was found.
    fn detach_table_from_worker(&self, name: &str, worker: &OperatorWorker) -> bool {
        let removed = {
            let mut inputs = worker.inputs.lock();
            let pos = inputs.iter().position(|i| i.table_name == name);
            pos.map(|i| inputs.remove(i))
        };

        if let Some(input) = removed {
            if let Err(error) = input.reader.delete() {
                log::error!(
                    "failed deleting consumer {:?} from table {name:?}: {error}",
                    worker.name
                );
            }
            true
        } else {
            false
        }
    }
}
