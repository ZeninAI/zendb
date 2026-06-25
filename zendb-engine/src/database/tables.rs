//! Table lookup and operator subscription maintenance.

use std::{fs, io, sync::Arc};

use parking_lot::RwLock;
use zendb_storage::core::traits::{Backend, DurableStorage};
use zendb_storage::frontend::table::{Table, TableConfig};

use crate::operator::worker::{OperatorInput, OperatorWorker};

use super::{not_found, CatalogEntry, ConcurrentTable, Database, TableHandle, TABLES_DIR};

impl Database {
    /// Open an existing table or create it with `config`. The lifecycle lock is
    /// taken only on the creation path; reads against an already-open table are
    /// lock-free because the tables map is exhaustive after `open()`.
    pub fn table(
        self: &Arc<Self>,
        name: &str,
        config: Option<TableConfig>,
    ) -> io::Result<TableHandle> {
        let _lifecycle = config.is_some().then(|| self.lifecycle.lock());

        if let Some(table) = self.tables.read().get(name).cloned() {
            return Ok(TableHandle::new(name, &table));
        }

        // The tables map is exhaustive: `open()` eagerly loads every catalog
        // table, so a name missing here means the table does not exist yet.
        let Some(config) = config else {
            return Err(not_found("table", name));
        };
        let mut catalog = self.catalog.lock();
        let path = self.path.join(TABLES_DIR).join(name);
        let raw = Table::create(&path, config.clone())?;
        let table = Arc::new(RwLock::new(raw));
        catalog.put(name.to_owned(), CatalogEntry::Table(config))?;
        self.attach_table_to_all_subscribers(name, &table)?;
        self.tables
            .write()
            .insert(name.to_owned(), Arc::clone(&table));
        Ok(TableHandle::new(name, &table))
    }

    /// Detach the table from all workers and collect orphaned operators (those
    /// whose only inputs were on this table). Split from `finish_drop_table` so
    /// callers can wait for orphan workers to stop outside the lifecycle lock.
    fn prepare_drop_table(&self, name: &str) -> io::Result<Vec<Arc<OperatorWorker>>> {
        if !self.tables.read().contains_key(name) {
            return Err(not_found("table", name));
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
        let table = tables.get(name).ok_or_else(|| not_found("table", name))?;
        if Arc::strong_count(table) != 1 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("table {name:?} still has active handles"),
            ));
        }
        let table = tables.remove(name).unwrap();
        drop(tables);

        self.catalog.lock().delete(&name.to_owned())?;
        drop(table);
        fs::remove_dir_all(self.path.join(TABLES_DIR).join(name))?;
        Ok(())
    }

    /// Wire a newly created table into every operator whose subscription pattern
    /// matches its name, creating a dedicated topic consumer for each.
    fn attach_table_to_all_subscribers(
        &self,
        name: &str,
        table: &ConcurrentTable,
    ) -> io::Result<()> {
        for worker in self.operators.read().values() {
            if worker
                .config
                .subscriptions
                .iter()
                .any(|s| s.matches(name))
            {
                let reader = table.read().consumer(&worker.name)?;
                worker.inputs.lock().push(OperatorInput {
                    table_name: name.to_owned(),
                    reader,
                });
            }
        }
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
}
