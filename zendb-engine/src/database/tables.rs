//! Table lookup and operator subscription maintenance.

use std::{fs, io, sync::Arc};

use zendb_storage::core::traits::{Backend, DurableStorage};
use zendb_storage::frontend::table::Table;

use crate::operator::{
    worker::{OperatorInput, OperatorWorker},
    Subscription,
};

use super::{already_exists, not_found, CatalogEntry, ConcurrentTable, DatabaseInner, TABLES_DIR};

impl DatabaseInner {
    pub(crate) fn table(
        self: &Arc<Self>,
        name: &str,
        config: Option<crate::TableConfig>,
    ) -> io::Result<ConcurrentTable> {
        if let Some(table) = self.tables.read().get(name).cloned() {
            return Ok(table);
        }

        let mut catalog = self.catalog.lock();
        let table = match catalog.get(&name.to_owned()) {
            Some(entry) => match entry.as_ref() {
                CatalogEntry::Table(config) => Arc::new(parking_lot::RwLock::new(Table::open(
                    &self.path.join(TABLES_DIR).join(name),
                    config.clone(),
                )?)),
                _ => return Err(already_exists("catalog resource", name)),
            },
            None => {
                let Some(config) = config else {
                    return Err(not_found("table", name));
                };
                let path = self.path.join(TABLES_DIR).join(name);
                let raw = Table::create(&path, config.clone())?;
                let table = Arc::new(parking_lot::RwLock::new(raw));
                catalog.put(name.to_owned(), CatalogEntry::Table(config))?;
                self.attach_table_to_all_subscribers(name, &table)?;
                table
            }
        };

        self.tables
            .write()
            .insert(name.to_owned(), Arc::clone(&table));
        Ok(table)
    }

    pub(crate) fn prepare_drop_table(&self, name: &str) -> io::Result<Vec<Arc<OperatorWorker>>> {
        if !self.tables.read().contains_key(name) {
            return Err(not_found("table", name));
        }

        let workers: Vec<_> = self.operators.read().values().cloned().collect();
        let mut orphaned_names = Vec::new();
        for worker in &workers {
            let removed = self.detach_table_from_worker(name, worker);
            if removed
                && !worker
                    .config
                    .subscriptions
                    .iter()
                    .any(|subscription| matches!(subscription, Subscription::AllTables))
                && worker.inputs.lock().is_empty()
            {
                orphaned_names.push(worker.name.clone());
            }
        }

        let mut orphaned = Vec::new();
        for operator in orphaned_names {
            if let Some(worker) = self.retire_operator_locked(&operator)? {
                orphaned.push(worker);
            }
        }
        Ok(orphaned)
    }

    pub(crate) fn finish_drop_table(&self, name: &str) -> io::Result<()> {
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

    pub(super) fn attach_table_to_all_subscribers(
        &self,
        name: &str,
        table: &ConcurrentTable,
    ) -> io::Result<()> {
        for worker in self.operators.read().values() {
            if worker
                .config
                .subscriptions
                .iter()
                .any(|subscription| matches!(subscription, Subscription::AllTables))
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

    fn detach_table_from_worker(&self, name: &str, worker: &OperatorWorker) -> bool {
        let removed = {
            let mut inputs = worker.inputs.lock();
            let mut retained = Vec::with_capacity(inputs.len());
            let mut removed = Vec::new();
            for input in std::mem::take(&mut *inputs) {
                if input.table_name == name {
                    removed.push(input);
                } else {
                    retained.push(input);
                }
            }
            *inputs = retained;
            removed
        };
        let removed_any = !removed.is_empty();

        for input in removed {
            if let Err(error) = input.reader.delete() {
                log::error!(
                    "failed deleting consumer {:?} from table {name:?}: {error}",
                    worker.name
                );
            }
        }

        removed_any
    }
}
