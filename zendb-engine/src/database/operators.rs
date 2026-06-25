//! Operator lifecycle and worker management.

use std::{io, sync::Arc};

use zendb_storage::core::traits::Backend;

use crate::operator::{
    worker::{OperatorInput, OperatorWorker},
    OperatorConfig,
};

use super::{already_exists, not_found, CatalogEntry, Database};

impl Database {
    /// Register a new operator: check catalog uniqueness, build the worker, persist
    /// to catalog (rolling back worker inputs on failure), then spawn the run loop.
    pub fn register_operator(
        self: &Arc<Self>,
        name: &str,
        config: OperatorConfig,
    ) -> io::Result<()> {
        let _lifecycle = self.lifecycle.lock();
        if self.catalog.lock().contains(&name.to_owned()) {
            return Err(already_exists("catalog resource", name));
        }

        let worker = self.build_worker(name.to_owned(), config.clone())?;
        if let Err(error) = self
            .catalog
            .lock()
            .put(name.to_owned(), CatalogEntry::Operator(config))
        {
            worker.delete_inputs();
            return Err(error);
        }
        self.operators
            .write()
            .insert(name.to_owned(), Arc::clone(&worker));
        OperatorWorker::spawn(self, worker);
        Ok(())
    }

    /// Retire the operator under the lifecycle lock, then block until its
    /// run loop has stopped.
    pub fn drop_operator(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let worker = {
            let _lifecycle = self.lifecycle.lock();
            self.deregister_operator(name)?
                .ok_or_else(|| not_found("operator", name))?
        };
        worker.wait_finished();
        Ok(())
    }

    /// Validate subscriptions, instantiate the operator via the registry, and
    /// acquire one topic consumer per matched table. Rolls back consumers on error.
    pub(super) fn build_worker(
        self: &Arc<Self>,
        name: String,
        config: OperatorConfig,
    ) -> io::Result<Arc<OperatorWorker>> {
        for subscription in &config.subscriptions {
            if !subscription.is_wildcard() && !self.tables.read().contains_key(&subscription.0) {
                return Err(not_found("subscribed table", &subscription.0));
            }
        }

        let instance = self
            .registry
            .create_operator(&config.implementation, &config.configuration)?;
        let mut inputs: Vec<OperatorInput> = Vec::new();
        for (table_name, table) in self.tables.read().iter() {
            if config
                .subscriptions
                .iter()
                .any(|s| s.matches(table_name))
            {
                let reader = match table.read().consumer(&name) {
                    Ok(reader) => reader,
                    Err(error) => {
                        for input in inputs {
                            let _ = input.reader.delete();
                        }
                        return Err(error);
                    }
                };
                inputs.push(OperatorInput {
                    table_name: table_name.clone(),
                    reader,
                });
            }
        }
        Ok(OperatorWorker::new(name, config, inputs, instance))
    }

    /// Signal stop, delete inputs, sweep pending timers, and remove the operator
    /// from the in-memory map and catalog. Caller must hold the lifecycle lock.
    pub(crate) fn deregister_operator(
        &self,
        name: &str,
    ) -> io::Result<Option<Arc<OperatorWorker>>> {
        let worker = self.operators.write().remove(name);
        if let Some(worker) = &worker {
            worker.stop();
            worker.delete_inputs();
            self.sweep_operator_timers(name);
        }

        let mut catalog = self.catalog.lock();
        match catalog.get(&name.to_owned()) {
            Some(entry) if matches!(entry.as_ref(), CatalogEntry::Operator(_)) => {
                catalog.delete(&name.to_owned())?;
            }
            Some(_) => return Err(already_exists("catalog resource", name)),
            None => {}
        }

        Ok(worker)
    }
}
