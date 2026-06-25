//! Operator lifecycle and worker management.

use std::{io, sync::Arc};

use zendb_storage::core::traits::Backend;

use crate::operator::{
    worker::{OperatorInput, OperatorWorker},
    OperatorConfig,
};

use super::{already_exists, not_found, CatalogEntry, Database};

impl Database {
    /// Register a new operator: persist to catalog, build the worker, and spawn
    /// the run loop.
    pub fn register_operator(
        self: &Arc<Self>,
        name: &str,
        config: OperatorConfig,
    ) -> io::Result<()> {
        let worker = {
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
            worker
        };
        OperatorWorker::spawn(self, worker);
        Ok(())
    }

    /// Start an operator that exists in the catalog but is not currently running.
    /// Only creates consumers for tables that are already open; remaining tables
    /// will attach when they are opened later.
    pub fn open_operator(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let worker = {
            let _lifecycle = self.lifecycle.lock();
            if self.operators.read().contains_key(name) {
                return Err(already_exists("operator", name));
            }
            let config = {
                let catalog = self.catalog.lock();
                match catalog.get(&name.to_owned()) {
                    Some(entry) => match entry.as_ref() {
                        CatalogEntry::Operator(config) => config.clone(),
                        _ => return Err(already_exists("catalog resource", name)),
                    },
                    None => return Err(not_found("operator", name)),
                }
            };
            let worker = self.build_worker(name.to_owned(), config)?;
            self.operators
                .write()
                .insert(name.to_owned(), Arc::clone(&worker));
            worker
        };
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

    /// Instantiate the operator via the registry and acquire one topic consumer
    /// per currently-open table that matches the subscription. Does NOT validate
    /// that all subscribed tables exist — in the lazy model, tables may open later.
    pub(super) fn build_worker(
        self: &Arc<Self>,
        name: String,
        config: OperatorConfig,
    ) -> io::Result<Arc<OperatorWorker>> {
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

    /// Remove an operator from the in-memory map and mark it evicted, preserving
    /// its catalog entry. Caller must hold the lifecycle lock.
    pub(crate) fn evict_operator_locked(&self, name: &str) -> Option<Arc<OperatorWorker>> {
        let worker = self.operators.write().remove(name)?;
        worker.mark_evicted();
        worker.stop();
        worker.delete_inputs();
        self.sweep_operator_timers(name);
        Some(worker)
    }

    /// Evict the operator from memory while keeping its catalog entry intact.
    /// Blocks until the run loop exits.
    pub fn close_operator(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let worker = {
            let _lifecycle = self.lifecycle.lock();
            self.evict_operator_locked(name)
                .ok_or_else(|| not_found("operator", name))?
        };
        worker.wait_finished();
        Ok(())
    }
}
