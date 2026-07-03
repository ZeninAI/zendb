//! Operator lifecycle and worker management.

use std::{io, sync::Arc};

use zendb_storage::core::traits::{Backend, DurableStorage};
use zendb_storage::frontend::table::{Table, TableConfig};

use crate::{
    operator::{
        worker::{OperatorInput, OperatorWorker},
        DispatchConfig, DispatchOperator, Operator, OperatorPhase, OperatorRuntimeConfig,
    },
    Subscription,
};

use super::{Database, OperatorEntry, TimerKey, TABLES_DIR};

impl<D> Database<D>
where
    D: DispatchOperator,
{
    /// Return `true` if an operator exists in the durable operator catalog.
    pub fn contains_operator(&self, name: &str) -> bool {
        self.operator_catalog.lock().contains(&name.to_owned())
    }

    /// Return `true` if an operator is currently loaded in memory.
    pub fn is_operator_open(&self, name: &str) -> bool {
        self.operators.read().contains_key(name)
    }

    /// List every operator known to the durable operator catalog.
    pub fn list_operators(&self) -> Vec<String> {
        self.operator_catalog
            .lock()
            .keys()
            .map(|name| name.into_owned())
            .collect()
    }

    /// List every operator currently loaded in memory.
    pub fn list_open_operators(&self) -> Vec<String> {
        self.operators.read().keys().cloned().collect()
    }

    /// Return the persisted phase for an operator, if the catalog contains one.
    pub fn operator_phase(&self, name: &str) -> Option<OperatorPhase> {
        self.operator_catalog
            .lock()
            .get(&name.to_owned())
            .map(|entry| entry.as_ref().phase.clone())
    }

    /// Return the persisted config for an operator, if the catalog contains one.
    pub fn operator_config(&self, name: &str) -> Option<D::Config> {
        self.operator_catalog
            .lock()
            .get(&name.to_owned())
            .map(|entry| entry.as_ref().config.clone())
    }

    /// Register a new operator. If matching tables are already open it is
    /// spawned immediately; otherwise it is persisted and will be spawned when
    /// a matching table opens. Returns an error if an operator with `name`
    /// already exists in the durable catalog.
    pub fn dispatch_operator<V>(
        self: &Arc<Self>,
        name: &str,
        config: V::Config,
        runtime_config: OperatorRuntimeConfig,
    ) -> io::Result<()>
    where
        V: Operator,
    {
        let config = D::Config::new::<V>(config, runtime_config)?;
        let worker_opt = {
            let mut catalog = self.operator_catalog.lock();
            if catalog.contains(&name.to_owned()) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("operator {name:?} already exists"),
                ));
            }

            let worker = self.build_worker(name.to_owned(), config.clone())?;
            if !worker.has_inputs() {
                catalog.put(
                    name.to_owned(),
                    OperatorEntry {
                        config,
                        phase: OperatorPhase::Active,
                    },
                )?;
                return Ok(());
            }
            catalog.put(
                name.to_owned(),
                OperatorEntry {
                    config,
                    phase: OperatorPhase::Active,
                },
            )?;
            self.operators
                .write()
                .insert(name.to_owned(), Arc::clone(&worker));
            Some(worker)
        };

        if let Some(worker) = worker_opt {
            worker.spawn(self);
        }
        Ok(())
    }

    /// Instantiate the operator from its typed config and acquire one topic
    /// consumer per currently-open table that matches the subscription. Does
    /// NOT validate that all subscribed tables exist - in the lazy model,
    /// tables may open later.
    pub(super) fn build_worker(
        self: &Arc<Self>,
        name: String,
        config: D::Config,
    ) -> io::Result<Arc<OperatorWorker<D>>> {
        let instance = D::new(&config)?;
        let mut inputs: Vec<OperatorInput> = Vec::new();
        for (table_name, table) in self.tables.read().iter() {
            if config
                .runtime_config()
                .subscriptions
                .iter()
                .any(|s| s.matches(table_name))
            {
                inputs.push(OperatorInput::new(
                    table_name.clone(),
                    table.read().consumer(&name)?,
                ));
            }
        }
        Ok(OperatorWorker::new(name, config, inputs, instance))
    }

    /// Transition catalog phase to `Finished` or `Failed` and remove from memory.
    /// Called by the run loop on natural exit.
    pub(crate) fn retire_operator(
        &self,
        name: &str,
        phase: OperatorPhase,
        subscriptions: &Vec<Subscription>,
    ) {
        let worker = self.operators.write().remove(name);

        if let Some(worker) = worker {
            worker.delete_inputs();
        }
        self.delete_operator_consumers(name, subscriptions);
        self.cancel_operator_timers(name);

        if let Err(error) = self
            .operator_catalog
            .lock()
            .update(&name.to_owned(), |entry| {
                entry.map(|entry| OperatorEntry {
                    config: entry.config,
                    phase,
                })
            })
        {
            log::error!("failed updating catalog phase for operator {name:?}: {error}");
        }
    }

    /// Delete an operator's topic consumer from every cataloged table.
    ///
    /// Live readers owned by the worker must be deleted before this sweep; a
    /// topic permits only one active reader for a consumer name.
    fn delete_operator_consumers(&self, operator: &str, subscriptions: &Vec<Subscription>) {
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

    /// Delete every timer belonging to `operator` from the store.
    /// Called on operator retirement to prevent stale timers from
    /// accumulating.
    fn cancel_operator_timers(&self, operator: &str) {
        let mut timers = self.timers.write();
        let keys: Vec<TimerKey> = timers
            .entries()
            .filter(|(key, _)| key.operator == operator)
            .map(|(k, _)| k.into_owned())
            .collect();
        for key in keys {
            let _ = timers.delete(&key);
        }
    }
}
