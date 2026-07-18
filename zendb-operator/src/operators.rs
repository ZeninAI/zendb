//! Operator lifecycle and worker management.

use std::{io, sync::Arc};

use log::{debug, info, trace};
use zendb_replication::{Table, TableConfig};
use zendb_storage::backend::_traits::{DurableStorage, ReadBackend, WriteBackend};

use crate::{
    operator::{
        worker::{OperatorInput, OperatorWorker},
        DispatchConfig, DispatchOperator, Operator, OperatorPhase, OperatorRuntimeConfig,
    },
    Subscription,
};

use crate::host::{OperatorEntry, OperatorHost, TimerKey, TABLES_DIR};

impl<D> OperatorHost<D>
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

    /// Return the persisted native execution adapter config for an operator.
    ///
    /// This is the local host catalog view and carries no distributed placement
    /// or lease meaning.
    pub fn operator_config(&self, name: &str) -> Option<D::Config> {
        self.operator_catalog
            .lock()
            .get(&name.to_owned())
            .map(|entry| entry.as_ref().config.clone())
    }

    /// Retrieve the typed facet published by a running operator.
    ///
    /// Returns an error if the operator is not currently running or has not
    /// published a facet, or if the facet type `F` does not match.
    pub fn facet<F: Send + Sync + 'static>(&self, name: &str) -> io::Result<Arc<F>> {
        let workers = self.operators.read();
        let worker = workers.get(name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("operator {name:?} is not running"),
            )
        })?;
        worker.get_facet::<F>().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "operator {name:?} has no facet or type mismatch (expected {})",
                    std::any::type_name::<F>()
                ),
            )
        })
    }

    /// Register and immediately realize a native local operator.
    ///
    /// The operator starts when at least one matching table is open and remains
    /// entirely local to this host.
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
            catalog.put(
                name.to_owned(),
                OperatorEntry {
                    config,
                    phase: OperatorPhase::Active,
                },
            )?;
            if worker.has_inputs() {
                self.operators
                    .write()
                    .insert(name.to_owned(), Arc::clone(&worker));
                Some(worker)
            } else {
                None
            }
        };

        if let Some(worker) = worker_opt {
            info!("dispatching operator {name:?} (immediate start)");
            worker.spawn(self);
        } else {
            info!("dispatching operator {name:?} (waiting for matching tables)");
        }
        Ok(())
    }

    /// Permanently cancel an operator. Live workers are asked to shut down;
    /// catalog-only operators are marked cancelled immediately.
    pub fn cancel_operator(&self, name: &str) -> io::Result<()> {
        info!("cancelling operator {name:?}");
        if let Some(worker) = self.operators.read().get(name).cloned() {
            worker.begin_shutdown(OperatorPhase::Cancelled);
            return Ok(());
        }

        let Some(config) = self.operator_config(name) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("operator {name:?} does not exist"),
            ));
        };
        self.retire_operator(
            name,
            OperatorPhase::Cancelled,
            &config.runtime_config().subscriptions,
        );
        Ok(())
    }

    /// Delete an operator from the durable catalog. Only operators in a
    /// terminal state (Finished, Failed, Cancelled) can be deleted. Active
    /// operators must be cancelled first.
    pub fn delete_terminal_operator(&self, name: &str) -> io::Result<()> {
        let mut catalog = self.operator_catalog.lock();
        let phase = catalog
            .get(&name.to_owned())
            .map(|entry| entry.as_ref().phase.clone());

        match phase {
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("operator {name:?} does not exist"),
            )),
            Some(OperatorPhase::Active) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("operator {name:?} is not in a terminal state, please cancel first"),
            )),
            Some(
                OperatorPhase::Finished | OperatorPhase::Failed { .. } | OperatorPhase::Cancelled,
            ) => {
                catalog.delete(&name.to_owned())?;
                info!("deleted terminal operator {name:?}");
                Ok(())
            }
        }
    }

    // --- Internal ---

    /// Acquire topic consumers for currently-open tables that match the
    /// subscription. The operator instance is created lazily by the run loop.
    pub(super) fn build_worker(
        self: &Arc<Self>,
        name: String,
        config: D::Config,
    ) -> io::Result<Arc<OperatorWorker<D>>> {
        let mut inputs: Vec<OperatorInput> = Vec::new();
        for table_name in self.workspace.list_open_tables() {
            if config
                .runtime_config()
                .subscriptions
                .iter()
                .any(|s| s.matches(&table_name))
            {
                let table = self.workspace.table(&table_name).open()?.get()?;
                inputs.push(OperatorInput::new(
                    table_name,
                    table.read().consumer(&name)?,
                ));
            }
        }
        debug!("built worker {name:?} with {} input(s)", inputs.len());
        Ok(OperatorWorker::new(name, config, inputs))
    }

    /// Transition catalog phase to a permanent terminal phase and remove from memory.
    /// Called by the run loop on natural finish/failure/cancellation.
    pub(crate) fn retire_operator(
        &self,
        name: &str,
        phase: OperatorPhase,
        subscriptions: &[Subscription],
    ) {
        info!("retiring operator {name:?} (phase: {phase:?})");

        // Remove from in-memory workers (drops the inputs/consumers).
        if let Some(worker) = self.operators.write().remove(name) {
            worker.delete_inputs();
        }

        // Delete consumer offsets from every matching table.
        // Hold table_catalog lock for the entire loop to serialize with
        // db.table()'s slow path — prevents two threads from opening the
        // same table file simultaneously.
        let tables: Vec<(String, TableConfig)> = self
            .workspace
            .list_catalog_tables()
            .into_iter()
            .filter(|table| subscriptions.iter().any(|sub| sub.matches(table)))
            .filter_map(|table| {
                self.workspace
                    .table_config(&table)
                    .map(|config| (table, config))
            })
            .collect();

        trace!(
            "retiring operator {name:?}: cleaning up {} consumer(s)",
            tables.len()
        );
        for (table_name, config) in tables {
            let result = if self.workspace.is_table_open(&table_name) {
                self.workspace
                    .table(&table_name)
                    .open()
                    .and_then(|handle| handle.get())
                    .and_then(|table| {
                        table
                            .read()
                            .consumer(name)
                            .and_then(|consumer| consumer.delete())
                    })
            } else {
                let path = self.workspace.path().join(TABLES_DIR).join(&table_name);
                Table::open(&path, config)
                    .and_then(|table| table.consumer(name).and_then(|consumer| consumer.delete()))
            };
            if let Err(error) = result {
                log::error!("failed deleting consumer {name:?} from table {table_name:?}: {error}");
            }
        }
        // Delete all timers belonging to this operator.
        let mut timers = self.timers.write();
        let keys: Vec<TimerKey> = timers
            .entries()
            .filter(|(key, _)| key.operator == name)
            .map(|(k, _)| k.into_owned())
            .collect();
        let num_timers = keys.len();
        for key in keys {
            let _ = timers.delete(&key);
        }
        drop(timers);

        debug!("retired operator {name:?}: cleaned up {num_timers} timer(s)");

        // Update the catalog phase LAST so external observers can rely on the
        // terminal phase as a signal that all cleanup is done.
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

    /// Remove a live worker from memory without changing its durable phase.
    /// Used when an active operator runs out of open input tables and should be
    /// respawned later if a matching table reopens.
    ///
    /// Returns `true` if the operator was suspended, `false` if it gained
    /// inputs during the race window or is being shut down by another thread.
    pub(crate) fn suspend_operator(&self, name: &str) -> bool {
        let mut operators = self.operators.write();
        if let Some(worker) = operators.get(name) {
            if worker.has_inputs() || worker.is_shutting_down() {
                return false;
            }
        }
        if operators.remove(name).is_some() {
            info!("suspended operator {name:?} (no open inputs)");
        }
        true
    }

    /// Attach a newly opened table to every matching active operator.
    pub(crate) fn activate_table_subscribers(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let matching: Vec<(String, D::Config)> = self
            .operator_catalog
            .lock()
            .entries()
            .filter_map(|(operator_name, entry)| {
                let entry = entry.as_ref();
                (entry.phase == OperatorPhase::Active
                    && entry
                        .config
                        .runtime_config()
                        .subscriptions
                        .iter()
                        .any(|subscription| subscription.matches(name)))
                .then(|| (operator_name.into_owned(), entry.config.clone()))
            })
            .collect();
        if matching.is_empty() {
            return Ok(());
        }

        let table = self.workspace.table(name).open()?.get()?;
        let mut to_spawn = Vec::new();
        for (operator_name, config) in matching {
            let mut operators = self.operators.write();
            if let Some(worker) = operators.get(&operator_name).cloned() {
                let consumer = table.read().consumer(worker.name())?;
                worker.attach_input(OperatorInput::new(name.to_owned(), consumer));
            } else {
                let worker = self.build_worker(operator_name.clone(), config)?;
                operators.insert(operator_name, Arc::clone(&worker));
                to_spawn.push(worker);
            }
        }
        for worker in to_spawn {
            worker.spawn(self);
        }
        Ok(())
    }
}
