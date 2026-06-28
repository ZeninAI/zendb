//! Operator lifecycle and worker management.

use std::{io, sync::Arc};

use zendb_storage::core::traits::Backend;

use crate::operator::{
    worker::{OperatorInput, OperatorWorker},
    DispatchOperator, DispatchOperatorConfig, OperatorPhase,
};

use super::{Database, OperatorEntry};

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
    pub fn operator_config(&self, name: &str) -> Option<D::DispatchConfig> {
        self.operator_catalog
            .lock()
            .get(&name.to_owned())
            .map(|entry| entry.as_ref().config.clone())
    }

    /// Return the phase and effective config of an operator, ensuring it is
    /// running unless it is in a terminal state or no matching tables are open.
    ///
    /// - If already running in memory, returns `(Active, config)` immediately.
    /// - If in the catalog as `Active`, re-opens using the stored config (a new
    ///   `config` replaces the stored one). If no matching tables are open yet,
    ///   the operator stays in the catalog without spawning - it will be started
    ///   automatically when a matching table opens.
    /// - If in a terminal state (`Finished` / `Failed`), returns the phase and
    ///   stored config without starting anything.
    /// - If not in the catalog, creates it with `config` (required for new
    ///   operators; returns an error if `config` is `None`). If no matching
    ///   tables are open yet, the operator is persisted to the catalog only and
    ///   will be started when a matching table opens.
    pub fn operator(
        self: &Arc<Self>,
        name: &str,
        config: Option<D::DispatchConfig>,
    ) -> io::Result<(OperatorPhase, D::DispatchConfig)> {
        // Fast path: already running - return its config.
        if let Some(worker) = self.operators.read().get(name).cloned() {
            return Ok((OperatorPhase::Active, worker.config.clone()));
        }

        let (phase, effective, worker_opt) = {
            let mut catalog = self.operator_catalog.lock();
            // Double-check under catalog lock
            if let Some(worker) = self.operators.read().get(name).cloned() {
                return Ok((OperatorPhase::Active, worker.config.clone()));
            }

            match catalog.get(&name.to_owned()) {
                Some(entry) if entry.phase == OperatorPhase::Active => {
                    let stored = &entry.as_ref().config;
                    let effective = match &config {
                        Some(new_config) if new_config != stored => {
                            catalog.put(
                                name.to_owned(),
                                OperatorEntry {
                                    config: new_config.clone(),
                                    phase: OperatorPhase::Active,
                                },
                            )?;
                            new_config.clone()
                        }
                        _ => stored.clone(),
                    };
                    let worker = self.build_worker(name.to_owned(), effective.clone())?;
                    if worker.inputs.lock().is_empty() {
                        // No matching tables open yet. Catalog entry is already
                        // Active - activate_table_subscribers will spawn when one opens.
                        return Ok((OperatorPhase::Active, effective));
                    }
                    self.operators
                        .write()
                        .insert(name.to_owned(), Arc::clone(&worker));
                    (OperatorPhase::Active, effective, Some(worker))
                }
                Some(entry) => {
                    let e = entry.as_ref();
                    (e.phase.clone(), e.config.clone(), None)
                }
                None => {
                    let config = config.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("operator {name:?} does not exist and no config was provided"),
                        )
                    })?;
                    let worker = self.build_worker(name.to_owned(), config.clone())?;
                    if worker.inputs.lock().is_empty() {
                        // No matching tables open yet - persist to catalog only.
                        catalog.put(
                            name.to_owned(),
                            OperatorEntry {
                                config: config.clone(),
                                phase: OperatorPhase::Active,
                            },
                        )?;
                        return Ok((OperatorPhase::Active, config));
                    }
                    if let Err(error) = catalog.put(
                        name.to_owned(),
                        OperatorEntry {
                            config: config.clone(),
                            phase: OperatorPhase::Active,
                        },
                    ) {
                        worker.delete_inputs();
                        return Err(error);
                    }
                    self.operators
                        .write()
                        .insert(name.to_owned(), Arc::clone(&worker));
                    (OperatorPhase::Active, config, Some(worker))
                }
            }
        };

        if let Some(worker) = worker_opt {
            worker.spawn(self);
        }
        Ok((phase, effective))
    }

    /// Instantiate the operator from its typed config and acquire one topic
    /// consumer per currently-open table that matches the subscription. Does
    /// NOT validate that all subscribed tables exist - in the lazy model,
    /// tables may open later.
    pub(super) fn build_worker(
        self: &Arc<Self>,
        name: String,
        config: D::DispatchConfig,
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

    /// Transition catalog phase to `Finished` or `Failed` and remove from memory.
    /// Called by the run loop on natural exit.
    pub(crate) fn retire_operator(&self, name: &str, phase: OperatorPhase) {
        let worker = self.operators.write().remove(name);

        if let Some(worker) = worker {
            worker.delete_inputs();
        }
        self.delete_operator_consumers(name);
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
}
