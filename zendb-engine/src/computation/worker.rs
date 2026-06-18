//! Poll-based computation execution.

use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
};

use parking_lot::Mutex;
use zendb_storage::core::topic::TopicConsumer;

use super::{Change, Computation, ComputationConfig, ComputationContext, ComputationStatus};
use crate::{database::DatabaseInner, runtime::Executor};

pub(crate) struct ComputationInput {
    pub(crate) table_name: String,
    pub(crate) reader: TopicConsumer<Change>,
}

pub(crate) struct ComputationWorker {
    pub(crate) name: String,
    pub(crate) config: ComputationConfig,
    pub(crate) inputs: Mutex<Vec<ComputationInput>>,
    pub(crate) stopped: AtomicBool,
}

impl ComputationWorker {
    pub(crate) fn new(
        name: String,
        config: ComputationConfig,
        inputs: Vec<ComputationInput>,
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            config,
            inputs: Mutex::new(inputs),
            stopped: AtomicBool::new(false),
        })
    }

    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    pub(crate) fn spawn(
        database: &Arc<DatabaseInner>,
        worker: Arc<Self>,
        computation: Box<dyn Computation>,
    ) {
        let executor = Arc::clone(&database.executor);
        let database = Arc::downgrade(database);
        executor
            .clone()
            .spawn(Box::pin(Self::run(database, executor, worker, computation)));
    }

    async fn run(
        database: Weak<DatabaseInner>,
        executor: Arc<dyn Executor>,
        worker: Arc<Self>,
        mut computation: Box<dyn Computation>,
    ) {
        let context = ComputationContext {
            database: database.clone(),
            computation: worker.name.clone(),
        };
        if let Err(error) = computation.open(context).await {
            log::error!("failed opening computation {:?}: {error}", worker.name);
            return;
        }

        while !worker.stopped.load(Ordering::Acquire) {
            let Some(inner) = database.upgrade() else {
                return;
            };
            let changes = worker.poll(inner.config.computation_poll_size);
            drop(inner);
            if changes.is_empty() {
                executor.idle().await;
                continue;
            }

            let context = ComputationContext {
                database: database.clone(),
                computation: worker.name.clone(),
            };
            match computation.process(changes, context).await {
                Ok(ComputationStatus::Continue) => {
                    if let Err(error) = worker.commit() {
                        log::error!("failed to commit computation {:?}: {error}", worker.name);
                        if let Err(reset_error) = worker.reset() {
                            log::error!(
                                "failed to reset computation {:?}: {reset_error}",
                                worker.name
                            );
                        }
                        executor.idle().await;
                    }
                }
                Ok(ComputationStatus::Finish) => {
                    worker.stop();
                    if let Some(inner) = database.upgrade() {
                        let _lifecycle = inner.lifecycle.lock();
                        if let Err(error) = inner.drop_computation_locked(&worker.name) {
                            log::error!("failed to finish computation {:?}: {error}", worker.name);
                        }
                    }
                    return;
                }
                Err(error) => {
                    log::error!("computation {:?} failed: {error}", worker.name);
                    if let Err(reset_error) = worker.reset() {
                        log::error!(
                            "failed to reset computation {:?}: {reset_error}",
                            worker.name
                        );
                    }
                    executor.idle().await;
                }
            }
        }
    }

    fn poll(&self, limit: usize) -> Vec<Change> {
        let mut changes = Vec::with_capacity(limit);
        let mut inputs = self.inputs.lock();
        'poll: while changes.len() < limit {
            let mut progressed = false;
            for input in inputs.iter_mut() {
                if changes.len() == limit {
                    break;
                }
                if let Some(change) = input.reader.next() {
                    match change {
                        Ok(change) => {
                            changes.push(change);
                            progressed = true;
                        }
                        Err(error) => {
                            log::error!(
                                "failed reading topic {:?} for computation {:?}: {error}",
                                input.table_name,
                                self.name
                            );
                            break 'poll;
                        }
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        changes
    }

    fn commit(&self) -> io::Result<()> {
        for input in self.inputs.lock().iter_mut() {
            input.reader.commit()?;
        }
        Ok(())
    }

    fn reset(&self) -> io::Result<()> {
        for input in self.inputs.lock().iter_mut() {
            input.reader.reset()?;
        }
        Ok(())
    }
}
