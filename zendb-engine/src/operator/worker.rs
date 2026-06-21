//! Poll-based operator execution.

use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
};

use parking_lot::{Condvar, Mutex};
use zendb_storage::core::topic::TopicConsumer;

use super::{Change, Operator, OperatorConfig, OperatorStatus};
use crate::{runtime::Executor, Database};

pub(crate) struct OperatorInput {
    pub(crate) table_name: String,
    pub(crate) reader: TopicConsumer<Change>,
}

pub(crate) struct OperatorWorker {
    pub(crate) name: String,
    pub(crate) config: OperatorConfig,
    pub(crate) inputs: Mutex<Vec<OperatorInput>>,
    pub(crate) stopped: AtomicBool,
    finished: Mutex<bool>,
    finished_signal: Condvar,
}

impl OperatorWorker {
    pub(crate) fn new(
        name: String,
        config: OperatorConfig,
        inputs: Vec<OperatorInput>,
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            config,
            inputs: Mutex::new(inputs),
            stopped: AtomicBool::new(false),
            finished: Mutex::new(false),
            finished_signal: Condvar::new(),
        })
    }

    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    pub(crate) fn wait_finished(&self) {
        let mut finished = self.finished.lock();
        while !*finished {
            self.finished_signal.wait(&mut finished);
        }
    }

    fn mark_finished(&self) {
        *self.finished.lock() = true;
        self.finished_signal.notify_all();
    }

    pub(crate) fn spawn(database: &Arc<Database>, worker: Arc<Self>, operator: Box<dyn Operator>) {
        let executor = Arc::clone(&database.inner.executor);
        let database = Arc::downgrade(database);
        executor
            .clone()
            .spawn(Box::pin(Self::run(database, executor, worker, operator)));
    }

    async fn run(
        database: Weak<Database>,
        executor: Arc<dyn Executor>,
        worker: Arc<Self>,
        mut operator: Box<dyn Operator>,
    ) {
        if let Err(error) = operator.open(database.clone()).await {
            log::error!("failed opening operator {:?}: {error}", worker.name);
            if let Some(database) = database.upgrade() {
                let _lifecycle = database.inner.lifecycle.lock();
                if let Err(error) = database.inner.finish_operator_locked(&worker.name) {
                    log::error!(
                        "failed to clean up operator {:?} after open error: {error}",
                        worker.name
                    );
                }
            }
            worker.mark_finished();
            return;
        }

        while !worker.stopped.load(Ordering::Acquire) {
            let Some(database_handle) = database.upgrade() else {
                worker.mark_finished();
                return;
            };
            let changes = worker.poll(database_handle.inner.config.operator_poll_size);
            drop(database_handle);
            if changes.is_empty() {
                executor.idle().await;
                continue;
            }

            match operator.process(changes, database.clone()).await {
                Ok(OperatorStatus::Continue) => {
                    if let Err(error) = worker.commit() {
                        log::error!("failed to commit operator {:?}: {error}", worker.name);
                        if let Err(reset_error) = worker.reset() {
                            log::error!(
                                "failed to reset operator {:?}: {reset_error}",
                                worker.name
                            );
                        }
                        executor.idle().await;
                    }
                }
                Ok(OperatorStatus::Finish) => {
                    worker.stop();
                    if let Err(error) = operator.finish(database.clone()).await {
                        log::error!("failed finishing operator {:?}: {error}", worker.name);
                    }
                    if let Some(database) = database.upgrade() {
                        let _lifecycle = database.inner.lifecycle.lock();
                        if let Err(error) = database.inner.finish_operator_locked(&worker.name) {
                            log::error!("failed to finish operator {:?}: {error}", worker.name);
                        }
                    }
                    worker.mark_finished();
                    return;
                }
                Err(error) => {
                    log::error!("operator {:?} failed: {error}", worker.name);
                    if let Err(reset_error) = worker.reset() {
                        log::error!("failed to reset operator {:?}: {reset_error}", worker.name);
                    }
                    executor.idle().await;
                }
            }
        }

        if let Err(error) = operator.finish(database.clone()).await {
            log::error!("failed finishing operator {:?}: {error}", worker.name);
        }
        if let Some(database) = database.upgrade() {
            let _lifecycle = database.inner.lifecycle.lock();
            if let Err(error) = database.inner.finish_operator_locked(&worker.name) {
                log::error!("failed to stop operator {:?}: {error}", worker.name);
            }
        }
        worker.mark_finished();
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
                                "failed reading topic {:?} for operator {:?}: {error}",
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
