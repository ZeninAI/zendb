//! Poll-based operator execution with exponential-backoff retry.

use std::{
    collections::VecDeque,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use parking_lot::{Condvar, Mutex};
use zendb_storage::core::topic::TopicConsumer;

use super::{Change, ErasedOperator, OperatorConfig, OperatorContext, OperatorStatus};
use crate::{runtime::Executor, Database};

pub(crate) struct OperatorInput {
    pub(crate) table_name: String,
    pub(crate) reader: TopicConsumer<Change>,
}

pub(crate) struct OperatorWorker {
    pub(crate) name: Arc<str>,
    pub(crate) config: Arc<OperatorConfig>,
    pub(crate) inputs: Mutex<Vec<OperatorInput>>,
    operator: Mutex<Option<Box<dyn ErasedOperator>>>,
    pub(crate) timer_inbox: Mutex<VecDeque<Vec<u8>>>,
    stopped: AtomicBool,
    finished: Mutex<bool>,
    finished_signal: Condvar,
}

impl OperatorWorker {
    pub(crate) fn new(
        name: impl Into<Arc<str>>,
        config: OperatorConfig,
        inputs: Vec<OperatorInput>,
        operator: Box<dyn ErasedOperator>,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            config: Arc::new(config),
            inputs: Mutex::new(inputs),
            operator: Mutex::new(Some(operator)),
            timer_inbox: Mutex::new(VecDeque::new()),
            stopped: AtomicBool::new(false),
            finished: Mutex::new(false),
            finished_signal: Condvar::new(),
        })
    }

    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    /// Delete all subscribed topic consumers and clear the inputs list on retirement.
    pub(crate) fn delete_inputs(&self) {
        for input in std::mem::take(&mut *self.inputs.lock()) {
            if let Err(error) = input.reader.delete() {
                log::error!(
                    "failed deleting consumer {:?} from table {:?}: {error}",
                    self.name,
                    input.table_name
                );
            }
        }
    }

    pub(crate) fn wait_finished(&self) {
        let mut done = self.finished.lock();
        while !*done {
            self.finished_signal.wait(&mut done);
        }
    }

    fn mark_finished(&self) {
        *self.finished.lock() = true;
        self.finished_signal.notify_all();
    }

    /// Deregister this operator from the database under the lifecycle lock.
    /// Called by the run loop when the operator finishes or exhausts retries.
    fn retire(database: &Weak<Database>, name: &str) {
        if let Some(database) = database.upgrade() {
            let _lifecycle = database.lifecycle.lock();
            if let Err(error) = database.deregister_operator(name) {
                log::error!("failed to retire operator {name:?}: {error}");
            }
        }
    }

    /// Extract the operator from its holding mutex and start the async run loop.
    pub(crate) fn spawn(database: &Arc<Database>, worker: Arc<Self>) {
        let executor = Arc::clone(&database.executor);
        let database = Arc::downgrade(database);
        let operator = worker
            .operator
            .lock()
            .take()
            .expect("operator already taken from worker");
        executor
            .clone()
            .spawn(Box::pin(Self::run(database, executor, worker, operator)));
    }

    /// Main event loop: drain timer inbox, poll changes, commit on success,
    /// apply exponential backoff on error, retire on `Finish` or exhausted retries.
    async fn run(
        database: Weak<Database>,
        executor: Arc<dyn Executor>,
        worker: Arc<Self>,
        mut operator: Box<dyn ErasedOperator>,
    ) {
        let make_ctx = || OperatorContext::new(database.clone(), Arc::clone(&worker.name), Arc::clone(&worker.config));

        if let Err(error) = operator.open(make_ctx()).await {
            log::error!("failed opening operator {:?}: {error}", worker.name);
            Self::retire(&database, &worker.name);
            worker.mark_finished();
            return;
        }

        let retry = worker.config.retry.clone();
        let mut attempt: usize = 0;

        while !worker.stopped.load(Ordering::Acquire) {
            let changes = worker.poll(worker.config.poll_size.max(1));
            let timers: Vec<Vec<u8>> = worker.timer_inbox.lock().drain(..).collect();

            if changes.is_empty() && timers.is_empty() {
                executor.idle().await;
                continue;
            }

            for payload in timers {
                if let Err(error) = operator.on_timer(payload, make_ctx()).await {
                    log::error!("operator {:?} on_timer failed: {error}", worker.name);
                }
            }

            if changes.is_empty() {
                continue;
            }

            match operator.process(changes, make_ctx()).await {
                Ok(OperatorStatus::Continue) => {
                    attempt = 0;
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
                    if let Err(error) = operator.finish(make_ctx()).await {
                        log::error!("failed finishing operator {:?}: {error}", worker.name);
                    }
                    Self::retire(&database, &worker.name);
                    worker.mark_finished();
                    return;
                }
                Err(error) => {
                    attempt += 1;
                    log::error!(
                        "operator {:?} failed (attempt {attempt}): {error}",
                        worker.name
                    );

                    if retry.max_attempts > 0 && attempt >= retry.max_attempts {
                        log::error!(
                            "operator {:?} exceeded max_attempts ({}) — retiring",
                            worker.name,
                            retry.max_attempts
                        );
                        if let Err(finish_err) = operator.finish(make_ctx()).await {
                            log::error!(
                                "failed finishing operator {:?}: {finish_err}",
                                worker.name
                            );
                        }
                        Self::retire(&database, &worker.name);
                        worker.mark_finished();
                        return;
                    }

                    if let Err(reset_error) = worker.reset() {
                        log::error!("failed to reset operator {:?}: {reset_error}", worker.name);
                    }

                    let delay = backoff_delay(&retry, attempt);
                    executor.sleep(delay).await;
                }
            }
        }

        if let Err(error) = operator.finish(make_ctx()).await {
            log::error!("failed finishing operator {:?}: {error}", worker.name);
        }
        Self::retire(&database, &worker.name);
        worker.mark_finished();
    }

    /// Round-robin across all table inputs, collecting up to `limit` changes.
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

    /// Advance all consumer read offsets after a successful `process` call.
    fn commit(&self) -> io::Result<()> {
        for input in self.inputs.lock().iter_mut() {
            input.reader.commit()?;
        }
        Ok(())
    }

    /// Roll back all consumer read offsets so the same changes are re-delivered.
    fn reset(&self) -> io::Result<()> {
        for input in self.inputs.lock().iter_mut() {
            input.reader.reset()?;
        }
        Ok(())
    }
}

/// Exponential back-off with LCG jitter (no external crate needed).
fn backoff_delay(retry: &super::RetryConfig, attempt: usize) -> Duration {
    // Cap shift at 62 so 1u64 << shift never overflows.
    let shift = (attempt as u32).saturating_sub(1).min(62);
    let base_ms = retry
        .initial_delay_ms
        .saturating_mul(1u64 << shift)
        .min(retry.max_delay_ms);

    // Minimal LCG — good enough for jitter; state is per-call.
    let seed = base_ms.wrapping_add(attempt as u64).wrapping_mul(6364136223846793005);
    let rand_fraction = (seed >> 33) as f64 / (u32::MAX as f64);
    let jitter_ms = (base_ms as f64 * retry.jitter_factor * rand_fraction) as u64;

    Duration::from_millis(base_ms.saturating_add(jitter_ms))
}

