//! Poll-based operator execution with exponential-backoff retry.

use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Weak},
    time::Duration,
};

use parking_lot::Mutex;
use zendb_storage::core::topic::TopicConsumer;

use super::{
    Change, DispatchOperator, DispatchOperatorConfig, OperatorContext, OperatorDirective,
    OperatorPhase,
};
use crate::{runtime::Executor, Database};

pub(crate) struct OperatorInput {
    pub(crate) table_name: String,
    pub(crate) reader: TopicConsumer<Change>,
}

pub(crate) struct OperatorWorker<D>
where
    D: DispatchOperator,
{
    pub(crate) name: String,
    pub(crate) config: D::DispatchConfig,
    pub(crate) inputs: Mutex<Vec<OperatorInput>>,
    operator: Mutex<Option<D>>,
    pub(crate) timer_inbox: Mutex<VecDeque<Vec<u8>>>,
}

impl<D> OperatorWorker<D>
where
    D: DispatchOperator,
{
    pub(crate) fn new(
        name: String,
        config: D::DispatchConfig,
        inputs: Vec<OperatorInput>,
        operator: D,
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            config,
            inputs: Mutex::new(inputs),
            operator: Mutex::new(Some(operator)),
            timer_inbox: Mutex::new(VecDeque::new()),
        })
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

    /// Transition the operator to its terminal phase in the catalog.
    /// Called by the run loop when the operator finishes or exhausts retries.
    fn retire(&self, database: &Weak<Database<D>>, phase: OperatorPhase) {
        if let Some(database) = database.upgrade() {
            database.retire_operator(&self.name, phase);
        }
    }

    /// Extract the operator from its holding mutex and start the async run loop.
    pub(crate) fn spawn(self: &Arc<Self>, database: &Arc<Database<D>>) {
        let executor = Arc::clone(&database.executor);
        let database = Arc::downgrade(database);
        let operator = self
            .operator
            .lock()
            .take()
            .expect("operator already taken from worker");
        executor
            .clone()
            .spawn(Box::pin(Arc::clone(self).run(database, executor, operator)));
    }

    /// Main event loop: drain timer inbox, poll changes, commit on success,
    /// apply exponential backoff on error, retire on `Finish` or exhausted retries.
    async fn run(
        self: Arc<Self>,
        database: Weak<Database<D>>,
        executor: Arc<dyn Executor>,
        mut operator: D,
    ) {
        let make_ctx = || OperatorContext {
            db: database.clone(),
            name: &self.name,
            config: self.config.clone(),
            _phantom: std::marker::PhantomData,
        };

        match operator.open(make_ctx()).await {
            Ok(OperatorDirective::Continue) => {}
            Ok(OperatorDirective::Finish) => {
                self.finish_and_retire(&database, &mut operator).await;
                return;
            }
            Err(error) => {
                log::error!("failed opening operator {:?}: {error}", self.name);
                self.retire(
                    &database,
                    OperatorPhase::Failed {
                        error: error.to_string(),
                    },
                );
                return;
            }
        }

        let mut attempt: usize = 0;

        loop {
            let runtime = self.config.runtime_config();
            let changes = self.poll(runtime.poll_size.max(1));
            let timers: Vec<Vec<u8>> = self.timer_inbox.lock().drain(..).collect();

            if changes.is_empty() && timers.is_empty() {
                executor.idle().await;
                continue;
            }

            for payload in timers {
                match operator.handle_timer(payload, make_ctx()).await {
                    Ok(OperatorDirective::Continue) => {}
                    Ok(OperatorDirective::Finish) => {
                        self.finish_and_retire(&database, &mut operator).await;
                        return;
                    }
                    Err(error) => {
                        log::error!("operator {:?} on_timer failed: {error}", self.name);
                    }
                }
            }

            if changes.is_empty() {
                continue;
            }

            match operator.process(changes, make_ctx()).await {
                Ok(OperatorDirective::Continue) => {
                    attempt = 0;
                    if let Err(error) = self.commit() {
                        log::error!("failed to commit operator {:?}: {error}", self.name);
                        if let Err(reset_error) = self.reset() {
                            log::error!("failed to reset operator {:?}: {reset_error}", self.name);
                        }
                        executor.idle().await;
                    }
                }
                Ok(OperatorDirective::Finish) => {
                    self.finish_and_retire(&database, &mut operator).await;
                    return;
                }
                Err(error) => {
                    attempt += 1;
                    let error_msg = error.to_string();
                    log::error!(
                        "operator {:?} failed (attempt {attempt}): {error_msg}",
                        self.name
                    );

                    if runtime.retry.max_attempts > 0 && attempt >= runtime.retry.max_attempts {
                        log::error!(
                            "operator {:?} exceeded max_attempts ({}) - retiring",
                            self.name,
                            runtime.retry.max_attempts
                        );
                        if let Err(finish_err) = operator.finish(make_ctx()).await {
                            log::error!("failed finishing operator {:?}: {finish_err}", self.name);
                        }
                        self.retire(&database, OperatorPhase::Failed { error: error_msg });
                        return;
                    }

                    if let Err(reset_error) = self.reset() {
                        log::error!("failed to reset operator {:?}: {reset_error}", self.name);
                    }

                    let delay = backoff_delay(&runtime.retry, attempt);
                    executor.sleep(delay).await;
                }
            }
        }
    }

    async fn finish_and_retire(&self, database: &Weak<Database<D>>, operator: &mut D) {
        let ctx = OperatorContext {
            db: database.clone(),
            name: &self.name,
            config: self.config.clone(),
            _phantom: std::marker::PhantomData,
        };
        if let Err(error) = operator.finish(ctx).await {
            log::error!("failed finishing operator {:?}: {error}", self.name);
        }
        self.retire(database, OperatorPhase::Finished);
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

    // Minimal LCG - good enough for jitter; state is per-call.
    let seed = base_ms
        .wrapping_add(attempt as u64)
        .wrapping_mul(6364136223846793005);
    let rand_fraction = (seed >> 33) as f64 / (u32::MAX as f64);
    let jitter_ms = (base_ms as f64 * retry.jitter_factor * rand_fraction) as u64;

    Duration::from_millis(base_ms.saturating_add(jitter_ms))
}
