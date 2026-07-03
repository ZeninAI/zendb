//! Poll-based operator execution with exponential-backoff retry.

use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Weak},
    time::Duration,
};

use parking_lot::Mutex;
use zendb_storage::core::topic::TopicConsumer;

use super::{Change, DispatchConfig, DispatchOperator, OperatorDirective, OperatorPhase};
use crate::{runtime::Executor, Database};

pub(crate) struct OperatorInput {
    table_name: String,
    reader: TopicConsumer<Change>,
}

impl OperatorInput {
    pub(crate) fn new(table_name: String, reader: TopicConsumer<Change>) -> Self {
        Self { table_name, reader }
    }
}

#[derive(Clone)]
enum OperatorWorkerEvent {
    Open,
    InputOpened(String),
    InputClosed(String),
    Close,
    Finish(OperatorPhase),
}

enum WorkerLoopAction {
    AdvanceEvent,
    BeginShutdown(OperatorPhase),
    Retire(OperatorPhase),
    Retry(io::Error),
}

/// Per-operator async run loop. Holds the operator instance, its inputs
/// (topic consumers), a timer inbox, and lifecycle events.
pub(crate) struct OperatorWorker<D>
where
    D: DispatchOperator,
{
    name: String,
    config: D::Config,
    inputs: Mutex<Vec<OperatorInput>>,
    operator: Mutex<Option<D>>,
    timer_inbox: Mutex<VecDeque<(u64, Vec<u8>)>>,
    events: Mutex<VecDeque<OperatorWorkerEvent>>,
    shutdown: Mutex<Option<OperatorPhase>>,
}

impl<D> OperatorWorker<D>
where
    D: DispatchOperator,
{
    pub(crate) fn new(
        name: String,
        config: D::Config,
        inputs: Vec<OperatorInput>,
        operator: D,
    ) -> Arc<Self> {
        let mut events = VecDeque::new();
        events.push_back(OperatorWorkerEvent::Open);
        for input in &inputs {
            events.push_back(OperatorWorkerEvent::InputOpened(input.table_name.clone()));
        }

        Arc::new(Self {
            name,
            config,
            inputs: Mutex::new(inputs),
            operator: Mutex::new(Some(operator)),
            timer_inbox: Mutex::new(VecDeque::new()),
            events: Mutex::new(events),
            shutdown: Mutex::new(None),
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn has_inputs(&self) -> bool {
        !self.inputs.lock().is_empty()
    }

    /// Attach a new topic consumer for `table_name`. Enqueues an `InputOpened` event.
    pub(crate) fn attach_input(&self, input: OperatorInput) {
        if self.is_shutting_down() {
            if let Err(error) = input.reader.delete() {
                log::error!(
                    "failed deleting consumer {:?} from table {:?}: {error}",
                    self.name,
                    input.table_name
                );
            }
            return;
        }

        let table_name = input.table_name.clone();
        self.inputs.lock().push(input);
        self.events
            .lock()
            .push_back(OperatorWorkerEvent::InputOpened(table_name));
    }

    /// Permanently cancel this operator. It will not be reopened from the catalog.
    pub(crate) fn cancel(&self) {
        self.begin_shutdown(OperatorPhase::Cancelled, true);
    }

    /// Push a timer payload from the scheduler into the worker's inbox.
    pub(crate) fn enqueue_timer(&self, fire_at_ms: u64, payload: Vec<u8>) {
        if self.is_shutting_down() {
            return;
        }
        self.timer_inbox.lock().push_back((fire_at_ms, payload));
    }

    /// Delete all subscribed topic consumers and clear the inputs list.
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

    /// Extract the operator from its holding mutex and start the async run loop.
    pub(crate) fn spawn(self: &Arc<Self>, database: &Arc<Database<D>>) {
        let executor = database.executor();
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

    /// Main event loop: process lifecycle events from the queue front, handle
    /// timers, poll changes, commit on success, and apply exponential backoff
    /// retries on failure.
    async fn run(
        self: Arc<Self>,
        database: Weak<Database<D>>,
        executor: Arc<dyn Executor>,
        mut operator: D,
    ) {
        let mut attempt: usize = 0;

        'outer: loop {
            let runtime = self.config.runtime_config();

            while let Some(event) = self.peek_event() {
                match self.handle_event(&database, &mut operator, event).await {
                    WorkerLoopAction::AdvanceEvent => {
                        self.pop_event();
                        attempt = 0;
                    }
                    WorkerLoopAction::BeginShutdown(phase) => {
                        if self.is_shutting_down() {
                            self.pop_event();
                        } else {
                            self.begin_shutdown(phase, true);
                        }
                        attempt = 0;
                        continue 'outer;
                    }
                    WorkerLoopAction::Retire(phase) => {
                        self.retire(&database, phase).await;
                        return;
                    }
                    WorkerLoopAction::Retry(error) => {
                        let error_msg = error.to_string();
                        attempt += 1;

                        if runtime.retry.max_attempts > 0 && attempt >= runtime.retry.max_attempts {
                            if self.is_shutting_down() {
                                self.retire(&database, OperatorPhase::Failed { error: error_msg })
                                    .await;
                                return;
                            }
                            self.begin_shutdown(OperatorPhase::Failed { error: error_msg }, true);
                            attempt = 0;
                            continue 'outer;
                        }

                        let delay = backoff_delay(&runtime.retry, attempt);
                        executor.sleep(delay).await;
                        continue 'outer;
                    }
                }
            }

            let changes = self.poll(runtime.poll_size);
            let timers: Vec<(u64, Vec<u8>)> = self.timer_inbox.lock().drain(..).collect();

            if changes.is_empty() && timers.is_empty() {
                executor.idle().await;
                continue;
            }

            // --- handle timers ---
            for (fire_at_ms, payload) in timers {
                match operator
                    .handle_timer(
                        payload,
                        fire_at_ms,
                        database.clone(),
                        &self.name,
                        &self.config,
                    )
                    .await
                {
                    Ok(OperatorDirective::Continue) => {
                        if let Some(db) = database.upgrade() {
                            let _ = db.cancel_timer(&self.name, fire_at_ms);
                        }
                        attempt = 0;
                    }
                    Ok(OperatorDirective::Finish) => {
                        if let Some(db) = database.upgrade() {
                            let _ = db.cancel_timer(&self.name, fire_at_ms);
                        }
                        self.begin_shutdown(OperatorPhase::Finished, true);
                        attempt = 0;
                        continue 'outer;
                    }
                    Err(error) => {
                        let error_msg = error.to_string();
                        log::error!("operator {:?} on_timer failed: {error_msg}", self.name);
                        attempt += 1;

                        if runtime.retry.max_attempts > 0 && attempt >= runtime.retry.max_attempts {
                            self.begin_shutdown(OperatorPhase::Failed { error: error_msg }, true);
                            attempt = 0;
                            continue 'outer;
                        }

                        self.reset();

                        let delay = backoff_delay(&runtime.retry, attempt);
                        executor.sleep(delay).await;
                        continue 'outer;
                    }
                }
            }

            if changes.is_empty() {
                continue;
            }

            // --- process changes ---
            match operator
                .process(changes, database.clone(), &self.name, &self.config)
                .await
            {
                Ok(OperatorDirective::Continue) => {
                    self.commit();
                    attempt = 0;
                }
                Ok(OperatorDirective::Finish) => {
                    self.commit();
                    self.begin_shutdown(OperatorPhase::Finished, true);
                    attempt = 0;
                    continue 'outer;
                }
                Err(error) => {
                    attempt += 1;
                    let error_msg = error.to_string();
                    log::error!(
                        "operator {:?} failed (attempt {attempt}): {error_msg}",
                        self.name
                    );

                    if runtime.retry.max_attempts > 0 && attempt >= runtime.retry.max_attempts {
                        self.begin_shutdown(OperatorPhase::Failed { error: error_msg }, true);
                        attempt = 0;
                        continue 'outer;
                    }

                    self.reset();

                    let delay = backoff_delay(&runtime.retry, attempt);
                    executor.sleep(delay).await;
                }
            }
        }
    }

    async fn handle_event(
        &self,
        database: &Weak<Database<D>>,
        operator: &mut D,
        event: OperatorWorkerEvent,
    ) -> WorkerLoopAction {
        match event {
            OperatorWorkerEvent::Open => match operator
                .open(database.clone(), &self.name, &self.config)
                .await
            {
                Ok(OperatorDirective::Continue) => WorkerLoopAction::AdvanceEvent,
                Ok(OperatorDirective::Finish) => {
                    WorkerLoopAction::BeginShutdown(OperatorPhase::Finished)
                }
                Err(error) => WorkerLoopAction::Retry(error),
            },
            OperatorWorkerEvent::InputOpened(table) => match operator
                .on_input_opened(table, database.clone(), &self.name, &self.config)
                .await
            {
                Ok(OperatorDirective::Continue) => WorkerLoopAction::AdvanceEvent,
                Ok(OperatorDirective::Finish) => {
                    WorkerLoopAction::BeginShutdown(OperatorPhase::Finished)
                }
                Err(error) => WorkerLoopAction::Retry(error),
            },
            OperatorWorkerEvent::InputClosed(table) => match operator
                .on_input_closed(table, database.clone(), &self.name, &self.config)
                .await
            {
                Ok(OperatorDirective::Continue) => WorkerLoopAction::AdvanceEvent,
                Ok(OperatorDirective::Finish) => {
                    WorkerLoopAction::BeginShutdown(OperatorPhase::Finished)
                }
                Err(error) => WorkerLoopAction::Retry(error),
            },
            OperatorWorkerEvent::Close => match operator
                .close(database.clone(), &self.name, &self.config)
                .await
            {
                Ok(()) => WorkerLoopAction::AdvanceEvent,
                Err(error) => WorkerLoopAction::Retry(error),
            },
            OperatorWorkerEvent::Finish(phase) => match operator
                .finish(database.clone(), &self.name, &self.config)
                .await
            {
                Ok(()) => WorkerLoopAction::Retire(phase),
                Err(error) => WorkerLoopAction::Retry(error),
            },
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shutdown.lock().is_some()
    }

    fn peek_event(&self) -> Option<OperatorWorkerEvent> {
        self.events.lock().front().cloned()
    }

    fn pop_event(&self) {
        let _ = self.events.lock().pop_front();
    }

    fn begin_shutdown(&self, phase: OperatorPhase, clear_pending: bool) {
        let mut events = self.events.lock();
        self.begin_shutdown_with_events(&mut events, phase, clear_pending);
    }

    fn begin_shutdown_with_events(
        &self,
        events: &mut VecDeque<OperatorWorkerEvent>,
        phase: OperatorPhase,
        clear_pending: bool,
    ) {
        let mut current = self.shutdown.lock();
        if current.is_some() {
            return;
        }
        *current = Some(phase.clone());
        drop(current);

        if clear_pending {
            events.clear();
            for table in self.input_tables() {
                events.push_back(OperatorWorkerEvent::InputClosed(table));
            }
        }
        self.push_shutdown_tail(events, phase);
    }

    fn push_shutdown_tail(&self, events: &mut VecDeque<OperatorWorkerEvent>, phase: OperatorPhase) {
        let has_close = events
            .iter()
            .any(|event| matches!(event, OperatorWorkerEvent::Close));
        if !has_close {
            events.push_back(OperatorWorkerEvent::Close);
        }

        let has_finish = events
            .iter()
            .any(|event| matches!(event, OperatorWorkerEvent::Finish(_)));
        if !has_finish {
            events.push_back(OperatorWorkerEvent::Finish(phase));
        }
    }

    fn input_tables(&self) -> Vec<String> {
        self.inputs
            .lock()
            .iter()
            .map(|input| input.table_name.clone())
            .collect()
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
    fn commit(&self) {
        for input in self.inputs.lock().iter_mut() {
            input.reader.commit().expect("commit must succeed");
        }
    }

    /// Roll back all consumer read offsets so the same changes are re-delivered.
    fn reset(&self) {
        for input in self.inputs.lock().iter_mut() {
            input.reader.reset().expect("reset must succeed");
        }
    }

    /// Retire terminal workers and perform durable cleanup.
    async fn retire(&self, database: &Weak<Database<D>>, phase: OperatorPhase) {
        let Some(database) = database.upgrade() else {
            return;
        };
        database.retire_operator(
            &self.name,
            phase,
            &self.config.runtime_config().subscriptions,
            Some(self),
        );
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
