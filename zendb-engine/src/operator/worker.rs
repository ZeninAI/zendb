//! Operator worker: input management, timer inbox, and spawn.
//!
//! The worker is a thin coordination layer between the database (which
//! attaches/detaches inputs and enqueues timers) and the async run loop
//! (which drives the operator through its lifecycle).
//!
//! # Responsibilities
//!
//! | Worker owns | Run loop owns |
//! |-------------|---------------|
//! | Input attachment/detachment | Event queue, shutdown state machine |
//! | Timer inbox | Poll + commit, idle/wake |
//! | Spawn entry point | Operator instance and lifecycle dispatch |

use std::{
    collections::VecDeque,
    sync::Arc,
};

use parking_lot::Mutex;
use zendb_storage::core::topic::TopicConsumer;

use super::{
    run_loop::{self, LifecycleEvent, LifecycleState},
    Change, DispatchOperator, OperatorPhase,
};
use crate::Database;

/// A single input source: a topic consumer bound to a table name.
pub(crate) struct OperatorInput {
    table_name: String,
    reader: TopicConsumer<Change>,
}

impl OperatorInput {
    pub(crate) fn new(table_name: String, reader: TopicConsumer<Change>) -> Self {
        Self { table_name, reader }
    }
}

/// Per-operator coordination struct.
///
/// Holds the inputs (topic consumers), timer inbox, and lifecycle state.
/// The actual operator instance is created and owned by the run loop.
pub(crate) struct OperatorWorker<D>
where
    D: DispatchOperator,
{
    name: String,
    config: D::Config,
    inputs: Mutex<Vec<OperatorInput>>,
    timer_inbox: Mutex<VecDeque<(u64, Vec<u8>)>>,
    lifecycle: Mutex<LifecycleState>,
}

impl<D> OperatorWorker<D>
where
    D: DispatchOperator,
{
    pub(crate) fn new(
        name: String,
        config: D::Config,
        inputs: Vec<OperatorInput>,
    ) -> Arc<Self> {
        let mut lifecycle = LifecycleState::new();
        for input in &inputs {
            lifecycle.push_event(LifecycleEvent::InputOpened(input.table_name.clone()));
        }

        Arc::new(Self {
            name,
            config,
            inputs: Mutex::new(inputs),
            timer_inbox: Mutex::new(VecDeque::new()),
            lifecycle: Mutex::new(lifecycle),
        })
    }

    // --- Public accessors ---

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn config(&self) -> &D::Config {
        &self.config
    }

    pub(crate) fn has_inputs(&self) -> bool {
        !self.inputs.lock().is_empty()
    }

    // --- Input management ---

    /// Attach a new topic consumer for `table_name`. Enqueues an `InputOpened` event.
    pub(crate) fn attach_input(&self, input: OperatorInput) {
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle.is_shutting_down() {
            return;
        }
        let table_name = input.table_name.clone();
        self.inputs.lock().push(input);
        lifecycle.push_event(LifecycleEvent::InputOpened(table_name));
    }

    /// Detach the topic consumer for `table_name`. Enqueues `InputClosed`.
    pub(crate) fn detach_input(&self, table_name: &str) -> bool {
        let removed = {
            let mut inputs = self.inputs.lock();
            inputs
                .iter()
                .position(|input| input.table_name == table_name)
                .map(|index| inputs.remove(index))
        };
        let Some(_input) = removed else {
            return false;
        };
        let mut lifecycle = self.lifecycle.lock();
        if !lifecycle.is_shutting_down() {
            lifecycle.push_event(LifecycleEvent::InputClosed(table_name.to_owned()));
        }
        true
    }

    /// Delete all subscribed topic consumers and clear the inputs list.
    pub(crate) fn delete_inputs(&self) {
        self.inputs.lock().clear();
    }

    // --- Timer inbox ---

    /// Push a timer payload from the scheduler into the worker's inbox.
    pub(crate) fn enqueue_timer(&self, fire_at_ms: u64, payload: Vec<u8>) {
        if self.lifecycle.lock().is_shutting_down() {
            return;
        }
        self.timer_inbox.lock().push_back((fire_at_ms, payload));
    }

    /// Drain all pending timers from the inbox.
    pub(crate) fn drain_timers(&self) -> Vec<(u64, Vec<u8>)> {
        self.timer_inbox.lock().drain(..).collect()
    }

    // --- Lifecycle state (called by run loop) ---

    /// Permanently cancel this operator.
    pub(crate) fn cancel(&self) {
        self.begin_shutdown(OperatorPhase::Cancelled);
    }

    /// Begin the shutdown sequence: clear events, enqueue InputClosed + Teardown.
    pub(crate) fn begin_shutdown(&self, phase: OperatorPhase) {
        let input_tables = self.input_tables();
        self.lifecycle.lock().begin_shutdown(phase, input_tables);
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.lifecycle.lock().is_shutting_down()
    }

    pub(crate) fn peek_event(&self) -> Option<LifecycleEvent> {
        self.lifecycle.lock().peek().cloned()
    }

    pub(crate) fn pop_event(&self) {
        self.lifecycle.lock().pop();
    }

    // --- Polling (called by run loop) ---

    /// Round-robin across all table inputs, collecting up to `limit` changes.
    pub(crate) fn poll(&self, limit: usize) -> Vec<Change> {
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
    pub(crate) fn commit(&self) {
        for input in self.inputs.lock().iter_mut() {
            input.reader.commit().expect("commit must succeed");
        }
    }

    // --- Spawn ---

    /// Start the async run loop for this worker.
    pub(crate) fn spawn(self: &Arc<Self>, database: &Arc<Database<D>>) {
        let executor = database.executor();
        let database = Arc::downgrade(database);
        let worker = Arc::clone(self);
        executor
            .clone()
            .spawn(Box::pin(run_loop::run(worker, database, executor)));
    }

    /// Remove the live worker from memory (no inputs left, may resume later).
    pub(crate) fn suspend(&self, database: &Arc<Database<D>>) {
        database.suspend_operator(&self.name, self);
    }

    // --- Helpers ---

    fn input_tables(&self) -> Vec<String> {
        self.inputs
            .lock()
            .iter()
            .map(|input| input.table_name.clone())
            .collect()
    }
}
