//! Operator worker: input management, timer inbox, lifecycle events, and spawn.
//!
//! The worker is the coordination layer between the database (which
//! attaches/detaches inputs and enqueues timers) and the async run loop
//! (which drives the operator through its lifecycle).

use std::{any::Any, collections::VecDeque, sync::Arc};

use log::{debug, trace};
use parking_lot::Mutex;
use zendb_storage::core::topic::TopicConsumer;

use super::{run_loop::LifecycleEvent, Change, DispatchOperator, OperatorPhase};
use crate::Database;

/// Type-erased facet stored by the worker.
pub(crate) type ErasedFacet = Arc<dyn Any + Send + Sync>;

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
/// Holds the inputs (topic consumers), timer inbox, and lifecycle event queue.
/// The actual operator instance is created and owned by the run loop.
pub(crate) struct OperatorWorker<D>
where
    D: DispatchOperator,
{
    name: String,
    config: D::Config,
    inputs: Mutex<Vec<OperatorInput>>,
    timer_inbox: Mutex<VecDeque<(u64, Vec<u8>)>>,
    events: Mutex<VecDeque<LifecycleEvent>>,
    shutdown_phase: Mutex<Option<OperatorPhase>>,
    facet: Mutex<Option<ErasedFacet>>,
}

impl<D> OperatorWorker<D>
where
    D: DispatchOperator,
{
    pub(crate) fn new(name: String, config: D::Config, inputs: Vec<OperatorInput>) -> Arc<Self> {
        let mut events = VecDeque::new();
        for input in &inputs {
            events.push_back(LifecycleEvent::InputOpened(input.table_name.clone()));
        }

        Arc::new(Self {
            name,
            config,
            inputs: Mutex::new(inputs),
            timer_inbox: Mutex::new(VecDeque::new()),
            events: Mutex::new(events),
            shutdown_phase: Mutex::new(None),
            facet: Mutex::new(None),
        })
    }

    // --- Accessors ---

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn config(&self) -> &D::Config {
        &self.config
    }

    pub(crate) fn has_inputs(&self) -> bool {
        !self.inputs.lock().is_empty()
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutdown_phase.lock().is_some()
    }

    // --- Facet management ---

    /// Store the operator's type-erased facet for external queries.
    pub(crate) fn publish_facet(&self, facet: Box<dyn Any + Send + Sync>) {
        *self.facet.lock() = Some(Arc::from(facet));
    }

    /// Remove the facet (called on teardown/suspend).
    pub(crate) fn clear_facet(&self) {
        *self.facet.lock() = None;
    }

    /// Retrieve the facet, downcasting to `F`. Returns `None` if no facet is
    /// published or the type does not match.
    pub(crate) fn get_facet<F: Send + Sync + 'static>(&self) -> Option<Arc<F>> {
        self.facet
            .lock()
            .as_ref()
            .and_then(|f| Arc::clone(f).downcast::<F>().ok())
    }

    // --- Input management ---

    /// Attach a new topic consumer. Enqueues `InputOpened` unless shutting down.
    pub(crate) fn attach_input(&self, input: OperatorInput) {
        if self.shutdown_phase.lock().is_some() {
            return;
        }
        trace!(
            "attaching input {:?} to operator {:?}",
            input.table_name,
            self.name
        );
        let table_name = input.table_name.clone();
        self.inputs.lock().push(input);
        self.events
            .lock()
            .push_back(LifecycleEvent::InputOpened(table_name));
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
        if removed.is_none() {
            return false;
        }
        trace!(
            "detaching input {table_name:?} from operator {:?}",
            self.name
        );
        if self.shutdown_phase.lock().is_none() {
            self.events
                .lock()
                .push_back(LifecycleEvent::InputClosed(table_name.to_owned()));
        }
        true
    }

    /// Drop all topic consumers.
    pub(crate) fn delete_inputs(&self) {
        self.inputs.lock().clear();
    }

    // --- Timer inbox ---

    /// Push a timer payload into the worker's inbox (ignored if shutting down).
    pub(crate) fn enqueue_timer(&self, fire_at_ms: u64, payload: Vec<u8>) {
        if self.shutdown_phase.lock().is_some() {
            return;
        }
        self.timer_inbox.lock().push_back((fire_at_ms, payload));
    }

    /// Drain all pending timers.
    pub(crate) fn drain_timers(&self) -> Vec<(u64, Vec<u8>)> {
        self.timer_inbox.lock().drain(..).collect()
    }

    // --- Lifecycle ---

    /// Begin shutdown: clear pending events, enqueue InputClosed for each
    /// open table, then enqueue Teardown. Idempotent — ignored if already
    /// shutting down.
    pub(crate) fn begin_shutdown(&self, phase: OperatorPhase) {
        let mut shutdown = self.shutdown_phase.lock();
        if shutdown.is_some() {
            return;
        }
        debug!("operator {:?} beginning shutdown ({phase:?})", self.name);
        let input_tables: Vec<String> = self
            .inputs
            .lock()
            .iter()
            .map(|i| i.table_name.clone())
            .collect();
        let mut events = self.events.lock();
        events.clear();
        for table in input_tables {
            events.push_back(LifecycleEvent::InputClosed(table));
        }
        events.push_back(LifecycleEvent::Teardown(phase.clone()));
        *shutdown = Some(phase);
    }

    /// Peek at the next lifecycle event without consuming it.
    pub(crate) fn peek_event(&self) -> Option<LifecycleEvent> {
        self.events.lock().front().cloned()
    }

    /// Consume the front lifecycle event.
    pub(crate) fn pop_event(&self) {
        self.events.lock().pop_front();
    }

    // --- Polling ---

    /// Round-robin across inputs, collecting up to `limit` changes.
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
            .spawn(Box::pin(super::run_loop::run(worker, database, executor)));
    }
}
