//! Operator run loop: event queue, shutdown state machine, poll + commit.
//!
//! This module owns the async execution logic that drives an operator through
//! its lifecycle. The [`OperatorWorker`] struct in `worker.rs` owns the
//! inputs and spawning; the run loop handles everything after spawn.

use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use crate::{runtime::Executor, Database};

use super::{
    worker::OperatorWorker, DispatchConfig, DispatchOperator, OperatorDirective, OperatorPhase,
    TeardownReason,
};

/// Events processed by the run loop in FIFO order.
#[derive(Clone)]
pub(crate) enum LifecycleEvent {
    InputOpened(String),
    InputClosed(String),
    Teardown(TeardownReason),
}

/// Encapsulates the shutdown state for an operator.
pub(crate) struct LifecycleState {
    events: VecDeque<LifecycleEvent>,
    phase: Option<OperatorPhase>,
}

impl LifecycleState {
    pub(crate) fn new() -> Self {
        Self {
            events: VecDeque::new(),
            phase: None,
        }
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.phase.is_some()
    }

    pub(crate) fn push_event(&mut self, event: LifecycleEvent) {
        self.events.push_back(event);
    }

    pub(crate) fn begin_shutdown(&mut self, phase: OperatorPhase, input_tables: Vec<String>) {
        if self.phase.is_some() {
            return;
        }
        let reason = phase_to_reason(&phase);
        self.phase = Some(phase);
        self.events.clear();
        for table in input_tables {
            self.events.push_back(LifecycleEvent::InputClosed(table));
        }
        self.events.push_back(LifecycleEvent::Teardown(reason));
    }

    pub(crate) fn peek(&self) -> Option<&LifecycleEvent> {
        self.events.front()
    }

    pub(crate) fn pop(&mut self) {
        self.events.pop_front();
    }
}

fn phase_to_reason(phase: &OperatorPhase) -> TeardownReason {
    match phase {
        OperatorPhase::Finished => TeardownReason::Finished,
        OperatorPhase::Failed { error } => TeardownReason::Failed {
            error: error.clone(),
        },
        OperatorPhase::Cancelled => TeardownReason::Cancelled,
        OperatorPhase::Active => TeardownReason::Finished,
    }
}

/// Main async run loop.
pub(crate) async fn run<D>(
    worker: Arc<OperatorWorker<D>>,
    database: Weak<Database<D>>,
    executor: Arc<dyn Executor>,
) where
    D: DispatchOperator,
{
    let config = worker.config();

    // Promote weak ref for create.
    let Some(db) = database.upgrade() else {
        return;
    };

    let mut operator = match D::create(&db, worker.name(), config).await {
        Ok(op) => op,
        Err(error) => {
            log::error!("operator {:?} create failed: {error}", worker.name());
            let phase = OperatorPhase::Failed {
                error: error.to_string(),
            };
            db.retire_operator(
                worker.name(),
                phase,
                &worker.config().runtime_config().subscriptions,
                Some(&worker),
            );
            return;
        }
    };

    // Drop the strong ref between calls.
    drop(db);

    'outer: loop {
        let Some(db) = database.upgrade() else {
            return;
        };
        let runtime = config.runtime_config();

        // --- Process lifecycle events ---
        while let Some(event) = worker.peek_event() {
            match event {
                LifecycleEvent::InputOpened(table) => {
                    match operator
                        .on_input_opened(table, &db, worker.name(), config)
                        .await
                    {
                        Ok(OperatorDirective::Continue) => {
                            worker.pop_event();
                        }
                        Ok(OperatorDirective::Finish) => {
                            worker.begin_shutdown(OperatorPhase::Finished);
                            continue 'outer;
                        }
                        Err(error) => {
                            worker.begin_shutdown(OperatorPhase::Failed {
                                error: error.to_string(),
                            });
                            continue 'outer;
                        }
                    }
                }
                LifecycleEvent::InputClosed(table) => {
                    match operator
                        .on_input_closed(table, &db, worker.name(), config)
                        .await
                    {
                        Ok(OperatorDirective::Continue) => {
                            worker.pop_event();
                        }
                        Ok(OperatorDirective::Finish) => {
                            worker.begin_shutdown(OperatorPhase::Finished);
                            continue 'outer;
                        }
                        Err(error) => {
                            worker.begin_shutdown(OperatorPhase::Failed {
                                error: error.to_string(),
                            });
                            continue 'outer;
                        }
                    }
                }
                LifecycleEvent::Teardown(reason) => {
                    let phase = reason_to_phase(&reason);
                    match operator.teardown(&reason, &db, worker.name(), config).await {
                        Ok(()) => {
                            db.retire_operator(
                                worker.name(),
                                phase,
                                &worker.config().runtime_config().subscriptions,
                                Some(&worker),
                            );
                            return;
                        }
                        Err(error) => {
                            db.retire_operator(
                                worker.name(),
                                OperatorPhase::Failed {
                                    error: error.to_string(),
                                },
                                &worker.config().runtime_config().subscriptions,
                                Some(&worker),
                            );
                            return;
                        }
                    }
                }
            }
        }

        // --- Suspend if no inputs remain ---
        if !worker.has_inputs() {
            worker.suspend(&db);
            return;
        }

        // --- Poll changes and timers ---
        let changes = worker.poll(runtime.poll_size);
        let timers: Vec<(u64, Vec<u8>)> = worker.drain_timers();

        if changes.is_empty() && timers.is_empty() {
            drop(db);
            executor.idle().await;
            continue;
        }

        // --- Handle timers ---
        for (fire_at_ms, payload) in timers {
            match operator
                .on_timer(payload, fire_at_ms, &db, worker.name(), config)
                .await
            {
                Ok(OperatorDirective::Continue) => {
                    let _ = db.cancel_timer(worker.name(), fire_at_ms);
                }
                Ok(OperatorDirective::Finish) => {
                    let _ = db.cancel_timer(worker.name(), fire_at_ms);
                    worker.begin_shutdown(OperatorPhase::Finished);
                    continue 'outer;
                }
                Err(error) => {
                    log::error!("operator {:?} on_timer failed: {error}", worker.name());
                    worker.begin_shutdown(OperatorPhase::Failed {
                        error: error.to_string(),
                    });
                    continue 'outer;
                }
            }
        }

        if changes.is_empty() {
            continue;
        }

        // --- Process changes ---
        match operator.process(changes, &db, worker.name(), config).await {
            Ok(OperatorDirective::Continue) => {
                worker.commit();
            }
            Ok(OperatorDirective::Finish) => {
                worker.commit();
                worker.begin_shutdown(OperatorPhase::Finished);
                continue 'outer;
            }
            Err(error) => {
                log::error!("operator {:?} failed: {error}", worker.name());
                worker.begin_shutdown(OperatorPhase::Failed {
                    error: error.to_string(),
                });
                continue 'outer;
            }
        }
    }
}

fn reason_to_phase(reason: &TeardownReason) -> OperatorPhase {
    match reason {
        TeardownReason::Finished => OperatorPhase::Finished,
        TeardownReason::Failed { error } => OperatorPhase::Failed {
            error: error.clone(),
        },
        TeardownReason::Cancelled => OperatorPhase::Cancelled,
    }
}
