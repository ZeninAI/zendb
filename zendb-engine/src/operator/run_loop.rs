//! Operator run loop: event processing, shutdown, poll + commit.
//!
//! This module owns the async execution logic that drives an operator through
//! its lifecycle. The [`OperatorWorker`] in `worker.rs` owns the inputs,
//! event queue, and spawning; this module drives the loop after spawn.

use std::sync::{Arc, Weak};

use crate::{runtime::Executor, Database};

use super::{
    worker::OperatorWorker, DispatchConfig, DispatchOperator, OperatorDirective, OperatorPhase,
};

/// Events processed by the run loop in FIFO order.
#[derive(Clone)]
pub(crate) enum LifecycleEvent {
    InputOpened(String),
    InputClosed(String),
    Teardown(OperatorPhase),
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
                LifecycleEvent::Teardown(phase) => {
                    let _ = operator.teardown(&phase, &db, worker.name(), config).await;
                    db.retire_operator(
                        worker.name(),
                        phase,
                        &worker.config().runtime_config().subscriptions,
                    );
                    return;
                }
            }
        }

        // --- Suspend if no inputs remain ---
        if !worker.has_inputs() {
            let _ = operator
                .teardown(&OperatorPhase::Active, &db, worker.name(), config)
                .await;
            if db.suspend_operator(worker.name()) {
                return;
            }
            // A table was attached during the race window — keep running.
            // Re-create since we already tore down.
            match D::create(&db, worker.name(), config).await {
                Ok(op) => operator = op,
                Err(error) => {
                    log::error!("operator {:?} re-create failed: {error}", worker.name());
                    db.retire_operator(
                        worker.name(),
                        OperatorPhase::Failed {
                            error: error.to_string(),
                        },
                        &worker.config().runtime_config().subscriptions,
                    );
                    return;
                }
            }
            continue 'outer;
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
