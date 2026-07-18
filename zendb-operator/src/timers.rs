//! Workspace-wide processing-time timer scheduler.
//!
//! All operators share a single ordered (B+ tree) timer store so that timer
//! delivery has one global priority order across the whole workspace.
//!
//! The key is `TimerKey { fire_at_ms, operator }`. There is exactly **one**
//! slot per `(operator, fire_at_ms)` pair - registering a timer with the same
//! operator and time as an existing entry overwrites it (last-write-wins).
//!
//! The scheduler loop uses a condvar to sleep until the next timer is due,
//! waking early whenever a nearer timer is registered. Timers for operators
//! that are not currently loaded are skipped (left in the store) and fire
//! once the operator is activated again.
//!
//! Timer eviction is owned by the worker: `cancel_timer` is called after
//! successful `on_timer`. On operator retirement, `retire_operator`
//! sweeps any remaining timers for that operator.

use std::{
    io,
    sync::{Arc, Weak},
    time::Duration,
};

use log::{debug, trace};
use parking_lot::{Condvar, Mutex};
use zendb_storage::backend::_traits::{ReadBackend, WriteBackend};

use crate::host::{now_ms, OperatorHost, TimerEntry, TimerKey};
use crate::DispatchOperator;

impl<D> OperatorHost<D>
where
    D: DispatchOperator,
{
    /// Register a typed processing-time timer for `operator`.
    ///
    /// The payload is serialized via bincode. If a timer already exists for the
    /// same `(operator, fire_at_ms)` pair it is overwritten (last-write-wins).
    pub fn register_timer<T: bincode::Encode>(
        self: &Arc<Self>,
        operator: &str,
        fire_at_ms: u64,
        payload: &T,
    ) -> io::Result<()> {
        trace!("registering timer for operator {operator:?} at {fire_at_ms}ms");
        let bytes = bincode::encode_to_vec(payload, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        self.register_timer_raw(operator, fire_at_ms, bytes)
    }

    /// Cancel a pending timer for `operator` at `fire_at_ms`.
    pub fn cancel_timer(self: &Arc<Self>, operator: &str, fire_at_ms: u64) -> io::Result<()> {
        trace!("cancelling timer for operator {operator:?} at {fire_at_ms}ms");
        let key = TimerKey {
            fire_at_ms,
            operator: operator.to_owned(),
        };
        self.timers.write().delete(&key).map(|_| ())
    }

    /// Register a raw (pre-serialized) timer. Used internally by the dispatch layer.
    pub(crate) fn register_timer_raw(
        self: &Arc<Self>,
        operator: &str,
        fire_at_ms: u64,
        payload: Vec<u8>,
    ) -> io::Result<()> {
        self.timers.write().put(
            TimerKey {
                fire_at_ms,
                operator: operator.to_owned(),
            },
            TimerEntry { payload },
        )?;
        self.timer_notify.1.notify_all();
        Ok(())
    }

    /// Pop due timers for loaded operators and return the next deliverable
    /// timestamp (if any) so the scheduler can sleep until then.
    ///
    /// Timers are NOT deleted here — eviction is owned by the worker after
    /// successful `on_timer`.
    ///
    /// Returns `(fired_timers, next_deliverable_ms)`.
    fn take_due_timers(&self, now: u64) -> (Vec<(String, u64, Vec<u8>)>, Option<u64>) {
        let operators = self.operators.read();
        let timers = self.timers.read();

        let mut fired = Vec::new();
        let mut next_ms: Option<u64> = None;
        let mut found_next = false;

        for item in timers.entries() {
            let key_ref = item.0;
            let loaded = operators.contains_key(&key_ref.operator);

            if key_ref.fire_at_ms <= now {
                if loaded {
                    let key = key_ref.into_owned();
                    let payload = item.1.into_owned().payload;
                    fired.push((key.operator, key.fire_at_ms, payload));
                }
            } else {
                if !found_next && loaded {
                    next_ms = Some(key_ref.fire_at_ms);
                    found_next = true;
                }
                if found_next {
                    break;
                }
            }
        }

        (fired, next_ms)
    }

    fn deliver_timer(&self, operator: &str, fire_at_ms: u64, payload: Vec<u8>) {
        if let Some(worker) = self.operators.read().get(operator) {
            worker.enqueue_timer(fire_at_ms, payload);
        }
    }
}

pub(super) async fn run_scheduler<D>(
    workspace: Weak<OperatorHost<D>>,
    notify: Arc<(Mutex<()>, Condvar)>,
) where
    D: DispatchOperator,
{
    let cap = Duration::from_secs(60);
    loop {
        let Some(workspace) = workspace.upgrade() else {
            return;
        };
        let now = now_ms();
        let (fired, next_ms) = workspace.take_due_timers(now);
        if !fired.is_empty() {
            debug!("timer scheduler firing {} timer(s)", fired.len());
        }
        for (operator, fire_at_ms, payload) in fired {
            workspace.deliver_timer(&operator, fire_at_ms, payload);
        }

        let sleep_for = match next_ms {
            None => cap,
            Some(next) if next <= now => {
                // A deliverable timer appeared during processing — loop immediately.
                drop(workspace);
                continue;
            }
            Some(next) => Duration::from_millis(next - now).min(cap),
        };
        drop(workspace);

        // Sleep until the next due time, or until woken by register_timer.
        let mut guard = notify.0.lock();
        let _ = notify.1.wait_for(&mut guard, sleep_for);
    }
}
