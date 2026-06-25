//! Database-wide processing-time timer scheduler.
//!
//! All operators share a single ordered (B+ tree) timer store so that timer
//! delivery has one global priority order across the whole database.
//!
//! The key is `TimerKey { fire_at_ms, operator }`. There is exactly **one**
//! slot per `(operator, fire_at_ms)` pair — registering a timer with the same
//! operator and time as an existing entry overwrites it (last-write-wins).
//!
//! The scheduler loop uses a condvar to sleep until the next timer is due,
//! waking early whenever a nearer timer is registered.

use std::{
    io,
    sync::{Arc, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use parking_lot::{Condvar, Mutex};
use zendb_storage::core::traits::Backend;
use zendb_storage::frontend::state::State;

use super::Database;

/// Ordering key: earliest `fire_at_ms` first; within the same millisecond,
/// lexicographic by operator name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub(crate) struct TimerKey {
    fire_at_ms: u64,
    operator: String,
}

/// Opaque payload stored with each timer.
#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct TimerEntry {
    payload: Vec<u8>,
}

pub(crate) type TimerStore = State<TimerKey, TimerEntry>;

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

impl Database {
    /// Register a processing-time timer for `operator`.
    ///
    /// If a timer already exists for the same `(operator, fire_at_ms)` pair it
    /// is overwritten — no FIFO guarantee for equal-time timers on the same
    /// operator.
    pub fn register_timer(
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

    /// Cancel a pending timer for `operator` at `fire_at_ms`.
    pub fn cancel_timer(self: &Arc<Self>, operator: &str, fire_at_ms: u64) -> io::Result<()> {
        let key = TimerKey {
            fire_at_ms,
            operator: operator.to_owned(),
        };
        self.timers.write().delete(&key).map(|_| ())
    }

    /// Remove every pending timer owned by `operator` (called on operator
    /// retirement so stale timers do not fire against a missing worker).
    pub(crate) fn sweep_operator_timers(&self, operator: &str) {
        let mut timers = self.timers.write();
        let stale: Vec<TimerKey> = timers
            .entries()
            .filter(|(key, _)| key.operator == operator)
            .map(|(key, _)| key.into_owned())
            .collect();
        for key in stale {
            if let Err(error) = timers.delete(&key) {
                log::error!("failed sweeping timer for operator {operator:?}: {error}");
            }
        }
    }

    /// Pop all timers whose fire time is ≤ `now`, in key order, removing them.
    /// Returns `(operator, payload)` pairs.
    fn take_due_timers(&self, now: u64) -> Vec<(String, Vec<u8>)> {
        let mut timers = self.timers.write();
        let due: Vec<(TimerKey, TimerEntry)> = timers
            .entries()
            .take_while(|(key, _)| key.fire_at_ms <= now)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        let mut fired = Vec::with_capacity(due.len());
        for (key, entry) in due {
            if let Err(error) = timers.delete(&key) {
                log::error!("failed removing fired timer: {error}");
                continue;
            }
            fired.push((key.operator, entry.payload));
        }
        fired
    }

    /// Peek at the `fire_at_ms` of the earliest pending timer (without removing it).
    fn next_timer_ms(&self) -> Option<u64> {
        self.timers
            .read()
            .entries()
            .next()
            .map(|(key, _)| key.fire_at_ms)
    }

    fn deliver_timer(&self, operator: &str, payload: Vec<u8>) {
        match self.operators.read().get(operator) {
            Some(worker) => worker.timer_inbox.lock().push_back(payload),
            None => log::debug!("dropping timer for unknown operator {operator:?}"),
        }
    }
}

pub(super) async fn run_scheduler(
    database: Weak<Database>,
    _executor: Arc<dyn crate::runtime::Executor>,
    notify: Arc<(Mutex<()>, Condvar)>,
) {
    let cap = Duration::from_secs(60);
    loop {
        let Some(db) = database.upgrade() else {
            return;
        };
        let now = now_ms();
        for (operator, payload) in db.take_due_timers(now) {
            db.deliver_timer(&operator, payload);
        }

        let sleep_for = match db.next_timer_ms() {
            None => cap,
            Some(next) => {
                if next <= now {
                    // Another timer is due immediately (edge case: new entry
                    // arrived between take_due_timers and next_timer_ms).
                    drop(db);
                    continue;
                }
                Duration::from_millis(next - now).min(cap)
            }
        };
        drop(db);

        // Sleep until the next due time, or until woken by register_timer.
        let mut guard = notify.0.lock();
        let _ = notify.1.wait_for(&mut guard, sleep_for);
    }
}

