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
//! waking early whenever a nearer timer is registered. Timers for operators
//! that are not currently loaded are skipped (left in the store) and fire
//! once the operator is activated again.

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

    /// Pop due timers only for operators that are currently loaded in memory.
    /// Timers for unloaded operators are left in the store untouched.
    fn take_due_timers(&self, now: u64) -> Vec<(String, Vec<u8>)> {
        let operators = self.operators.read();
        let mut timers = self.timers.write();

        let due: Vec<(TimerKey, TimerEntry)> = timers
            .entries()
            .take_while(|(key, _)| key.fire_at_ms <= now)
            .filter(|(key, _)| operators.contains_key(&key.operator))
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

    /// Peek at the earliest pending timer that has a loaded operator, so the
    /// scheduler only wakes when there is something deliverable.
    fn next_deliverable_timer_ms(&self) -> Option<u64> {
        let operators = self.operators.read();
        self.timers
            .read()
            .entries()
            .find(|(key, _)| operators.contains_key(&key.operator))
            .map(|(key, _)| key.fire_at_ms)
    }

    fn deliver_timer(&self, operator: &str, payload: Vec<u8>) {
        if let Some(worker) = self.operators.read().get(operator).cloned() {
            worker.timer_inbox.lock().push_back(payload);
        }
    }
}

pub(super) async fn run_scheduler(database: Weak<Database>, notify: Arc<(Mutex<()>, Condvar)>) {
    let cap = Duration::from_secs(60);
    loop {
        let Some(db) = database.upgrade() else {
            return;
        };
        let now = now_ms();
        for (operator, payload) in db.take_due_timers(now) {
            db.deliver_timer(&operator, payload);
        }

        let sleep_for = match db.next_deliverable_timer_ms() {
            None => cap,
            Some(next) => {
                if next <= now {
                    // Deliverable timer is due immediately — loop without sleeping.
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
