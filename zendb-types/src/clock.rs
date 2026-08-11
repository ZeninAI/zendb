//! Process-wide lock-free hybrid logical clock used by generated CRDT facades.

use std::sync::OnceLock;

use crate::{EventTime, utils::time::physical_ms};
use portable_atomic::{AtomicU128, Ordering};

pub struct HybridClock {
    last_time: AtomicU128,
}

impl HybridClock {
    pub fn new(last_time: EventTime) -> Self {
        Self {
            last_time: AtomicU128::new(pack(last_time)),
        }
    }

    pub fn mint(&self) -> EventTime {
        let wall = physical_ms().unwrap_or(0);
        loop {
            let current = self.last_time.load(Ordering::Acquire);
            let last_time = unpack(current);
            let next = next_local(last_time, wall);
            if self
                .last_time
                .compare_exchange_weak(current, pack(next), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return next;
            }
        }
    }

    pub fn observe(&self, remote: EventTime) {
        let wall = physical_ms().unwrap_or(remote.physical_ms);
        loop {
            let current = self.last_time.load(Ordering::Acquire);
            let last_time = unpack(current);
            let next = next_observed(last_time, remote, wall);
            if self
                .last_time
                .compare_exchange_weak(current, pack(next), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn snapshot(&self) -> EventTime {
        unpack(self.last_time.load(Ordering::Acquire))
    }
}

const LOGICAL_MASK: u128 = u32::MAX as u128;

fn pack(time: EventTime) -> u128 {
    (u128::from(time.physical_ms) << u32::BITS) | u128::from(time.logical)
}

fn unpack(value: u128) -> EventTime {
    EventTime {
        physical_ms: (value >> u32::BITS) as u64,
        logical: (value & LOGICAL_MASK) as u32,
    }
}

fn next_local(last: EventTime, wall: u64) -> EventTime {
    if wall > last.physical_ms {
        EventTime {
            physical_ms: wall,
            logical: 0,
        }
    } else if last.logical == u32::MAX {
        EventTime {
            physical_ms: last
                .physical_ms
                .checked_add(1)
                .expect("hybrid clock exhausted"),
            logical: 0,
        }
    } else {
        EventTime {
            physical_ms: last.physical_ms,
            logical: last.logical + 1,
        }
    }
}

fn next_observed(last: EventTime, remote: EventTime, wall: u64) -> EventTime {
    let physical = wall.max(last.physical_ms).max(remote.physical_ms);
    if physical > last.physical_ms && physical > remote.physical_ms {
        EventTime {
            physical_ms: physical,
            logical: 0,
        }
    } else if physical == last.physical_ms && physical == remote.physical_ms {
        increment(EventTime {
            physical_ms: physical,
            logical: last.logical.max(remote.logical),
        })
    } else if physical == last.physical_ms {
        increment(last)
    } else if physical == remote.physical_ms {
        increment(remote)
    } else {
        unreachable!("physical time must match the maximum source")
    }
}

fn increment(time: EventTime) -> EventTime {
    if time.logical == u32::MAX {
        EventTime {
            physical_ms: time
                .physical_ms
                .checked_add(1)
                .expect("hybrid clock exhausted"),
            logical: 0,
        }
    } else {
        EventTime {
            physical_ms: time.physical_ms,
            logical: time.logical + 1,
        }
    }
}

static GLOBAL_CLOCK: OnceLock<HybridClock> = OnceLock::new();

pub fn global_clock() -> &'static HybridClock {
    GLOBAL_CLOCK.get_or_init(|| HybridClock::new(EventTime::ZERO))
}
