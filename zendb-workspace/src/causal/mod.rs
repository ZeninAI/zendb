//! Local event-clock allocation and committed-event receipt tracking.

mod receipts;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::Mutex;
use zendb_storage::{DurableStorage, ReadBackend, WriteBackend};
use zendb_types::{EventStamp, EventTime, InstallationId, utils::time::physical_ms};

use self::receipts::{ObserveOutcome, ReceiptWindow};
use crate::{Error, Result, states::StateHandle};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct EventClock {
    pub next_sequence: u64,
    pub last_time: EventTime,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub(crate) struct CausalState {
    pub receipts: ReceiptWindow,
    pub clock: Option<EventClock>,
}

impl CausalState {
    /// Seed the clock and receipt window from the final bootstrap event.
    /// Subsequent local events must start at the following sequence.
    pub(crate) fn from_initial_stamp(stamp: EventStamp) -> Self {
        Self {
            receipts: ReceiptWindow {
                max_seen: stamp.id.sequence,
                missing: Vec::new(),
            },
            clock: Some(EventClock {
                next_sequence: stamp.id.sequence + 1,
                last_time: stamp.time,
            }),
        }
    }
}

struct CausalCache {
    local: CausalState,
    local_dirty: bool,
    remote_states: BTreeMap<InstallationId, CausalState>,
    dirty_remote_installations: BTreeSet<InstallationId>,
}

pub(crate) struct CausalTracker {
    local_installation_id: InstallationId,
    state: Arc<StateHandle<InstallationId, CausalState>>,
    cache: Mutex<CausalCache>,
}

impl CausalTracker {
    pub(crate) fn create(
        state: Arc<StateHandle<InstallationId, CausalState>>,
        local_installation_id: InstallationId,
        local: CausalState,
    ) -> Self {
        Self {
            local_installation_id,
            state,
            cache: Mutex::new(CausalCache {
                local,
                local_dirty: false,
                remote_states: BTreeMap::new(),
                dirty_remote_installations: BTreeSet::new(),
            }),
        }
    }

    pub(crate) fn open(
        state: Arc<StateHandle<InstallationId, CausalState>>,
        local_installation_id: InstallationId,
    ) -> Result<Self> {
        let mut causal_states = state
            .read()
            .entries()
            .map(|(installation_id, state)| (installation_id.into_owned(), state.into_owned()))
            .collect::<BTreeMap<_, _>>();
        let local = causal_states
            .remove(&local_installation_id)
            .ok_or(Error::LocalCausalStateNotFound(local_installation_id))?;
        if local.clock.is_none() {
            return Err(Error::CorruptCausalState(
                "local installation has no clock checkpoint".to_owned(),
            ));
        }

        Ok(Self {
            local_installation_id,
            state,
            cache: Mutex::new(CausalCache {
                local,
                local_dirty: false,
                remote_states: causal_states,
                dirty_remote_installations: BTreeSet::new(),
            }),
        })
    }

    pub(crate) fn mint(&self) -> Result<EventStamp> {
        let mut cache = self.cache.lock();
        let clock = cache
            .local
            .clock
            .as_mut()
            .expect("the local causal state always owns a clock");
        if clock.next_sequence == u64::MAX {
            return Err(Error::ClockExhausted);
        }

        let wall = physical_ms().ok_or(Error::ClockExhausted)?;
        let time = if wall > clock.last_time.physical_ms {
            EventTime {
                physical_ms: wall,
                logical: 0,
            }
        } else {
            EventTime {
                physical_ms: clock.last_time.physical_ms,
                logical: clock
                    .last_time
                    .logical
                    .checked_add(1)
                    .ok_or(Error::ClockExhausted)?,
            }
        };
        let stamp = EventStamp {
            id: zendb_types::EventId {
                author: self.local_installation_id,
                sequence: clock.next_sequence,
            },
            time,
        };
        // Advance the allocator before table I/O. A failed commit therefore
        // consumes this sequence and leaves an explicit causal gap.
        clock.next_sequence += 1;
        clock.last_time = time;
        cache.local_dirty = true;
        Ok(stamp)
    }

    pub(crate) fn observe(&self, stamp: EventStamp) -> Result<ObserveOutcome> {
        let wall = physical_ms().ok_or(Error::ClockExhausted)?;

        let mut cache = self.cache.lock();
        // Observation records receipts and merges the HLC, but never allocates
        // a local sequence; only mint() owns that counter.
        let outcome = if stamp.id.author == self.local_installation_id {
            cache.local.receipts.observe(stamp.id.sequence)?
        } else {
            cache
                .remote_states
                .entry(stamp.id.author)
                .or_default()
                .receipts
                .observe(stamp.id.sequence)?
        };

        let clock = cache
            .local
            .clock
            .as_mut()
            .expect("the local causal state always owns a clock");
        let physical = wall
            .max(clock.last_time.physical_ms)
            .max(stamp.time.physical_ms);
        let logical = if physical > clock.last_time.physical_ms && physical > stamp.time.physical_ms
        {
            0
        } else if physical == clock.last_time.physical_ms && physical == stamp.time.physical_ms {
            clock.last_time.logical.max(stamp.time.logical)
        } else if physical == clock.last_time.physical_ms {
            clock.last_time.logical
        } else {
            stamp.time.logical
        };
        clock.last_time = EventTime {
            physical_ms: physical,
            logical,
        };
        cache.local_dirty = true;
        if stamp.id.author != self.local_installation_id {
            cache.dirty_remote_installations.insert(stamp.id.author);
        }
        Ok(outcome)
    }

    pub(crate) fn flush(&self) -> Result<()> {
        let mut cache = self.cache.lock();
        let mut state = self.state.write_internal();
        if cache.local_dirty {
            state.put(self.local_installation_id, cache.local.clone())?;
        }
        for installation_id in &cache.dirty_remote_installations {
            state.put(
                *installation_id,
                cache
                    .remote_states
                    .get(installation_id)
                    .expect("dirty installations remain in the causal cache")
                    .clone(),
            )?;
        }
        state.flush()?;
        cache.local_dirty = false;
        cache.dirty_remote_installations.clear();
        Ok(())
    }

    pub(crate) fn sync(&self) -> Result<()> {
        let mut cache = self.cache.lock();
        let mut state = self.state.write_internal();
        if cache.local_dirty {
            state.put(self.local_installation_id, cache.local.clone())?;
        }
        for installation_id in &cache.dirty_remote_installations {
            state.put(
                *installation_id,
                cache
                    .remote_states
                    .get(installation_id)
                    .expect("dirty installations remain in the causal cache")
                    .clone(),
            )?;
        }
        state.sync()?;
        cache.local_dirty = false;
        cache.dirty_remote_installations.clear();
        Ok(())
    }
}

impl Drop for CausalTracker {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
