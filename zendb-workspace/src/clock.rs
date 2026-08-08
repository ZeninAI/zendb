//! Workspace hybrid logical clock used to mint and merge event timestamps.

use parking_lot::Mutex;
use zendb_types::{EventStamp, EventTime, utils::time::physical_ms};

use crate::{Error, Result};

pub(crate) struct HybridClock {
    last_time: Mutex<EventTime>,
}

impl HybridClock {
    pub(crate) fn new(last_time: EventTime) -> Self {
        Self {
            last_time: Mutex::new(last_time),
        }
    }

    pub(crate) fn mint(&self) -> Result<EventTime> {
        let wall = physical_ms().ok_or(Error::ClockExhausted)?;
        let mut last_time = self.last_time.lock();
        let time = if wall > last_time.physical_ms {
            EventTime {
                physical_ms: wall,
                logical: 0,
            }
        } else {
            EventTime {
                physical_ms: last_time.physical_ms,
                logical: last_time
                    .logical
                    .checked_add(1)
                    .ok_or(Error::ClockExhausted)?,
            }
        };
        *last_time = time;
        Ok(time)
    }

    pub(crate) fn observe(&self, stamp: EventStamp) -> Result<()> {
        let wall = physical_ms().ok_or(Error::ClockExhausted)?;
        let mut last_time = self.last_time.lock();
        let physical = wall.max(last_time.physical_ms).max(stamp.time.physical_ms);
        let logical = if physical > last_time.physical_ms && physical > stamp.time.physical_ms {
            0
        } else if physical == last_time.physical_ms && physical == stamp.time.physical_ms {
            last_time.logical.max(stamp.time.logical)
        } else if physical == last_time.physical_ms {
            last_time.logical
        } else {
            stamp.time.logical
        };
        *last_time = EventTime {
            physical_ms: physical,
            logical,
        };
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> EventTime {
        *self.last_time.lock()
    }
}
