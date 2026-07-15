//! Durable per-Workspace shared event journal.

use std::{io, path::Path};

use zendb_storage::{
    core::{
        btree::BPlusTreeConfig,
        keydir::KeyDirConfig,
        traits::{Backend, DurableStorage, OrderedBackend},
    },
    frontend::state::{State, StateConfig},
};
use zendb_types::{ContiguousFrontier, DeviceId, EventIdentity, Hlc, ReplicatedEvent};

const FRONTIER_KEY: &str = "local";

pub(crate) struct SharedJournal {
    events: State<EventIdentity, ReplicatedEvent>,
    frontier_state: State<String, ContiguousFrontier>,
    frontier: ContiguousFrontier,
}

impl SharedJournal {
    pub(crate) fn create(events_path: &Path, frontier_path: &Path) -> io::Result<Self> {
        let events = State::create(
            events_path,
            StateConfig::Ordered(BPlusTreeConfig::default()),
        )?;
        let mut frontier_state = State::create(
            frontier_path,
            StateConfig::Unordered(KeyDirConfig::default()),
        )?;
        let frontier = ContiguousFrontier::default();
        frontier_state.put(FRONTIER_KEY.into(), frontier.clone())?;
        frontier_state.sync()?;
        Ok(Self {
            events,
            frontier_state,
            frontier,
        })
    }

    pub(crate) fn open(events_path: &Path, frontier_path: &Path) -> io::Result<Self> {
        let events = State::open(
            events_path,
            StateConfig::Ordered(BPlusTreeConfig::default()),
        )?;
        let frontier_state: State<String, ContiguousFrontier> = State::open(
            frontier_path,
            StateConfig::Unordered(KeyDirConfig::default()),
        )?;
        let frontier = frontier_state
            .get(&FRONTIER_KEY.to_owned())
            .map(|value| value.into_owned())
            .unwrap_or_default();
        Ok(Self {
            events,
            frontier_state,
            frontier,
        })
    }

    pub(crate) fn store(&mut self, event: ReplicatedEvent) -> io::Result<bool> {
        let identity = event.envelope.event_id;
        if let Some(existing) = self.events.get(&identity) {
            if existing.envelope.payload_hash != event.envelope.payload_hash {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "conflicting payloads use the same EventIdentity",
                ));
            }
            return Ok(false);
        }
        self.events.put(identity, event)?;
        self.events.sync()?;
        Ok(true)
    }

    pub(crate) fn next_unapplied(&self, origin: DeviceId) -> Option<ReplicatedEvent> {
        let identity = EventIdentity {
            origin_device_id: origin,
            origin_seq: self.frontier.applied_through(&origin) + 1,
        };
        self.events.get(&identity).map(|event| event.into_owned())
    }

    pub(crate) fn mark_applied(&mut self, identity: EventIdentity) -> io::Result<()> {
        let expected = self.frontier.applied_through(&identity.origin_device_id) + 1;
        if identity.origin_seq != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot advance a frontier across a sequence gap",
            ));
        }
        if !self.frontier.observe(identity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frontier did not advance after applying its next event",
            ));
        }
        self.frontier_state
            .put(FRONTIER_KEY.into(), self.frontier.clone())?;
        self.frontier_state.sync()
    }

    pub(crate) fn frontier(&self) -> &ContiguousFrontier {
        &self.frontier
    }

    pub(crate) fn range(
        &self,
        origin: DeviceId,
        from_inclusive: u64,
        to_inclusive: u64,
    ) -> Vec<ReplicatedEvent> {
        if from_inclusive > to_inclusive {
            return Vec::new();
        }
        let start = EventIdentity {
            origin_device_id: origin,
            origin_seq: from_inclusive,
        };
        if let Some(after_end) = to_inclusive.checked_add(1) {
            let end = EventIdentity {
                origin_device_id: origin,
                origin_seq: after_end,
            };
            self.events
                .range(&start, &end)
                .map(|(_, event)| event.into_owned())
                .collect()
        } else {
            self.events
                .entries()
                .filter(|(identity, _)| {
                    identity.origin_device_id == origin && identity.origin_seq >= from_inclusive
                })
                .map(|(_, event)| event.into_owned())
                .collect()
        }
    }

    pub(crate) fn event_hlc(&self, origin: DeviceId, sequence: u64) -> Option<Hlc> {
        self.events
            .get(&EventIdentity {
                origin_device_id: origin,
                origin_seq: sequence,
            })
            .map(|event| event.event.hlc)
    }

    pub(crate) fn highest_sequence(&self, origin: DeviceId) -> u64 {
        self.events
            .range(
                &EventIdentity {
                    origin_device_id: origin,
                    origin_seq: 0,
                },
                &EventIdentity {
                    origin_device_id: origin,
                    origin_seq: u64::MAX,
                },
            )
            .last()
            .map_or(0, |(identity, _)| identity.origin_seq)
    }

    pub(crate) fn install_snapshot_frontier(
        &mut self,
        frontier: ContiguousFrontier,
    ) -> io::Result<()> {
        self.frontier = frontier;
        self.frontier_state
            .put(FRONTIER_KEY.into(), self.frontier.clone())?;
        self.frontier_state.sync()
    }
}
