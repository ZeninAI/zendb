//! Device registry, cached peer clocks, receipts, and authorization.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::{Mutex, RwLock};
use zendb_storage::{DurableStorage, ReadBackend, Table, WriteBackend};
use zendb_types::{
    Blob, Event, EventId, EventStamp, EventTime, Op, Path, PeerId, PeerIdentity, PrimaryKey, Role,
    Value, utils::time::physical_ms,
};

use super::receipts::{ObserveOutcome, ReceiptWindow};
use crate::{Error, Result, consts::DEVICES_TABLE_NAME, states::StateHandle, tables::TableHandle};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceRecord {
    pub display_name: String,
    pub role: Option<Role>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct EventClock {
    pub next_sequence: u64,
    pub last_time: EventTime,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub(crate) struct PeerState {
    pub receipts: ReceiptWindow,
    pub clock: Option<EventClock>,
}

pub(crate) struct RegistryCache {
    pub(crate) entries: BTreeMap<PeerId, DeviceRecord>,
    pub(crate) local_role: Option<Role>,
}

struct PeerCache {
    local: PeerState,
    local_dirty: bool,
    others: BTreeMap<PeerId, PeerState>,
    dirty_others: BTreeSet<PeerId>,
}

/// Device registry, hybrid clock, roles, and duplicate-event tracking.
pub struct Devices {
    local_peer_id: PeerId,
    pub(crate) registry: Arc<TableHandle>,
    pub(crate) registry_cache: RwLock<RegistryCache>,
    peer_state: Arc<StateHandle<PeerId, PeerState>>,
    peer_cache: Mutex<PeerCache>,
}

impl Devices {
    pub(crate) fn create(
        mut registry: Table,
        peer_state: Arc<StateHandle<PeerId, PeerState>>,
        identity: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>> {
        let local_peer_id = *identity.peer_id();
        let record = DeviceRecord {
            display_name: identity.display_name().to_owned(),
            role: Some(Role::Admin),
        };
        let time = EventTime {
            physical_ms: physical_ms().ok_or(Error::ClockExhausted)?,
            logical: 0,
        };
        registry.insert(Event {
            primary_key: PrimaryKey::PeerId(local_peer_id),
            path: Path::new(),
            op: Op::Upsert {
                value: Value::Blob(Blob::encode(&record)?),
            },
            stamp: EventStamp {
                id: EventId {
                    peer_id: local_peer_id,
                    sequence: 1,
                },
                time,
            },
        })?;

        Ok(Arc::new_cyclic(|devices_weak| Self {
            local_peer_id,
            registry: TableHandle::new(
                DEVICES_TABLE_NAME.to_owned(),
                registry,
                devices_weak.clone(),
                true,
            ),
            registry_cache: RwLock::new(RegistryCache {
                entries: BTreeMap::from([(local_peer_id, record)]),
                local_role: Some(Role::Admin),
            }),
            peer_state,
            peer_cache: Mutex::new(PeerCache {
                local: PeerState {
                    receipts: ReceiptWindow {
                        max_seen: 1,
                        missing: Vec::new(),
                    },
                    clock: Some(EventClock {
                        next_sequence: 2,
                        last_time: time,
                    }),
                },
                local_dirty: true,
                others: BTreeMap::new(),
                dirty_others: BTreeSet::new(),
            }),
        }))
    }

    pub(crate) fn open(
        registry: Table,
        peer_state: Arc<StateHandle<PeerId, PeerState>>,
        identity: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>> {
        let local_peer_id = *identity.peer_id();

        // The registry — not the clock checkpoint — is the authorization gate:
        // only an enrolled device may open the workspace. Load and validate it
        // first so an unenrolled peer fails before any clock bookkeeping.
        let mut entries = BTreeMap::new();
        for (key, cell) in registry.entries() {
            let PrimaryKey::PeerId(peer_id) = key.into_owned() else {
                return Err(Error::CorruptDeviceRegistry(
                    "devices table contains a non-peer key".to_owned(),
                ));
            };
            let blob = match cell.into_owned().value {
                Some(Value::Blob(blob)) => blob,
                None => continue,
                Some(_) => {
                    return Err(Error::CorruptDeviceRegistry(format!(
                        "peer {peer_id} is not stored as a Blob"
                    )));
                }
            };
            let record: DeviceRecord = blob.decode().map_err(|error| {
                Error::CorruptDeviceRegistry(format!("peer {peer_id} cannot be decoded: {error}"))
            })?;
            entries.insert(peer_id, record);
        }
        let local_role = entries
            .get(&local_peer_id)
            .ok_or(Error::DeviceNotRegistered(local_peer_id))?
            .role;

        // Load clock checkpoints for every peer that has written to this
        // workspace. The local peer may be absent: a device can be enrolled in
        // the registry yet have never minted from this on-disk state (e.g. it
        // opened the workspace from a different machine, or joined but has not
        // written yet). In that case we seed a fresh starting clock rather than
        // refusing to open.
        let mut peer_states = peer_state
            .read()
            .entries()
            .map(|(peer_id, state)| (peer_id.into_owned(), state.into_owned()))
            .collect::<BTreeMap<_, _>>();
        let (local, seeded) = match peer_states.remove(&local_peer_id) {
            Some(state) => {
                if state.clock.is_none() {
                    return Err(Error::CorruptLocalState(
                        "local peer has no clock checkpoint".to_owned(),
                    ));
                }
                (state, false)
            }
            None => {
                let time = EventTime {
                    physical_ms: physical_ms().ok_or(Error::ClockExhausted)?,
                    logical: 0,
                };
                (
                    PeerState {
                        receipts: ReceiptWindow::default(),
                        clock: Some(EventClock {
                            next_sequence: 1,
                            last_time: time,
                        }),
                    },
                    true,
                )
            }
        };

        Ok(Arc::new_cyclic(|devices_weak| Self {
            local_peer_id,
            registry: TableHandle::new(
                DEVICES_TABLE_NAME.to_owned(),
                registry,
                devices_weak.clone(),
                true,
            ),
            registry_cache: RwLock::new(RegistryCache {
                entries,
                local_role,
            }),
            peer_state,
            peer_cache: Mutex::new(PeerCache {
                local,
                local_dirty: seeded,
                others: peer_states,
                dirty_others: BTreeSet::new(),
            }),
        }))
    }

    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    pub fn list(&self) -> Vec<(PeerId, DeviceRecord)> {
        self.registry_cache
            .read()
            .entries
            .iter()
            .map(|(id, record)| (*id, record.clone()))
            .collect()
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<DeviceRecord> {
        self.registry_cache.read().entries.get(peer_id).cloned()
    }

    pub fn upsert(&self, peer_id: PeerId, record: DeviceRecord) -> Result<bool> {
        self.require_access(&self.local_peer_id, Role::Admin)?;
        if self.registry_cache.read().entries.get(&peer_id) == Some(&record) {
            return Ok(false);
        }
        let stamp = self.mint()?;
        self.registry.insert_internal(Event {
            primary_key: PrimaryKey::PeerId(peer_id),
            path: Path::new(),
            op: Op::Upsert {
                value: Value::Blob(Blob::encode(&record)?),
            },
            stamp,
        })?;
        Ok(true)
    }

    pub fn has_access(&self, peer_id: &PeerId, required: Role) -> bool {
        let cache = self.registry_cache.read();
        let role = if peer_id == &self.local_peer_id {
            cache.local_role
        } else {
            cache.entries.get(peer_id).and_then(|record| record.role)
        };
        role.is_some_and(|role| role.has_at_least(required))
    }

    pub fn flush(&self) -> Result<()> {
        let mut cache = self.peer_cache.lock();
        let mut state = self.peer_state.write_internal();
        if cache.local_dirty {
            state.put(self.local_peer_id, cache.local.clone())?;
        }
        for peer_id in &cache.dirty_others {
            state.put(
                *peer_id,
                cache
                    .others
                    .get(peer_id)
                    .expect("dirty peers remain in the peer cache")
                    .clone(),
            )?;
        }
        state.flush()?;
        cache.local_dirty = false;
        cache.dirty_others.clear();
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut cache = self.peer_cache.lock();
        let mut state = self.peer_state.write_internal();
        if cache.local_dirty {
            state.put(self.local_peer_id, cache.local.clone())?;
        }
        for peer_id in &cache.dirty_others {
            state.put(
                *peer_id,
                cache
                    .others
                    .get(peer_id)
                    .expect("dirty peers remain in the peer cache")
                    .clone(),
            )?;
        }
        state.sync()?;
        cache.local_dirty = false;
        cache.dirty_others.clear();
        Ok(())
    }

    pub(crate) fn mint(&self) -> Result<EventStamp> {
        let mut cache = self.peer_cache.lock();
        let stamp = {
            let clock = cache
                .local
                .clock
                .as_mut()
                .expect("the local PeerState always owns a clock");
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
                id: EventId {
                    peer_id: self.local_peer_id,
                    sequence: clock.next_sequence,
                },
                time,
            };
            clock.next_sequence += 1;
            clock.last_time = time;
            stamp
        };
        cache.local_dirty = true;
        Ok(stamp)
    }

    pub(crate) fn observe(&self, stamp: EventStamp) -> Result<ObserveOutcome> {
        let wall = physical_ms().ok_or(Error::ClockExhausted)?;
        if stamp.id.peer_id == self.local_peer_id && stamp.id.sequence == u64::MAX {
            return Err(Error::ClockExhausted);
        }

        let mut cache = self.peer_cache.lock();
        let outcome = if stamp.id.peer_id == self.local_peer_id {
            cache.local.receipts.observe(stamp.id.sequence)?
        } else {
            cache
                .others
                .entry(stamp.id.peer_id)
                .or_default()
                .receipts
                .observe(stamp.id.sequence)?
        };

        let clock = cache
            .local
            .clock
            .as_mut()
            .expect("the local PeerState always owns a clock");
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
        if stamp.id.peer_id == self.local_peer_id {
            clock.next_sequence = clock.next_sequence.max(stamp.id.sequence + 1);
        }
        cache.local_dirty = true;
        if stamp.id.peer_id != self.local_peer_id {
            cache.dirty_others.insert(stamp.id.peer_id);
        }
        Ok(outcome)
    }

    pub(crate) fn require_access(&self, peer_id: &PeerId, required: Role) -> Result<()> {
        if self.has_access(peer_id, required) {
            Ok(())
        } else {
            Err(Error::PermissionDenied)
        }
    }
}

impl Drop for Devices {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
