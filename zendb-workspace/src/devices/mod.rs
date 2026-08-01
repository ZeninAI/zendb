//! Device registry, cached installation bookkeeping, and receipt tracking.

pub(crate) mod listeners;
mod receipts;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::{Mutex, RwLock};
use zendb_storage::{DurableStorage, ReadBackend, Table, WriteBackend};
use zendb_types::{
    Blob, Event, EventId, EventStamp, EventTime, InstallationId, Op, Path, PublicKey, Role, Value,
    utils::time::physical_ms,
};

use self::receipts::{ObserveOutcome, ReceiptWindow};
use crate::{Error, Result, consts::DEVICES_TABLE_NAME, states::StateHandle, tables::TableHandle};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceRecord {
    pub display_name: String,
    pub role: Option<Role>,
    pub public_key: PublicKey,
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
    pub(crate) entries: BTreeMap<InstallationId, DeviceRecord>,
    pub(crate) local_role: Option<Role>,
}

struct PeerCache {
    local: PeerState,
    local_dirty: bool,
    others: BTreeMap<InstallationId, PeerState>,
    dirty_others: BTreeSet<InstallationId>,
}

/// Device registry, hybrid clock, roles, and duplicate-event tracking.
pub struct Devices {
    local_installation_id: InstallationId,
    pub(crate) registry: Arc<TableHandle>,
    pub(crate) registry_cache: RwLock<RegistryCache>,
    peer_state: Arc<StateHandle<InstallationId, PeerState>>,
    peer_cache: Mutex<PeerCache>,
}

impl Devices {
    pub(crate) fn create(
        mut registry: Table,
        peer_state: Arc<StateHandle<InstallationId, PeerState>>,
        local_installation_id: InstallationId,
        display_name: String,
        public_key: PublicKey,
    ) -> Result<Arc<Self>> {
        let record = DeviceRecord {
            display_name,
            role: Some(Role::Admin),
            public_key,
        };
        let time = EventTime {
            physical_ms: physical_ms().ok_or(Error::ClockExhausted)?,
            logical: 0,
        };
        registry.insert(Event {
            primary_key: local_installation_id.into(),
            path: Path::new(),
            op: Op::Upsert {
                value: Value::Blob(Blob::encode(&record)?),
            },
            stamp: EventStamp {
                id: EventId {
                    author: local_installation_id,
                    sequence: 1,
                },
                time,
            },
        })?;

        Ok(Arc::new_cyclic(|devices_weak| Self {
            local_installation_id,
            registry: TableHandle::new(
                DEVICES_TABLE_NAME.to_owned(),
                registry,
                devices_weak.clone(),
                true,
            ),
            registry_cache: RwLock::new(RegistryCache {
                entries: BTreeMap::from([(local_installation_id, record)]),
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
        peer_state: Arc<StateHandle<InstallationId, PeerState>>,
        local_installation_id: InstallationId,
        expected_public_key: &PublicKey,
    ) -> Result<Arc<Self>> {
        let mut entries = BTreeMap::new();
        for (key, cell) in registry.entries() {
            let key = key.into_owned();
            let installation_id = InstallationId::try_from(&key).map_err(|error| {
                Error::CorruptDeviceRegistry(format!("invalid installation key: {error}"))
            })?;
            let blob = match cell.into_owned().value {
                Some(Value::Blob(blob)) => blob,
                None => continue,
                Some(_) => {
                    return Err(Error::CorruptDeviceRegistry(format!(
                        "installation {installation_id} is not stored as a Blob"
                    )));
                }
            };
            let record: DeviceRecord = blob.decode().map_err(|error| {
                Error::CorruptDeviceRegistry(format!(
                    "installation {installation_id} cannot be decoded: {error}"
                ))
            })?;
            entries.insert(installation_id, record);
        }
        let local_role = match entries.get(&local_installation_id) {
            Some(local_record) => {
                if &local_record.public_key != expected_public_key {
                    return Err(Error::LocalDeviceKeyMismatch);
                }
                local_record.role
            }
            None if entries.is_empty() => None,
            None => return Err(Error::DeviceNotRegistered(local_installation_id)),
        };

        let mut peer_states = peer_state
            .read()
            .entries()
            .map(|(installation_id, state)| (installation_id.into_owned(), state.into_owned()))
            .collect::<BTreeMap<_, _>>();
        let (local, seeded) = match peer_states.remove(&local_installation_id) {
            Some(state) => {
                if state.clock.is_none() {
                    return Err(Error::CorruptLocalState(
                        "local installation has no clock checkpoint".to_owned(),
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
            local_installation_id,
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

    pub(crate) fn join(
        registry: Table,
        peer_state: Arc<StateHandle<InstallationId, PeerState>>,
        local_installation_id: InstallationId,
    ) -> Result<Arc<Self>> {
        let time = EventTime {
            physical_ms: physical_ms().ok_or(Error::ClockExhausted)?,
            logical: 0,
        };
        Ok(Arc::new_cyclic(|devices_weak| Self {
            local_installation_id,
            registry: TableHandle::new(
                DEVICES_TABLE_NAME.to_owned(),
                registry,
                devices_weak.clone(),
                true,
            ),
            registry_cache: RwLock::new(RegistryCache {
                entries: BTreeMap::new(),
                local_role: None,
            }),
            peer_state,
            peer_cache: Mutex::new(PeerCache {
                local: PeerState {
                    receipts: ReceiptWindow::default(),
                    clock: Some(EventClock {
                        next_sequence: 1,
                        last_time: time,
                    }),
                },
                local_dirty: true,
                others: BTreeMap::new(),
                dirty_others: BTreeSet::new(),
            }),
        }))
    }

    pub const fn local_installation_id(&self) -> InstallationId {
        self.local_installation_id
    }

    pub fn list(&self) -> Vec<(InstallationId, DeviceRecord)> {
        self.registry_cache
            .read()
            .entries
            .iter()
            .map(|(id, record)| (*id, record.clone()))
            .collect()
    }

    pub fn get(&self, installation_id: &InstallationId) -> Option<DeviceRecord> {
        self.registry_cache
            .read()
            .entries
            .get(installation_id)
            .cloned()
    }

    pub fn upsert(&self, installation_id: InstallationId, record: DeviceRecord) -> Result<bool> {
        self.require_access(&self.local_installation_id, Role::Admin)?;
        if self.registry_cache.read().entries.get(&installation_id) == Some(&record) {
            return Ok(false);
        }
        let stamp = self.mint()?;
        self.registry.insert_internal(Event {
            primary_key: installation_id.into(),
            path: Path::new(),
            op: Op::Upsert {
                value: Value::Blob(Blob::encode(&record)?),
            },
            stamp,
        })?;
        Ok(true)
    }

    pub fn remove(&self, installation_id: InstallationId) -> Result<bool> {
        self.require_access(&self.local_installation_id, Role::Admin)?;
        if !self
            .registry_cache
            .read()
            .entries
            .contains_key(&installation_id)
        {
            return Ok(false);
        }
        let stamp = self.mint()?;
        self.registry.insert_internal(Event {
            primary_key: installation_id.into(),
            path: Path::new(),
            op: Op::Delete,
            stamp,
        })?;
        Ok(true)
    }

    pub fn has_access(&self, installation_id: &InstallationId, required: Role) -> bool {
        let cache = self.registry_cache.read();
        let role = if installation_id == &self.local_installation_id {
            cache.local_role
        } else {
            cache
                .entries
                .get(installation_id)
                .and_then(|record| record.role)
        };
        role.is_some_and(|role| role.has_at_least(required))
    }

    pub fn flush(&self) -> Result<()> {
        let mut cache = self.peer_cache.lock();
        let mut state = self.peer_state.write_internal();
        if cache.local_dirty {
            state.put(self.local_installation_id, cache.local.clone())?;
        }
        for installation_id in &cache.dirty_others {
            state.put(
                *installation_id,
                cache
                    .others
                    .get(installation_id)
                    .expect("dirty installations remain in the peer cache")
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
            state.put(self.local_installation_id, cache.local.clone())?;
        }
        for installation_id in &cache.dirty_others {
            state.put(
                *installation_id,
                cache
                    .others
                    .get(installation_id)
                    .expect("dirty installations remain in the peer cache")
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
                    author: self.local_installation_id,
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
        if stamp.id.author == self.local_installation_id && stamp.id.sequence == u64::MAX {
            return Err(Error::ClockExhausted);
        }

        let mut cache = self.peer_cache.lock();
        let outcome = if stamp.id.author == self.local_installation_id {
            cache.local.receipts.observe(stamp.id.sequence)?
        } else {
            cache
                .others
                .entry(stamp.id.author)
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
        if stamp.id.author == self.local_installation_id {
            clock.next_sequence = clock.next_sequence.max(stamp.id.sequence + 1);
        }
        cache.local_dirty = true;
        if stamp.id.author != self.local_installation_id {
            cache.dirty_others.insert(stamp.id.author);
        }
        Ok(outcome)
    }

    pub(crate) fn require_access(
        &self,
        installation_id: &InstallationId,
        required: Role,
    ) -> Result<()> {
        if self.has_access(installation_id, required) {
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
