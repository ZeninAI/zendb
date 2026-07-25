//! Peer metadata, centralized role checks, and stamped Table mutations.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::RwLock;
use zendb_storage::{InsertOutcome, ReadBackend};
use zendb_types::{Blob, Event, EventId, EventStamp, Op, Path, PeerId, PrimaryKey, Roles, Value};

use super::{
    clock::{PeerRecord, PeerStore},
    receipts::ObserveOutcome,
};
use crate::{
    catalog::{StateHandle, TableEntry},
    Error, Result,
};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceRecord {
    pub name: String,
    pub roles: BTreeSet<Roles>,
}

struct DevicesInner {
    registry: Arc<TableEntry>,
    peers: Arc<PeerStore>,
    records: RwLock<BTreeMap<PeerId, DeviceRecord>>,
}

#[derive(Clone)]
pub struct Devices {
    inner: Arc<DevicesInner>,
}

impl Devices {
    pub(crate) fn create(
        registry: Arc<TableEntry>,
        state: StateHandle<PeerId, PeerRecord>,
        local_peer_id: PeerId,
    ) -> Result<Self> {
        Self::bind(registry, PeerStore::create(state, local_peer_id)?)
    }

    pub(crate) fn open(
        registry: Arc<TableEntry>,
        state: StateHandle<PeerId, PeerRecord>,
        local_peer_id: PeerId,
    ) -> Result<Self> {
        Self::bind(registry, PeerStore::open(state, local_peer_id)?)
    }

    fn bind(registry: Arc<TableEntry>, peers: Arc<PeerStore>) -> Result<Self> {
        let devices = Self {
            inner: Arc::new(DevicesInner {
                registry,
                peers,
                records: RwLock::new(BTreeMap::new()),
            }),
        };
        devices.reload()?;
        Ok(devices)
    }

    pub(crate) fn bootstrap_local(&self, name: String) -> Result<()> {
        if self
            .inner
            .records
            .read()
            .contains_key(&self.local_peer_id())
        {
            return Ok(());
        }
        self.write_record(
            self.local_peer_id(),
            DeviceRecord {
                name,
                roles: BTreeSet::from([Roles::Operator]),
            },
        )
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.inner.peers.local_peer_id()
    }

    pub fn list(&self) -> Vec<(PeerId, DeviceRecord)> {
        self.inner
            .records
            .read()
            .iter()
            .map(|(id, record)| (*id, record.clone()))
            .collect()
    }

    pub fn record(&self, peer: PeerId) -> Option<DeviceRecord> {
        self.inner.records.read().get(&peer).cloned()
    }

    pub fn upsert(&self, peer: PeerId, record: DeviceRecord) -> Result<()> {
        let operator = self.has_role(Roles::Operator);
        let contributor_self_edit = peer == self.local_peer_id()
            && self.has_role(Roles::Contributor)
            && self
                .inner
                .records
                .read()
                .get(&peer)
                .is_some_and(|current| current.roles == record.roles);
        if !operator && !contributor_self_edit {
            return Err(Error::PermissionDenied);
        }
        self.write_record(peer, record)
    }

    pub fn mint(&self) -> Result<EventStamp> {
        self.inner.peers.mint()
    }

    pub fn has_received(&self, id: EventId) -> bool {
        self.inner.peers.has_received(id)
    }

    pub fn observe(&self, stamp: EventStamp) -> Result<ObserveOutcome> {
        self.inner.peers.observe(stamp)
    }

    pub fn missing(&self, peer: PeerId) -> Vec<RangeInclusive<u64>> {
        self.inner.peers.missing(peer)
    }

    pub fn flush(&self) -> Result<()> {
        self.inner.peers.flush()
    }

    pub(crate) fn authorize(&self, required: Roles) -> Result<()> {
        if self.has_role(required) {
            Ok(())
        } else {
            Err(Error::PermissionDenied)
        }
    }

    pub(crate) fn insert_application(
        &self,
        entry: &Arc<TableEntry>,
        primary_key: PrimaryKey,
        path: Path,
        op: Op,
    ) -> Result<InsertOutcome> {
        self.authorize(Roles::Contributor)?;
        let stamp = self.mint()?;
        let outcome = entry.table.write().insert(Event {
            primary_key,
            path,
            op,
            stamp,
        })?;
        self.observe(stamp)?;
        Ok(outcome)
    }

    fn has_role(&self, required: Roles) -> bool {
        self.inner
            .records
            .read()
            .get(&self.local_peer_id())
            .is_some_and(|record| {
                record.roles.contains(&required)
                    || (required == Roles::Contributor && record.roles.contains(&Roles::Operator))
            })
    }

    fn write_record(&self, peer: PeerId, record: DeviceRecord) -> Result<()> {
        let stamp = self.mint()?;
        let blob = Blob::encode(&record)?;
        self.inner.registry.table.write().insert(Event {
            primary_key: PrimaryKey::PeerId(peer),
            path: Path::new(),
            op: Op::Upsert {
                value: Value::Blob(blob),
            },
            stamp,
        })?;
        self.observe(stamp)?;
        self.inner.records.write().insert(peer, record);
        Ok(())
    }

    fn reload(&self) -> Result<()> {
        let table = self.inner.registry.table.read();
        let mut records = BTreeMap::new();
        for (key, cell) in table.entries() {
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
            let record = blob.decode().map_err(|error| {
                Error::CorruptDeviceRegistry(format!("peer {peer_id} cannot be decoded: {error}"))
            })?;
            records.insert(peer_id, record);
        }
        *self.inner.records.write() = records;
        Ok(())
    }
}
