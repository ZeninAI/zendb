//! Peer metadata, centralized role checks, and stamped Table mutations.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::RwLock;
use zendb_storage::ReadBackend;
use zendb_types::{
    Blob, Event, EventId, EventStamp, Op, Path, PeerId, PeerIdentity, PrimaryKey, Roles, Value,
};

use super::{
    clock::{PeerRecord, PeerStore},
    receipts::ObserveOutcome,
};
use crate::{states::StateHandle, tables::TableHandle, Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceRecord {
    pub name: String,
    pub roles: BTreeSet<Roles>,
}

/// Peer registry, hybrid clock, roles, and duplicate-event tracking.
///
/// Owns the `_devices` table handle, the `PeerStore` (clock + receipts), and
/// the in-memory device records map. The records map is updated reactively by
/// the `DeviceSync` listener after `reload` performs the initial cold-start
/// load.
pub struct Devices {
    pub(crate) registry: Arc<TableHandle>,
    pub(crate) peers: Arc<PeerStore>,
    pub(crate) records: RwLock<BTreeMap<PeerId, DeviceRecord>>,
}

impl Devices {
    pub(crate) fn create(
        registry: Arc<TableHandle>,
        peer_state: Arc<StateHandle<PeerId, PeerRecord>>,
        peer: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>> {
        Self::bind(registry, PeerStore::create(peer_state, peer)?)
    }

    pub(crate) fn open(
        registry: Arc<TableHandle>,
        peer_state: Arc<StateHandle<PeerId, PeerRecord>>,
        peer: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>> {
        Self::bind(registry, PeerStore::open(peer_state, peer)?)
    }

    fn bind(registry: Arc<TableHandle>, peers: Arc<PeerStore>) -> Result<Arc<Self>> {
        let devices = Arc::new(Self {
            registry,
            peers,
            records: RwLock::new(BTreeMap::new()),
        });
        devices.reload()?;
        Ok(devices)
    }

    pub(crate) fn bootstrap_local(&self) -> Result<()> {
        if self.records.read().contains_key(&self.local_peer_id()) {
            return Ok(());
        }
        self.write_record(
            self.local_peer_id(),
            DeviceRecord {
                name: String::new(),
                roles: BTreeSet::from([Roles::Operator]),
            },
        )
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.peers.local_peer_id()
    }

    pub fn list(&self) -> Vec<(PeerId, DeviceRecord)> {
        self.records
            .read()
            .iter()
            .map(|(id, record)| (*id, record.clone()))
            .collect()
    }

    pub fn record(&self, peer: PeerId) -> Option<DeviceRecord> {
        self.records.read().get(&peer).cloned()
    }

    pub fn upsert(&self, peer: PeerId, record: DeviceRecord) -> Result<()> {
        let operator = self.has_role(Roles::Operator);
        let contributor_self_edit = peer == self.local_peer_id()
            && self.has_role(Roles::Contributor)
            && self
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
        self.peers.mint()
    }

    pub fn has_received(&self, id: EventId) -> bool {
        self.peers.has_received(id)
    }

    pub fn observe(&self, stamp: EventStamp) -> Result<ObserveOutcome> {
        self.peers.observe(stamp)
    }

    pub fn missing(&self, peer: PeerId) -> Vec<RangeInclusive<u64>> {
        self.peers.missing(peer)
    }

    pub fn flush(&self) -> Result<()> {
        self.peers.flush()
    }

    pub(crate) fn authorize(&self, required: Roles) -> Result<()> {
        if self.has_role(required) {
            Ok(())
        } else {
            Err(Error::PermissionDenied)
        }
    }

    fn has_role(&self, required: Roles) -> bool {
        self.records
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
        self.registry.insert_internal(Event {
            primary_key: PrimaryKey::PeerId(peer),
            path: Path::new(),
            op: Op::Upsert {
                value: Value::Blob(blob),
            },
            stamp,
        })?;
        Ok(())
    }

    fn reload(&self) -> Result<()> {
        let table = self.registry.read();
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
        *self.records.write() = records;
        Ok(())
    }
}
