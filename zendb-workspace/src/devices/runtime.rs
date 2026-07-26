//! Device registry runtime, role checks, and stamped table mutations.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::RwLock;
use zendb_storage::{ReadBackend, Table};
use zendb_types::{
    Blob, Event, EventStamp, Op, Path, PeerId, PeerIdentity, PrimaryKey, Roles, Value,
};

use super::{
    peer::{PeerRecord, PeerStore},
    receipts::ObserveOutcome,
};
use crate::{consts::DEVICES_TABLE_NAME, states::StateHandle, tables::TableHandle, Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceRecord {
    pub name: String,
    pub roles: BTreeSet<Roles>,
}

/// Peer registry, hybrid clock, roles, and duplicate-event tracking.
///
/// Owns the `_devices` table handle, the `PeerStore` (clock + receipts), and
/// the in-memory device records map. The records map is populated from durable
/// storage during `open` and then updated reactively by the `DeviceSync`
/// listener.
pub struct Devices {
    pub(crate) registry: Arc<TableHandle>,
    peer_store: Arc<PeerStore>,
    pub(crate) records: RwLock<BTreeMap<PeerId, DeviceRecord>>,
}

impl Devices {
    pub(crate) fn create(
        table: Table,
        peer_state: Arc<StateHandle<PeerId, PeerRecord>>,
        peer: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>> {
        Self::bind(table, PeerStore::create(peer_state, peer)?)
    }

    pub(crate) fn open(
        table: Table,
        peer_state: Arc<StateHandle<PeerId, PeerRecord>>,
        peer: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>> {
        let devices = Self::bind(table, PeerStore::open(peer_state, peer)?)?;
        devices.load_records()?;
        Ok(devices)
    }

    fn bind(table: Table, peers: Arc<PeerStore>) -> Result<Arc<Self>> {
        Ok(Arc::new_cyclic(|devices_weak| Self {
            registry: TableHandle::new(
                DEVICES_TABLE_NAME.to_owned(),
                table,
                devices_weak.clone(),
                true,
            ),
            peer_store: peers,
            records: RwLock::new(BTreeMap::new()),
        }))
    }

    pub(crate) fn register_local_device(&self) -> Result<()> {
        if self.records.read().contains_key(&self.local_peer_id()) {
            return Ok(());
        }
        self.upsert_internal(
            self.local_peer_id(),
            DeviceRecord {
                name: String::new(),
                roles: BTreeSet::from([Roles::Operator]),
            },
        )
    }

    pub(crate) fn require_local_device(&self) -> Result<()> {
        if self.records.read().contains_key(&self.local_peer_id()) {
            Ok(())
        } else {
            Err(Error::DeviceNotRegistered(self.local_peer_id()))
        }
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.peer_store.local_peer_id()
    }

    pub fn list(&self) -> Vec<(PeerId, DeviceRecord)> {
        self.records
            .read()
            .iter()
            .map(|(id, record)| (*id, record.clone()))
            .collect()
    }

    pub fn get(&self, peer: PeerId) -> Option<DeviceRecord> {
        self.records.read().get(&peer).cloned()
    }

    pub fn upsert(&self, peer: PeerId, record: DeviceRecord) -> Result<bool> {
        let current = self.records.read().get(&peer).cloned();
        if current.as_ref() == Some(&record) {
            return Ok(false);
        }
        let operator = self.has_role(Roles::Operator);
        let contributor_self_edit = peer == self.local_peer_id()
            && self.has_role(Roles::Contributor)
            && current
                .as_ref()
                .is_some_and(|current| current.roles == record.roles);
        if !operator && !contributor_self_edit {
            return Err(Error::PermissionDenied);
        }
        self.upsert_internal(peer, record)?;
        Ok(true)
    }

    pub(crate) fn mint(&self) -> Result<EventStamp> {
        self.peer_store.mint()
    }

    pub(crate) fn observe(&self, stamp: EventStamp) -> Result<ObserveOutcome> {
        self.peer_store.observe(stamp)
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

    pub(crate) fn upsert_internal(&self, peer: PeerId, record: DeviceRecord) -> Result<()> {
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

    fn load_records(&self) -> Result<()> {
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
