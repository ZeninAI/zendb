//! Unified in-memory peer state with Catalog-backed durability.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
    sync::Arc,
};

use arc_swap::ArcSwap;
use bincode::{Decode, Encode};
use parking_lot::Mutex;
use zendb_storage::{DurableStorage, ReadBackend, WriteBackend};
use zendb_types::{utils::time::physical_ms, EventId, EventStamp, EventTime, PeerId, PeerIdentity};

use super::receipts::{ObserveOutcome, ReceiptWindow};
use crate::{states::StateHandle, Error, Result};

type PeerMap = BTreeMap<PeerId, PeerRecord>;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ClockCheckpoint {
    pub next_sequence: u64,
    pub time: EventTime,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct PeerRecord {
    pub receipts: ReceiptWindow,
    pub clock: Option<ClockCheckpoint>,
}

struct PeerMutationState {
    dirty: BTreeSet<PeerId>,
}

pub(crate) struct PeerStore {
    // Retained for future event signing. The workspace never sees the
    // private key; `PeerIdentity::sign` goes through this handle.
    #[allow(dead_code)]
    peer: Arc<dyn PeerIdentity>,
    local_peer_id: PeerId,
    state: Arc<StateHandle<PeerId, PeerRecord>>,
    snapshot: ArcSwap<PeerMap>,
    writer: Mutex<PeerMutationState>,
}

impl PeerStore {
    pub(crate) fn create(
        state: Arc<StateHandle<PeerId, PeerRecord>>,
        peer: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>> {
        let local_peer_id = peer.peer_id();
        let record = PeerRecord {
            receipts: ReceiptWindow::default(),
            clock: Some(ClockCheckpoint {
                next_sequence: 1,
                time: EventTime::ZERO,
            }),
        };
        {
            let mut storage = state.write_internal();
            storage.put(local_peer_id, record.clone())?;
            storage.sync()?;
        }
        Ok(Arc::new(Self {
            peer,
            local_peer_id,
            state,
            snapshot: ArcSwap::from_pointee(BTreeMap::from([(local_peer_id, record)])),
            writer: Mutex::new(PeerMutationState {
                dirty: BTreeSet::new(),
            }),
        }))
    }

    pub(crate) fn open(
        state: Arc<StateHandle<PeerId, PeerRecord>>,
        peer: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>> {
        let local_peer_id = peer.peer_id();
        let peers = state
            .read()
            .entries()
            .map(|(peer, record)| (peer.into_owned(), record.into_owned()))
            .collect::<BTreeMap<_, _>>();
        if peers
            .get(&local_peer_id)
            .and_then(|record| record.clock.as_ref())
            .is_none()
        {
            return Err(Error::CorruptLocalState(
                "local peer has no clock checkpoint".to_owned(),
            ));
        }
        Ok(Arc::new(Self {
            peer,
            local_peer_id,
            state,
            snapshot: ArcSwap::from_pointee(peers),
            writer: Mutex::new(PeerMutationState {
                dirty: BTreeSet::new(),
            }),
        }))
    }

    pub(crate) fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub(crate) fn mint(&self) -> Result<EventStamp> {
        let mut writer = self.writer.lock();
        let mut peers = (*self.snapshot.load_full()).clone();
        let record = peers
            .get_mut(&self.local_peer_id)
            .expect("the local PeerRecord is established during bootstrap");
        let checkpoint = record
            .clock
            .as_mut()
            .expect("the local PeerRecord always owns a clock");
        if checkpoint.next_sequence == u64::MAX {
            return Err(Error::ClockExhausted);
        }

        let wall = wall_time_ms()?;
        let time = if wall > checkpoint.time.physical_ms {
            EventTime::new(wall, 0)
        } else {
            EventTime::new(
                checkpoint.time.physical_ms,
                checkpoint
                    .time
                    .logical
                    .checked_add(1)
                    .ok_or(Error::ClockExhausted)?,
            )
        };
        let stamp = EventStamp::new(
            EventId::new(self.local_peer_id, checkpoint.next_sequence),
            time,
        );
        checkpoint.next_sequence = checkpoint
            .next_sequence
            .checked_add(1)
            .ok_or(Error::ClockExhausted)?;
        checkpoint.time = time;

        {
            let mut state = self.state.write_internal();
            state.put(self.local_peer_id, record.clone())?;
            state.sync()?;
        }
        writer.dirty.remove(&self.local_peer_id);
        self.snapshot.store(Arc::new(peers));
        Ok(stamp)
    }

    pub(crate) fn observe(&self, stamp: EventStamp) -> Result<ObserveOutcome> {
        let mut writer = self.writer.lock();
        let mut peers = (*self.snapshot.load_full()).clone();
        let outcome = peers
            .entry(stamp.id.peer_id)
            .or_default()
            .receipts
            .observe(stamp.id.sequence)?;
        writer.dirty.insert(stamp.id.peer_id);

        let local = peers
            .get_mut(&self.local_peer_id)
            .expect("the local PeerRecord is established during bootstrap");
        let checkpoint = local
            .clock
            .as_mut()
            .expect("the local PeerRecord always owns a clock");
        checkpoint.time = observe_time(checkpoint.time, stamp.time, wall_time_ms()?);
        if stamp.id.peer_id == self.local_peer_id {
            checkpoint.next_sequence = checkpoint.next_sequence.max(
                stamp
                    .id
                    .sequence
                    .checked_add(1)
                    .ok_or(Error::ClockExhausted)?,
            );
        }
        writer.dirty.insert(self.local_peer_id);
        self.snapshot.store(Arc::new(peers));
        Ok(outcome)
    }

    pub(crate) fn has_received(&self, id: EventId) -> bool {
        self.snapshot
            .load()
            .get(&id.peer_id)
            .is_some_and(|record| record.receipts.has_received(id.sequence))
    }

    pub(crate) fn missing(&self, peer: PeerId) -> Vec<RangeInclusive<u64>> {
        self.snapshot
            .load()
            .get(&peer)
            .map(|record| record.receipts.missing.clone())
            .unwrap_or_default()
    }

    pub(crate) fn flush(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        if writer.dirty.is_empty() {
            return Ok(());
        }
        let snapshot = self.snapshot.load_full();
        let mut state = self.state.write_internal();
        for peer in &writer.dirty {
            if let Some(record) = snapshot.get(peer) {
                state.put(*peer, record.clone())?;
            }
        }
        state.sync()?;
        writer.dirty.clear();
        Ok(())
    }
}

impl Drop for PeerStore {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

fn observe_time(local: EventTime, remote: EventTime, wall: u64) -> EventTime {
    let physical = wall.max(local.physical_ms).max(remote.physical_ms);
    let logical = if physical > local.physical_ms && physical > remote.physical_ms {
        0
    } else if physical == local.physical_ms && physical == remote.physical_ms {
        local.logical.max(remote.logical)
    } else if physical == local.physical_ms {
        local.logical
    } else {
        remote.logical
    };
    EventTime::new(physical, logical)
}

fn wall_time_ms() -> Result<u64> {
    physical_ms().ok_or(Error::ClockExhausted)
}
