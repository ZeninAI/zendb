//! Authorized peer routes, discovery addresses, connection state, and dial retry state.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};
use zendb_types::InstallationId;

use super::{DialConfig, command::PeerRoute};

struct RetryState {
    initial: Duration,
    maximum: Duration,
    delay: Duration,
    next_attempt: tokio::time::Instant,
}

impl RetryState {
    fn new(config: &DialConfig) -> Self {
        let initial = config.initial_backoff.max(Duration::from_millis(1));
        let maximum = config.max_backoff.max(initial);
        Self {
            initial,
            maximum,
            delay: initial,
            next_attempt: tokio::time::Instant::now(),
        }
    }

    fn reset(&mut self) {
        self.delay = self.initial;
        self.next_attempt = tokio::time::Instant::now();
    }

    fn attempted(&mut self) {
        self.next_attempt = tokio::time::Instant::now() + self.delay;
    }

    fn failed(&mut self) {
        self.next_attempt = tokio::time::Instant::now() + self.delay;
        self.delay = self.delay.saturating_mul(2).min(self.maximum);
    }
}

struct PeerEntry {
    installation_id: InstallationId,
    catalog: Vec<Multiaddr>,
    mdns: HashSet<Multiaddr>,
    identified: HashSet<Multiaddr>,
    connected: bool,
    retry: RetryState,
}

impl PeerEntry {
    fn new(
        installation_id: InstallationId,
        addresses: Vec<Multiaddr>,
        dial_config: &DialConfig,
    ) -> Self {
        Self {
            installation_id,
            catalog: addresses,
            mdns: HashSet::new(),
            identified: HashSet::new(),
            connected: false,
            retry: RetryState::new(dial_config),
        }
    }

    fn candidate_addresses(&self) -> Vec<Multiaddr> {
        let mut seen = HashSet::new();
        self.catalog
            .iter()
            .chain(&self.mdns)
            .chain(&self.identified)
            .filter(|address| seen.insert((*address).clone()))
            .cloned()
            .collect()
    }
}

pub(super) struct PeerBook {
    entries: HashMap<PeerId, PeerEntry>,
    lan: HashSet<PeerId>,
    revoked: HashSet<PeerId>,
    dial_config: DialConfig,
}

impl PeerBook {
    pub(super) fn new(
        routes: BTreeMap<InstallationId, PeerRoute>,
        dial_config: DialConfig,
    ) -> Self {
        let entries = routes
            .into_iter()
            .map(|(installation_id, route)| {
                (
                    route.peer_id,
                    PeerEntry::new(installation_id, route.addresses, &dial_config),
                )
            })
            .collect();
        Self {
            entries,
            lan: HashSet::new(),
            revoked: HashSet::new(),
            dial_config,
        }
    }

    pub(super) fn accepts(&self, peer_id: &PeerId) -> bool {
        self.entries.contains_key(peer_id) && !self.revoked.contains(peer_id)
    }

    pub(super) fn upsert(
        &mut self,
        installation_id: InstallationId,
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    ) {
        self.revoked.remove(&peer_id);
        let entry = self
            .entries
            .entry(peer_id)
            .or_insert_with(|| PeerEntry::new(installation_id, Vec::new(), &self.dial_config));
        entry.installation_id = installation_id;
        entry.catalog = addresses;
        entry.retry.reset();
    }

    pub(super) fn remove(&mut self, installation_id: InstallationId, peer_id: PeerId) {
        if self
            .entries
            .get(&peer_id)
            .is_some_and(|entry| entry.installation_id == installation_id)
        {
            self.entries.remove(&peer_id);
        }
        self.lan.remove(&peer_id);
        self.revoked.insert(peer_id);
    }

    pub(super) fn discover_mdns(&mut self, peer_id: PeerId, address: Multiaddr) -> bool {
        let Some(entry) = self.entries.get_mut(&peer_id) else {
            return false;
        };
        let Some(address) = route_address(address, peer_id) else {
            return false;
        };
        entry.mdns.insert(address);
        entry.retry.reset();
        self.lan.insert(peer_id);
        true
    }

    pub(super) fn expire_mdns(&mut self, peer_id: PeerId, address: Multiaddr) {
        let Some(entry) = self.entries.get_mut(&peer_id) else {
            return;
        };
        let Some(address) = route_address(address, peer_id) else {
            return;
        };
        entry.mdns.remove(&address);
        if entry.mdns.is_empty() {
            self.lan.remove(&peer_id);
        }
    }

    pub(super) fn is_lan(&self, peer_id: &PeerId) -> bool {
        self.lan.contains(peer_id)
    }

    pub(super) fn identify(&mut self, peer_id: PeerId, addresses: Vec<Multiaddr>) {
        let Some(entry) = self.entries.get_mut(&peer_id) else {
            return;
        };
        entry.identified = addresses
            .into_iter()
            .filter_map(|address| route_address(address, peer_id))
            .collect();
        entry.retry.reset();
    }

    pub(super) fn connected(&mut self, peer_id: PeerId) -> bool {
        let Some(entry) = self.entries.get_mut(&peer_id) else {
            self.revoked.insert(peer_id);
            return false;
        };
        entry.connected = true;
        entry.retry.reset();
        true
    }

    pub(super) fn disconnected(&mut self, peer_id: PeerId) {
        if let Some(entry) = self.entries.get_mut(&peer_id) {
            entry.connected = false;
            entry.retry.reset();
        }
    }

    pub(super) fn dial_failed(&mut self, peer_id: PeerId) {
        if let Some(entry) = self.entries.get_mut(&peer_id) {
            entry.connected = false;
            entry.retry.failed();
        }
    }

    pub(super) fn due_dials(&mut self, now: tokio::time::Instant) -> Vec<(PeerId, Vec<Multiaddr>)> {
        self.entries
            .iter_mut()
            .filter_map(|(peer_id, entry)| {
                if entry.connected || entry.retry.next_attempt > now {
                    return None;
                }
                let addresses = entry.candidate_addresses();
                entry.retry.attempted();
                (!addresses.is_empty()).then_some((*peer_id, addresses))
            })
            .collect()
    }
}

fn route_address(mut address: Multiaddr, peer_id: PeerId) -> Option<Multiaddr> {
    match address.iter().last() {
        Some(Protocol::P2p(address_peer_id)) if address_peer_id == peer_id => {
            address.pop();
            Some(address)
        }
        Some(Protocol::P2p(_)) => None,
        _ => Some(address),
    }
}
