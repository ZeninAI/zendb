//! Authorized peer routes, discovery addresses, connection state, and dial retry state.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};
use zendb_types::{InstallationId, Permission, Permissions};

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

    fn record_attempt(&mut self) {
        self.next_attempt = tokio::time::Instant::now() + self.delay;
    }

    fn record_failure(&mut self) {
        self.next_attempt = tokio::time::Instant::now() + self.delay;
        self.delay = self.delay.saturating_mul(2).min(self.maximum);
    }
}

struct PeerEntry {
    configured_addresses: Vec<Multiaddr>,
    mdns_addresses: HashSet<Multiaddr>,
    identified_addresses: HashSet<Multiaddr>,
    connected: bool,
    retry: RetryState,
    permissions: Permissions,
}

impl PeerEntry {
    fn new(addresses: Vec<Multiaddr>, permissions: Permissions, dial_config: &DialConfig) -> Self {
        Self {
            configured_addresses: addresses,
            mdns_addresses: HashSet::new(),
            identified_addresses: HashSet::new(),
            connected: false,
            retry: RetryState::new(dial_config),
            permissions,
        }
    }

    fn candidate_addresses(&self) -> Vec<Multiaddr> {
        // Configured, mDNS, and identify addresses are independent sources;
        // deduplicate them before the worker attempts a dial.
        let mut seen = HashSet::new();
        self.configured_addresses
            .iter()
            .chain(&self.mdns_addresses)
            .chain(&self.identified_addresses)
            .filter(|address| seen.insert((*address).clone()))
            .cloned()
            .collect()
    }
}

pub(super) struct PeerDirectory {
    entries: HashMap<PeerId, PeerEntry>,
    lan: HashSet<PeerId>,
    revoked: HashSet<PeerId>,
    dial_config: DialConfig,
}

impl PeerDirectory {
    pub(super) fn new(
        routes: BTreeMap<InstallationId, PeerRoute>,
        dial_config: DialConfig,
    ) -> Self {
        let entries = routes
            .into_values()
            .map(|route| {
                (
                    route.peer_id,
                    PeerEntry::new(route.addresses, route.permissions, &dial_config),
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

    pub(super) fn is_active(&self, peer_id: &PeerId) -> bool {
        self.entries.contains_key(peer_id) && !self.revoked.contains(peer_id)
    }

    pub(super) fn can_read_data(&self, peer_id: &PeerId) -> bool {
        self.entries
            .get(peer_id)
            .is_some_and(|entry| entry.permissions.allows(Permission::ReadData))
            && !self.revoked.contains(peer_id)
    }

    pub(super) fn upsert(
        &mut self,
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
        permissions: Permissions,
    ) {
        self.revoked.remove(&peer_id);
        let entry = self
            .entries
            .entry(peer_id)
            .or_insert_with(|| PeerEntry::new(Vec::new(), permissions, &self.dial_config));
        entry.configured_addresses = addresses;
        entry.permissions = permissions;
        entry.retry.reset();
    }

    pub(super) fn remove(&mut self, peer_id: PeerId) {
        self.entries.remove(&peer_id);
        self.lan.remove(&peer_id);
        self.revoked.insert(peer_id);
    }

    pub(super) fn discover_mdns(&mut self, peer_id: PeerId, address: Multiaddr) -> bool {
        let Some(entry) = self.entries.get_mut(&peer_id) else {
            return false;
        };
        let Some(address) = normalize_route_address(address, peer_id) else {
            return false;
        };
        entry.mdns_addresses.insert(address);
        entry.retry.reset();
        self.lan.insert(peer_id);
        true
    }

    pub(super) fn expire_mdns(&mut self, peer_id: PeerId, address: Multiaddr) {
        let Some(entry) = self.entries.get_mut(&peer_id) else {
            return;
        };
        let Some(address) = normalize_route_address(address, peer_id) else {
            return;
        };
        entry.mdns_addresses.remove(&address);
        if entry.mdns_addresses.is_empty() {
            self.lan.remove(&peer_id);
        }
    }

    pub(super) fn is_lan(&self, peer_id: &PeerId) -> bool {
        self.lan.contains(peer_id)
    }

    pub(super) fn update_identified_addresses(
        &mut self,
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    ) {
        let Some(entry) = self.entries.get_mut(&peer_id) else {
            return;
        };
        entry.identified_addresses = addresses
            .into_iter()
            .filter_map(|address| normalize_route_address(address, peer_id))
            .collect();
        entry.retry.reset();
    }

    pub(super) fn mark_connected(&mut self, peer_id: PeerId) -> bool {
        let Some(entry) = self.entries.get_mut(&peer_id) else {
            self.revoked.insert(peer_id);
            return false;
        };
        entry.connected = true;
        entry.retry.reset();
        true
    }

    pub(super) fn mark_disconnected(&mut self, peer_id: PeerId) {
        if let Some(entry) = self.entries.get_mut(&peer_id) {
            entry.connected = false;
            entry.retry.reset();
        }
    }

    pub(super) fn record_dial_failure(&mut self, peer_id: PeerId) {
        if let Some(entry) = self.entries.get_mut(&peer_id) {
            entry.connected = false;
            entry.retry.record_failure();
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
                // Reserve the next attempt before returning so the worker's
                // short dial tick cannot schedule the same peer repeatedly.
                entry.retry.record_attempt();
                (!addresses.is_empty()).then_some((*peer_id, addresses))
            })
            .collect()
    }
}

fn normalize_route_address(mut address: Multiaddr, peer_id: PeerId) -> Option<Multiaddr> {
    // A matching trailing /p2p component is redundant for DialOpts; a
    // component naming another peer is an invalid route and must be rejected.
    match address.iter().last() {
        Some(Protocol::P2p(address_peer_id)) if address_peer_id == peer_id => {
            address.pop();
            Some(address)
        }
        Some(Protocol::P2p(_)) => None,
        _ => Some(address),
    }
}
