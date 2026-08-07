//! Event-driven mesh topology: peer selection and forwarding neighbourhood.
//!
//! The mesh maintains two layers:
//! - `routes`: all known non-rejected installations with their addresses.
//! - `peers`: the active forwarding subset selected from routes.
//!
//! The mesh is updated incrementally via `add`/`remove` rather than bulk
//! refresh. On each maintenance tick it deterministically ranks active routes
//! and produces Graft/Prune actions to converge toward `target_peers`.

use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

use libp2p::{Multiaddr, PeerId};
use zendb_types::{Installation, InstallationId};

use super::config::MeshConfig;

// ─── Public actions ──────────────────────────────────────────────────────────

pub(super) enum MeshAction {
    Graft { peer_id: PeerId },
    Prune { peer_id: PeerId },
}

// ─── Mesh state ──────────────────────────────────────────────────────────────

struct Route {
    addresses: Vec<Multiaddr>,
    active: bool,
}

pub(super) struct Mesh {
    local_peer_id: PeerId,
    config: MeshConfig,
    routes: HashMap<PeerId, Route>,
    /// Peer ID → Installation ID mapping for authorization lookups.
    peer_to_installation: HashMap<PeerId, InstallationId>,
    /// The current forwarding neighbourhood (subset of routes where active=true).
    peers: HashSet<PeerId>,
    /// Monotonically increasing epoch used to shuffle selection ranking.
    epoch: u64,
}

impl Mesh {
    pub(super) fn new(local_peer_id: PeerId, config: MeshConfig) -> Self {
        Self {
            local_peer_id,
            config,
            routes: HashMap::new(),
            peer_to_installation: HashMap::new(),
            peers: HashSet::new(),
            epoch: 0,
        }
    }

    // ── Incremental membership events ────────────────────────────────────

    /// Register or update an installation. Called on startup for each known
    /// installation and incrementally when an installation is admitted.
    pub(super) fn add(&mut self, installation_id: InstallationId, installation: &Installation) {
        let peer_id = installation.public_key.as_libp2p().to_peer_id();
        if peer_id == self.local_peer_id {
            return;
        }
        let active = installation.state.is_active();
        let addresses: Vec<Multiaddr> = installation
            .addresses
            .iter()
            .map(|a| a.as_libp2p().clone())
            .collect();
        self.routes.insert(peer_id, Route { addresses, active });
        self.peer_to_installation.insert(peer_id, installation_id);
        if !active {
            self.peers.remove(&peer_id);
        }
    }

    /// Remove an installation (e.g. when rejected).
    pub(super) fn remove(&mut self, installation_id: InstallationId, public_key: &zendb_types::PublicKey) {
        let peer_id = public_key.as_libp2p().to_peer_id();
        self.routes.remove(&peer_id);
        self.peer_to_installation.remove(&peer_id);
        self.peers.remove(&peer_id);
        let _ = installation_id;
    }

    // ── Mesh maintenance ─────────────────────────────────────────────────

    /// Evaluate the current mesh and return Graft/Prune actions to converge
    /// toward `target_peers`.
    pub(super) fn maintain(&mut self) -> Vec<MeshAction> {
        let mut candidates: Vec<(PeerId, u64)> = self
            .routes
            .iter()
            .filter(|(_, route)| route.active)
            .map(|(peer_id, _)| (*peer_id, mesh_rank(self.local_peer_id, *peer_id, self.epoch)))
            .collect();
        candidates.sort_unstable_by_key(|(_, rank)| *rank);

        let target = self.config.target_peers.min(candidates.len());
        let desired: HashSet<PeerId> = candidates
            .into_iter()
            .take(target)
            .map(|(peer_id, _)| peer_id)
            .collect();

        let mut actions = Vec::new();
        for &peer_id in self.peers.difference(&desired) {
            actions.push(MeshAction::Prune { peer_id });
        }
        for &peer_id in desired.difference(&self.peers) {
            actions.push(MeshAction::Graft { peer_id });
        }
        self.epoch = self.epoch.wrapping_add(1);
        actions
    }

    /// Accept a graft request from a remote peer.
    pub(super) fn accepts(&self, peer_id: PeerId) -> bool {
        self.routes.get(&peer_id).is_some_and(|r| r.active)
            && (self.peers.len() < self.config.high_watermark || self.peers.contains(&peer_id))
    }

    pub(super) fn graft(&mut self, peer_id: PeerId) {
        if self.routes.get(&peer_id).is_some_and(|r| r.active) {
            self.peers.insert(peer_id);
        }
    }

    pub(super) fn prune(&mut self, peer_id: PeerId) {
        self.peers.remove(&peer_id);
    }

    pub(super) fn disconnected(&mut self, peer_id: PeerId) {
        self.peers.remove(&peer_id);
    }

    // ── Queries ──────────────────────────────────────────────────────────

    pub(super) fn is_known(&self, peer_id: PeerId) -> bool {
        self.routes.contains_key(&peer_id)
    }

    pub(super) fn is_active(&self, peer_id: PeerId) -> bool {
        self.routes.get(&peer_id).is_some_and(|r| r.active)
    }

    pub(super) fn installation_id(&self, peer_id: PeerId) -> Option<InstallationId> {
        self.peer_to_installation.get(&peer_id).copied()
    }

    pub(super) fn known_peers(&self) -> Vec<PeerId> {
        self.routes
            .iter()
            .filter(|(_, route)| route.active)
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    pub(super) fn addresses(&self, peer_id: PeerId) -> Vec<Multiaddr> {
        self.routes
            .get(&peer_id)
            .map(|r| r.addresses.clone())
            .unwrap_or_default()
    }

    /// All current mesh neighbours, optionally excluding one peer.
    pub(super) fn neighbours(&self, exclude: Option<PeerId>) -> Vec<PeerId> {
        self.peers
            .iter()
            .copied()
            .filter(|p| Some(*p) != exclude)
            .collect()
    }
}

/// Deterministic peer ranking based on local+remote identity and epoch.
/// Shuffles the selection every 8 epochs so the mesh slowly rotates.
fn mesh_rank(local: PeerId, remote: PeerId, epoch: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    local.hash(&mut hasher);
    remote.hash(&mut hasher);
    (epoch / 8).hash(&mut hasher);
    hasher.finish()
}
