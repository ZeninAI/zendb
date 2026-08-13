//! Replication thread lifecycle, swarm event loop, and dispatch.
//!
//! The controller spawns a dedicated OS thread with a single-threaded Tokio
//! runtime. The event loop selects over:
//! - Shutdown signal
//! - Workspace notifications (events and membership changes)
//! - Linger-based batcher flush timer
//! - Periodic heartbeat (ready-peer retries + anti-entropy)
//! - Swarm network events

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
    thread,
    time::Duration,
};

use futures::StreamExt;
use libp2p::{
    Multiaddr as Libp2pMultiaddr, PeerId, Swarm, mdns,
    swarm::behaviour::toggle::Toggle,
    swarm::{NetworkBehaviour, SwarmEvent, dial_opts::DialOpts},
};
use libp2p_identity::Keypair;
use parking_lot::RwLock;
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};
use tokio::time::Instant;
use zendb_types::{InstallationId, WorkspaceId};

use super::{
    batcher::Batcher,
    engine::Engine,
    protocol::{BehaviourEvent, ZeninBehaviour},
    transport::build_swarm,
    wire::Message,
};
use crate::{Error, Result, config::ReplicationConfig, core::WorkspaceCore};

// ─── Composite swarm behaviour ───────────────────────────────────────────────

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct SwarmBehaviour {
    zenin: ZeninBehaviour,
    mdns: Toggle<mdns::tokio::Behaviour>,
}

// ─── Notifications from workspace ────────────────────────────────────────────

/// Granular notifications sent by WorkspaceCore to the replication runtime.
pub(crate) enum ReplicationNotification {
    /// A locally committed event ready for replication.
    Event {
        table: String,
        event: zendb_types::Event,
    },
    /// An installation's materialized metadata or state changed.
    InstallationChanged { installation_id: InstallationId },
}

// ─── Controller ──────────────────────────────────────────────────────────────

/// Owns the replication thread and provides a graceful shutdown mechanism.
pub(crate) struct ReplicationController {
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<thread::JoinHandle<()>>,
    discovered_peers: Arc<RwLock<HashMap<PeerId, HashSet<Libp2pMultiaddr>>>>,
    discovery_listeners: Arc<RwLock<Vec<Arc<dyn PeerDiscoveryListener>>>>,
}

/// A peer and the addresses currently advertised for it through mDNS.
#[derive(Clone, Debug)]
pub struct DiscoveredPeer {
    pub peer_id: PeerId,
    pub addresses: Vec<Libp2pMultiaddr>,
}

/// Receives edge-triggered notifications when the ephemeral mDNS peer set changes.
pub trait PeerDiscoveryListener: Send + Sync {
    fn on_discovery_changed(&self);
}

impl ReplicationController {
    pub(crate) fn start(
        core: &Arc<WorkspaceCore>,
        workspace_id: WorkspaceId,
        keypair: Keypair,
        config: ReplicationConfig,
        notifications: UnboundedReceiver<ReplicationNotification>,
    ) -> Result<Self> {
        let local_installation_id = core.membership.local_installation_id();
        let local_installation = core
            .membership
            .get(&local_installation_id)
            .ok_or(Error::LocalInstallationNotEnrolled(local_installation_id))?;

        let core_weak = Arc::downgrade(core);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let thread_name = keypair.public().to_peer_id().to_base58();
        let discovered_peers = Arc::new(RwLock::new(HashMap::new()));
        let discovered_peers_runtime = discovered_peers.clone();
        let discovery_listeners = Arc::new(RwLock::new(Vec::new()));
        let discovery_listeners_runtime = discovery_listeners.clone();

        let join = thread::Builder::new().name(thread_name).spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx.send(Err(e.to_string()));
                    return;
                }
            };
            runtime.block_on(async move {
                match build_runtime(
                    keypair,
                    core_weak.clone(),
                    workspace_id,
                    local_installation_id,
                    &local_installation,
                    &config,
                ) {
                    Ok((swarm, engine)) => {
                        let _ = ready_tx.send(Ok(()));
                        run(
                            swarm,
                            engine,
                            core_weak,
                            workspace_id,
                            local_installation_id,
                            config,
                            notifications,
                            discovered_peers_runtime,
                            discovery_listeners_runtime,
                            shutdown_rx,
                        )
                        .await;
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                    }
                }
            });
        })?;

        match ready_rx.blocking_recv() {
            Ok(Ok(())) => Ok(Self {
                shutdown: Some(shutdown_tx),
                join: Some(join),
                discovered_peers,
                discovery_listeners,
            }),
            Ok(Err(e)) => {
                let _ = join.join();
                Err(Error::Replication(e))
            }
            Err(e) => {
                let _ = join.join();
                Err(Error::Replication(e.to_string()))
            }
        }
    }

    pub(crate) fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }

    pub(crate) fn discovered_peers(&self) -> Vec<DiscoveredPeer> {
        self.discovered_peers
            .read()
            .iter()
            .map(|(peer_id, addresses)| DiscoveredPeer {
                peer_id: *peer_id,
                addresses: addresses.iter().cloned().collect(),
            })
            .collect()
    }

    pub(crate) fn add_discovery_listener(&self, listener: Arc<dyn PeerDiscoveryListener>) {
        self.discovery_listeners.write().push(listener);
    }
}

impl Drop for ReplicationController {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ─── Swarm construction ──────────────────────────────────────────────────────

fn build_runtime(
    keypair: Keypair,
    core: Weak<WorkspaceCore>,
    workspace_id: WorkspaceId,
    local_installation_id: InstallationId,
    local_installation: &zendb_types::Installation,
    config: &ReplicationConfig,
) -> std::result::Result<(Swarm<SwarmBehaviour>, Engine), String> {
    let local_peer_id = keypair.public().to_peer_id();

    let handshake = local_handshake(workspace_id, local_installation_id, local_installation);

    let behaviour = SwarmBehaviour {
        zenin: ZeninBehaviour::new(local_peer_id, handshake),
        mdns: if config.transport.enable_mdns {
            Some(
                mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        }
        .into(),
    };

    let mut swarm = build_swarm(keypair, behaviour, &config.transport)?;
    for addr in &config.transport.listener_addresses {
        let protocols = addr.as_libp2p().iter().collect::<Vec<_>>();
        let is_tcp = protocols
            .iter()
            .any(|protocol| matches!(protocol, libp2p::multiaddr::Protocol::Tcp(_)));
        let is_quic = protocols
            .iter()
            .any(|protocol| matches!(protocol, libp2p::multiaddr::Protocol::QuicV1));
        if (is_tcp && !config.transport.enable_tcp) || (is_quic && !config.transport.enable_quic) {
            continue;
        }
        swarm
            .listen_on(addr.as_libp2p().clone())
            .map_err(|e| e.to_string())?;
    }

    let engine = Engine::new(core, local_peer_id, config.sync.clone());
    Ok((swarm, engine))
}

// ─── Event loop ──────────────────────────────────────────────────────────────

async fn run(
    mut swarm: Swarm<SwarmBehaviour>,
    mut engine: Engine,
    core: Weak<WorkspaceCore>,
    workspace_id: WorkspaceId,
    local_installation_id: InstallationId,
    config: ReplicationConfig,
    mut notifications: UnboundedReceiver<ReplicationNotification>,
    discovered_peers: Arc<RwLock<HashMap<PeerId, HashSet<Libp2pMultiaddr>>>>,
    discovery_listeners: Arc<RwLock<Vec<Arc<dyn PeerDiscoveryListener>>>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut heartbeat = tokio::time::interval(config.sync.interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut batcher = Batcher::new(config.batch.clone());
    let mut batch_sleep = Box::pin(tokio::time::sleep(Duration::from_secs(3600)));
    let mut dialing = HashSet::new();
    let enable_port_reuse = config.transport.enable_port_reuse;

    connect_ready(
        &mut swarm,
        &engine,
        &mut dialing,
        enable_port_reuse,
        &discovered_peers,
    );

    loop {
        let sleep_until = batcher
            .next_flush()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
        batch_sleep.as_mut().reset(sleep_until);

        tokio::select! {
            biased;

            _ = &mut shutdown => {
                if !batcher.is_empty() {
                    flush_batcher(
                        &mut batcher,
                        &engine,
                        &mut swarm,
                        &mut dialing,
                        enable_port_reuse,
                    );
                }
                break;
            }

            // ── Workspace notifications ──────────────────────────────────
            notification = notifications.recv() => {
                let Some(notification) = notification else { break };
                let completed = process_notification(
                    &core,
                    workspace_id,
                    local_installation_id,
                    &mut engine,
                    &mut batcher,
                    &mut swarm,
                    &mut dialing,
                    enable_port_reuse,
                    &discovered_peers,
                    notification,
                );
                if completed {
                    flush_batcher(
                        &mut batcher,
                        &engine,
                        &mut swarm,
                        &mut dialing,
                        enable_port_reuse,
                    );
                }
                // Drain remaining queued notifications.
                while let Ok(n) = notifications.try_recv() {
                    let completed = process_notification(
                        &core,
                        workspace_id,
                        local_installation_id,
                        &mut engine,
                        &mut batcher,
                        &mut swarm,
                        &mut dialing,
                        enable_port_reuse,
                        &discovered_peers,
                        n,
                    );
                    if completed {
                        flush_batcher(
                            &mut batcher,
                            &engine,
                            &mut swarm,
                            &mut dialing,
                            enable_port_reuse,
                        );
                    }
                }
                if batcher.is_due() {
                    flush_batcher(
                        &mut batcher,
                        &engine,
                        &mut swarm,
                        &mut dialing,
                        enable_port_reuse,
                    );
                }
            }

            // ── Batcher linger expiry ────────────────────────────────────
            _ = &mut batch_sleep => {
                if !batcher.is_empty() {
                    flush_batcher(
                        &mut batcher,
                        &engine,
                        &mut swarm,
                        &mut dialing,
                        enable_port_reuse,
                    );
                }
            }

            // ── Heartbeat: retry ready peers and run anti-entropy ─────────
            _ = heartbeat.tick() => {
                if batcher.is_due() {
                    flush_batcher(
                        &mut batcher,
                        &engine,
                        &mut swarm,
                        &mut dialing,
                        enable_port_reuse,
                    );
                }
                connect_ready(
                    &mut swarm,
                    &engine,
                    &mut dialing,
                    enable_port_reuse,
                    &discovered_peers,
                );
                let outbound = engine.send_summaries();
                dispatch(
                    &mut swarm,
                    &engine,
                    &mut dialing,
                    enable_port_reuse,
                    outbound,
                );
            }

            // ── Swarm events ─────────────────────────────────────────────
            event = swarm.select_next_some() => {
                handle_swarm_event(
                    event,
                    &mut engine,
                    &mut swarm,
                    &mut dialing,
                    enable_port_reuse,
                    &discovered_peers,
                    &discovery_listeners,
                );
            }
        }
    }
}

// ─── Notification processing ─────────────────────────────────────────────────

fn process_notification(
    core: &Weak<WorkspaceCore>,
    workspace_id: WorkspaceId,
    local_installation_id: InstallationId,
    engine: &mut Engine,
    batcher: &mut Batcher,
    swarm: &mut Swarm<SwarmBehaviour>,
    dialing: &mut HashSet<PeerId>,
    enable_port_reuse: bool,
    discovered_peers: &Arc<RwLock<HashMap<PeerId, HashSet<Libp2pMultiaddr>>>>,
    notification: ReplicationNotification,
) -> bool {
    match notification {
        ReplicationNotification::Event { table, event } => {
            engine.cache.insert(&table, &event);
            batcher.push(table, event)
        }
        ReplicationNotification::InstallationChanged { installation_id } => {
            if installation_id == local_installation_id
                && let Some(installation) = core
                    .upgrade()
                    .and_then(|core| core.membership.get(&installation_id))
            {
                swarm.behaviour_mut().zenin.set_handshake(local_handshake(
                    workspace_id,
                    installation_id,
                    &installation,
                ));
            }
            connect_ready(swarm, engine, dialing, enable_port_reuse, discovered_peers);
            false
        }
    }
}

fn local_handshake(
    workspace_id: WorkspaceId,
    installation_id: InstallationId,
    installation: &zendb_types::Installation,
) -> Message {
    Message::Handshake {
        workspace_id,
        installation_id,
        display_name: installation.display_name.clone(),
        public_key: installation.public_key.clone(),
        addresses: installation.addresses.clone(),
    }
}

fn flush_batcher(
    batcher: &mut Batcher,
    engine: &Engine,
    swarm: &mut Swarm<SwarmBehaviour>,
    dialing: &mut HashSet<PeerId>,
    enable_port_reuse: bool,
) {
    let batches = batcher.flush();
    let outbound = engine.broadcast(&batches, None);
    dispatch(swarm, engine, dialing, enable_port_reuse, outbound);
}

// ─── Swarm event handling ────────────────────────────────────────────────────

fn handle_swarm_event(
    event: SwarmEvent<SwarmBehaviourEvent>,
    engine: &mut Engine,
    swarm: &mut Swarm<SwarmBehaviour>,
    dialing: &mut HashSet<PeerId>,
    enable_port_reuse: bool,
    discovered_peers: &Arc<RwLock<HashMap<PeerId, HashSet<Libp2pMultiaddr>>>>,
    discovery_listeners: &Arc<RwLock<Vec<Arc<dyn PeerDiscoveryListener>>>>,
) {
    match event {
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Zenin(BehaviourEvent::SessionEstablished {
            peer_id,
            installation_id,
            display_name,
            public_key,
            addresses,
        })) => {
            dialing.remove(&peer_id);
            let outbound = engine.session_established(
                peer_id,
                installation_id,
                display_name,
                public_key,
                addresses,
            );
            // If the engine returned an error (e.g. NotAdmitted), send it and
            // close the logical session in the behaviour.
            let has_error = outbound.iter().any(|(_, m)| matches!(m, Message::Error(_)));
            dispatch(swarm, engine, dialing, enable_port_reuse, outbound);
            if has_error {
                swarm.behaviour_mut().zenin.close_session(peer_id);
            }
        }
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Zenin(BehaviourEvent::MessageReceived {
            peer_id,
            message,
        })) => {
            let outbound = engine.receive(peer_id, message);
            dispatch(swarm, engine, dialing, enable_port_reuse, outbound);
        }
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Zenin(BehaviourEvent::SessionClosed {
            peer_id,
        })) => {
            dialing.remove(&peer_id);
        }
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
            let mut newly_discovered = HashMap::<PeerId, Vec<Libp2pMultiaddr>>::new();
            let mut changed = false;
            {
                let mut discovered_peers = discovered_peers.write();
                for (peer_id, address) in list {
                    changed |= discovered_peers
                        .entry(peer_id)
                        .or_default()
                        .insert(address.clone());
                    newly_discovered
                        .entry(peer_id)
                        .or_default()
                        .push(dial_address(peer_id, address));
                }
            }
            if changed {
                for listener in discovery_listeners.read().iter() {
                    listener.on_discovery_changed();
                }
            }
            for (peer_id, addresses) in newly_discovered {
                if engine.is_active_peer(peer_id)
                    && !swarm.is_connected(&peer_id)
                    && dialing.insert(peer_id)
                    && swarm
                        .dial(dial_opts(peer_id, addresses, enable_port_reuse))
                        .is_err()
                {
                    dialing.remove(&peer_id);
                }
            }
        }
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
            let mut discovered_peers = discovered_peers.write();
            let mut changed = false;
            for (peer_id, address) in list {
                if let Some(addresses) = discovered_peers.get_mut(&peer_id) {
                    changed |= addresses.remove(&address);
                    if addresses.is_empty() {
                        discovered_peers.remove(&peer_id);
                    }
                }
            }
            drop(discovered_peers);
            if changed {
                for listener in discovery_listeners.read().iter() {
                    listener.on_discovery_changed();
                }
            }
        }
        SwarmEvent::OutgoingConnectionError {
            peer_id: Some(peer_id),
            ..
        } => {
            dialing.remove(&peer_id);
        }
        _ => {}
    }
}

// ─── Dispatch and dialing ────────────────────────────────────────────────────

fn dispatch(
    swarm: &mut Swarm<SwarmBehaviour>,
    engine: &Engine,
    dialing: &mut HashSet<PeerId>,
    enable_port_reuse: bool,
    outbound: Vec<(PeerId, Message)>,
) {
    for (peer_id, message) in outbound {
        dial_if_needed(
            swarm,
            dialing,
            peer_id,
            engine.addresses(peer_id),
            enable_port_reuse,
        );
        swarm.behaviour_mut().zenin.send(peer_id, message);
    }
}

/// Dial every active peer that we are not already connected to.
fn connect_ready(
    swarm: &mut Swarm<SwarmBehaviour>,
    engine: &Engine,
    dialing: &mut HashSet<PeerId>,
    enable_port_reuse: bool,
    discovered_peers: &Arc<RwLock<HashMap<PeerId, HashSet<Libp2pMultiaddr>>>>,
) {
    for (peer_id, route_hints) in engine.active_peer_routes() {
        let addresses = if route_hints.is_empty() {
            discovered_peers
                .read()
                .get(&peer_id)
                .into_iter()
                .flatten()
                .cloned()
                .map(|address| dial_address(peer_id, address))
                .collect()
        } else {
            route_hints
        };
        dial_if_needed(swarm, dialing, peer_id, addresses, enable_port_reuse);
    }
}

fn dial_if_needed(
    swarm: &mut Swarm<SwarmBehaviour>,
    dialing: &mut HashSet<PeerId>,
    peer_id: PeerId,
    addresses: Vec<Libp2pMultiaddr>,
    enable_port_reuse: bool,
) {
    if !swarm.is_connected(&peer_id)
        && !addresses.is_empty()
        && dialing.insert(peer_id)
        && swarm
            .dial(dial_opts(peer_id, addresses, enable_port_reuse))
            .is_err()
    {
        dialing.remove(&peer_id);
    }
}

fn dial_opts(
    peer_id: PeerId,
    addresses: Vec<libp2p::Multiaddr>,
    enable_port_reuse: bool,
) -> libp2p::swarm::dial_opts::DialOpts {
    let dial = DialOpts::peer_id(peer_id).addresses(addresses);
    if enable_port_reuse {
        dial.build()
    } else {
        dial.allocate_new_port().build()
    }
}

/// mDNS appends the discovered peer ID to each advertised listen address.
/// `DialOpts::peer_id` already pins the remote identity and expects transport
/// addresses, so passing that suffix through prevents some transports from
/// constructing a usable dial attempt.
fn dial_address(peer_id: PeerId, mut address: Libp2pMultiaddr) -> Libp2pMultiaddr {
    if address.iter().last().is_some_and(
        |protocol| matches!(protocol, libp2p::multiaddr::Protocol::P2p(id) if id == peer_id),
    ) {
        address.pop();
    }
    address
}
