//! Replication thread lifecycle, swarm event loop, and dispatch.
//!
//! The controller spawns a dedicated OS thread with a single-threaded Tokio
//! runtime. The event loop selects over:
//! - Shutdown signal
//! - Workspace notifications (events and membership changes)
//! - Linger-based batcher flush timer
//! - Periodic heartbeat (mesh maintenance + anti-entropy)
//! - Swarm network events

use std::{
    collections::HashSet,
    sync::{Arc, Weak},
    thread,
    time::Duration,
};

use futures::StreamExt;
use libp2p::{
    PeerId, Swarm, mdns,
    swarm::{NetworkBehaviour, SwarmEvent, dial_opts::DialOpts},
};
use libp2p_identity::Keypair;
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};
use tokio::time::Instant;
use zendb_types::{InstallationId, WorkspaceId};

use super::{
    batcher::Batcher,
    config::ReplicationConfig,
    engine::Engine,
    protocol::{BehaviourEvent, ZeninBehaviour},
    transport::build_swarm,
    wire::Message,
};
use crate::{Error, Result, core::WorkspaceCore};

// ─── Composite swarm behaviour ───────────────────────────────────────────────

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "SwarmBehaviourEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
struct SwarmBehaviour {
    zenin: ZeninBehaviour,
    mdns: mdns::tokio::Behaviour,
}

enum SwarmBehaviourEvent {
    Zenin(BehaviourEvent),
    Mdns(mdns::Event),
}

impl From<BehaviourEvent> for SwarmBehaviourEvent {
    fn from(event: BehaviourEvent) -> Self {
        Self::Zenin(event)
    }
}

impl From<mdns::Event> for SwarmBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        Self::Mdns(event)
    }
}

// ─── Notifications from workspace ────────────────────────────────────────────

/// Granular notifications sent by WorkspaceCore to the replication runtime.
pub(crate) enum ReplicationNotification {
    /// A locally committed event ready for replication.
    Event { table: String, event: zendb_types::Event },
    /// An installation transitioned to Active.
    Admitted { installation_id: InstallationId },
    /// An installation transitioned to Rejected.
    Rejected { installation_id: InstallationId },
}

// ─── Controller ──────────────────────────────────────────────────────────────

/// Owns the replication thread and provides a graceful shutdown mechanism.
pub(crate) struct ReplicationController {
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<thread::JoinHandle<()>>,
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

        let join = thread::Builder::new()
            .name("zendb-zenin".to_owned())
            .spawn(move || {
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
                            run(swarm, engine, core_weak, config, notifications, shutdown_rx).await;
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

    let addresses = if local_installation.addresses.is_empty() {
        config.listen_addresses.clone()
    } else {
        local_installation.addresses.clone()
    };

    let handshake = Message::Handshake {
        workspace_id,
        installation_id: local_installation_id,
        display_name: local_installation.display_name.clone(),
        public_key: local_installation.public_key.clone(),
        addresses,
    };

    let behaviour = SwarmBehaviour {
        zenin: ZeninBehaviour::new(handshake, config.zenin.max_frame_bytes),
        mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
            .map_err(|e| e.to_string())?,
    };

    let mut swarm = build_swarm(keypair, behaviour)?;
    for addr in &config.listen_addresses {
        swarm
            .listen_on(addr.as_libp2p().clone())
            .map_err(|e| e.to_string())?;
    }

    let engine = Engine::new(core, local_peer_id, config.zenin.clone(), config.mesh);
    Ok((swarm, engine))
}

// ─── Event loop ──────────────────────────────────────────────────────────────

async fn run(
    mut swarm: Swarm<SwarmBehaviour>,
    mut engine: Engine,
    _core: Weak<WorkspaceCore>,
    config: ReplicationConfig,
    mut notifications: UnboundedReceiver<ReplicationNotification>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut heartbeat = tokio::time::interval(config.zenin.heartbeat);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut sync_deadline = Instant::now() + config.zenin.sync_interval;
    let mut batcher = Batcher::new(config.zenin.linger);
    let mut dialing = HashSet::new();

    // Initialize mesh from current membership.
    engine.initialize_mesh();
    let outbound = engine.maintain_mesh();
    dispatch(&mut swarm, &engine.mesh, &mut dialing, outbound);
    connect_active(&mut swarm, &engine.mesh, &mut dialing);

    loop {
        // Compute next batcher flush deadline for tokio::select!.
        let flush_at = batcher.next_flush().unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));

        tokio::select! {
            _ = &mut shutdown => break,

            // ── Workspace notifications ──────────────────────────────────
            notification = notifications.recv() => {
                let Some(notification) = notification else { break };
                process_notification(
                    &mut engine, &mut batcher, &mut swarm, &mut dialing, notification,
                );
                // Drain remaining queued notifications.
                while let Ok(n) = notifications.try_recv() {
                    process_notification(&mut engine, &mut batcher, &mut swarm, &mut dialing, n);
                }
                // Flush immediately if linger is zero.
                if config.zenin.linger.is_zero() && !batcher.is_empty() {
                    flush_batcher(&mut batcher, &engine, &mut swarm, &mut dialing);
                }
            }

            // ── Batcher linger expiry ────────────────────────────────────
            _ = tokio::time::sleep_until(flush_at), if !batcher.is_empty() => {
                flush_batcher(&mut batcher, &engine, &mut swarm, &mut dialing);
            }

            // ── Heartbeat: mesh maintenance + anti-entropy ───────────────
            _ = heartbeat.tick() => {
                let outbound = engine.maintain_mesh();
                dispatch(&mut swarm, &engine.mesh, &mut dialing, outbound);
                connect_active(&mut swarm, &engine.mesh, &mut dialing);

                let now = Instant::now();
                if now >= sync_deadline {
                    sync_deadline = now + config.zenin.sync_interval;
                    let outbound = engine.send_summaries();
                    dispatch(&mut swarm, &engine.mesh, &mut dialing, outbound);
                }
            }

            // ── Swarm events ─────────────────────────────────────────────
            event = swarm.select_next_some() => {
                handle_swarm_event(event, &mut engine, &mut swarm, &mut dialing);
            }
        }
    }
}

// ─── Notification processing ─────────────────────────────────────────────────

fn process_notification(
    engine: &mut Engine,
    batcher: &mut Batcher,
    swarm: &mut Swarm<SwarmBehaviour>,
    dialing: &mut HashSet<PeerId>,
    notification: ReplicationNotification,
) {
    match notification {
        ReplicationNotification::Event { table, event } => {
            engine.cache.insert(&table, &event);
            batcher.push(table, event);
        }
        ReplicationNotification::Admitted { installation_id } => {
            engine.installation_admitted(installation_id);
            connect_active(swarm, &engine.mesh, dialing);
        }
        ReplicationNotification::Rejected { installation_id } => {
            engine.installation_rejected(installation_id);
        }
    }
}

fn flush_batcher(
    batcher: &mut Batcher,
    engine: &Engine,
    swarm: &mut Swarm<SwarmBehaviour>,
    dialing: &mut HashSet<PeerId>,
) {
    let batches = batcher.flush();
    let outbound = engine.broadcast(&batches, None);
    dispatch(swarm, &engine.mesh, dialing, outbound);
}

// ─── Swarm event handling ────────────────────────────────────────────────────

fn handle_swarm_event(
    event: SwarmEvent<SwarmBehaviourEvent>,
    engine: &mut Engine,
    swarm: &mut Swarm<SwarmBehaviour>,
    dialing: &mut HashSet<PeerId>,
) {
    match event {
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Zenin(
            BehaviourEvent::SessionEstablished {
                peer_id,
                installation_id,
                display_name,
                public_key,
                addresses,
            },
        )) => {
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
            dispatch(swarm, &engine.mesh, dialing, outbound);
            if has_error {
                swarm.behaviour_mut().zenin.close_session(peer_id);
            }
        }
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Zenin(
            BehaviourEvent::MessageReceived { peer_id, message },
        )) => {
            let outbound = engine.receive(peer_id, message);
            dispatch(swarm, &engine.mesh, dialing, outbound);
        }
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Zenin(
            BehaviourEvent::SessionClosed { peer_id },
        )) => {
            dialing.remove(&peer_id);
            engine.mesh.disconnected(peer_id);
        }
        SwarmEvent::Behaviour(SwarmBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
            for (peer_id, address) in list {
                if engine.mesh.is_active(peer_id)
                    && !swarm.is_connected(&peer_id)
                    && dialing.insert(peer_id)
                    && swarm
                        .dial(DialOpts::peer_id(peer_id).addresses(vec![address]).build())
                        .is_err()
                {
                    dialing.remove(&peer_id);
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
    mesh: &super::mesh::Mesh,
    dialing: &mut HashSet<PeerId>,
    outbound: Vec<(PeerId, Message)>,
) {
    for (peer_id, message) in outbound {
        dial_if_needed(swarm, mesh, dialing, peer_id);
        swarm.behaviour_mut().zenin.send(peer_id, message);
    }
}

/// Dial active peers that we are not already connected to.
fn connect_active(
    swarm: &mut Swarm<SwarmBehaviour>,
    mesh: &super::mesh::Mesh,
    dialing: &mut HashSet<PeerId>,
) {
    for peer_id in mesh.known_peers() {
        dial_if_needed(swarm, mesh, dialing, peer_id);
    }
}

fn dial_if_needed(
    swarm: &mut Swarm<SwarmBehaviour>,
    mesh: &super::mesh::Mesh,
    dialing: &mut HashSet<PeerId>,
    peer_id: PeerId,
) {
    let addresses = mesh.addresses(peer_id);
    if !swarm.is_connected(&peer_id)
        && !addresses.is_empty()
        && dialing.insert(peer_id)
        && swarm
            .dial(DialOpts::peer_id(peer_id).addresses(addresses).build())
            .is_err()
    {
        dialing.remove(&peer_id);
    }
}
