//! Tokio/libp2p worker, Gossipsub batching, discovery, and mesh control.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, mpsc as std_mpsc},
    thread,
    time::Duration,
};

use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder,
    gossipsub::{
        self, IdentTopic, MessageAuthenticity, PeerScoreParams, PeerScoreThresholds, ValidationMode,
    },
    identify, mdns, noise, ping,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use libp2p_identity::Keypair;
use tokio::sync::mpsc;
use zendb_types::{
    CompactEvent, Envelope, Event, WorkspaceId,
    utils::{deserialize_from, serdes::serialized_size, serialize_to_vec},
};

use super::{BatchConfig, ReplicationConfig, TopologyConfig};
use crate::{Error, Result, admission, devices::Devices, tables::Tables};

pub(crate) enum Command {
    Event { table: String, event: Event },
    Allow(PeerId),
    Revoke(PeerId),
    Stop,
}

pub(crate) struct RunningReplication {
    pub(crate) tx: mpsc::Sender<Command>,
    join: Option<thread::JoinHandle<()>>,
    admission_join: Option<thread::JoinHandle<()>>,
}

impl RunningReplication {
    pub(crate) fn start(
        workspace_id: WorkspaceId,
        keypair: Keypair,
        tables: Arc<Tables>,
        devices: Arc<Devices>,
        config: ReplicationConfig,
        bootstrap_peers: Vec<String>,
    ) -> Result<Self> {
        if config.outbound_capacity == 0 {
            return Err(Error::Replication(
                "outbound_capacity must be greater than zero".to_owned(),
            ));
        }
        let local_installation_id = devices.local_installation_id();
        let (inbound_tx, inbound_rx) =
            std_mpsc::sync_channel::<(Envelope, PeerId)>(config.outbound_capacity);
        let admission_join = thread::Builder::new()
            .name(format!("zendb-admission-{workspace_id}"))
            .spawn(move || {
                while let Ok((envelope, source)) = inbound_rx.recv() {
                    let _ = admission::admit_event(&tables, &devices, envelope, source);
                }
            })?;
        let (tx, rx) = mpsc::channel(config.outbound_capacity);
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(0);
        let join = thread::Builder::new()
            .name(format!("zendb-replication-{workspace_id}"))
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    match build_swarm(workspace_id, keypair, &config) {
                        Ok((swarm, topic)) => {
                            let _ = ready_tx.send(Ok(()));
                            run_worker(
                                swarm,
                                rx,
                                WorkerContext {
                                    topic,
                                    local_installation_id,
                                    batch_config: config.batch,
                                    topology_config: config.topology,
                                    bootstrap_peers,
                                    inbound: inbound_tx,
                                },
                            )
                            .await;
                        }
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                        }
                    }
                });
            })?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                tx,
                join: Some(join),
                admission_join: Some(admission_join),
            }),
            Ok(Err(error)) => {
                let _ = join.join();
                let _ = admission_join.join();
                Err(Error::Replication(error))
            }
            Err(error) => {
                let _ = join.join();
                let _ = admission_join.join();
                Err(Error::Replication(error.to_string()))
            }
        }
    }

    pub(crate) fn stop(mut self) {
        let _ = self.tx.blocking_send(Command::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        if let Some(admission_join) = self.admission_join.take() {
            let _ = admission_join.join();
        }
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "ReplBehaviourEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
struct ReplBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
}

enum ReplBehaviourEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
    Ping(ping::Event),
    Identify(Box<identify::Event>),
}

impl From<gossipsub::Event> for ReplBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossipsub(event)
    }
}

impl From<mdns::Event> for ReplBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        Self::Mdns(event)
    }
}

impl From<ping::Event> for ReplBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}

impl From<identify::Event> for ReplBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(Box::new(event))
    }
}

fn build_swarm(
    workspace_id: WorkspaceId,
    keypair: Keypair,
    config: &ReplicationConfig,
) -> std::result::Result<(Swarm<ReplBehaviour>, IdentTopic), String> {
    let local_peer_id = keypair.public().to_peer_id();
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .validation_mode(ValidationMode::Strict)
        .max_transmit_size(config.gossipsub_max_transmit_size)
        .mesh_n(config.topology.mesh_size)
        .build()
        .map_err(|error| error.to_string())?;
    let mut gossipsub = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(keypair.clone()),
        gossipsub_config,
    )
    .map_err(|error| error.to_string())?;
    gossipsub
        .with_peer_score(PeerScoreParams::default(), PeerScoreThresholds::default())
        .map_err(|error| error.to_string())?;
    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
        .map_err(|error| error.to_string())?;
    let behaviour = ReplBehaviour {
        gossipsub,
        mdns,
        ping: ping::Behaviour::default(),
        identify: identify::Behaviour::new(identify::Config::new(
            "/zendb/replication/1".to_owned(),
            keypair.public(),
        )),
    };
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|error| error.to_string())?
        .with_dns()
        .map_err(|error| error.to_string())?
        .with_behaviour(|_| behaviour)
        .map_err(|error| error.to_string())?
        .build();
    let topic = IdentTopic::new(format!("zendb/{workspace_id}/events/v1"));
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic)
        .map_err(|error| error.to_string())?;
    swarm
        .listen_on(
            "/ip4/0.0.0.0/tcp/0"
                .parse::<Multiaddr>()
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    Ok((swarm, topic))
}

struct PendingBatch {
    events: Vec<CompactEvent>,
    bytes: usize,
}

struct WorkerContext {
    topic: IdentTopic,
    local_installation_id: zendb_types::InstallationId,
    batch_config: BatchConfig,
    topology_config: TopologyConfig,
    bootstrap_peers: Vec<String>,
    inbound: std_mpsc::SyncSender<(Envelope, PeerId)>,
}

async fn run_worker(
    mut swarm: Swarm<ReplBehaviour>,
    mut rx: mpsc::Receiver<Command>,
    context: WorkerContext,
) {
    for address in context.bootstrap_peers {
        if let Ok(address) = address.parse::<Multiaddr>() {
            let _ = swarm.dial(address);
        }
    }

    let mut batches = HashMap::<String, PendingBatch>::new();
    let mut lan_peers = HashSet::new();
    let mut revoked_peers = HashSet::new();
    let linger_duration = Duration::from_millis(context.batch_config.linger_ms.max(1));
    let mut linger = tokio::time::interval_at(
        tokio::time::Instant::now() + linger_duration,
        linger_duration,
    );
    loop {
        tokio::select! {
            command = rx.recv() => {
                match command {
                    Some(Command::Event { table, event }) => {
                        let compact = CompactEvent {
                            sequence: event.stamp.id.sequence,
                            time: event.stamp.time,
                            primary_key: event.primary_key,
                            path: event.path,
                            op: event.op,
                        };
                        let event_bytes = serialized_size(&compact).unwrap_or(0);
                        let batch = batches.entry(table.clone()).or_insert_with(|| PendingBatch {
                            events: Vec::new(),
                            bytes: serialized_size(&Envelope {
                                author: context.local_installation_id,
                                table: table.clone(),
                                events: Vec::new(),
                            })
                            .unwrap_or(0),
                        });
                        batch.bytes = batch.bytes.saturating_add(event_bytes);
                        batch.events.push(compact);
                        if (batch.events.len() >= context.batch_config.max_events
                            || batch.bytes >= context.batch_config.max_bytes)
                            && let Some(batch) = batches.remove(&table)
                        {
                            publish_batch(
                                &mut swarm,
                                &context.topic,
                                context.local_installation_id,
                                table,
                                batch,
                            );
                        }
                    }
                    Some(Command::Revoke(peer_id)) => {
                        revoked_peers.insert(peer_id);
                        swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        let _ = swarm.disconnect_peer_id(peer_id);
                    }
                    Some(Command::Allow(peer_id)) => {
                        revoked_peers.remove(&peer_id);
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .remove_blacklisted_peer(&peer_id);
                    }
                    Some(Command::Stop) | None => {
                        for (table, batch) in batches.drain() {
                            publish_batch(
                                &mut swarm,
                                &context.topic,
                                context.local_installation_id,
                                table,
                                batch,
                            );
                        }
                        break;
                    }
                }
            }
            _ = linger.tick() => {
                for (table, batch) in batches.drain() {
                    publish_batch(
                        &mut swarm,
                        &context.topic,
                        context.local_installation_id,
                        table,
                        batch,
                    );
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Gossipsub(
                        gossipsub::Event::Message { message, .. }
                    )) => {
                        if let (Some(source), Ok(envelope)) = (
                            message.source,
                            deserialize_from::<Envelope>(&message.data),
                        ) {
                            let _ = context.inbound.send((envelope, source));
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Mdns(
                        mdns::Event::Discovered(peers)
                    )) => {
                        for (peer_id, address) in peers {
                            if revoked_peers.contains(&peer_id) {
                                continue;
                            }
                            lan_peers.insert(peer_id);
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                            swarm
                                .behaviour_mut()
                                .gossipsub
                                .set_application_score(
                                    &peer_id,
                                    context.topology_config.lan_score_bonus,
                                );
                            let _ = swarm.dial(address);
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Mdns(
                        mdns::Event::Expired(peers)
                    )) => {
                        for (peer_id, _) in peers {
                            lan_peers.remove(&peer_id);
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Ping(event)) => {
                        if let Ok(round_trip) = event.result {
                            let lan_bonus = if lan_peers.contains(&event.peer) {
                                context.topology_config.lan_score_bonus
                            } else {
                                0.0
                            };
                            let score = lan_bonus
                                - round_trip.as_secs_f64()
                                    * 1_000.0
                                    * context.topology_config.latency_weight;
                            swarm
                                .behaviour_mut()
                                .gossipsub
                                .set_application_score(&event.peer, score);
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Identify(event)) => {
                        let _ = event;
                    }
                    _ => {}
                }
            }
        }
    }
}

fn publish_batch(
    swarm: &mut Swarm<ReplBehaviour>,
    topic: &IdentTopic,
    author: zendb_types::InstallationId,
    table: String,
    batch: PendingBatch,
) {
    let envelope = Envelope {
        author,
        table,
        events: batch.events,
    };
    if let Ok(payload) = serialize_to_vec(&envelope) {
        let _ = swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), payload);
    }
}
