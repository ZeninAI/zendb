//! Tokio/libp2p worker, Gossipsub batching, discovery, and mesh control.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
    identify, mdns,
    multiaddr::Protocol,
    noise, ping,
    swarm::{
        NetworkBehaviour, SwarmEvent,
        dial_opts::{DialOpts, PeerCondition},
    },
    tcp, yamux,
};
use libp2p_identity::Keypair;
use tokio::sync::mpsc;
use zendb_types::{
    CompactEvent, Envelope, Event, WorkspaceId,
    utils::{deserialize_from, serdes::serialized_size, serialize_to_vec},
};

use super::{BatchConfig, DialConfig, PeerRoute, ReplicationConfig, TopologyConfig};
use crate::{Error, Result, admission, devices::Devices, tables::Tables};

pub(crate) enum Command {
    Event {
        table: String,
        event: Event,
    },
    UpsertPeer {
        installation_id: zendb_types::InstallationId,
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },
    RemovePeer {
        installation_id: zendb_types::InstallationId,
        peer_id: PeerId,
    },
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
        remote_devices: BTreeMap<zendb_types::InstallationId, PeerRoute>,
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
                                    dial_config: config.dial,
                                    remote_devices,
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
    for address in &config.listen_addresses {
        swarm
            .listen_on(address.clone())
            .map_err(|error| error.to_string())?;
    }
    Ok((swarm, topic))
}

struct PendingBatch {
    events: Vec<CompactEvent>,
    bytes: usize,
}

struct RetryState {
    initial: Duration,
    maximum: Duration,
    delay: Duration,
    next_attempt: tokio::time::Instant,
}

impl RetryState {
    fn new(config: &DialConfig) -> Self {
        let initial = Duration::from_millis(config.initial_backoff_ms.max(1));
        let maximum = Duration::from_millis(config.max_backoff_ms.max(initial.as_millis() as u64));
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
    installation_id: zendb_types::InstallationId,
    catalog: Vec<Multiaddr>,
    mdns: HashSet<Multiaddr>,
    identified: HashSet<Multiaddr>,
    connected: bool,
    retry: RetryState,
}

impl PeerEntry {
    fn new(
        installation_id: zendb_types::InstallationId,
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

struct WorkerContext {
    topic: IdentTopic,
    local_installation_id: zendb_types::InstallationId,
    batch_config: BatchConfig,
    topology_config: TopologyConfig,
    dial_config: DialConfig,
    remote_devices: BTreeMap<zendb_types::InstallationId, PeerRoute>,
    inbound: std_mpsc::SyncSender<(Envelope, PeerId)>,
}

async fn run_worker(
    mut swarm: Swarm<ReplBehaviour>,
    mut rx: mpsc::Receiver<Command>,
    context: WorkerContext,
) {
    let mut batches = HashMap::<String, PendingBatch>::new();
    let mut peers = context
        .remote_devices
        .into_iter()
        .map(|(installation_id, route)| {
            (
                route.peer_id,
                PeerEntry::new(installation_id, route.addresses, &context.dial_config),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut lan_peers = HashSet::new();
    let mut revoked_peers = HashSet::new();
    let linger_duration = Duration::from_millis(context.batch_config.linger_ms.max(1));
    let mut linger = tokio::time::interval_at(
        tokio::time::Instant::now() + linger_duration,
        linger_duration,
    );
    let mut dial = tokio::time::interval(Duration::from_millis(100));
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
                    Some(Command::UpsertPeer {
                        installation_id,
                        peer_id,
                        addresses,
                    }) => {
                        revoked_peers.remove(&peer_id);
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .remove_blacklisted_peer(&peer_id);
                        let entry = peers.entry(peer_id).or_insert_with(|| {
                            PeerEntry::new(installation_id, Vec::new(), &context.dial_config)
                        });
                        entry.installation_id = installation_id;
                        entry.catalog = addresses;
                        entry.retry.reset();
                    }
                    Some(Command::RemovePeer {
                        installation_id,
                        peer_id,
                    }) => {
                        if peers
                            .get(&peer_id)
                            .is_some_and(|entry| entry.installation_id == installation_id)
                        {
                            peers.remove(&peer_id);
                        }
                        lan_peers.remove(&peer_id);
                        revoked_peers.insert(peer_id);
                        swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        let _ = swarm.disconnect_peer_id(peer_id);
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
            _ = dial.tick() => {
                let now = tokio::time::Instant::now();
                let due = peers
                    .iter()
                    .filter_map(|(peer_id, entry)| {
                        (!entry.connected && entry.retry.next_attempt <= now).then_some(*peer_id)
                    })
                    .collect::<Vec<_>>();
                for peer_id in due {
                    let Some(entry) = peers.get_mut(&peer_id) else {
                        continue;
                    };
                    let addresses = entry.candidate_addresses();
                    if addresses.is_empty() {
                        entry.retry.attempted();
                        continue;
                    }
                    let options = DialOpts::peer_id(peer_id)
                        .condition(PeerCondition::DisconnectedAndNotDialing)
                        .addresses(addresses)
                        .build();
                    let _ = swarm.dial(options);
                    entry.retry.attempted();
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
                        ) && peers.contains_key(&source)
                            && !revoked_peers.contains(&source)
                        {
                            let _ = context.inbound.send((envelope, source));
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Mdns(
                        mdns::Event::Discovered(discovered)
                    )) => {
                        for (peer_id, address) in discovered {
                            let Some(entry) = peers.get_mut(&peer_id) else {
                                continue;
                            };
                            let Some(address) = route_address(address, peer_id) else {
                                continue;
                            };
                            entry.mdns.insert(address);
                            entry.retry.reset();
                            lan_peers.insert(peer_id);
                            swarm
                                .behaviour_mut()
                                .gossipsub
                                .set_application_score(
                                    &peer_id,
                                    context.topology_config.lan_score_bonus,
                                );
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Mdns(
                        mdns::Event::Expired(expired)
                    )) => {
                        for (peer_id, address) in expired {
                            let Some(entry) = peers.get_mut(&peer_id) else {
                                continue;
                            };
                            let Some(address) = route_address(address, peer_id) else {
                                continue;
                            };
                            entry.mdns.remove(&address);
                            if entry.mdns.is_empty() {
                                lan_peers.remove(&peer_id);
                            }
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
                        if let identify::Event::Received { peer_id, info, .. } = *event
                            && let Some(entry) = peers.get_mut(&peer_id)
                        {
                            let addresses = info
                                .listen_addrs
                                .into_iter()
                                .filter_map(|address| route_address(address, peer_id))
                                .collect();
                            entry.identified = addresses;
                            entry.retry.reset();
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        if let Some(entry) = peers.get_mut(&peer_id) {
                            entry.connected = true;
                            entry.retry.reset();
                        } else {
                            revoked_peers.insert(peer_id);
                            swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                            let _ = swarm.disconnect_peer_id(peer_id);
                        }
                    }
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        num_established: 0,
                        ..
                    } => {
                        if let Some(entry) = peers.get_mut(&peer_id) {
                            entry.connected = false;
                            entry.retry.reset();
                        }
                    }
                    SwarmEvent::OutgoingConnectionError {
                        peer_id: Some(peer_id),
                        ..
                    } => {
                        if let Some(entry) = peers.get_mut(&peer_id) {
                            entry.connected = false;
                            entry.retry.failed();
                        }
                    }
                    _ => {}
                }
            }
        }
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
