//! Async replication loop coordinating commands, batching, peers, and swarm events.

use std::{collections::BTreeMap, sync::mpsc as std_mpsc, time::Duration};

use futures::StreamExt;
use libp2p::{
    PeerId, Swarm,
    gossipsub::{self, IdentTopic},
    identify, mdns,
    swarm::{
        SwarmEvent,
        dial_opts::{DialOpts, PeerCondition},
    },
};
use tokio::sync::mpsc;
use zendb_types::{Envelope, InstallationId, utils::serialize_to_vec};

use super::{
    BatchConfig, DialConfig, TopologyConfig,
    batcher::Batcher,
    command::{Command, PeerRoute},
    peer_book::PeerBook,
    swarm::{ReplBehaviour, ReplBehaviourEvent},
};

pub(super) struct WorkerContext {
    pub(super) topic: IdentTopic,
    pub(super) local_installation_id: InstallationId,
    pub(super) batch_config: BatchConfig,
    pub(super) topology_config: TopologyConfig,
    pub(super) dial_config: DialConfig,
    pub(super) remote_installations: BTreeMap<InstallationId, PeerRoute>,
    pub(super) inbound: std_mpsc::SyncSender<(Envelope, PeerId)>,
}

pub(super) async fn run_worker(
    mut swarm: Swarm<ReplBehaviour>,
    mut rx: mpsc::Receiver<Command>,
    context: WorkerContext,
) {
    let linger_duration = context.batch_config.linger.max(Duration::from_millis(1));
    let mut batches = Batcher::new(context.local_installation_id, context.batch_config);
    let mut peers = PeerBook::new(context.remote_installations, context.dial_config);
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
                        if let Some(envelope) = batches.push(table, event) {
                            publish(&mut swarm, &context.topic, envelope);
                        }
                    }
                    Some(Command::UpsertPeer {
                        installation_id,
                        peer_id,
                        addresses,
                    }) => {
                        peers.upsert(installation_id, peer_id, addresses);
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .remove_blacklisted_peer(&peer_id);
                    }
                    Some(Command::RemovePeer {
                        installation_id,
                        peer_id,
                    }) => {
                        peers.remove(installation_id, peer_id);
                        swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        let _ = swarm.disconnect_peer_id(peer_id);
                    }
                    Some(Command::Stop) | None => {
                        for envelope in batches.drain() {
                            publish(&mut swarm, &context.topic, envelope);
                        }
                        break;
                    }
                }
            }
            _ = linger.tick() => {
                for envelope in batches.drain() {
                    publish(&mut swarm, &context.topic, envelope);
                }
            }
            _ = dial.tick() => {
                for (peer_id, addresses) in peers.due_dials(tokio::time::Instant::now()) {
                    let options = DialOpts::peer_id(peer_id)
                        .condition(PeerCondition::DisconnectedAndNotDialing)
                        .addresses(addresses)
                        .build();
                    let _ = swarm.dial(options);
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Gossipsub(
                        gossipsub::Event::Message { message, .. }
                    )) => {
                        if let (Some(source), Ok(envelope)) = (
                            message.source,
                            zendb_types::utils::deserialize_from::<Envelope>(&message.data),
                        ) && peers.accepts(&source)
                        {
                            let _ = context.inbound.send((envelope, source));
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Mdns(
                        mdns::Event::Discovered(discovered)
                    )) => {
                        for (peer_id, address) in discovered {
                            if peers.discover_mdns(peer_id, address) {
                                swarm
                                    .behaviour_mut()
                                    .gossipsub
                                    .set_application_score(
                                        &peer_id,
                                        context.topology_config.lan_score_bonus,
                                    );
                            }
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Mdns(
                        mdns::Event::Expired(expired)
                    )) => {
                        for (peer_id, address) in expired {
                            peers.expire_mdns(peer_id, address);
                        }
                    }
                    SwarmEvent::Behaviour(ReplBehaviourEvent::Ping(event)) => {
                        if let Ok(round_trip) = event.result {
                            let lan_bonus = if peers.is_lan(&event.peer) {
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
                        if let identify::Event::Received { peer_id, info, .. } = *event {
                            peers.identify(peer_id, info.listen_addrs);
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        if !peers.connected(peer_id) {
                            swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                            let _ = swarm.disconnect_peer_id(peer_id);
                        }
                    }
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        num_established: 0,
                        ..
                    } => peers.disconnected(peer_id),
                    SwarmEvent::OutgoingConnectionError {
                        peer_id: Some(peer_id),
                        ..
                    } => peers.dial_failed(peer_id),
                    _ => {}
                }
            }
        }
    }
}

fn publish(swarm: &mut Swarm<ReplBehaviour>, topic: &IdentTopic, envelope: Envelope) {
    if let Ok(payload) = serialize_to_vec(&envelope) {
        let _ = swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), payload);
    }
}
