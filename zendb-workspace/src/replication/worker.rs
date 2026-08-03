//! Async network loop coordinating live planes, batching, peers, and swarm events.

use std::{collections::BTreeMap, sync::mpsc as std_mpsc, time::Duration};

use futures::StreamExt;
use libp2p::{
    PeerId, Swarm, gossipsub, identify, mdns,
    swarm::{
        SwarmEvent,
        dial_opts::{DialOpts, PeerCondition},
    },
};
use tokio::sync::mpsc;
use zendb_types::{Envelope, InstallationId, Permission, Permissions, utils::serialize_to_vec};

use super::{
    BatchConfig, DialConfig, TopologyConfig,
    batcher::Batcher,
    command::{Command, PeerRoute},
    peers::PeerDirectory,
    swarm::{
        DataGossipEvent, MembershipGossipEvent, WorkspaceBehaviour, WorkspaceBehaviourEvent,
        WorkspaceTopics,
    },
};
use crate::system::INSTALLATIONS_TABLE_NAME;

pub(super) struct WorkerContext {
    pub(super) topics: WorkspaceTopics,
    pub(super) local_permissions: Permissions,
    pub(super) local_installation_id: InstallationId,
    pub(super) batch: BatchConfig,
    pub(super) topology: TopologyConfig,
    pub(super) dial: DialConfig,
    pub(super) initial_routes: BTreeMap<InstallationId, PeerRoute>,
    pub(super) inbound: std_mpsc::SyncSender<(Envelope, PeerId)>,
}

pub(super) async fn run_worker(
    mut swarm: Swarm<WorkspaceBehaviour>,
    mut commands: mpsc::Receiver<Command>,
    context: WorkerContext,
) {
    let WorkerContext {
        topics,
        mut local_permissions,
        local_installation_id,
        batch,
        topology,
        dial,
        initial_routes,
        inbound,
    } = context;
    let linger_duration = batch.linger.max(Duration::from_millis(1));
    let mut batches = Batcher::new(local_installation_id, batch);
    for route in initial_routes.values() {
        apply_peer_permissions(&mut swarm, route.peer_id, route.permissions);
    }
    let mut peers = PeerDirectory::new(initial_routes, dial);
    let mut linger = tokio::time::interval_at(
        tokio::time::Instant::now() + linger_duration,
        linger_duration,
    );
    let mut dial = tokio::time::interval(Duration::from_millis(100));
    loop {
        // The worker is the sole owner of swarm and peer state; controller
        // commands and network events are serialized through this select loop.
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(Command::PublishEvent { table, event }) => {
                        if let Some(envelope) = batches.push(table, event) {
                            publish_envelope(&mut swarm, &topics, envelope);
                        }
                    }
                    Some(Command::UpsertPeer {
                        peer_id,
                        addresses,
                        permissions,
                    }) => {
                        peers.upsert(peer_id, addresses, permissions);
                        apply_peer_permissions(&mut swarm, peer_id, permissions);
                    }
                    Some(Command::SetLocalPermissions(permissions)) => {
                        if local_permissions.allows(Permission::ReadData)
                            != permissions.allows(Permission::ReadData)
                        {
                            if permissions.allows(Permission::ReadData) {
                                let _ = swarm
                                    .behaviour_mut()
                                    .data
                                    .behaviour
                                    .subscribe(&topics.data);
                            } else {
                                swarm
                                    .behaviour_mut()
                                    .data
                                    .behaviour
                                    .unsubscribe(&topics.data);
                            }
                        }
                        local_permissions = permissions;
                    }
                    Some(Command::RemovePeer { peer_id }) => {
                        peers.remove(peer_id);
                        let behaviour = swarm.behaviour_mut();
                        behaviour.membership.behaviour.blacklist_peer(&peer_id);
                        behaviour
                            .membership
                            .behaviour
                            .remove_explicit_peer(&peer_id);
                        behaviour.data.behaviour.blacklist_peer(&peer_id);
                        behaviour.data.behaviour.remove_explicit_peer(&peer_id);
                        let _ = swarm.disconnect_peer_id(peer_id);
                    }
                    Some(Command::Stop) | None => {
                        for envelope in batches.drain() {
                            publish_envelope(&mut swarm, &topics, envelope);
                        }
                        break;
                    }
                }
            }
            _ = linger.tick() => {
                for envelope in batches.drain() {
                    publish_envelope(&mut swarm, &topics, envelope);
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
                    SwarmEvent::Behaviour(WorkspaceBehaviourEvent::Membership(
                        MembershipGossipEvent(gossipsub::Event::Message { message, .. })
                    )) => {
                        if let (Some(source), Ok(envelope)) = (
                            message.source,
                            zendb_types::utils::deserialize_from::<Envelope>(&message.data),
                        ) && envelope.table == INSTALLATIONS_TABLE_NAME
                            && peers.is_active(&source)
                        {
                            let _ = inbound.send((envelope, source));
                        }
                    }
                    SwarmEvent::Behaviour(WorkspaceBehaviourEvent::Data(
                        DataGossipEvent(gossipsub::Event::Message { message, .. })
                    )) => {
                        if local_permissions.allows(Permission::ReadData)
                            && let (Some(source), Ok(envelope)) = (
                                message.source,
                                zendb_types::utils::deserialize_from::<Envelope>(&message.data),
                            )
                            && envelope.table != INSTALLATIONS_TABLE_NAME
                            && peers.is_active(&source)
                        {
                            let _ = inbound.send((envelope, source));
                        }
                    }
                    SwarmEvent::Behaviour(WorkspaceBehaviourEvent::Mdns(
                        mdns::Event::Discovered(discovered)
                    )) => {
                        for (peer_id, address) in discovered {
                            if peers.discover_mdns(peer_id, address) {
                                set_peer_score(
                                    &mut swarm,
                                    peer_id,
                                    topology.lan_score_bonus,
                                    peers.can_read_data(&peer_id),
                                );
                            }
                        }
                    }
                    SwarmEvent::Behaviour(WorkspaceBehaviourEvent::Mdns(
                        mdns::Event::Expired(expired)
                    )) => {
                        for (peer_id, address) in expired {
                            peers.expire_mdns(peer_id, address);
                        }
                    }
                    SwarmEvent::Behaviour(WorkspaceBehaviourEvent::Ping(event)) => {
                        if let Ok(round_trip) = event.result {
                            let lan_bonus = if peers.is_lan(&event.peer) {
                                topology.lan_score_bonus
                            } else {
                                0.0
                            };
                            let score = lan_bonus
                                - round_trip.as_secs_f64()
                                    * 1_000.0
                                    * topology.latency_weight;
                            set_peer_score(
                                &mut swarm,
                                event.peer,
                                score,
                                peers.can_read_data(&event.peer),
                            );
                        }
                    }
                    SwarmEvent::Behaviour(WorkspaceBehaviourEvent::Identify(event)) => {
                        if let identify::Event::Received { peer_id, info, .. } = *event {
                            peers.update_identified_addresses(peer_id, info.listen_addrs);
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        if !peers.mark_connected(peer_id) {
                            let behaviour = swarm.behaviour_mut();
                            behaviour.membership.behaviour.blacklist_peer(&peer_id);
                            behaviour.data.behaviour.blacklist_peer(&peer_id);
                            let _ = swarm.disconnect_peer_id(peer_id);
                        }
                    }
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        num_established: 0,
                        ..
                    } => peers.mark_disconnected(peer_id),
                    SwarmEvent::OutgoingConnectionError {
                        peer_id: Some(peer_id),
                        ..
                    } => peers.record_dial_failure(peer_id),
                    _ => {}
                }
            }
        }
    }
}

fn apply_peer_permissions(
    swarm: &mut Swarm<WorkspaceBehaviour>,
    peer_id: PeerId,
    permissions: Permissions,
) {
    let behaviour = swarm.behaviour_mut();
    behaviour
        .membership
        .behaviour
        .remove_blacklisted_peer(&peer_id);
    if permissions.allows(Permission::ReadData) {
        behaviour.data.behaviour.remove_blacklisted_peer(&peer_id);
    } else {
        behaviour.data.behaviour.blacklist_peer(&peer_id);
        behaviour.data.behaviour.remove_explicit_peer(&peer_id);
    }
}

fn set_peer_score(
    swarm: &mut Swarm<WorkspaceBehaviour>,
    peer_id: PeerId,
    score: f64,
    can_read_data: bool,
) {
    let behaviour = swarm.behaviour_mut();
    let _ = behaviour
        .membership
        .behaviour
        .set_application_score(&peer_id, score);
    if can_read_data {
        let _ = behaviour
            .data
            .behaviour
            .set_application_score(&peer_id, score);
    }
}

fn publish_envelope(
    swarm: &mut Swarm<WorkspaceBehaviour>,
    topics: &WorkspaceTopics,
    envelope: Envelope,
) {
    let Ok(payload) = serialize_to_vec(&envelope) else {
        return;
    };
    if envelope.table == INSTALLATIONS_TABLE_NAME {
        let _ = swarm
            .behaviour_mut()
            .membership
            .behaviour
            .publish(topics.membership.clone(), payload);
    } else {
        let _ = swarm
            .behaviour_mut()
            .data
            .behaviour
            .publish(topics.data.clone(), payload);
    }
}
