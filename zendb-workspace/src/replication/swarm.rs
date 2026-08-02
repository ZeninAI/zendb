//! Construction and event types for the libp2p replication swarm.

use libp2p::{
    Swarm, SwarmBuilder,
    gossipsub::{
        self, IdentTopic, MessageAuthenticity, PeerScoreParams, PeerScoreThresholds, ValidationMode,
    },
    identify, mdns, noise, ping,
    swarm::NetworkBehaviour,
    tcp, yamux,
};
use libp2p_identity::Keypair;
use zendb_types::WorkspaceId;

use super::ReplicationConfig;

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "ReplicationBehaviourEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
pub(super) struct ReplicationBehaviour {
    pub(super) gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
}

pub(super) enum ReplicationBehaviourEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
    Ping(ping::Event),
    Identify(Box<identify::Event>),
}

impl From<gossipsub::Event> for ReplicationBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossipsub(event)
    }
}

impl From<mdns::Event> for ReplicationBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        Self::Mdns(event)
    }
}

impl From<ping::Event> for ReplicationBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}

impl From<identify::Event> for ReplicationBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(Box::new(event))
    }
}

pub(super) fn build_swarm(
    workspace_id: WorkspaceId,
    keypair: Keypair,
    config: &ReplicationConfig,
) -> std::result::Result<(Swarm<ReplicationBehaviour>, IdentTopic), String> {
    let local_peer_id = keypair.public().to_peer_id();
    // The derived workspace key signs gossipsub messages and identify data;
    // strict validation therefore binds transport identity to installation
    // membership checked later by admission.
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
    let behaviour = ReplicationBehaviour {
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
            .listen_on(address.as_libp2p().clone())
            .map_err(|error| error.to_string())?;
    }
    Ok((swarm, topic))
}
