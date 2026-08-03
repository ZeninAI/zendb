//! Construction of the workspace swarm and its isolated live replication planes.

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
use zendb_types::{Permission, Permissions, WorkspaceId};

use super::ReplicationConfig;

const MEMBERSHIP_GOSSIP_PROTOCOL: &str = "/zendb/membership/meshsub";
const DATA_GOSSIP_PROTOCOL: &str = "/zendb/data/meshsub";

#[derive(Clone)]
pub(super) struct WorkspaceTopics {
    pub(super) membership: IdentTopic,
    pub(super) data: IdentTopic,
}

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "MembershipGossipEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
pub(super) struct MembershipGossip {
    pub(super) behaviour: gossipsub::Behaviour,
}

pub(super) struct MembershipGossipEvent(pub(super) gossipsub::Event);

impl From<gossipsub::Event> for MembershipGossipEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self(event)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "DataGossipEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
pub(super) struct DataGossip {
    pub(super) behaviour: gossipsub::Behaviour,
}

pub(super) struct DataGossipEvent(pub(super) gossipsub::Event);

impl From<gossipsub::Event> for DataGossipEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self(event)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "WorkspaceBehaviourEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
pub(super) struct WorkspaceBehaviour {
    pub(super) membership: MembershipGossip,
    pub(super) data: DataGossip,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
}

pub(super) enum WorkspaceBehaviourEvent {
    Membership(MembershipGossipEvent),
    Data(DataGossipEvent),
    Mdns(mdns::Event),
    Ping(ping::Event),
    Identify(Box<identify::Event>),
}

impl From<MembershipGossipEvent> for WorkspaceBehaviourEvent {
    fn from(event: MembershipGossipEvent) -> Self {
        Self::Membership(event)
    }
}

impl From<DataGossipEvent> for WorkspaceBehaviourEvent {
    fn from(event: DataGossipEvent) -> Self {
        Self::Data(event)
    }
}

impl From<mdns::Event> for WorkspaceBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        Self::Mdns(event)
    }
}

impl From<ping::Event> for WorkspaceBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}

impl From<identify::Event> for WorkspaceBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(Box::new(event))
    }
}

pub(super) fn build_swarm(
    workspace_id: WorkspaceId,
    keypair: Keypair,
    local_permissions: Permissions,
    config: &ReplicationConfig,
) -> std::result::Result<(Swarm<WorkspaceBehaviour>, WorkspaceTopics), String> {
    let local_peer_id = keypair.public().to_peer_id();
    let mut membership = build_gossip(&keypair, config, MEMBERSHIP_GOSSIP_PROTOCOL)?;
    let mut data = build_gossip(&keypair, config, DATA_GOSSIP_PROTOCOL)?;
    let topics = WorkspaceTopics {
        membership: IdentTopic::new(format!("zendb/{workspace_id}/membership/v1")),
        data: IdentTopic::new(format!("zendb/{workspace_id}/data/v1")),
    };
    membership
        .subscribe(&topics.membership)
        .map_err(|error| error.to_string())?;
    if local_permissions.allows(Permission::ReadData) {
        data.subscribe(&topics.data)
            .map_err(|error| error.to_string())?;
    }

    let behaviour = WorkspaceBehaviour {
        membership: MembershipGossip {
            behaviour: membership,
        },
        data: DataGossip { behaviour: data },
        mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
            .map_err(|error| error.to_string())?,
        ping: ping::Behaviour::default(),
        identify: identify::Behaviour::new(identify::Config::new(
            "/zendb/network/1".to_owned(),
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
    for address in &config.listen_addresses {
        swarm
            .listen_on(address.as_libp2p().clone())
            .map_err(|error| error.to_string())?;
    }
    Ok((swarm, topics))
}

fn build_gossip(
    keypair: &Keypair,
    config: &ReplicationConfig,
    protocol: &'static str,
) -> std::result::Result<gossipsub::Behaviour, String> {
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .protocol_id_prefix(protocol)
        .validation_mode(ValidationMode::Strict)
        .max_transmit_size(config.gossipsub_max_transmit_size)
        .mesh_n(config.topology.mesh_size)
        .build()
        .map_err(|error| error.to_string())?;
    let mut behaviour = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(keypair.clone()),
        gossipsub_config,
    )
    .map_err(|error| error.to_string())?;
    behaviour
        .with_peer_score(PeerScoreParams::default(), PeerScoreThresholds::default())
        .map_err(|error| error.to_string())?;
    Ok(behaviour)
}
