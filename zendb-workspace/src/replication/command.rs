//! Commands and peer routes exchanged between replication control and transport.

use libp2p::{Multiaddr, PeerId};
use zendb_types::{Event, InstallationId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PeerRoute {
    pub(super) peer_id: PeerId,
    pub(super) addresses: Vec<Multiaddr>,
}

pub(super) enum Command {
    Event {
        table: String,
        event: Event,
    },
    UpsertPeer {
        installation_id: InstallationId,
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },
    RemovePeer {
        installation_id: InstallationId,
        peer_id: PeerId,
    },
    Stop,
}
