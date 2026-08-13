//! `/zenin/1` libp2p protocol layer: ConnectionHandler and NetworkBehaviour.
//!
//! `ZeninHandler` owns one connection and pumps framed messages between a read
//! substream (opened by the remote) and a write substream (opened locally).
//!
//! `ZeninBehaviour` is the peer-level coordinator: it validates handshakes,
//! tracks sessions, and routes messages between the swarm event loop and
//! individual connection handlers.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use libp2p::{
    PeerId, Stream, StreamProtocol,
    core::{Endpoint, transport::PortUse, upgrade::ReadyUpgrade},
    swarm::{
        CloseConnection, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
        handler::{
            ConnectionEvent, ConnectionHandler, ConnectionHandlerEvent, FullyNegotiatedInbound,
            FullyNegotiatedOutbound, SubstreamProtocol,
        },
    },
};
use zendb_types::{InstallationId, Multiaddr, PublicKey};

use super::wire::{Message, read_message, write_message};

static ZENIN_PROTOCOL: StreamProtocol = StreamProtocol::new("/zenin/1");

type ReadFuture = Pin<Box<dyn Future<Output = (Stream, io::Result<Message>)> + Send>>;
type WriteFuture = Pin<Box<dyn Future<Output = (Stream, Message, io::Result<()>)> + Send>>;

// ─── ConnectionHandler ───────────────────────────────────────────────────────

/// Per-connection byte pump for the `/zenin/1` protocol.
///
/// Manages two substreams: one for reading (inbound) and one for writing
/// (outbound). The handler is agnostic to replication semantics; it simply
/// serializes outbound `Message` values and deserializes inbound ones.
pub(super) struct ZeninHandler {
    local_handshake: Message,
    outbound_requested: bool,
    reader_stream: Option<Stream>,
    reader: Option<ReadFuture>,
    writer_stream: Option<Stream>,
    writer: Option<WriteFuture>,
    outbound: VecDeque<Message>,
    events: VecDeque<HandlerEvent>,
    handshake_sent: bool,
}

impl ZeninHandler {
    fn new(local_handshake: Message) -> Self {
        Self {
            local_handshake,
            outbound_requested: false,
            reader_stream: None,
            reader: None,
            writer_stream: None,
            writer: None,
            outbound: VecDeque::new(),
            events: VecDeque::new(),
            handshake_sent: false,
        }
    }
}

#[derive(Debug)]
pub(super) enum HandlerEvent {
    MessageReceived { message: Message },
}

impl ConnectionHandler for ZeninHandler {
    type FromBehaviour = Message;
    type ToBehaviour = HandlerEvent;
    type InboundProtocol = ReadyUpgrade<StreamProtocol>;
    type OutboundProtocol = ReadyUpgrade<StreamProtocol>;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(ReadyUpgrade::new(ZENIN_PROTOCOL.clone()), ())
    }

    fn connection_keep_alive(&self) -> bool {
        true
    }

    fn on_behaviour_event(&mut self, message: Message) {
        self.outbound.push_back(message);
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol, ..
            }) => {
                if self.reader_stream.is_none() && self.reader.is_none() {
                    self.reader_stream = Some(protocol);
                }
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol, ..
            }) => {
                self.outbound_requested = false;
                self.handshake_sent = false;
                if self.writer_stream.is_none() && self.writer.is_none() {
                    self.writer_stream = Some(protocol);
                }
            }
            ConnectionEvent::DialUpgradeError(_) => {
                self.outbound_requested = false;
                self.writer_stream = None;
                self.writer = None;
                self.handshake_sent = false;
            }
            ConnectionEvent::ListenUpgradeError(_) => {
                self.reader_stream = None;
                self.reader = None;
            }
            _ => {}
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        // ── Read side ────────────────────────────────────────────────────
        if self.reader.is_none()
            && let Some(stream) = self.reader_stream.take()
        {
            self.reader = Some(Box::pin(async move {
                let mut s = stream;
                let result = read_message(&mut s).await;
                (s, result)
            }));
        }
        if let Some(reader) = &mut self.reader {
            match reader.as_mut().poll(cx) {
                Poll::Ready((stream, Ok(message))) => {
                    self.reader = None;
                    self.reader_stream = Some(stream);
                    self.events
                        .push_back(HandlerEvent::MessageReceived { message });
                }
                Poll::Ready((_stream, Err(error))) => {
                    tracing::error!(%error, "replication frame read failed");
                    self.reader = None;
                }
                Poll::Pending => {}
            }
        }

        // ── Write side ───────────────────────────────────────────────────
        if self.writer.is_none()
            && let Some(stream) = self.writer_stream.take()
        {
            let message = if !self.handshake_sent {
                Some(self.local_handshake.clone())
            } else {
                self.outbound.pop_front()
            };
            if let Some(message) = message {
                self.writer = Some(Box::pin(async move {
                    let mut s = stream;
                    let result = {
                        use futures::AsyncWriteExt;
                        let r = write_message(&mut s, &message).await;
                        if r.is_ok() { s.flush().await } else { r }
                    };
                    (s, message, result)
                }));
            } else {
                self.writer_stream = Some(stream);
            }
        }
        if let Some(writer) = &mut self.writer {
            match writer.as_mut().poll(cx) {
                Poll::Ready((stream, message, Ok(()))) => {
                    self.handshake_sent |= matches!(message, Message::Handshake { .. });
                    self.writer = None;
                    self.writer_stream = Some(stream);
                }
                Poll::Ready((_stream, _message, Err(error))) => {
                    tracing::error!(%error, "replication frame write failed");
                    self.writer = None;
                    self.handshake_sent = false;
                }
                Poll::Pending => {}
            }
        }

        // If we have no write substream at all, request one.
        if !self.outbound_requested && self.writer_stream.is_none() && self.writer.is_none() {
            self.outbound_requested = true;
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(ReadyUpgrade::new(ZENIN_PROTOCOL.clone()), ()),
            });
        }

        // Deliver queued events to the behaviour.
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        Poll::Pending
    }
}

// ─── NetworkBehaviour ────────────────────────────────────────────────────────

/// Events emitted by `ZeninBehaviour` to the controller event loop.
#[derive(Debug)]
pub(super) enum BehaviourEvent {
    /// A peer completed a valid handshake for the correct workspace.
    SessionEstablished {
        peer_id: PeerId,
        installation_id: InstallationId,
        display_name: String,
        public_key: PublicKey,
        addresses: Vec<Multiaddr>,
    },
    /// A validated peer sent a protocol message.
    MessageReceived { peer_id: PeerId, message: Message },
    /// All connections to a peer have closed.
    SessionClosed { peer_id: PeerId },
}

/// Peer-level session manager for the `/zenin/1` protocol.
///
/// Responsibilities:
/// - Creates a `ZeninHandler` for every new connection.
/// - Validates incoming handshakes (workspace ID match, PeerId derivation).
/// - Queues outbound messages for peers (even before connection is established).
/// - Tracks logical sessions independently of physical connection count.
pub(super) struct ZeninBehaviour {
    local_peer_id: PeerId,
    local_handshake: Message,
    outbound: VecDeque<(PeerId, Message)>,
    events: VecDeque<ToSwarm<BehaviourEvent, Message>>,
    sessions: HashMap<PeerId, InstallationId>,
    connected: HashMap<PeerId, HashMap<ConnectionId, Endpoint>>,
    active: HashMap<PeerId, ConnectionId>,
    closing: std::collections::HashSet<(PeerId, ConnectionId)>,
    pending: HashMap<PeerId, VecDeque<Message>>,
}

impl ZeninBehaviour {
    pub(super) fn new(local_peer_id: PeerId, local_handshake: Message) -> Self {
        Self {
            local_peer_id,
            local_handshake,
            outbound: VecDeque::new(),
            events: VecDeque::new(),
            sessions: HashMap::new(),
            connected: HashMap::new(),
            active: HashMap::new(),
            closing: std::collections::HashSet::new(),
            pending: HashMap::new(),
        }
    }

    /// Queue a message for delivery to the given peer.
    pub(super) fn send(&mut self, peer_id: PeerId, message: Message) {
        self.pending.entry(peer_id).or_default().push_back(message);
        self.flush_peer(peer_id);
    }

    /// Replace the handshake used for future connections.
    pub(super) fn set_handshake(&mut self, handshake: Message) {
        self.local_handshake = handshake;
    }

    /// Forcefully close a session (e.g. after discovering the peer is not admitted).
    pub(super) fn close_session(&mut self, peer_id: PeerId) {
        self.sessions.remove(&peer_id);
        self.pending.remove(&peer_id);

        let connections = self
            .connected
            .get(&peer_id)
            .map(|connections| connections.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        for connection_id in connections {
            self.close_connection(peer_id, connection_id);
        }
    }

    fn close_connection(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        if self.closing.insert((peer_id, connection_id)) {
            self.events.push_back(ToSwarm::CloseConnection {
                peer_id,
                connection: CloseConnection::One(connection_id),
            });
        }
    }

    fn flush_peer(&mut self, peer_id: PeerId) {
        if !self.active.contains_key(&peer_id) {
            return;
        }
        if let Some(pending) = self.pending.get_mut(&peer_id) {
            while let Some(message) = pending.pop_front() {
                self.outbound.push_back((peer_id, message));
            }
        }
        if self.pending.get(&peer_id).is_some_and(|p| p.is_empty()) {
            self.pending.remove(&peer_id);
        }
    }

    fn register_connection(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        endpoint: Endpoint,
    ) {
        self.connected
            .entry(peer_id)
            .or_default()
            .insert(connection_id, endpoint);

        let Some(active) = self.choose_connection(peer_id) else {
            return;
        };
        self.active.insert(peer_id, active);

        let has_dialer = self.connected.get(&peer_id).is_some_and(|connections| {
            connections
                .values()
                .any(|endpoint| *endpoint == Endpoint::Dialer)
        });
        let has_listener = self.connected.get(&peer_id).is_some_and(|connections| {
            connections
                .values()
                .any(|endpoint| *endpoint == Endpoint::Listener)
        });
        if has_dialer
            && has_listener
            && let Some(connections) = self.connected.get(&peer_id)
        {
            let closing = connections
                .keys()
                .copied()
                .filter(|connection_id| *connection_id != active)
                .collect::<Vec<_>>();
            for connection_id in closing {
                self.close_connection(peer_id, connection_id);
            }
        }
        self.flush_peer(peer_id);
    }

    fn choose_connection(&self, peer_id: PeerId) -> Option<ConnectionId> {
        let connections = self.connected.get(&peer_id)?;
        if connections.len() == 1 {
            return connections.keys().next().copied();
        }

        let has_dialer = connections
            .values()
            .any(|endpoint| *endpoint == Endpoint::Dialer);
        let has_listener = connections
            .values()
            .any(|endpoint| *endpoint == Endpoint::Listener);
        if has_dialer && has_listener {
            let preferred = if self.local_peer_id.to_bytes() < peer_id.to_bytes() {
                Endpoint::Dialer
            } else {
                Endpoint::Listener
            };
            return connections
                .iter()
                .filter(|(_, endpoint)| **endpoint == preferred)
                .map(|(connection_id, _)| *connection_id)
                .min();
        }

        self.active
            .get(&peer_id)
            .copied()
            .filter(|connection_id| connections.contains_key(connection_id))
            .or_else(|| connections.keys().min().copied())
    }
}

impl NetworkBehaviour for ZeninBehaviour {
    type ConnectionHandler = ZeninHandler;
    type ToSwarm = BehaviourEvent;

    fn handle_pending_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: &libp2p::Multiaddr,
        _: &libp2p::Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        Ok(())
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        _: &libp2p::Multiaddr,
        _: &libp2p::Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.register_connection(peer, connection_id, Endpoint::Listener);
        Ok(ZeninHandler::new(self.local_handshake.clone()))
    }

    fn handle_pending_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: Option<PeerId>,
        _: &[libp2p::Multiaddr],
        _: Endpoint,
    ) -> Result<Vec<libp2p::Multiaddr>, ConnectionDenied> {
        Ok(Vec::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        _: &libp2p::Multiaddr,
        endpoint: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.register_connection(peer, connection_id, endpoint);
        Ok(ZeninHandler::new(self.local_handshake.clone()))
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        if let FromSwarm::ConnectionClosed(event) = event {
            if let Some(connections) = self.connected.get_mut(&event.peer_id) {
                connections.remove(&event.connection_id);
                self.closing.remove(&(event.peer_id, event.connection_id));
                if self.active.get(&event.peer_id) == Some(&event.connection_id) {
                    self.active.remove(&event.peer_id);
                }
                if connections.is_empty() {
                    self.connected.remove(&event.peer_id);
                    self.active.remove(&event.peer_id);
                    self.sessions.remove(&event.peer_id);
                    self.events
                        .push_back(ToSwarm::GenerateEvent(BehaviourEvent::SessionClosed {
                            peer_id: event.peer_id,
                        }));
                } else if let Some(active) = self.choose_connection(event.peer_id) {
                    self.active.insert(event.peer_id, active);
                    self.flush_peer(event.peer_id);
                }
            }
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        let HandlerEvent::MessageReceived { message } = event;

        // Handshake validation: workspace must match, PeerId must derive from
        // the advertised public key.
        if let Message::Handshake {
            workspace_id,
            installation_id,
            display_name,
            public_key,
            addresses,
        } = message
        {
            let Message::Handshake {
                workspace_id: our_workspace_id,
                ..
            } = &self.local_handshake
            else {
                self.close_connection(peer_id, connection_id);
                return;
            };
            if workspace_id != *our_workspace_id {
                self.close_connection(peer_id, connection_id);
                return;
            }
            if public_key.as_libp2p().to_peer_id() != peer_id {
                self.close_connection(peer_id, connection_id);
                return;
            }
            if self.sessions.get(&peer_id) != Some(&installation_id) {
                self.sessions.insert(peer_id, installation_id);
                self.events
                    .push_back(ToSwarm::GenerateEvent(BehaviourEvent::SessionEstablished {
                        peer_id,
                        installation_id,
                        display_name,
                        public_key,
                        addresses,
                    }));
            }
            return;
        }

        // Non-handshake messages are only accepted after a valid session.
        if self.active.get(&peer_id) == Some(&connection_id) && self.sessions.contains_key(&peer_id)
        {
            self.events
                .push_back(ToSwarm::GenerateEvent(BehaviourEvent::MessageReceived {
                    peer_id,
                    message,
                }));
        }
    }

    fn poll(&mut self, _: &mut Context<'_>) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        if let Some((peer_id, message)) = self.outbound.pop_front() {
            let handler = self.active.get(&peer_id).copied().map_or(
                libp2p::swarm::NotifyHandler::Any,
                libp2p::swarm::NotifyHandler::One,
            );
            return Poll::Ready(ToSwarm::NotifyHandler {
                peer_id,
                handler,
                event: message,
            });
        }
        Poll::Pending
    }
}
