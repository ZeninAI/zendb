//! Authenticated TCP anti-entropy for a Workspace.

use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};

use bincode::{Decode, Encode};
use zendb_sync::{
    EventBatch, RangeRequest, SnapshotExport, SnapshotManifest, SyncSnapshotChunk,
    SyncSnapshotMeta, WorkspaceSyncSummary,
};
use zendb_transport::{
    HandshakePeer, PresenceStatus, PresenceTracker, SecureTcpSession, SessionPurpose,
};
use zendb_types::{DepartureNotice, DeviceId, PresenceHeartbeat, SignatureBytes, WorkspaceId};

use crate::DispatchOperator;

use super::{now_ms, Workspace};

const DEFAULT_BATCH_EVENTS: usize = 256;
const MAX_EVENT_BATCH_BYTES: usize = 8 * 1024 * 1024;
const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Encode, Decode)]
enum WorkspaceMessage {
    Summary {
        summary: WorkspaceSyncSummary,
        heartbeat: PresenceHeartbeat,
    },
    Pull(Vec<RangeRequest>),
    Events(EventBatch),
    SnapshotMeta(SyncSnapshotMeta),
    SnapshotChunk(SyncSnapshotChunk),
    PullComplete,
    Complete,
    Departure(DepartureNotice),
    Error(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub events_received: u64,
    pub events_sent: u64,
    pub snapshot_installed: bool,
}

impl<D> Workspace<D>
where
    D: DispatchOperator,
{
    pub fn sync_summary(&self) -> WorkspaceSyncSummary {
        WorkspaceSyncSummary {
            workspace_id: self.workspace_id().clone(),
            frontier: self.shared_frontier(),
            snapshot_generation: None,
            compacted_through: self.shared_watermark().ok().flatten(),
        }
    }

    /// Perform one complete, bidirectional anti-entropy cycle over an
    /// authenticated encrypted TCP connection.
    pub fn sync_tcp(self: &Arc<Self>, address: SocketAddr) -> io::Result<SyncReport> {
        let workspace = Arc::clone(self);
        let mut session = SecureTcpSession::connect(
            address,
            self.workspace_id(),
            self.device_profile(),
            SessionPurpose::Replication,
            move |peer| workspace.authorize_replication_peer(peer),
        )?;
        session.set_timeouts(Some(Duration::from_secs(30)))?;
        self.run_sync_initiator(&mut session)
    }

    /// Accept one already-connected TCP stream and serve its declared session
    /// purpose. Bootstrap is handled by the onboarding module.
    pub fn serve_tcp(self: &Arc<Self>, stream: TcpStream) -> io::Result<SyncReport> {
        let workspace = Arc::clone(self);
        let mut session = SecureTcpSession::accept(
            stream,
            self.workspace_id(),
            self.device_profile(),
            move |peer| match peer.purpose {
                SessionPurpose::Replication => workspace.authorize_replication_peer(peer),
                // Candidate possession is proven by the secure handshake. Its
                // admission authority is validated from the following request.
                SessionPurpose::Bootstrap => Ok(()),
            },
        )?;
        let timeout = match session.peer().purpose {
            SessionPurpose::Replication => Duration::from_secs(30),
            SessionPurpose::Bootstrap => Duration::from_secs(60),
        };
        session.set_timeouts(Some(timeout))?;
        match session.peer().purpose {
            SessionPurpose::Replication => self.run_sync_responder(&mut session),
            SessionPurpose::Bootstrap => {
                self.serve_bootstrap_session(&mut session)?;
                Ok(SyncReport::default())
            }
        }
    }

    /// Bind a listener without introducing a server-side abstraction. The
    /// returned standard listener can be integrated with any application event
    /// loop; [`serve_tcp`](Self::serve_tcp) handles each accepted connection.
    pub fn bind_tcp(address: SocketAddr) -> io::Result<TcpListener> {
        TcpListener::bind(address)
    }

    fn authorize_replication_peer(&self, peer: &HandshakePeer) -> io::Result<()> {
        if peer.purpose != SessionPurpose::Replication {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer did not request a replication session",
            ));
        }
        let device = self.device(peer.device_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer Device is not admitted",
            )
        })?;
        if device.key_ring.primary_key != peer.public_key
            && device.key_ring.secondary_key != Some(peer.public_key)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer session key is not in its Device key ring",
            ));
        }
        Ok(())
    }

    fn run_sync_initiator(
        self: &Arc<Self>,
        session: &mut SecureTcpSession,
    ) -> io::Result<SyncReport> {
        let local_summary = self.sync_summary();
        session.send_value(&WorkspaceMessage::Summary {
            summary: local_summary.clone(),
            heartbeat: self.make_heartbeat()?,
        })?;
        let WorkspaceMessage::Summary {
            summary: remote_summary,
            heartbeat,
        } = session.receive_value()?
        else {
            return protocol_error("expected responder summary");
        };
        self.verify_heartbeat(session.peer(), &heartbeat)?;

        session.send_value(&WorkspaceMessage::Pull(missing_ranges(
            &local_summary,
            &remote_summary,
        )))?;
        let mut report = self.receive_pull(session)?;

        let WorkspaceMessage::Pull(requests) = session.receive_value()? else {
            return protocol_error("expected responder pull request");
        };
        report.events_sent = self.send_pull(session, requests)?;
        session.send_value(&WorkspaceMessage::Complete)?;
        match session.receive_value()? {
            WorkspaceMessage::Complete => Ok(report),
            WorkspaceMessage::Error(error) => Err(io::Error::other(error)),
            _ => protocol_error("expected sync completion"),
        }
    }

    fn run_sync_responder(
        self: &Arc<Self>,
        session: &mut SecureTcpSession,
    ) -> io::Result<SyncReport> {
        let (remote_summary, heartbeat) = match session.receive_value()? {
            WorkspaceMessage::Summary { summary, heartbeat } => (summary, heartbeat),
            WorkspaceMessage::Departure(notice) => {
                self.verify_departure(session.peer(), &notice)?;
                return Ok(SyncReport::default());
            }
            _ => return protocol_error("expected initiator summary or departure"),
        };
        self.verify_heartbeat(session.peer(), &heartbeat)?;
        let local_summary = self.sync_summary();
        session.send_value(&WorkspaceMessage::Summary {
            summary: local_summary.clone(),
            heartbeat: self.make_heartbeat()?,
        })?;

        let WorkspaceMessage::Pull(requests) = session.receive_value()? else {
            return protocol_error("expected initiator pull request");
        };
        let mut report = SyncReport {
            events_sent: self.send_pull(session, requests)?,
            ..SyncReport::default()
        };

        session.send_value(&WorkspaceMessage::Pull(missing_ranges(
            &local_summary,
            &remote_summary,
        )))?;
        let received = self.receive_pull(session)?;
        report.events_received = received.events_received;
        report.snapshot_installed = received.snapshot_installed;
        match session.receive_value()? {
            WorkspaceMessage::Complete => {
                session.send_value(&WorkspaceMessage::Complete)?;
                Ok(report)
            }
            WorkspaceMessage::Error(error) => Err(io::Error::other(error)),
            _ => protocol_error("expected sync completion"),
        }
    }

    fn send_pull(
        self: &Arc<Self>,
        session: &mut SecureTcpSession,
        requests: Vec<RangeRequest>,
    ) -> io::Result<u64> {
        let mut sent = 0;
        for request in requests {
            if request.workspace_id != *self.workspace_id()
                || request.from_exclusive >= request.to_inclusive
            {
                return protocol_error("invalid shared journal range request");
            }
            let events = self.shared_range(
                request.origin_device_id,
                request.from_exclusive + 1,
                request.to_inclusive,
            );
            let expected = request.to_inclusive - request.from_exclusive;
            if events.len() as u64 != expected {
                send_snapshot(session, self.export_snapshot()?)?;
                session.send_value(&WorkspaceMessage::PullComplete)?;
                return Ok(sent);
            }
            let mut batch = Vec::new();
            let mut batch_bytes = 0;
            for event in events {
                let event_bytes = bincode::encode_to_vec(&event, bincode::config::standard())
                    .map_err(|error| io::Error::other(error.to_string()))?
                    .len();
                if event_bytes > MAX_EVENT_BATCH_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "one shared event exceeds the network batch limit",
                    ));
                }
                if !batch.is_empty()
                    && (batch.len() == DEFAULT_BATCH_EVENTS
                        || batch_bytes + event_bytes > MAX_EVENT_BATCH_BYTES)
                {
                    sent += batch.len() as u64;
                    session.send_value(&WorkspaceMessage::Events(EventBatch {
                        workspace_id: self.workspace_id().clone(),
                        events: std::mem::take(&mut batch),
                    }))?;
                    batch_bytes = 0;
                }
                batch_bytes += event_bytes;
                batch.push(event);
            }
            if !batch.is_empty() {
                sent += batch.len() as u64;
                session.send_value(&WorkspaceMessage::Events(EventBatch {
                    workspace_id: self.workspace_id().clone(),
                    events: batch,
                }))?;
            }
        }
        session.send_value(&WorkspaceMessage::PullComplete)?;
        Ok(sent)
    }

    fn receive_pull(self: &Arc<Self>, session: &mut SecureTcpSession) -> io::Result<SyncReport> {
        let mut report = SyncReport::default();
        loop {
            match session.receive_value()? {
                WorkspaceMessage::Events(batch) => {
                    if batch.workspace_id != *self.workspace_id() {
                        return protocol_error("event batch workspace mismatch");
                    }
                    for event in batch.events {
                        self.ingest_shared_event(event)?;
                        report.events_received += 1;
                    }
                }
                WorkspaceMessage::SnapshotMeta(meta) => {
                    let snapshot = receive_snapshot(session, meta)?;
                    self.install_snapshot(&snapshot)?;
                    report.snapshot_installed = true;
                }
                WorkspaceMessage::SnapshotChunk(_) => {
                    return protocol_error("snapshot chunk arrived before metadata");
                }
                WorkspaceMessage::PullComplete => return Ok(report),
                WorkspaceMessage::Error(error) => return Err(io::Error::other(error)),
                _ => return protocol_error("unexpected pull response"),
            }
        }
    }

    pub(crate) fn make_heartbeat(&self) -> io::Result<PresenceHeartbeat> {
        let advertised_idle_period_ms = self
            .presence_idle_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let presence_seq = self.device_profile.allocate_presence_seq()?;
        let emitted_hlc = self.device_profile.next_hlc(now_ms())?;
        let bytes = presence_signing_bytes(
            self.workspace_id(),
            self.device_id(),
            presence_seq,
            emitted_hlc,
            Some(advertised_idle_period_ms),
        )?;
        Ok(PresenceHeartbeat {
            device_id: self.device_id(),
            presence_seq,
            emitted_hlc,
            advertised_idle_period_ms,
            signature: self.device_profile.sign_primary(&bytes),
        })
    }

    pub(crate) fn verify_heartbeat(
        &self,
        peer: &HandshakePeer,
        heartbeat: &PresenceHeartbeat,
    ) -> io::Result<()> {
        if heartbeat.device_id != peer.device_id
            || heartbeat.emitted_hlc.device_id() != heartbeat.device_id
        {
            return protocol_error("presence Device does not match authenticated session");
        }
        let bytes = presence_signing_bytes(
            self.workspace_id(),
            heartbeat.device_id,
            heartbeat.presence_seq,
            heartbeat.emitted_hlc,
            Some(heartbeat.advertised_idle_period_ms),
        )?;
        self.verify_current_presence_signature(peer.device_id, &bytes, &heartbeat.signature)?;
        let mut presence = self.presence.lock();
        let tracker = presence.entry(peer.device_id).or_insert_with(|| {
            PresenceTracker::new(
                peer.device_id,
                self.presence_grace_multiplier
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
        });
        tracker.observe_heartbeat(heartbeat, Instant::now());
        Ok(())
    }

    pub(crate) fn make_departure(&self) -> io::Result<DepartureNotice> {
        let presence_seq = self.device_profile.allocate_presence_seq()?;
        let emitted_hlc = self.device_profile.next_hlc(now_ms())?;
        let bytes = presence_signing_bytes(
            self.workspace_id(),
            self.device_id(),
            presence_seq,
            emitted_hlc,
            None,
        )?;
        Ok(DepartureNotice {
            device_id: self.device_id(),
            presence_seq,
            emitted_hlc,
            signature: self.device_profile.sign_primary(&bytes),
        })
    }

    /// Validate a heartbeat learned through another peer without treating it
    /// as proof of a direct path to the heartbeat's author.
    pub fn observe_indirect_heartbeat(&self, heartbeat: &PresenceHeartbeat) -> io::Result<bool> {
        if heartbeat.emitted_hlc.device_id() != heartbeat.device_id {
            return protocol_error("indirect heartbeat HLC names another Device");
        }
        let device = self.device(heartbeat.device_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "indirect heartbeat author is not admitted",
            )
        })?;
        let bytes = presence_signing_bytes(
            self.workspace_id(),
            heartbeat.device_id,
            heartbeat.presence_seq,
            heartbeat.emitted_hlc,
            Some(heartbeat.advertised_idle_period_ms),
        )?;
        if !zendb_transport::DeviceProfile::verify(
            device.key_ring.primary_key,
            &bytes,
            &heartbeat.signature,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "indirect heartbeat signature is invalid",
            ));
        }
        let mut presence = self.presence.lock();
        let tracker = presence.entry(heartbeat.device_id).or_insert_with(|| {
            PresenceTracker::new(
                heartbeat.device_id,
                self.presence_grace_multiplier
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
        });
        Ok(tracker.observe_indirect_heartbeat(heartbeat, Instant::now()))
    }

    /// Return this replica's local reachability classification.
    pub fn presence_status(&self, device_id: DeviceId) -> PresenceStatus {
        self.presence
            .lock()
            .get(&device_id)
            .map_or(PresenceStatus::Unknown, |tracker| {
                tracker.status_at(Instant::now())
            })
    }

    /// Return a continuous local suspicion score when direct evidence exists.
    pub fn presence_suspicion(&self, device_id: DeviceId) -> Option<f64> {
        self.presence
            .lock()
            .get(&device_id)
            .and_then(|tracker| tracker.suspicion_at(Instant::now()))
    }

    pub(crate) fn send_departure_tcp(self: &Arc<Self>, address: SocketAddr) -> io::Result<()> {
        let workspace = Arc::clone(self);
        let mut session = SecureTcpSession::connect(
            address,
            self.workspace_id(),
            self.device_profile(),
            SessionPurpose::Replication,
            move |peer| workspace.authorize_replication_peer(peer),
        )?;
        session.set_timeouts(Some(Duration::from_secs(3)))?;
        session.send_value(&WorkspaceMessage::Departure(self.make_departure()?))
    }

    fn verify_departure(&self, peer: &HandshakePeer, notice: &DepartureNotice) -> io::Result<()> {
        if notice.device_id != peer.device_id || notice.emitted_hlc.device_id() != notice.device_id
        {
            return protocol_error("departure Device does not match authenticated session");
        }
        let bytes = presence_signing_bytes(
            self.workspace_id(),
            notice.device_id,
            notice.presence_seq,
            notice.emitted_hlc,
            None,
        )?;
        self.verify_current_presence_signature(peer.device_id, &bytes, &notice.signature)?;
        let mut presence = self.presence.lock();
        let tracker = presence.entry(peer.device_id).or_insert_with(|| {
            PresenceTracker::new(
                peer.device_id,
                self.presence_grace_multiplier
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
        });
        tracker.observe_departure(notice);
        Ok(())
    }

    fn verify_current_presence_signature(
        &self,
        device_id: DeviceId,
        bytes: &[u8],
        signature: &SignatureBytes,
    ) -> io::Result<()> {
        let device = self.device(device_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "presence author is not admitted",
            )
        })?;
        if zendb_transport::DeviceProfile::verify(device.key_ring.primary_key, bytes, signature) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "presence signature is not from the current primary key",
            ))
        }
    }
}

fn missing_ranges(
    local: &WorkspaceSyncSummary,
    remote: &WorkspaceSyncSummary,
) -> Vec<RangeRequest> {
    remote
        .frontier
        .entries()
        .filter_map(|(origin, remote_through)| {
            let local_through = local.frontier.applied_through(origin);
            (local_through < *remote_through).then(|| RangeRequest {
                workspace_id: local.workspace_id.clone(),
                origin_device_id: *origin,
                from_exclusive: local_through,
                to_inclusive: *remote_through,
            })
        })
        .collect()
}

fn presence_signing_bytes(
    workspace_id: &WorkspaceId,
    device_id: DeviceId,
    sequence: u64,
    emitted_hlc: zendb_types::Hlc,
    idle_ms: Option<u64>,
) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(
        (
            b"zendb-presence-v1".as_slice(),
            workspace_id,
            device_id,
            sequence,
            emitted_hlc,
            idle_ms,
        ),
        bincode::config::standard(),
    )
    .map_err(|error| io::Error::other(error.to_string()))
}

fn protocol_error<T>(message: &str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn send_snapshot(session: &mut SecureTcpSession, snapshot: SnapshotExport) -> io::Result<()> {
    let chunk_count = snapshot_chunk_count(snapshot.bytes.len())?;
    session.send_value(&WorkspaceMessage::SnapshotMeta(SyncSnapshotMeta {
        workspace_id: snapshot.manifest.workspace_id.clone(),
        chunk_count,
        total_bytes: snapshot.manifest.total_bytes,
        summary: snapshot.manifest.summary.clone(),
        snapshot_hash: snapshot.manifest.snapshot_hash,
    }))?;
    for (index, bytes) in snapshot.bytes.chunks(SNAPSHOT_CHUNK_BYTES).enumerate() {
        session.send_value(&WorkspaceMessage::SnapshotChunk(SyncSnapshotChunk {
            workspace_id: snapshot.manifest.workspace_id.clone(),
            chunk_index: index as u32,
            chunk_hash: *blake3::hash(bytes).as_bytes(),
            bytes: bytes.to_vec(),
        }))?;
    }
    Ok(())
}

pub(super) fn snapshot_chunk_count(byte_len: usize) -> io::Result<u32> {
    expected_snapshot_chunk_count(byte_len as u64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot requires more chunks than the wire format can represent",
        )
    })
}

pub(super) fn expected_snapshot_chunk_count(total_bytes: u64) -> io::Result<u32> {
    u32::try_from(total_bytes.div_ceil(SNAPSHOT_CHUNK_BYTES as u64)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot requires more chunks than the wire format can represent",
        )
    })
}

fn receive_snapshot(
    session: &mut SecureTcpSession,
    meta: SyncSnapshotMeta,
) -> io::Result<SnapshotExport> {
    if meta.chunk_count != expected_snapshot_chunk_count(meta.total_bytes)? {
        return protocol_error("snapshot chunk count does not match total bytes");
    }
    let initial_capacity = usize::try_from(meta.total_bytes)
        .unwrap_or(usize::MAX)
        .min(SNAPSHOT_CHUNK_BYTES * 2);
    let mut bytes = Vec::with_capacity(initial_capacity);
    for expected_index in 0..meta.chunk_count {
        let WorkspaceMessage::SnapshotChunk(chunk) = session.receive_value()? else {
            return protocol_error("expected snapshot chunk");
        };
        let remaining = meta.total_bytes.saturating_sub(bytes.len() as u64);
        let expected_length = remaining.min(SNAPSHOT_CHUNK_BYTES as u64) as usize;
        if chunk.workspace_id != meta.workspace_id
            || chunk.chunk_index != expected_index
            || chunk.bytes.len() != expected_length
            || chunk.chunk_hash != *blake3::hash(&chunk.bytes).as_bytes()
        {
            return protocol_error("snapshot chunk validation failed");
        }
        bytes
            .try_reserve(chunk.bytes.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        bytes.extend_from_slice(&chunk.bytes);
    }
    if bytes.len() as u64 != meta.total_bytes
        || *blake3::hash(&bytes).as_bytes() != meta.snapshot_hash
    {
        return protocol_error("assembled snapshot validation failed");
    }
    Ok(SnapshotExport {
        manifest: SnapshotManifest {
            workspace_id: meta.workspace_id,
            compacted_through: meta.summary.compacted_through,
            total_bytes: meta.total_bytes,
            snapshot_hash: meta.snapshot_hash,
            summary: meta.summary,
        },
        bytes,
    })
}
