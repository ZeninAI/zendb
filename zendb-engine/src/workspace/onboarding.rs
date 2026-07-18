//! Direct and bearer-ticket Device onboarding.

use std::{collections::BTreeSet, io, net::SocketAddr, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use zendb_replication::{SnapshotExport, SnapshotManifest, SyncSnapshotChunk};
use zendb_transport::{
    build_direct_request, evidence_from_request, ticket_admission_signing_bytes,
    verify_candidate_request, BootstrapApproval, BootstrapRequest, ConnectionHint,
    EnrollmentPresentation, HandshakePeer, SecureSession, SessionPurpose, TcpLink,
    TcpSecureSession,
};
use zendb_types::{
    CapabilityId, ContiguousFrontier, DeviceKeyPhase, DeviceKeyRing, DevicePublicKey, DeviceRecord,
    EnrollmentTicketId, Hlc,
};

use super::{now_ms, snapshot, system, Workspace};

const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Encode, Decode)]
enum BootstrapMessage {
    Request(BootstrapRequest),
    Approved {
        approval: BootstrapApproval,
        manifest: SnapshotManifest,
        chunk_count: u32,
    },
    SnapshotChunk(SyncSnapshotChunk),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnboardingResult {
    pub peer_device_id: zendb_types::DeviceId,
    pub admitted_device_id: zendb_types::DeviceId,
}

impl Workspace {
    /// Create a replicated ticket verifier and return its secret QR/link
    /// presentation. Ordinary Manager authorization is enforced when the
    /// control event is applied.
    pub fn create_enrollment_ticket(
        self: &Arc<Self>,
        valid_for: Duration,
        connection_hints: Vec<ConnectionHint>,
    ) -> io::Result<EnrollmentPresentation> {
        let ticket_id = random_ticket_id()?;
        let now = now_ms();
        let expires_ms = now
            .checked_add(valid_for.as_millis().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "ticket duration is too large")
            })?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ticket expiry overflow"))?;
        let expires_at = Hlc::with_device_id(expires_ms, 0, self.device_id()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "ticket expiry exceeds HLC range",
            )
        })?;
        let (ticket, presentation) = EnrollmentPresentation::generate(
            self.workspace_id().clone(),
            ticket_id.clone(),
            expires_at,
            connection_hints,
        )?;
        let event_hlc = self.device_profile.next_hlc(now)?;
        self.commit_shared_event(system::ticket_event(event_hlc, &ticket_id, &ticket))?;
        Ok(presentation)
    }

    /// Tombstone a replicated ticket verifier. Requires Manager.
    pub fn delete_enrollment_ticket(
        self: &Arc<Self>,
        ticket_id: &EnrollmentTicketId,
    ) -> io::Result<()> {
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(system::delete_ticket_event(at, ticket_id))?;
        Ok(())
    }

    /// Bootstrap a candidate using a QR/link presentation. The ticket verifier
    /// inside the returned snapshot is the trust anchor, so the rendezvous peer
    /// itself need not have been known in advance.
    pub fn bootstrap_with_ticket(
        self: &Arc<Self>,
        address: SocketAddr,
        presentation: &EnrollmentPresentation,
        requested_name: String,
        capabilities: BTreeSet<CapabilityId>,
    ) -> io::Result<OnboardingResult> {
        if presentation.workspace_id != *self.workspace_id() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "enrollment presentation belongs to another Workspace",
            ));
        }
        let request =
            presentation.build_request(self.device_profile(), requested_name, capabilities)?;
        let verifier_key = presentation.verifier_key();
        self.bootstrap_client(address, request, None, Some(verifier_key))
    }

    /// Bootstrap after a Manager has directly pre-admitted this Device. The
    /// out-of-band flow must supply the expected bootstrap peer key.
    pub fn bootstrap_direct(
        self: &Arc<Self>,
        address: SocketAddr,
        expected_peer_key: DevicePublicKey,
        requested_name: String,
        capabilities: BTreeSet<CapabilityId>,
    ) -> io::Result<OnboardingResult> {
        let request = build_direct_request(
            self.workspace_id().clone(),
            self.device_profile(),
            requested_name,
            capabilities,
        )?;
        self.bootstrap_client(address, request, Some(expected_peer_key), None)
    }

    fn bootstrap_client(
        self: &Arc<Self>,
        address: SocketAddr,
        request: BootstrapRequest,
        expected_peer_key: Option<DevicePublicKey>,
        ticket_verifier: Option<DevicePublicKey>,
    ) -> io::Result<OnboardingResult> {
        let mut session = SecureSession::connect(
            TcpLink::connect(address)?,
            self.workspace_id(),
            self.device_profile(),
            SessionPurpose::Bootstrap,
            |peer| {
                if expected_peer_key.is_some_and(|expected| expected != peer.public_key) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "bootstrap peer key does not match out-of-band trust",
                    ));
                }
                Ok(())
            },
        )?;
        session.set_timeouts(Some(Duration::from_secs(60)))?;
        let peer = session.peer().clone();
        session.send_value(&BootstrapMessage::Request(request.clone()))?;
        let BootstrapMessage::Approved {
            approval,
            manifest,
            chunk_count,
        } = session.receive_value()?
        else {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "bootstrap peer rejected the request",
            ));
        };
        if approval.accepted_device_id != self.device_id() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bootstrap approval names another Device",
            ));
        }
        let snapshot = receive_bootstrap_snapshot(&mut session, manifest, chunk_count)?;
        self.validate_bootstrap_snapshot(&peer, &request, ticket_verifier, &snapshot)?;
        self.install_snapshot(&snapshot)?;
        Ok(OnboardingResult {
            peer_device_id: peer.device_id,
            admitted_device_id: approval.accepted_device_id,
        })
    }

    pub(crate) fn serve_bootstrap_session(
        self: &Arc<Self>,
        session: &mut TcpSecureSession,
    ) -> io::Result<()> {
        let BootstrapMessage::Request(request) = session.receive_value()? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected bootstrap request",
            ));
        };
        if request.workspace_id != *self.workspace_id()
            || request.candidate_device_id != session.peer().device_id
            || request.candidate_public_key != session.peer().public_key
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "bootstrap request does not match its authenticated session",
            ));
        }
        verify_candidate_request(&request)?;

        if request.ticket_admission.is_some() {
            let evidence = evidence_from_request(&request)?;
            let record = candidate_record(&request);
            let at = self.device_profile.next_hlc(now_ms())?;
            self.commit_shared_event_with_admission(
                system::admit_event(at, request.candidate_device_id, record),
                Some(evidence),
            )?;
        } else {
            let admitted = self.device(request.candidate_device_id)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "direct-bootstrap Device has not been pre-admitted",
                )
            })?;
            if admitted.key_ring.primary_key != request.candidate_public_key
                && admitted.key_ring.secondary_key != Some(request.candidate_public_key)
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "direct-bootstrap key is not in the admitted Device record",
                ));
            }
        }

        let snapshot = self.export_snapshot()?;
        let chunk_count = super::network::snapshot_chunk_count(snapshot.bytes.len())?;
        session.send_value(&BootstrapMessage::Approved {
            approval: BootstrapApproval {
                snapshot_hint_bytes: Some(snapshot.bytes.len() as u64),
                accepted_device_id: request.candidate_device_id,
            },
            manifest: snapshot.manifest.clone(),
            chunk_count,
        })?;
        for (index, bytes) in snapshot.bytes.chunks(SNAPSHOT_CHUNK_BYTES).enumerate() {
            session.send_value(&BootstrapMessage::SnapshotChunk(SyncSnapshotChunk {
                workspace_id: snapshot.manifest.workspace_id.clone(),
                chunk_index: index as u32,
                chunk_hash: *blake3::hash(bytes).as_bytes(),
                bytes: bytes.to_vec(),
            }))?;
        }
        Ok(())
    }

    fn validate_bootstrap_snapshot(
        &self,
        peer: &HandshakePeer,
        request: &BootstrapRequest,
        ticket_verifier: Option<DevicePublicKey>,
        export: &SnapshotExport,
    ) -> io::Result<()> {
        let decoded = snapshot::decode_snapshot(self.workspace_id(), export)?;
        let candidate = decoded
            .system
            .device(request.candidate_device_id)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "snapshot did not admit candidate",
                )
            })?;
        if candidate.name != request.requested_name
            || candidate.key_ring.primary_key != request.candidate_public_key
            || !candidate.roles.is_empty()
            || candidate.capabilities != request.capabilities
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "snapshot candidate record does not match bootstrap request",
            ));
        }
        let server = decoded.system.device(peer.device_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "bootstrap peer is absent from snapshot membership",
            )
        })?;
        if server.key_ring.primary_key != peer.public_key
            && server.key_ring.secondary_key != Some(peer.public_key)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "bootstrap peer session key is absent from snapshot membership",
            ));
        }
        if let Some(expected_verifier) = ticket_verifier {
            let proof = request.ticket_admission.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing ticket proof")
            })?;
            let ticket = decoded.system.ticket(&proof.ticket_id)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "snapshot omitted enrollment ticket",
                )
            })?;
            if ticket.verifier_key != expected_verifier {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "snapshot ticket verifier does not match QR credential",
                ));
            }
            let bytes = ticket_admission_signing_bytes(
                self.workspace_id(),
                &proof.ticket_id,
                request.candidate_device_id,
                request.candidate_public_key,
                &request.requested_name,
                &request.capabilities,
            )?;
            if !zendb_transport::DeviceProfile::verify(
                ticket.verifier_key,
                &bytes,
                &proof.signature,
            ) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "snapshot ticket does not verify the candidate admission",
                ));
            }
        }
        Ok(())
    }
}

fn candidate_record(request: &BootstrapRequest) -> DeviceRecord {
    DeviceRecord {
        name: request.requested_name.clone(),
        key_ring: DeviceKeyRing {
            primary_key: request.candidate_public_key,
            secondary_key: None,
            primary_from_seq: 1,
            phase: DeviceKeyPhase::Stable,
        },
        roles: BTreeSet::new(),
        capabilities: request.capabilities.clone(),
        replication_frontier: ContiguousFrontier::default(),
    }
}

fn random_ticket_id() -> io::Result<EnrollmentTicketId> {
    EnrollmentTicketId::generate()
}

fn receive_bootstrap_snapshot(
    session: &mut TcpSecureSession,
    manifest: SnapshotManifest,
    chunk_count: u32,
) -> io::Result<SnapshotExport> {
    if chunk_count != super::network::expected_snapshot_chunk_count(manifest.total_bytes)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bootstrap chunk count does not match total bytes",
        ));
    }
    let initial_capacity = usize::try_from(manifest.total_bytes)
        .unwrap_or(usize::MAX)
        .min(SNAPSHOT_CHUNK_BYTES * 2);
    let mut bytes = Vec::with_capacity(initial_capacity);
    for expected_index in 0..chunk_count {
        let BootstrapMessage::SnapshotChunk(chunk) = session.receive_value()? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected bootstrap snapshot chunk",
            ));
        };
        let remaining = manifest.total_bytes.saturating_sub(bytes.len() as u64);
        let expected_length = remaining.min(SNAPSHOT_CHUNK_BYTES as u64) as usize;
        if chunk.workspace_id != manifest.workspace_id
            || chunk.chunk_index != expected_index
            || chunk.bytes.len() != expected_length
            || chunk.chunk_hash != *blake3::hash(&chunk.bytes).as_bytes()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bootstrap snapshot chunk validation failed",
            ));
        }
        bytes
            .try_reserve(chunk.bytes.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        bytes.extend_from_slice(&chunk.bytes);
    }
    if bytes.len() as u64 != manifest.total_bytes
        || *blake3::hash(&bytes).as_bytes() != manifest.snapshot_hash
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "assembled bootstrap snapshot validation failed",
        ));
    }
    Ok(SnapshotExport { manifest, bytes })
}
