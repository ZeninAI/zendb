//! QR/link enrollment credentials and canonical bootstrap proofs.

use std::{collections::BTreeSet, io};

use bincode::{Decode, Encode};
use ed25519_dalek::{Signer, SigningKey};
use zendb_types::{
    CapabilityId, DeviceId, DevicePublicKey, EnrollmentTicket, EnrollmentTicketId, Hlc,
    SignatureBytes, TicketAdmissionEvidence, WorkspaceId,
};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::{BootstrapRequest, BootstrapTicketProof, DeviceProfile, NetworkEndpoint};

/// Secret bearer presentation encoded into a QR code or link. It is never
/// written to Workspace state and should be erased after use or expiry.
#[derive(Encode, Decode, Zeroize, ZeroizeOnDrop)]
pub struct EnrollmentPresentation {
    #[zeroize(skip)]
    pub workspace_id: WorkspaceId,
    #[zeroize(skip)]
    pub ticket_id: EnrollmentTicketId,
    #[zeroize(skip)]
    pub expires_at: Hlc,
    #[zeroize(skip)]
    pub rendezvous_hints: Vec<NetworkEndpoint>,
    ticket_secret: [u8; 32],
}

impl EnrollmentPresentation {
    pub fn verifier_key(&self) -> DevicePublicKey {
        DevicePublicKey(
            SigningKey::from_bytes(&self.ticket_secret)
                .verifying_key()
                .to_bytes(),
        )
    }

    pub fn generate(
        workspace_id: WorkspaceId,
        ticket_id: EnrollmentTicketId,
        expires_at: Hlc,
        rendezvous_hints: Vec<NetworkEndpoint>,
    ) -> io::Result<(EnrollmentTicket, Self)> {
        let mut ticket_secret = [0; 32];
        getrandom::fill(&mut ticket_secret).map_err(|error| io::Error::other(error.to_string()))?;
        let verifier_key = DevicePublicKey(
            SigningKey::from_bytes(&ticket_secret)
                .verifying_key()
                .to_bytes(),
        );
        Ok((
            EnrollmentTicket {
                verifier_key,
                expires_at,
            },
            Self {
                workspace_id,
                ticket_id,
                expires_at,
                rendezvous_hints,
                ticket_secret,
            },
        ))
    }

    pub fn encode(&self) -> io::Result<Vec<u8>> {
        bincode::encode_to_vec(self, bincode::config::standard())
            .map_err(|error| io::Error::other(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let (presentation, consumed) =
            bincode::decode_from_slice(bytes, bincode::config::standard())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        if consumed != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "enrollment presentation contains trailing bytes",
            ));
        }
        Ok(presentation)
    }

    pub fn build_request(
        &self,
        profile: &DeviceProfile,
        requested_name: String,
        capabilities: BTreeSet<CapabilityId>,
    ) -> io::Result<BootstrapRequest> {
        let candidate_device_id = profile.device_id();
        let candidate_public_key = profile.primary_public_key();
        let admission_bytes = ticket_admission_signing_bytes(
            &self.workspace_id,
            &self.ticket_id,
            candidate_device_id,
            candidate_public_key,
            &requested_name,
            &capabilities,
        )?;
        let ticket_signature = SignatureBytes(
            SigningKey::from_bytes(&self.ticket_secret)
                .sign(&admission_bytes)
                .to_bytes()
                .to_vec(),
        );
        let candidate_bytes = candidate_proof_signing_bytes(
            &self.workspace_id,
            candidate_device_id,
            candidate_public_key,
            &requested_name,
            &capabilities,
            Some((&self.ticket_id, &ticket_signature)),
        )?;
        Ok(BootstrapRequest {
            workspace_id: self.workspace_id.clone(),
            candidate_device_id,
            candidate_public_key,
            requested_name,
            capabilities,
            candidate_proof: profile.sign_primary(&candidate_bytes),
            ticket_admission: Some(BootstrapTicketProof {
                ticket_id: self.ticket_id.clone(),
                signature: ticket_signature,
            }),
        })
    }
}

pub fn build_direct_request(
    workspace_id: WorkspaceId,
    profile: &DeviceProfile,
    requested_name: String,
    capabilities: BTreeSet<CapabilityId>,
) -> io::Result<BootstrapRequest> {
    let candidate_device_id = profile.device_id();
    let candidate_public_key = profile.primary_public_key();
    let bytes = candidate_proof_signing_bytes(
        &workspace_id,
        candidate_device_id,
        candidate_public_key,
        &requested_name,
        &capabilities,
        None,
    )?;
    Ok(BootstrapRequest {
        workspace_id,
        candidate_device_id,
        candidate_public_key,
        requested_name,
        capabilities,
        candidate_proof: profile.sign_primary(&bytes),
        ticket_admission: None,
    })
}

pub fn verify_candidate_request(request: &BootstrapRequest) -> io::Result<()> {
    let ticket = request
        .ticket_admission
        .as_ref()
        .map(|proof| (&proof.ticket_id, &proof.signature));
    let bytes = candidate_proof_signing_bytes(
        &request.workspace_id,
        request.candidate_device_id,
        request.candidate_public_key,
        &request.requested_name,
        &request.capabilities,
        ticket,
    )?;
    if DeviceProfile::verify(
        request.candidate_public_key,
        &bytes,
        &request.candidate_proof,
    ) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "candidate bootstrap proof is invalid",
        ))
    }
}

pub fn evidence_from_request(request: &BootstrapRequest) -> io::Result<TicketAdmissionEvidence> {
    let proof = request.ticket_admission.as_ref().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "request has no ticket proof")
    })?;
    Ok(TicketAdmissionEvidence {
        ticket_id: proof.ticket_id.clone(),
        candidate_device_id: request.candidate_device_id,
        candidate_public_key: request.candidate_public_key,
        requested_name: request.requested_name.clone(),
        capabilities: request.capabilities.clone(),
        ticket_signature: proof.signature.clone(),
    })
}

pub fn ticket_admission_signing_bytes(
    workspace_id: &WorkspaceId,
    ticket_id: &EnrollmentTicketId,
    candidate_device_id: DeviceId,
    candidate_public_key: DevicePublicKey,
    requested_name: &str,
    capabilities: &BTreeSet<CapabilityId>,
) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(
        (
            b"zendb-ticket-admission-v1".as_slice(),
            workspace_id,
            ticket_id,
            candidate_device_id,
            candidate_public_key,
            requested_name,
            capabilities,
        ),
        bincode::config::standard(),
    )
    .map_err(|error| io::Error::other(error.to_string()))
}

fn candidate_proof_signing_bytes(
    workspace_id: &WorkspaceId,
    candidate_device_id: DeviceId,
    candidate_public_key: DevicePublicKey,
    requested_name: &str,
    capabilities: &BTreeSet<CapabilityId>,
    ticket: Option<(&EnrollmentTicketId, &SignatureBytes)>,
) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(
        (
            b"zendb-candidate-bootstrap-v1".as_slice(),
            workspace_id,
            candidate_device_id,
            candidate_public_key,
            requested_name,
            capabilities,
            ticket,
        ),
        bincode::config::standard(),
    )
    .map_err(|error| io::Error::other(error.to_string()))
}
