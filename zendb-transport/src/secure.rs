//! Mutually authenticated, forward-secret sessions over reliable frame links.

use std::{io, time::Duration};

use bincode::{Decode, Encode};
use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    ChaCha20Poly1305, Nonce,
};
use hkdf::Hkdf;
use sha2::Sha256;
use x25519_dalek::{EphemeralSecret, PublicKey as EphemeralPublicKey};
use zendb_types::{DeviceId, DevicePublicKey, SignatureBytes, WorkspaceId};

use crate::{ConnectionHint, DeviceProfile, FramedLink};

pub const SECURE_SESSION_VERSION: u16 = 1;
pub const DEFAULT_MAX_SECURE_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Authentication purpose is signed into the handshake. Bootstrap access is
/// therefore never silently upgraded into an admitted replication session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum SessionPurpose {
    Replication,
    Bootstrap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakePeer {
    pub device_id: DeviceId,
    pub public_key: DevicePublicKey,
    pub purpose: SessionPurpose,
}

#[derive(Debug, Clone, Encode, Decode)]
struct SecureHello {
    version: u16,
    workspace_id: WorkspaceId,
    device_id: DeviceId,
    signing_key: DevicePublicKey,
    ephemeral_key: [u8; 32],
    nonce: [u8; 32],
    purpose: SessionPurpose,
}

#[derive(Debug, Clone, Encode, Decode)]
struct SecureProof {
    signature: SignatureBytes,
}

/// An encrypted ordered byte-message session. Protocol multiplexing belongs
/// above this type; plaintext never reaches the underlying carrier after
/// the handshake completes.
pub struct SecureSession<L: FramedLink> {
    link: L,
    peer: HandshakePeer,
    send_cipher: ChaCha20Poly1305,
    receive_cipher: ChaCha20Poly1305,
    send_counter: u64,
    receive_counter: u64,
    max_frame_bytes: usize,
}

impl<L: FramedLink> SecureSession<L> {
    pub fn connect(
        mut link: L,
        workspace_id: &WorkspaceId,
        profile: &DeviceProfile,
        purpose: SessionPurpose,
        authorize_peer: impl FnOnce(&HandshakePeer) -> io::Result<()>,
    ) -> io::Result<Self> {
        let secret = EphemeralSecret::random();
        let local = hello(workspace_id, profile, purpose, &secret)?;
        write_plain(&mut link, &local)?;
        let remote: SecureHello = read_plain(&mut link, 64 * 1024)?;
        validate_scope(&remote, workspace_id, purpose)?;
        let transcript = transcript(&local, &remote)?;
        let local_proof = SecureProof {
            signature: profile.sign_primary(&proof_bytes(&transcript, true)),
        };
        write_plain(&mut link, &local_proof)?;
        let remote_proof: SecureProof = read_plain(&mut link, 64 * 1024)?;
        verify_proof(&remote, &remote_proof, &transcript, false)?;
        let peer = peer_from(&remote);
        authorize_peer(&peer)?;
        let keys = derive_keys(secret, remote.ephemeral_key, &transcript)?;
        Ok(Self::new(link, peer, keys.0, keys.1))
    }

    pub fn accept(
        mut link: L,
        workspace_id: &WorkspaceId,
        profile: &DeviceProfile,
        authorize_peer: impl FnOnce(&HandshakePeer) -> io::Result<()>,
    ) -> io::Result<Self> {
        let remote: SecureHello = read_plain(&mut link, 64 * 1024)?;
        if remote.workspace_id != *workspace_id || remote.version != SECURE_SESSION_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "secure handshake workspace or version mismatch",
            ));
        }
        let secret = EphemeralSecret::random();
        let local = hello(workspace_id, profile, remote.purpose, &secret)?;
        write_plain(&mut link, &local)?;
        let transcript = transcript(&remote, &local)?;
        let remote_proof: SecureProof = read_plain(&mut link, 64 * 1024)?;
        verify_proof(&remote, &remote_proof, &transcript, true)?;
        let peer = peer_from(&remote);
        authorize_peer(&peer)?;
        write_plain(
            &mut link,
            &SecureProof {
                signature: profile.sign_primary(&proof_bytes(&transcript, false)),
            },
        )?;
        let keys = derive_keys(secret, remote.ephemeral_key, &transcript)?;
        Ok(Self::new(link, peer, keys.1, keys.0))
    }

    fn new(link: L, peer: HandshakePeer, send_key: [u8; 32], receive_key: [u8; 32]) -> Self {
        Self {
            link,
            peer,
            send_cipher: ChaCha20Poly1305::new((&send_key).into()),
            receive_cipher: ChaCha20Poly1305::new((&receive_key).into()),
            send_counter: 0,
            receive_counter: 0,
            max_frame_bytes: DEFAULT_MAX_SECURE_FRAME_BYTES,
        }
    }

    pub fn peer(&self) -> &HandshakePeer {
        &self.peer
    }

    pub fn remote_endpoint(&self) -> io::Result<ConnectionHint> {
        self.link.remote_endpoint()
    }

    pub fn set_timeouts(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.link.set_timeouts(timeout)
    }

    pub fn send(&mut self, plaintext: &[u8]) -> io::Result<()> {
        if plaintext.len() > self.max_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "secure frame exceeds configured limit",
            ));
        }
        let counter = self.send_counter;
        let nonce = counter_nonce(counter);
        let aad = frame_aad(counter);
        let encrypted = self
            .send_cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame encryption failed"))?;
        self.send_counter = counter
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "send nonce exhausted"))?;
        self.link.send_frame(&encrypted)
    }

    pub fn receive(&mut self) -> io::Result<Vec<u8>> {
        let encrypted = self.link.receive_frame(self.max_frame_bytes + 16)?;
        let counter = self.receive_counter;
        let nonce = counter_nonce(counter);
        let aad = frame_aad(counter);
        let plaintext = self
            .receive_cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: &encrypted,
                    aad: &aad,
                },
            )
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "secure frame authentication failed",
                )
            })?;
        self.receive_counter = counter
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "receive nonce exhausted"))?;
        Ok(plaintext)
    }

    pub fn send_value<T: Encode>(&mut self, value: &T) -> io::Result<()> {
        let bytes = bincode::encode_to_vec(value, bincode::config::standard())
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.send(&bytes)
    }

    pub fn receive_value<T: Decode<()>>(&mut self) -> io::Result<T> {
        let bytes = self.receive()?;
        let (value, consumed) = bincode::decode_from_slice(&bytes, bincode::config::standard())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        if consumed != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "secure frame contains trailing bytes",
            ));
        }
        Ok(value)
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.link.close()
    }
}

fn hello(
    workspace_id: &WorkspaceId,
    profile: &DeviceProfile,
    purpose: SessionPurpose,
    secret: &EphemeralSecret,
) -> io::Result<SecureHello> {
    let mut nonce = [0; 32];
    getrandom::fill(&mut nonce).map_err(|error| io::Error::other(error.to_string()))?;
    Ok(SecureHello {
        version: SECURE_SESSION_VERSION,
        workspace_id: workspace_id.clone(),
        device_id: profile.device_id(),
        signing_key: profile.primary_public_key(),
        ephemeral_key: EphemeralPublicKey::from(secret).to_bytes(),
        nonce,
        purpose,
    })
}

fn validate_scope(
    hello: &SecureHello,
    workspace_id: &WorkspaceId,
    purpose: SessionPurpose,
) -> io::Result<()> {
    if hello.version != SECURE_SESSION_VERSION
        || hello.workspace_id != *workspace_id
        || hello.purpose != purpose
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "secure handshake scope mismatch",
        ));
    }
    Ok(())
}

fn peer_from(hello: &SecureHello) -> HandshakePeer {
    HandshakePeer {
        device_id: hello.device_id,
        public_key: hello.signing_key,
        purpose: hello.purpose,
    }
}

fn transcript(initiator: &SecureHello, responder: &SecureHello) -> io::Result<[u8; 32]> {
    let bytes = bincode::encode_to_vec(
        (b"zendb-secure-session-v1".as_slice(), initiator, responder),
        bincode::config::standard(),
    )
    .map_err(|error| io::Error::other(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn proof_bytes(transcript: &[u8; 32], initiator: bool) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(33);
    bytes.extend_from_slice(transcript);
    bytes.push(u8::from(initiator));
    bytes
}

fn verify_proof(
    remote: &SecureHello,
    proof: &SecureProof,
    transcript: &[u8; 32],
    remote_is_initiator: bool,
) -> io::Result<()> {
    if !DeviceProfile::verify(
        remote.signing_key,
        &proof_bytes(transcript, remote_is_initiator),
        &proof.signature,
    ) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "secure handshake signature is invalid",
        ));
    }
    Ok(())
}

fn derive_keys(
    secret: EphemeralSecret,
    remote: [u8; 32],
    transcript: &[u8; 32],
) -> io::Result<([u8; 32], [u8; 32])> {
    let shared = secret.diffie_hellman(&EphemeralPublicKey::from(remote));
    let hkdf = Hkdf::<Sha256>::new(Some(transcript), shared.as_bytes());
    let mut keys = [0; 64];
    hkdf.expand(b"zendb-session-traffic-v1", &mut keys)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "session key derivation failed"))?;
    let mut initiator_send = [0; 32];
    let mut responder_send = [0; 32];
    initiator_send.copy_from_slice(&keys[..32]);
    responder_send.copy_from_slice(&keys[32..]);
    Ok((initiator_send, responder_send))
}

fn counter_nonce(counter: u64) -> Nonce {
    let mut bytes = [0; 12];
    bytes[4..].copy_from_slice(&counter.to_be_bytes());
    bytes.into()
}

fn frame_aad(counter: u64) -> [u8; 16] {
    let mut aad = [0; 16];
    aad[..8].copy_from_slice(b"ZENDB001");
    aad[8..].copy_from_slice(&counter.to_be_bytes());
    aad
}

fn write_plain<L: FramedLink, T: Encode>(link: &mut L, value: &T) -> io::Result<()> {
    let bytes = bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))?;
    link.send_frame(&bytes)
}

fn read_plain<L: FramedLink, T: Decode<()>>(link: &mut L, max: usize) -> io::Result<T> {
    let bytes = link.receive_frame(max)?;
    let (value, consumed) = bincode::decode_from_slice(&bytes, bincode::config::standard())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    if consumed != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "handshake message contains trailing bytes",
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, thread};

    use crate::{TcpLink, TcpLinkListener};

    fn profile(name: &str) -> Arc<DeviceProfile> {
        let path = std::env::temp_dir().join(format!(
            "zendb-secure-{name}-{}",
            DeviceId::generate().unwrap()
        ));
        Arc::new(DeviceProfile::create(&path).unwrap())
    }

    #[test]
    fn encrypted_session_authenticates_and_exchanges_messages() {
        let listener = TcpLinkListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr().unwrap();
        let workspace = WorkspaceId::generate().unwrap();
        let client = profile("client");
        let server = profile("server");
        let expected_client = client.device_id();
        let expected_server = server.device_id();
        let server_workspace = workspace.clone();
        let server_profile = Arc::clone(&server);
        let worker = thread::spawn(move || {
            let (link, _) = listener.accept().unwrap();
            let mut session =
                SecureSession::accept(link, &server_workspace, &server_profile, |peer| {
                    (peer.device_id == expected_client)
                        .then_some(())
                        .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "client"))
                })
                .unwrap();
            assert_eq!(session.receive().unwrap(), b"hello");
            session.send(b"world").unwrap();
        });

        let mut session = SecureSession::connect(
            TcpLink::connect(address).unwrap(),
            &workspace,
            &client,
            SessionPurpose::Replication,
            |peer| {
                (peer.device_id == expected_server)
                    .then_some(())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "server"))
            },
        )
        .unwrap();
        session.send(b"hello").unwrap();
        assert_eq!(session.receive().unwrap(), b"world");
        worker.join().unwrap();
    }
}
