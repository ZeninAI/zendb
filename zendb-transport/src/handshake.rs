use bincode::{Decode, Encode};
use zendb_identity::{PeerAuthContext, SignatureBytes, WorkspaceId};

use crate::ProtocolChannel;

/// First message exchanged before peer authentication is attempted.
///
/// This message is intentionally lightweight. It answers:
///
/// - what protocol version do I speak?
/// - which logical channels do I support?
/// - which workspace do I think I want to talk about?
///
/// It does *not* authenticate the peer yet.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct HandshakeHello {
    pub protocol_version: u16,
    pub supported_channels: Vec<ProtocolChannel>,
    pub workspace_hint: Option<WorkspaceId>,
}

/// Challenge sent by the accepting side before it trusts peer credentials.
///
/// The nonce is intended to prevent replay of stale peer-auth payloads.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct HandshakeChallenge {
    pub workspace_id: WorkspaceId,
    pub session_id: crate::SessionId,
    pub nonce: [u8; 32],
    pub requested_channels: Vec<ProtocolChannel>,
    pub policy_epoch: u64,
    /// `false` is permitted only for explicitly public/non-data channels such
    /// as presence. Sync and bootstrap must always require a credential.
    pub require_workspace_credential: bool,
}

/// Auth payload proving the peer identity and workspace/device authorization.
///
/// This is where the earlier auth split comes together:
///
/// - `auth.peer_identity` identifies the device/user
/// - `auth.workspace_credential` proves that device is authorized for the workspace
/// - `access_token` is optional and may be used when an online service-backed
///   validation path is available
///
/// In a fully offline peer-to-peer scenario, the durable workspace credential
/// is the main artifact that matters.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct HandshakeAuthenticate {
    pub auth: PeerAuthContext,
    pub challenge_signature: SignatureBytes,
    pub access_token: Option<String>,
}

/// Final acknowledgement that the logical session is established.
///
/// At this point both sides should agree on:
///
/// - the workspace in scope
/// - the logical channels that may be used on this session
/// - the lifetime of the current authenticated session, if bounded
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SessionEstablished {
    pub session_id: crate::SessionId,
    pub workspace_id: WorkspaceId,
    pub accepted_channels: Vec<ProtocolChannel>,
    pub expires_at_ms: Option<u64>,
    pub policy_epoch: u64,
}
