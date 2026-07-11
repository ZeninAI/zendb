//! Shared replication identity types.

use bincode::{Decode, Encode};
use std::collections::BTreeMap;

use crate::{DeviceId, Event, GrantId, KeyId, PrincipalId, Signature, UserId, WorkspaceId};

/// Stable identity used for deduplication and anti-entropy ranges. It is not a
/// global ordering or a causal timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct EventIdentity {
    pub origin_device_id: DeviceId,
    pub origin_seq: u64,
}

/// Per-origin progress used for anti-entropy. This is a completeness summary,
/// not a causal total order and not proof that a partial replica lacks data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct VersionVector {
    pub seen: BTreeMap<DeviceId, u64>,
}

impl VersionVector {
    pub fn max_seen(&self, device_id: &DeviceId) -> u64 {
        self.seen.get(device_id).copied().unwrap_or(0)
    }

    pub fn observe(&mut self, device_id: DeviceId, origin_seq: u64) {
        let entry = self.seen.entry(device_id).or_default();
        if origin_seq > *entry {
            *entry = origin_seq;
        }
    }
}

/// Metadata attached to an event when it enters the shared journal.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SyncEnvelope {
    /// Binds the record to one workspace before any table-level processing.
    pub workspace_id: WorkspaceId,
    pub event_id: EventIdentity,
    /// A device signature alone does not identify the user, guest, or operator
    /// on whose behalf the mutation was requested.
    pub author_principal: PrincipalId,
    pub author_user_id: Option<UserId>,
    pub policy_epoch: u64,
    pub grant_id: Option<GrantId>,
    /// Hash of the canonical event bytes, checked before CRDT application.
    pub payload_hash: [u8; 32],
    pub previous_origin_hash: Option<[u8; 32]>,
    /// Key ID used by the origin device to sign this envelope. Credential
    /// issuer keys belong to the credential, not to the event signature.
    pub origin_key_id: KeyId,
    pub signature: Signature,
}

/// Replication form of an event. Local table topics remain separate.
#[derive(Debug, Clone, Encode, Decode)]
pub struct ReplicatedEvent {
    pub envelope: SyncEnvelope,
    pub event: Event,
}
