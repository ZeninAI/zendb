//! Shared replication identity types.

use bincode::{Decode, Encode};
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CapabilityId, DeviceId, DevicePublicKey, EnrollmentTicketId, Event, Hlc, Signature,
    SignatureBytes, WorkspaceId,
};

/// Public evidence attached only to an exceptional ticket-based Device
/// admission event. It lets every replica validate the admission without the
/// private QR credential.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct TicketAdmissionEvidence {
    pub ticket_id: EnrollmentTicketId,
    pub candidate_device_id: DeviceId,
    pub candidate_public_key: DevicePublicKey,
    pub requested_name: String,
    pub capabilities: BTreeSet<CapabilityId>,
    pub ticket_signature: SignatureBytes,
}

/// Stable identity used for deduplication and anti-entropy ranges. It is not a
/// global ordering or a causal timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventIdentity {
    pub origin_device_id: DeviceId,
    pub origin_seq: u64,
}

/// Per-origin maximum-observed progress used for anti-entropy hints. This is
/// not a contiguous-completeness proof, a causal total order, or a compaction
/// watermark. ADR 003 defines the required durable contiguous frontier.
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

/// Durable per-origin contiguous receipt proof. Unlike VersionVector, a value
/// of `n` proves that every event `1..=n` was applied.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct ContiguousFrontier {
    applied_through: BTreeMap<DeviceId, u64>,
    pending: BTreeMap<DeviceId, std::collections::BTreeSet<u64>>,
}

impl ContiguousFrontier {
    pub fn from_applied(entries: impl IntoIterator<Item = (DeviceId, u64)>) -> Self {
        Self {
            applied_through: entries.into_iter().collect(),
            pending: BTreeMap::new(),
        }
    }

    pub fn applied_through(&self, origin: &DeviceId) -> u64 {
        self.applied_through.get(origin).copied().unwrap_or(0)
    }

    /// Observe a durably applied shared identity and advance through any
    /// previously received out-of-order suffix.
    pub fn observe(&mut self, identity: EventIdentity) -> bool {
        let origin = identity.origin_device_id;
        let current = self.applied_through(&origin);
        if identity.origin_seq <= current {
            return false;
        }
        self.pending
            .entry(origin)
            .or_default()
            .insert(identity.origin_seq);
        let mut next = current + 1;
        let pending = self
            .pending
            .get_mut(&origin)
            .expect("pending entry inserted");
        while pending.remove(&next) {
            next += 1;
        }
        let advanced = next - 1;
        if advanced > current {
            self.applied_through.insert(origin, advanced);
            true
        } else {
            false
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = (&DeviceId, &u64)> {
        self.applied_through.iter()
    }

    /// Raise one known-contiguous origin checkpoint. Callers are responsible
    /// for supplying only progress already made durable locally.
    pub fn advance_to(&mut self, origin: DeviceId, sequence: u64) -> bool {
        let current = self.applied_through(&origin);
        if sequence <= current {
            return false;
        }
        self.applied_through.insert(origin, sequence);
        if let Some(pending) = self.pending.get_mut(&origin) {
            pending.retain(|candidate| *candidate > sequence);
        }
        true
    }

    pub fn missing_from(&self, origin: DeviceId, remote_through: u64) -> Option<(u64, u64)> {
        let local = self.applied_through(&origin);
        (local < remote_through).then_some((local + 1, remote_through))
    }
}

/// Element-wise minimum across every admitted device's frontier checkpoints.
pub fn stable_frontier<'a>(
    devices: impl IntoIterator<Item = &'a ContiguousFrontier>,
) -> ContiguousFrontier {
    let devices: Vec<_> = devices.into_iter().collect();
    let mut origins = std::collections::BTreeSet::new();
    for frontier in &devices {
        origins.extend(frontier.applied_through.keys().copied());
    }
    let mut stable = ContiguousFrontier::default();
    for origin in origins {
        let minimum = devices
            .iter()
            .map(|frontier| frontier.applied_through(&origin))
            .min()
            .unwrap_or(0);
        if minimum > 0 {
            stable.applied_through.insert(origin, minimum);
        }
    }
    stable
}

/// Derive the scalar watermark consumed by `Type::compact` from a stable
/// frontier and journal lookup. Missing origin history yields no watermark.
pub fn compaction_watermark(
    stable: &ContiguousFrontier,
    event_hlc: impl Fn(DeviceId, u64) -> Option<Hlc>,
) -> Option<Hlc> {
    let mut watermark = None;
    for (origin, sequence) in stable.entries() {
        let hlc = event_hlc(*origin, *sequence)?;
        watermark = Some(watermark.map_or(hlc, |current: Hlc| current.min(hlc)));
    }
    watermark
}

/// Metadata attached to an event when it enters the shared journal. The
/// workspace identifier scopes transport and storage; device membership is
/// represented by the Workspace's Device Cells, not a credential in this
/// envelope.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SyncEnvelope {
    /// Binds the record to one workspace before any table-level processing.
    pub workspace_id: WorkspaceId,
    pub event_id: EventIdentity,
    /// Hash of the canonical event bytes, checked before CRDT application.
    pub payload_hash: [u8; 32],
    pub signature: Signature,
    pub ticket_admission: Option<TicketAdmissionEvidence>,
}

impl SyncEnvelope {
    pub fn matches_event(&self, event: &Event) -> bool {
        self.event_id.origin_device_id == event.hlc.device_id()
    }
}

/// Replication form of an event. Local table topics remain separate.
#[derive(Debug, Clone, Encode, Decode)]
pub struct ReplicatedEvent {
    pub envelope: SyncEnvelope,
    pub event: Event,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(device: u8, sequence: u64) -> EventIdentity {
        EventIdentity {
            origin_device_id: DeviceId::from_bytes([device; 16]),
            origin_seq: sequence,
        }
    }

    #[test]
    fn frontier_advances_only_after_a_gap_is_filled() {
        let mut frontier = ContiguousFrontier::default();
        assert!(!frontier.observe(id(1, 2)));
        assert_eq!(frontier.applied_through(&DeviceId::from_bytes([1; 16])), 0);
        assert!(frontier.observe(id(1, 1)));
        assert_eq!(frontier.applied_through(&DeviceId::from_bytes([1; 16])), 2);
    }

    #[test]
    fn stable_frontier_uses_the_slowest_admitted_device() {
        let mut first = ContiguousFrontier::default();
        let mut second = ContiguousFrontier::default();
        first.observe(id(1, 1));
        first.observe(id(1, 2));
        second.observe(id(1, 1));
        let stable = stable_frontier([&first, &second]);
        assert_eq!(stable.applied_through(&DeviceId::from_bytes([1; 16])), 1);
    }
}
