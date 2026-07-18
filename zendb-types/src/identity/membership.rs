use std::collections::BTreeSet;

use bincode::{Decode, Encode};

use crate::{
    CapabilityId, CellCodec, CellCodecError, ContiguousFrontier, Hlc, WorkspaceAction,
    WorkspaceRole,
};

/// Fixed Ed25519 public signing key. Supporting one algorithm keeps event and
/// admission verification canonical; a protocol version can introduce another
/// algorithm later if it is genuinely needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, CellCodec)]
pub struct DevicePublicKey(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SignatureBytes(pub Vec<u8>);

impl SignatureBytes {
    pub fn empty() -> Self {
        Self(Vec::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode, CellCodec)]
pub enum DeviceKeyPhase {
    Stable,
    Staged,
}

/// Atomically replaced two-slot signing-key state for a stable DeviceId.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, CellCodec)]
pub struct DeviceKeyRing {
    pub primary_key: DevicePublicKey,
    pub secondary_key: Option<DevicePublicKey>,
    pub primary_from_seq: u64,
    pub phase: DeviceKeyPhase,
}

impl DeviceKeyRing {
    pub fn stage(&self, candidate: DevicePublicKey) -> Option<Self> {
        (self.phase == DeviceKeyPhase::Stable).then(|| Self {
            primary_key: self.primary_key,
            secondary_key: Some(candidate),
            primary_from_seq: self.primary_from_seq,
            phase: DeviceKeyPhase::Staged,
        })
    }

    pub fn promote(&self, origin_seq: u64) -> Option<Self> {
        (self.phase == DeviceKeyPhase::Staged && origin_seq >= self.primary_from_seq).then(|| {
            Self {
                primary_key: self
                    .secondary_key
                    .expect("staged ring requires secondary key"),
                secondary_key: Some(self.primary_key),
                primary_from_seq: origin_seq,
                phase: DeviceKeyPhase::Stable,
            }
        })
    }

    pub fn verification_key(&self, origin_seq: u64) -> Option<&DevicePublicKey> {
        if origin_seq >= self.primary_from_seq {
            Some(&self.primary_key)
        } else if self.phase == DeviceKeyPhase::Stable {
            self.secondary_key.as_ref()
        } else {
            None
        }
    }
}

/// Durable device membership and its fixed Workspace authorization state.
/// Its containing Device Cell supplies membership liveness; no status field is
/// needed here.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, CellCodec)]
pub struct DeviceRecord {
    pub name: String,
    pub key_ring: DeviceKeyRing,
    pub roles: BTreeSet<WorkspaceRole>,
    #[cell(crdt = "or_set")]
    pub capabilities: BTreeSet<CapabilityId>,
    pub replication_frontier: ContiguousFrontier,
}

impl DeviceRecord {
    pub fn allows(&self, action: WorkspaceAction) -> bool {
        action == WorkspaceAction::Read || self.roles.iter().any(|role| role.allows(action))
    }
}

/// Compatibility name for callers that previously received membership-specific
/// decoding errors.
pub type DeviceRecordError = CellCodecError;

/// Replicated verifier record for bearer admission. The private credential is
/// intentionally absent: it is carried only by the QR code or link.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, CellCodec)]
pub struct EnrollmentTicket {
    pub verifier_key: DevicePublicKey,
    pub expires_at: Hlc,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeviceId, Value};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, CellCodec)]
    enum TestKind {
        First,
        Second,
    }

    #[derive(Debug, PartialEq, Eq, CellCodec)]
    struct AutomaticRecord {
        a: String,
        nested: TestKind,
        values: BTreeSet<TestKind>,
        optional: Option<String>,
    }

    #[derive(Debug, PartialEq, Eq, CellCodec)]
    struct ExplicitTimestampRecord {
        #[cell(codec = "crate::crdt::values::TimestampCodec")]
        observed_at: u64,
    }

    fn key(value: u8) -> DevicePublicKey {
        DevicePublicKey([value; 32])
    }

    #[test]
    fn key_ring_promotes_only_a_staged_secondary() {
        let ring = DeviceKeyRing {
            primary_key: key(1),
            secondary_key: None,
            primary_from_seq: 1,
            phase: DeviceKeyPhase::Stable,
        };
        assert!(ring.promote(2).is_none());
        let staged = ring.stage(key(2)).unwrap();
        assert_eq!(staged.verification_key(2), Some(&key(1)));
        let promoted = staged.promote(5).unwrap();
        assert_eq!(promoted.primary_key, key(2));
        assert_eq!(promoted.verification_key(4), Some(&key(1)));
        assert_eq!(promoted.verification_key(5), Some(&key(2)));
    }

    #[test]
    fn reader_is_implicit_but_write_roles_are_not() {
        let record = DeviceRecord {
            name: "device".into(),
            key_ring: DeviceKeyRing {
                primary_key: key(1),
                secondary_key: None,
                primary_from_seq: 1,
                phase: DeviceKeyPhase::Stable,
            },
            roles: BTreeSet::new(),
            capabilities: BTreeSet::new(),
            replication_frontier: ContiguousFrontier::default(),
        };
        assert!(record.allows(WorkspaceAction::Read));
        assert!(!record.allows(WorkspaceAction::Contribute));
        assert_eq!(DeviceId::ZERO, DeviceId::from_bytes([0; 16]));
    }

    #[test]
    fn cell_codecs_round_trip_membership_records() {
        let hlc = Hlc::with_device_id(10, 0, DeviceId::from_bytes([1; 16])).unwrap();
        let record = DeviceRecord {
            name: "device".into(),
            key_ring: DeviceKeyRing {
                primary_key: key(1),
                secondary_key: None,
                primary_from_seq: 1,
                phase: DeviceKeyPhase::Stable,
            },
            roles: BTreeSet::from([WorkspaceRole::Contributor]),
            capabilities: BTreeSet::from([CapabilityId("publish".into())]),
            replication_frontier: ContiguousFrontier::from_applied([(
                DeviceId::from_bytes([2; 16]),
                4,
            )]),
        };

        let cell = crate::CellCodec::to_cell(&record, hlc);
        let Value::Record(encoded) = cell.value.as_ref().expect("encoded device is live") else {
            panic!("encoded device is a record");
        };
        let Value::Record(key_ring) = encoded
            .get("key_ring")
            .and_then(|cell| cell.value.as_ref())
            .expect("encoded key ring is live")
        else {
            panic!("encoded key ring is a record");
        };
        assert!(!key_ring.contains("secondary_key"));
        assert!(matches!(
            encoded.get("roles").and_then(|cell| cell.value.as_ref()),
            Some(Value::Set(_))
        ));
        assert!(matches!(
            encoded
                .get("capabilities")
                .and_then(|cell| cell.value.as_ref()),
            Some(Value::OrSet(_))
        ));
        assert_eq!(
            <DeviceRecord as crate::CellCodec>::from_cell(&cell).unwrap(),
            record,
        );

        let ticket = EnrollmentTicket {
            verifier_key: key(3),
            expires_at: Hlc::with_device_id(20, 1, DeviceId::from_bytes([4; 16])).unwrap(),
        };
        let cell = ticket.to_cell(hlc);
        assert_eq!(EnrollmentTicket::from_cell(&cell).unwrap(), ticket);
    }

    #[test]
    fn derived_records_select_codecs_from_field_types() {
        let hlc = Hlc::with_device_id(10, 0, DeviceId::from_bytes([1; 16])).unwrap();
        let value = AutomaticRecord {
            a: "value".into(),
            nested: TestKind::First,
            values: BTreeSet::from([TestKind::Second]),
            optional: None,
        };

        let cell = value.to_cell(hlc);
        let Value::Record(record) = cell.value.as_ref().expect("record is live") else {
            panic!("derived struct must encode as a record");
        };
        assert!(!record.contains("optional"));
        assert_eq!(AutomaticRecord::from_cell(&cell).unwrap(), value);

        let mut tombstoned = AutomaticRecord {
            optional: Some("removed".into()),
            ..value
        }
        .to_cell(hlc);
        let Value::Record(record) = tombstoned.value.as_mut().expect("record is live") else {
            panic!("derived struct must encode as a record");
        };
        record
            .get_mut("optional")
            .expect("optional field exists")
            .value = None;
        assert_eq!(
            AutomaticRecord::from_cell(&tombstoned).unwrap().optional,
            None
        );
    }

    #[test]
    fn derived_records_can_select_an_alternate_codec() {
        let hlc = Hlc::with_device_id(10, 0, DeviceId::from_bytes([1; 16])).unwrap();
        let value = ExplicitTimestampRecord { observed_at: 42 };

        let cell = value.to_cell(hlc);
        let Value::Record(record) = cell.value.as_ref().expect("record is live") else {
            panic!("derived struct must encode as a record");
        };
        assert!(matches!(
            record
                .get("observed_at")
                .and_then(|cell| cell.value.as_ref()),
            Some(Value::Timestamp(42))
        ));
        assert_eq!(ExplicitTimestampRecord::from_cell(&cell).unwrap(), value);
    }
}
