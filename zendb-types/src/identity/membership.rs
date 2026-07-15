use std::collections::BTreeSet;

use bincode::{Decode, Encode};

use crate::crdt::values::{OrSet, OrSetOp, Record, Set, SetOp};
use crate::{
    CapabilityId, Cell, ContiguousFrontier, Hlc, PrimaryKey, Type, Value, WorkspaceAction,
    WorkspaceRole,
};

/// Fixed Ed25519 public signing key. Supporting one algorithm keeps event and
/// admission verification canonical; a protocol version can introduce another
/// algorithm later if it is genuinely needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct DevicePublicKey(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SignatureBytes(pub Vec<u8>);

impl SignatureBytes {
    pub fn empty() -> Self {
        Self(Vec::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum DeviceKeyPhase {
    Stable,
    Staged,
}

/// Atomically replaced two-slot signing-key state for a stable DeviceId.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceKeyRing {
    pub primary_key: DevicePublicKey,
    pub secondary_key: Option<DevicePublicKey>,
    pub primary_from_seq: u64,
    pub phase: DeviceKeyPhase,
}

impl DeviceKeyRing {
    pub fn stage(&self, candidate: DevicePublicKey) -> Option<Self> {
        (self.phase == DeviceKeyPhase::Stable).then(|| Self {
            primary_key: self.primary_key.clone(),
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
                    .clone()
                    .expect("staged ring requires secondary key"),
                secondary_key: Some(self.primary_key.clone()),
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
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceRecord {
    pub name: String,
    pub key_ring: DeviceKeyRing,
    pub roles: BTreeSet<WorkspaceRole>,
    pub capabilities: BTreeSet<CapabilityId>,
    pub replication_frontier: ContiguousFrontier,
}

impl DeviceRecord {
    pub fn allows(&self, action: WorkspaceAction) -> bool {
        action == WorkspaceAction::Read || self.roles.iter().any(|role| role.allows(action))
    }

    /// Encode the resolved view as the canonical nested CRDT Device Cell used
    /// in Workspace control state.
    pub fn to_cell(&self, hlc: Hlc) -> Cell {
        let mut roles = Set::default();
        for role in &self.roles {
            Type::apply(
                &mut roles,
                &SetOp::Add {
                    key: PrimaryKey::String(role.as_str().into()),
                },
                hlc,
            )
            .expect("Set operation is infallible");
        }

        let mut capabilities = OrSet::default();
        for capability in &self.capabilities {
            Type::apply(
                &mut capabilities,
                &OrSetOp::Add {
                    key: PrimaryKey::String(capability.0.clone()),
                },
                hlc,
            )
            .expect("OR-Set operation is infallible");
        }

        let frontier = Record::from_fields(self.replication_frontier.entries().map(
            |(origin, sequence)| {
                (
                    origin.to_string(),
                    live_cell(Value::Int((*sequence).try_into().unwrap_or(i64::MAX)), hlc),
                )
            },
        ));

        let record = Record::from_fields([
            (
                "name".into(),
                live_cell(Value::String(self.name.clone()), hlc),
            ),
            ("key_ring".into(), self.key_ring.to_cell(hlc)),
            ("roles".into(), live_cell(Value::Set(roles), hlc)),
            (
                "capabilities".into(),
                live_cell(Value::OrSet(capabilities), hlc),
            ),
            (
                "replication_frontier".into(),
                live_cell(Value::Record(frontier), hlc),
            ),
        ]);
        live_cell(Value::Record(record), hlc)
    }

    pub fn from_cell(cell: &Cell) -> Result<Self, DeviceRecordError> {
        let record = as_record(cell, "device")?;
        let name = match live_value(record, "name")? {
            Value::String(name) => name.clone(),
            _ => return Err(DeviceRecordError::InvalidField("name")),
        };
        let key_ring = DeviceKeyRing::from_cell(field(record, "key_ring")?)?;
        let roles = match live_value(record, "roles")? {
            Value::Set(set) => set
                .keys()
                .map(|key| match key {
                    PrimaryKey::String(value) => WorkspaceRole::parse(value)
                        .ok_or(DeviceRecordError::InvalidRole(value.clone())),
                    _ => Err(DeviceRecordError::InvalidField("roles")),
                })
                .collect::<Result<_, _>>()?,
            _ => return Err(DeviceRecordError::InvalidField("roles")),
        };
        let capabilities = match live_value(record, "capabilities")? {
            Value::OrSet(set) => set
                .keys()
                .map(|key| match key {
                    PrimaryKey::String(value) => Ok(CapabilityId(value.clone())),
                    _ => Err(DeviceRecordError::InvalidField("capabilities")),
                })
                .collect::<Result<_, _>>()?,
            _ => return Err(DeviceRecordError::InvalidField("capabilities")),
        };
        let frontier_record = match live_value(record, "replication_frontier")? {
            Value::Record(record) => record,
            _ => return Err(DeviceRecordError::InvalidField("replication_frontier")),
        };
        let mut frontier = Vec::new();
        for (origin, cell) in frontier_record.fields() {
            let origin = parse_device_id(origin)?;
            let sequence = match cell.value.as_ref() {
                Some(Value::Int(value)) if *value >= 0 => *value as u64,
                _ => return Err(DeviceRecordError::InvalidField("replication_frontier")),
            };
            frontier.push((origin, sequence));
        }
        Ok(Self {
            name,
            key_ring,
            roles,
            capabilities,
            replication_frontier: ContiguousFrontier::from_applied(frontier),
        })
    }
}

impl DeviceKeyRing {
    pub fn to_cell(&self, hlc: Hlc) -> Cell {
        let mut fields = vec![
            (
                "primary_key".into(),
                live_cell(Value::Blob(self.primary_key.0.to_vec()), hlc),
            ),
            (
                "primary_from_seq".into(),
                live_cell(
                    Value::Int(self.primary_from_seq.try_into().unwrap_or(i64::MAX)),
                    hlc,
                ),
            ),
            (
                "phase".into(),
                live_cell(Value::String(self.phase.as_str().into()), hlc),
            ),
        ];
        if let Some(secondary) = self.secondary_key {
            fields.push((
                "secondary_key".into(),
                live_cell(Value::Blob(secondary.0.to_vec()), hlc),
            ));
        }
        live_cell(Value::Record(Record::from_fields(fields)), hlc)
    }

    pub fn from_cell(cell: &Cell) -> Result<Self, DeviceRecordError> {
        let record = as_record(cell, "key_ring")?;
        let primary_key = parse_public_key(live_value(record, "primary_key")?)?;
        let secondary_key = record
            .get("secondary_key")
            .filter(|cell| !cell.is_tombstone())
            .map(|cell| {
                cell.value
                    .as_ref()
                    .ok_or(DeviceRecordError::InvalidField("secondary_key"))
                    .and_then(parse_public_key)
            })
            .transpose()?;
        let primary_from_seq = match live_value(record, "primary_from_seq")? {
            Value::Int(value) if *value > 0 => *value as u64,
            _ => return Err(DeviceRecordError::InvalidField("primary_from_seq")),
        };
        let phase = match live_value(record, "phase")? {
            Value::String(value) => {
                DeviceKeyPhase::parse(value).ok_or(DeviceRecordError::InvalidField("phase"))?
            }
            _ => return Err(DeviceRecordError::InvalidField("phase")),
        };
        Ok(Self {
            primary_key,
            secondary_key,
            primary_from_seq,
            phase,
        })
    }
}

impl WorkspaceRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Contributor => "contributor",
            Self::Dispatcher => "dispatcher",
            Self::Manager => "manager",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "contributor" => Some(Self::Contributor),
            "dispatcher" => Some(Self::Dispatcher),
            "manager" => Some(Self::Manager),
            _ => None,
        }
    }
}

impl DeviceKeyPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Staged => "staged",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "stable" => Some(Self::Stable),
            "staged" => Some(Self::Staged),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceRecordError {
    MissingField(&'static str),
    InvalidField(&'static str),
    InvalidRole(String),
    InvalidDeviceId(String),
}

impl std::fmt::Display for DeviceRecordError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field) => write!(formatter, "missing Device field {field}"),
            Self::InvalidField(field) => write!(formatter, "invalid Device field {field}"),
            Self::InvalidRole(role) => write!(formatter, "invalid Workspace role {role}"),
            Self::InvalidDeviceId(id) => write!(formatter, "invalid DeviceId {id}"),
        }
    }
}

impl std::error::Error for DeviceRecordError {}

fn live_cell(value: Value, hlc: Hlc) -> Cell {
    Cell {
        value: Some(value),
        hlc,
        sync: None,
    }
}

fn as_record<'a>(
    cell: &'a Cell,
    field_name: &'static str,
) -> Result<&'a Record, DeviceRecordError> {
    match cell.value.as_ref() {
        Some(Value::Record(record)) => Ok(record),
        _ => Err(DeviceRecordError::InvalidField(field_name)),
    }
}

fn field<'a>(record: &'a Record, name: &'static str) -> Result<&'a Cell, DeviceRecordError> {
    record
        .get(name)
        .ok_or(DeviceRecordError::MissingField(name))
}

fn live_value<'a>(record: &'a Record, name: &'static str) -> Result<&'a Value, DeviceRecordError> {
    field(record, name)?
        .value
        .as_ref()
        .ok_or(DeviceRecordError::InvalidField(name))
}

fn parse_public_key(value: &Value) -> Result<DevicePublicKey, DeviceRecordError> {
    let Value::Blob(bytes) = value else {
        return Err(DeviceRecordError::InvalidField("public_key"));
    };
    let bytes: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| DeviceRecordError::InvalidField("public_key"))?;
    Ok(DevicePublicKey(bytes))
}

fn parse_device_id(value: &str) -> Result<crate::DeviceId, DeviceRecordError> {
    if value.len() != 32 {
        return Err(DeviceRecordError::InvalidDeviceId(value.into()));
    }
    let mut bytes = [0; 16];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk)
            .map_err(|_| DeviceRecordError::InvalidDeviceId(value.into()))?;
        bytes[index] = u8::from_str_radix(text, 16)
            .map_err(|_| DeviceRecordError::InvalidDeviceId(value.into()))?;
    }
    Ok(crate::DeviceId::from_bytes(bytes))
}

/// Replicated verifier record for bearer admission. The private credential is
/// intentionally absent: it is carried only by the QR code or link.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct EnrollmentTicket {
    pub verifier_key: DevicePublicKey,
    pub expires_at: Hlc,
}

impl EnrollmentTicket {
    pub fn to_cell(&self, hlc: Hlc) -> Cell {
        live_cell(
            Value::Record(Record::from_fields([
                (
                    "verifier_key".into(),
                    live_cell(Value::Blob(self.verifier_key.0.to_vec()), hlc),
                ),
                (
                    "expires_at".into(),
                    live_cell(Value::Blob(self.expires_at.as_bytes().to_vec()), hlc),
                ),
            ])),
            hlc,
        )
    }

    pub fn from_cell(cell: &Cell) -> Result<Self, DeviceRecordError> {
        let record = as_record(cell, "enrollment_ticket")?;
        let verifier_key = parse_public_key(live_value(record, "verifier_key")?)?;
        let Value::Blob(expires_at) = live_value(record, "expires_at")? else {
            return Err(DeviceRecordError::InvalidField("expires_at"));
        };
        let bytes: [u8; 24] = expires_at
            .as_slice()
            .try_into()
            .map_err(|_| DeviceRecordError::InvalidField("expires_at"))?;
        Ok(Self {
            verifier_key,
            expires_at: Hlc::from_bytes(bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeviceId;

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
}
