//! Declarative, device-hosted operator control records.

use std::collections::BTreeSet;

use bincode::{Decode, Encode};

use crate::{CapabilityId, DeviceId, Hlc, OperatorId, WorkspaceAction};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OperatorDesiredState {
    Running,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OperatorEffect {
    LocalOnly,
    WriteShared,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OperatorSource {
    Native { type_name: String, api_version: u32 },
    Rhai { source_hash: [u8; 32] },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorInput {
    pub table_pattern: String,
    pub include_existing: bool,
}

/// Desired state replicated inside the Workspace. The containing control Cell
/// supplies the namespace, so this record deliberately has no workspace_id.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorSpec {
    pub operator_id: OperatorId,
    pub name: String,
    pub desired_state: OperatorDesiredState,
    pub source: OperatorSource,
    pub config: Vec<u8>,
    pub inputs: Vec<OperatorInput>,
    pub required_capabilities: BTreeSet<CapabilityId>,
    pub effect: OperatorEffect,
    /// The derived-output namespace. Every shared output is additionally
    /// attributed to the winning lease fence.
    pub output_table: Option<String>,
}

impl OperatorSpec {
    pub const fn required_action(&self) -> WorkspaceAction {
        match self.effect {
            OperatorEffect::LocalOnly => WorkspaceAction::Read,
            OperatorEffect::WriteShared => WorkspaceAction::Contribute,
        }
    }
}

/// The resolved lease record for a singleton declarative operator. This is an
/// advisory partition-tolerant handoff record, not a distributed lock.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorLease {
    pub operator_id: OperatorId,
    pub holder_device_id: DeviceId,
    pub fence: Hlc,
    pub expires_at: Hlc,
}

impl OperatorLease {
    pub fn is_held_by(&self, device_id: DeviceId, fence: Hlc) -> bool {
        self.holder_device_id == device_id && self.fence == fence
    }

    pub fn is_expired_at(&self, now: Hlc) -> bool {
        self.expires_at <= now
    }
}

/// Attribution carried by shared derived output. Consumers expose the output
/// whose fence matches the resolved lease; stale output remains compactable
/// data rather than a write to arbitrary user-owned rows.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorOutputFence {
    pub operator_id: OperatorId,
    pub fence: Hlc,
}

/// Locally observed host facts used by a reconciler. They are not replicated
/// authority records; the Device record is authoritative for capability labels
/// and roles.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceCapabilitySummary {
    pub device_id: DeviceId,
    pub capabilities: BTreeSet<CapabilityId>,
    pub can_contribute: bool,
}

impl DeviceCapabilitySummary {
    pub fn supports(&self, spec: &OperatorSpec) -> bool {
        spec.required_capabilities.is_subset(&self.capabilities)
            && (spec.effect != OperatorEffect::WriteShared || self.can_contribute)
    }
}
