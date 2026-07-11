//! Shared declarative automation objects.
//!
//! These types describe desired operator state and the durable evidence used
//! to reconcile it. They are database objects, not server resources: a local
//! database may replicate them, evaluate them, and realize them on its own
//! device. Hosted schedulers, if added later, must use an external adapter.

use bincode::{Decode, Encode};

use crate::{
    Action, CapabilityId, CheckpointId, DeviceId, DeviceTrust, JobId, LeaseId, OperatorId,
    PrincipalId, ResourceSelector, Sensitivity, VersionVector, WorkspaceId,
};

/// Semantic execution class. The class constrains what the reconciler and
/// policy evaluator may allow; it is not merely an informational label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OperatorClass {
    /// Device-local indexes or caches. Outputs are local unless explicitly
    /// requested through a separate, policy-approved publication path.
    LocalIndexer,
    /// Deterministic derived data that is part of shared workspace truth.
    SharedMaterializer,
    /// Nondeterministic or external work, normally represented as jobs.
    ExternalRunner,
    /// Explicit user-triggered work rather than a standing worker.
    AssistantAction,
}

/// Desired lifecycle of an operator specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OperatorDesiredState {
    Enabled,
    Disabled,
    Suspended,
    Deleted,
}

/// Source understood by a device-local operator registry.
///
/// A source reference is deliberately not a network URL. Fetching source from
/// a registry or hosted service is an optional client adapter concern; the
/// database only stores the source identity and, when appropriate, the source
/// bytes in `config`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OperatorSource {
    Native { type_name: String, api_version: u32 },
    Rhai { source_hash: [u8; 32] },
}

/// Trigger policy for a standing operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OperatorTrigger {
    InputChanges,
    Timer,
    InputChangesAndTimer,
    Manual,
    JobCompletion,
}

/// Input selector stored in desired state. The engine may compile this into
/// its local `Subscription` matcher, but the control object remains portable.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorInput {
    pub table_pattern: String,
    pub include_existing: bool,
}

/// Placement strategy for a desired operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum PlacementMode {
    EveryEligibleDevice,
    Singleton,
    PinnedDevice,
    Manual,
}

/// Declarative placement constraints evaluated against local device facts.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PlacementPolicy {
    pub mode: PlacementMode,
    pub required_labels: Vec<String>,
    pub preferred_labels: Vec<String>,
    pub required_capabilities: Vec<CapabilityId>,
    pub minimum_trust: Option<DeviceTrust>,
    pub pinned_device_id: Option<DeviceId>,
    pub fallback_allowed: bool,
}

/// Permissions an operator asks to exercise. These are a request, never an
/// automatic grant; the workspace evaluator intersects them with the creator,
/// device, class, and current policy.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorPermissionRequest {
    pub actions: Vec<Action>,
    pub resources: Vec<ResourceSelector>,
    pub max_sensitivity: Sensitivity,
    pub capabilities: Vec<CapabilityId>,
    pub requires_online: bool,
    pub requires_approval: bool,
}

/// Approval requirement for creating or activating an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OperatorApprovalPolicy {
    None,
    WorkspaceMember,
    WorkspaceAdmin,
    Owner,
    OnlineAuthority,
}

/// Retry behavior for local worker failures and external jobs.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RetryPolicy {
    pub max_attempts: Option<u32>,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub retry_on_lost_lease: bool,
}

/// Declares where outputs may be written. This prevents an operator class from
/// silently turning a local cache or external result into shared truth.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorOutputPolicy {
    pub shared_resources: Vec<ResourceSelector>,
    pub local_state_names: Vec<String>,
    pub max_sensitivity: Sensitivity,
    pub publish_external_results: bool,
}

/// Canonical desired state for one operator.
///
/// `config` is the canonical bincode payload for the registered source. It is
/// kept separate from placement, permissions, and lifecycle so changing local
/// poll tuning cannot accidentally change authorization or ownership policy.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorSpec {
    pub workspace_id: WorkspaceId,
    pub operator_id: OperatorId,
    pub name: String,
    pub generation: u64,
    pub desired_state: OperatorDesiredState,
    pub class: OperatorClass,
    pub source: OperatorSource,
    pub config: Vec<u8>,
    pub inputs: Vec<OperatorInput>,
    pub trigger: OperatorTrigger,
    pub placement: PlacementPolicy,
    pub permissions: OperatorPermissionRequest,
    pub approval: OperatorApprovalPolicy,
    pub retry: RetryPolicy,
    pub outputs: OperatorOutputPolicy,
    pub parent_operator_id: Option<OperatorId>,
    pub created_by: PrincipalId,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// Durable condition explaining why a desired operator is or is not realized.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OperatorCondition {
    AwaitingInputs,
    AwaitingCapability,
    AwaitingApproval,
    AwaitingLease,
    Running,
    Suspended,
    Failed,
    Stopped,
}

/// Local observation of a desired operator. It is status, not desired state,
/// and may be recomputed after a device restart.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorObservation {
    pub operator_id: OperatorId,
    pub observed_generation: u64,
    pub condition: OperatorCondition,
    pub worker_id: Option<String>,
    pub holder_device_id: Option<DeviceId>,
    pub lease_epoch: Option<u64>,
    pub last_error: Option<String>,
    pub observed_at_ms: u64,
}

/// A capability advertised by a device-local host runner.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct CapabilityDescriptor {
    pub capability_id: CapabilityId,
    pub version: String,
    pub labels: Vec<String>,
    pub deterministic: bool,
    pub may_have_external_side_effects: bool,
}

/// Device facts used for placement and local authorization. The summary is a
/// hint until authenticated device membership and policy are checked.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceCapabilitySummary {
    pub device_id: DeviceId,
    pub labels: Vec<String>,
    pub capabilities: Vec<CapabilityDescriptor>,
    pub trust: DeviceTrust,
    pub observed_at_ms: u64,
}

/// Identity of one singleton lease slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct OperatorLeaseKey {
    pub operator_id: OperatorId,
    pub shard_id: String,
}

/// Fenced ownership record for a singleton or sharded shared operator.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorLease {
    pub lease_id: LeaseId,
    pub key: OperatorLeaseKey,
    pub holder_device_id: DeviceId,
    pub epoch: u64,
    pub fencing_token: u64,
    pub policy_epoch: u64,
    pub issued_at_ms: u64,
    pub renewed_at_ms: u64,
    pub expires_at_ms: u64,
}

/// Durable progress from which a replacement shared worker can resume.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub operator_id: OperatorId,
    pub shard_id: String,
    pub generation: u64,
    pub lease_epoch: u64,
    pub input_version: VersionVector,
    pub state_hash: [u8; 32],
    pub created_at_ms: u64,
}

/// Durable job lifecycle. Jobs are the boundary for nondeterministic or
/// external effects; ordinary materializers should remain deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum JobStatus {
    Pending,
    Claimed,
    Running,
    Succeeded,
    RetryableFailure,
    TerminalFailure,
    Cancelled,
}

/// A reference to input data without copying sensitive payloads into the job
/// control record.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct JobInputRef {
    pub resource: ResourceSelector,
    pub content_hash: [u8; 32],
    pub sensitivity: Sensitivity,
}

/// Durable unit of external or explicitly asynchronous work.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorJob {
    pub job_id: JobId,
    pub workspace_id: WorkspaceId,
    pub operator_id: OperatorId,
    pub input: JobInputRef,
    pub requested_capability: CapabilityId,
    pub idempotency_key: String,
    pub status: JobStatus,
    pub claimed_by: Option<DeviceId>,
    pub claim_epoch: u64,
    pub claim_expires_at_ms: Option<u64>,
    pub attempt: u32,
    pub last_error: Option<String>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub completed_at_ms: Option<u64>,
}

/// Durable result metadata. The payload may live in a policy-controlled table
/// or local store; the job record retains the integrity hash and classification.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OperatorJobResult {
    pub job_id: JobId,
    pub result_hash: [u8; 32],
    pub output_resource: Option<ResourceSelector>,
    pub sensitivity: Sensitivity,
    pub produced_by: DeviceId,
    pub claim_epoch: u64,
    pub created_at_ms: u64,
}
