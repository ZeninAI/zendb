//! Client-side operator control-plane interfaces.
//!
//! The workspace owns these boundaries. A local `Workspace` persists desired
//! operator objects, observes its own runtime, and reconciles workers on the
//! current device. It does not assume a Kubernetes server or central lease
//! authority exists.

use std::{fmt, io};

use zendb_types::{
    AuthorizationDecision, CapabilityDescriptor, CapabilityInvocation, CapabilityResult,
    DeviceCapabilitySummary, OperatorId, OperatorLease, OperatorObservation, OperatorSpec,
};

use super::OperatorPhase;

/// Semantic failures produced by the operator control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorControlError {
    Io(String),
    Unsupported(&'static str),
    PolicyDenied,
    LeaseConflict,
    StaleGeneration,
    UnsupportedSource,
}

impl fmt::Display for OperatorControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => formatter.write_str(message),
            Self::Unsupported(operation) => write!(formatter, "{operation} is not implemented"),
            Self::PolicyDenied => formatter.write_str("operator policy denied the operation"),
            Self::LeaseConflict => formatter.write_str("operator lease conflict"),
            Self::StaleGeneration => formatter.write_str("operator generation is stale"),
            Self::UnsupportedSource => formatter.write_str("operator source is unsupported"),
        }
    }
}

impl std::error::Error for OperatorControlError {}

impl From<io::Error> for OperatorControlError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

pub type OperatorControlResult<T> = Result<T, OperatorControlError>;

/// Consistency guarantee offered by a lease implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseConsistency {
    /// Split-brain ownership is possible during partitions; outputs must be
    /// idempotent or CRDT-compatible.
    Advisory,
    /// Epoch acquisition is serialized by a quorum or external authority.
    Authoritative,
}

/// Inputs a reconciler reads at one point in time.
#[derive(Debug)]
pub struct ReconcileSnapshot {
    pub specs: Vec<OperatorSpec>,
    pub observations: Vec<OperatorObservation>,
    pub admissions: Vec<OperatorAdmission>,
    pub device: DeviceCapabilitySummary,
    pub leases: Vec<OperatorLease>,
    pub policy_epoch: u64,
    pub now_ms: u64,
}

/// Policy result for one desired operator generation.
#[derive(Debug)]
pub struct OperatorAdmission {
    pub operator_id: OperatorId,
    pub generation: u64,
    pub decision: AuthorizationDecision,
}

/// Planned local actions. Observation is a result written after an action; it
/// is intentionally not an action variant.
#[derive(Debug)]
pub enum ReconcileAction {
    Start {
        spec: OperatorSpec,
        lease: Option<OperatorLease>,
    },
    Stop {
        operator_id: OperatorId,
        reason: OperatorPhase,
    },
    RenewLease(OperatorLease),
    ReleaseLease(OperatorLease),
}

/// Pure default planning entrypoint. A future implementation may produce a
/// plan from the snapshot; callers receive an explicit error until then.
pub fn plan_reconciliation(
    _snapshot: &ReconcileSnapshot,
) -> OperatorControlResult<Vec<ReconcileAction>> {
    Err(OperatorControlError::Unsupported("operator reconciliation"))
}

/// Local capability host. Descriptors and execution belong together.
pub trait CapabilityHost: Send + Sync + 'static {
    fn descriptors(&self) -> OperatorControlResult<Vec<CapabilityDescriptor>>;
    fn execute(&self, invocation: CapabilityInvocation) -> OperatorControlResult<CapabilityResult>;
}
