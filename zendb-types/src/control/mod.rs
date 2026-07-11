//! Shared database control-plane types.
//!
//! These types are intentionally free of storage, networking, and cryptographic
//! implementation details. The engine owns their persistence and the identity
//! crate owns credential verification.

pub mod authorization;
pub mod capability;
pub mod operator;

pub use authorization::{
    Action, AuthorizationContext, AuthorizationDecision, AuthorizationEvaluator, DecisionCode,
    DecisionEffect, Obligation, PolicyRule, ResourceId, ResourceSelector, RuleEffect, Sensitivity,
};
pub use capability::{CapabilityInvocation, CapabilityResult};
pub use operator::{
    CapabilityDescriptor, DeviceCapabilitySummary, JobInputRef, JobStatus, OperatorApprovalPolicy,
    OperatorCheckpoint, OperatorClass, OperatorCondition, OperatorDesiredState, OperatorInput,
    OperatorJob, OperatorJobResult, OperatorLease, OperatorLeaseKey, OperatorObservation,
    OperatorOutputPolicy, OperatorPermissionRequest, OperatorSource, OperatorSpec, OperatorTrigger,
    PlacementMode, PlacementPolicy, RetryPolicy,
};
