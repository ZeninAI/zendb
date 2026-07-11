//! Pure authorization vocabulary shared by the engine and protocol crates.

use bincode::{Decode, Encode};

use crate::{
    CapabilityId, DeviceId, DeviceTrust, OperatorId, Path, PrimaryKey, PrincipalId, Role, UserId,
    WorkspaceId,
};

/// An operation that can be authorized independently of its transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum Action {
    Read,
    Enumerate,
    Create,
    Update,
    Delete,
    Merge,
    Comment,
    Export,
    Import,
    JoinWorkspace,
    ReceiveBootstrap,
    SyncSend,
    SyncReceive,
    ManageMembership,
    ApproveDevice,
    RevokeDevice,
    ManagePolicy,
    CreateOperator,
    EnableOperator,
    DisableOperator,
    ExecuteOperator,
    CreateJob,
    ClaimJob,
    PublishJobResult,
    ReadAudit,
    ExportSnapshot,
    UseCapability,
    RelayTraffic,
}

/// The result of evaluating one policy request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum DecisionEffect {
    Allow,
    Deny,
    Indeterminate,
}

/// Rule effects intentionally exclude `Indeterminate`. An indeterminate
/// decision is produced by evaluation failure, never authored as a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum RuleEffect {
    Allow,
    Deny,
}

/// Coarse data classification used by policy rules. Applications may define
/// richer labels, but they must map them to this ordered boundary first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub enum Sensitivity {
    Public,
    Internal,
    Confidential,
    Restricted,
    Secret,
}

/// Stable machine-readable reason for an authorization result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum DecisionCode {
    Allowed,
    NoMatchingAllow,
    ExplicitDeny,
    InvalidPrincipal,
    InvalidWorkspace,
    DeviceRevoked,
    CredentialExpired,
    PolicyUnavailable,
    PolicyConflict,
    RequiresOnline,
    RequiresApproval,
    SensitivityExceeded,
    CapabilityMissing,
}

/// A resource address. `path` is empty when the request targets the row root.
#[derive(Debug, Clone, Encode, Decode)]
pub struct ResourceId {
    pub workspace_id: WorkspaceId,
    pub table_id: String,
    pub primary_key: Option<PrimaryKey>,
    pub path: Path,
}

/// A compact selector used by policy rules and sync scopes.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ResourceSelector {
    Any,
    Workspace(WorkspaceId),
    Table {
        workspace_id: WorkspaceId,
        table_id: String,
    },
    Row {
        workspace_id: WorkspaceId,
        table_id: String,
        primary_key: PrimaryKey,
    },
    SystemCatalog(String),
}

/// Additional enforcement returned with an allow decision.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum Obligation {
    RequireOnline,
    RequireFreshPolicy,
    RequireApproval,
    RedactSensitiveFields,
    LimitBytes(u64),
    LimitEvents(u64),
}

/// All inputs required by the pure policy evaluator.
#[derive(Debug, Clone, Encode, Decode)]
pub struct AuthorizationContext {
    pub principal: PrincipalId,
    pub acting_user_id: Option<UserId>,
    pub device_id: Option<DeviceId>,
    pub workspace_id: WorkspaceId,
    pub resource: ResourceId,
    pub action: Action,
    pub sensitivity: Sensitivity,
    pub device_trust: Option<DeviceTrust>,
    pub capabilities: Vec<CapabilityId>,
    pub operator_id: Option<OperatorId>,
    /// The policy snapshot the caller observed. The evaluator's own snapshot
    /// is authoritative; callers cannot select a more permissive epoch.
    pub observed_policy_epoch: u64,
    pub online: bool,
    pub now_ms: u64,
}

/// The evaluator result is deliberately more detailed than `bool` so denied
/// sync, bootstrap, and operator requests can be audited consistently.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct AuthorizationDecision {
    pub effect: DecisionEffect,
    pub code: DecisionCode,
    pub obligations: Vec<Obligation>,
    pub matched_rule_ids: Vec<String>,
    pub policy_epoch: u64,
}

/// A serializable policy rule. Evaluation precedence is defined by the engine:
/// explicit denies override allows, and missing policy fails closed.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PolicyRule {
    pub rule_id: String,
    pub effect: RuleEffect,
    pub principals: Vec<PrincipalId>,
    pub roles: Vec<Role>,
    pub actions: Vec<Action>,
    pub resources: Vec<ResourceSelector>,
    pub max_sensitivity: Option<Sensitivity>,
    pub required_device_trust: Option<DeviceTrust>,
    pub required_capabilities: Vec<CapabilityId>,
    pub requires_online: bool,
    pub requires_approval: bool,
    pub expires_at_ms: Option<u64>,
}

/// Pure policy evaluation belongs behind this interface. Implementations may
/// be backed by an engine snapshot, but must not perform I/O during evaluation.
pub trait AuthorizationEvaluator: Send + Sync {
    fn evaluate(&self, context: &AuthorizationContext) -> AuthorizationDecision;
}
