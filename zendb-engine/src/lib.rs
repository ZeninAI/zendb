//! ZeninDB table engine.

pub mod operator;
pub mod runtime;
pub mod workspace;

#[cfg(test)]
mod tests;

pub use operator::{
    plan_reconciliation, BoxFuture, CapabilityHost, Change, DispatchConfig, DispatchOperator,
    LeaseConsistency, Operator, OperatorAdmission, OperatorControlError, OperatorControlResult,
    OperatorDirective, OperatorPhase, OperatorRuntimeConfig, ReconcileAction, ReconcileSnapshot,
    State, Subscription,
};
pub use runtime::{Executor, RuntimeFuture};
pub use workspace::{
    ConcurrentState, ConcurrentTable, StateHandle, TableHandle, Workspace, WorkspaceConfig,
    WorkspaceError, WorkspaceJoinPlan, WorkspaceJoinRequest, WorkspaceReconcileRequest,
    WorkspaceSyncRequest,
};
pub use zendb_storage::frontend::{
    state::{StateConfig, StateStats},
    table::{Table, TableConfig, TableStats, DEFAULT_MAX_BUFFERED_RECORDS},
};
