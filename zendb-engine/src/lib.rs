//! ZeninDB table engine.

pub mod operator;
pub mod runtime;
pub mod workspace;

#[cfg(test)]
mod tests;

pub use operator::{
    plan_reconciliation, BoxFuture, Change, DispatchConfig, DispatchOperator, Operator,
    OperatorDirective, OperatorPhase, OperatorRuntimeConfig, ReconcileAction, ReconcileSnapshot,
    State, StopReason, Subscription,
};
pub use runtime::{Executor, RuntimeFuture};
pub use workspace::{
    ClusterConfig, ClusterRuntime, ConcurrentState, ConcurrentTable, OnboardingResult, StateHandle,
    SyncReport, TableHandle, Workspace, WorkspaceConfig,
};
pub use zendb_storage::frontend::{
    state::{StateConfig, StateStats},
    table::{Table, TableConfig, TableStats, DEFAULT_MAX_BUFFERED_RECORDS},
};
