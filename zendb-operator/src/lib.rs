//! Optional local operator execution layered over a concrete Workspace.

mod host;
pub mod operator;
mod operators;
pub mod runtime;
mod states;
mod timers;

#[cfg(test)]
mod tests;

pub use host::{ConcurrentState, OperatorHost, StateHandle};
pub use operator::{
    BoxFuture, Change, DispatchConfig, DispatchOperator, Operator, OperatorDirective,
    OperatorPhase, OperatorRuntimeConfig, Subscription,
};
pub use runtime::{Executor, RuntimeFuture};
pub use zendb_engine::{
    ClusterConfig, ClusterRuntime, OnboardingResult, SyncReport, Table, TableConfig, TableHandle,
    Workspace, WorkspaceConfig,
};
pub use zendb_storage::State;
pub use zendb_storage::{StateConfig, StateStats};
