//! ZeninDB table engine.

pub mod database;
pub mod operator;
pub mod runtime;

pub use database::{
    ConcurrentState, ConcurrentTable, Database, DatabaseConfig, StateHandle, TableHandle,
};
pub use operator::{
    BoxFuture, Change, DispatchConfig, DispatchOperator, Operator,
    OperatorDirective, OperatorPhase, OperatorRuntimeConfig, State, Subscription, TeardownReason,
};
pub use runtime::{Executor, RuntimeFuture};
pub use zendb_storage::frontend::{
    state::{StateConfig, StateStats},
    table::{Table, TableConfig, TableStats, DEFAULT_MAX_BUFFERED_RECORDS},
};
