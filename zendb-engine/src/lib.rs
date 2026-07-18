//! ZeninDB table engine.

pub mod workspace;

pub use workspace::{
    ClusterConfig, ClusterRuntime, ConcurrentTable, OnboardingResult, RowRequest, SyncReport,
    TableHandle, TableLifecycleEvent, TableObservation, TableRequest, Workspace, WorkspaceConfig,
};
pub use zendb_replication::{Table, TableConfig, TableStats, DEFAULT_MAX_BUFFERED_RECORDS};
