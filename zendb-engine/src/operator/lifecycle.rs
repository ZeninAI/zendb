use bincode::{Decode, Encode};

/// Persistent lifecycle phase of an operator.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OperatorPhase {
    /// Running or waiting for input tables.
    Active,
    /// Completed normally via [`OperatorDirective::Finish`].
    Finished,
    /// Terminated by an unrecoverable error.
    Failed { error: String },
    /// Permanently cancelled by the database owner.
    Cancelled,
}

/// Returned by operator lifecycle methods to steer the worker run loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorDirective {
    /// Keep polling.
    Continue,
    /// Tear down the operator cleanly.
    Finish,
}

/// Reason passed to [`Operator::teardown`] explaining why the operator is
/// being permanently removed.
///
/// ```text
/// ┌─────────┐
/// │ create  │  ← Context available, set up state/tables
/// └────┬────┘
///      │  (for each matching table already open)
///      ▼
/// ┌────────────────┐
/// │ on_input_opened│
/// └────────┬───────┘
///          │
///          ▼
/// ┌─────────────────────────────────────────────────────┐
/// │              ACTIVE LOOP                             │
/// │  ┌─────────┐   ┌──────────┐   ┌────────────────┐   │
/// │  │ process │   │ on_timer │   │ on_input_opened │   │
/// │  │ changes │   │  fires   │   │ / _closed       │   │
/// │  └─────────┘   └──────────┘   └────────────────┘   │
/// │                                                     │
/// │  Any method may return Finish ─────────────────────►│
/// └─────────────────────────────────┬───────────────────┘
///                                   │
///                                   ▼
///                          ┌────────────────┐
///                          │   teardown     │ ← reason: Finished/Failed/Cancelled
///                          └────────────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeardownReason {
    /// The operator returned [`OperatorDirective::Finish`] from a lifecycle
    /// method, signalling normal completion.
    Finished,
    /// An unrecoverable error occurred during processing.
    Failed { error: String },
    /// The database owner permanently cancelled this operator.
    Cancelled,
}
