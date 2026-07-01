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
}

/// Returned by operator lifecycle methods to steer the worker run loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorDirective {
    /// Keep polling.
    Continue,
    /// Tear down the operator cleanly.
    Finish,
}
