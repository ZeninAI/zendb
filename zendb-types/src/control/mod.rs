//! Shared database control-plane records.

pub mod operator;
pub mod presence;

pub use operator::{
    DeviceCapabilitySummary, OperatorDesiredState, OperatorEffect, OperatorInput, OperatorLease,
    OperatorOutputFence, OperatorSource, OperatorSpec,
};
pub use presence::{DepartureNotice, PresenceHeartbeat};
