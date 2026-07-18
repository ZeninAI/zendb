//! Device-centric shared identity and membership vocabulary.

#[macro_use]
pub mod _macros;

pub mod ids;
pub mod membership;
pub mod role;

pub use ids::{
    CapabilityId, DeviceId, EnrollmentTicketId, EntityIdGenerator, IdParseError, OperatorId,
    WorkspaceId,
};
pub use membership::{
    DeviceKeyPhase, DeviceKeyRing, DevicePublicKey, DeviceRecord, DeviceRecordError,
    EnrollmentTicket, SignatureBytes,
};
pub use role::{WorkspaceAction, WorkspaceRole};
