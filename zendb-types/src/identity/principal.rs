//! Principal identities used by authorization and audit records.

use bincode::{Decode, Encode};

use super::{DeviceId, GuestId, OperatorId, ServiceId, UserId};

/// The subject on whose behalf an operation is performed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub enum PrincipalId {
    User(UserId),
    Device(DeviceId),
    Guest(GuestId),
    Service(ServiceId),
    Operator(OperatorId),
}
