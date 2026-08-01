//! Persisted workspace capabilities used by replication authorization.

use bincode::{Decode, Encode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum Role {
    Contributor,
    Operator,
    Admin,
}

impl Role {
    pub const fn has_at_least(self, required: Self) -> bool {
        match required {
            Self::Contributor => true,
            Self::Operator => matches!(self, Self::Operator | Self::Admin),
            Self::Admin => matches!(self, Self::Admin),
        }
    }
}
