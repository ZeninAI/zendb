use std::fmt;

use bincode::{Decode, Encode};

/// Stable 128-bit identity for one database installation/profile.
///
/// This is deliberately not derived from a machine identifier. Two logical
/// replicas on one computer must be able to have different identities, and a
/// device identity must be safe to bind to a public key and revoke.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct DeviceId(pub [u8; 16]);

impl DeviceId {
    pub const ZERO: Self = Self([0; 16]);

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Generate an installation identity from the operating system CSPRNG.
    /// The caller must persist the result before creating replicated state.
    pub fn generate() -> std::io::Result<Self> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(Self(bytes))
    }
}

impl fmt::Debug for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DeviceId").field(&self.to_string()).finish()
    }
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

// Other IDs are opaque labels. DeviceId is intentionally binary because it is
// part of signatures, HLCs, and replication ranges.
define_id!(WorkspaceId);
define_id!(OperatorId);
define_id!(CapabilityId);
define_id!(EnrollmentTicketId);
