//! Shared device trust classification.

use bincode::{Decode, Encode};

/// Ordered from least to most trusted for minimum-trust comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub enum DeviceTrust {
    Untrusted,
    Restricted,
    Trusted,
    Personal,
}
