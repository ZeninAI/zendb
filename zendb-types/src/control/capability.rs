//! Shared request/result types for policy-gated host capabilities.

use bincode::{Decode, Encode};

use super::authorization::AuthorizationContext;
use crate::CapabilityId;

/// A request to a named host capability. The host decides whether this exact
/// request is allowed; capability names are not permissions by themselves.
#[derive(Debug, Clone, Encode, Decode)]
pub struct CapabilityInvocation {
    pub capability: CapabilityId,
    pub authorization: AuthorizationContext,
    pub idempotency_key: String,
    pub input: Vec<u8>,
}

/// Result returned by a host runner. External execution is at-least-once, so
/// callers must use the idempotency key when the underlying tool supports it.
#[derive(Debug, Clone, Encode, Decode)]
pub struct CapabilityResult {
    pub output: Vec<u8>,
    pub output_hash: [u8; 32],
    pub retryable: bool,
}
