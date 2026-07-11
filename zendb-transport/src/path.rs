//! Deterministic path ranking and logical-session handoff policy.

use crate::{DiscoveredPeer, NetworkEndpoint};

/// Configurable local path selector. Path choice is a policy algorithm, not a
/// dependency-injection boundary, so it is a concrete type rather than a
/// single-method trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathSelector {
    /// A candidate must have at least this score to replace the current path.
    pub minimum_handoff_score: u16,
}

impl Default for PathSelector {
    fn default() -> Self {
        Self {
            minimum_handoff_score: 1,
        }
    }
}

impl PathSelector {
    /// Rank candidates by local score, then prefer candidates that expire later.
    /// Discovery data remains untrusted and is never used for authorization.
    pub fn rank(&self, candidates: &[DiscoveredPeer]) -> Vec<DiscoveredPeer> {
        let mut ranked = candidates.to_vec();
        ranked.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| right.expires_at_ms.cmp(&left.expires_at_ms))
        });
        ranked
    }

    /// Return true only when the candidate is strong enough to authenticate
    /// before the current bearer is closed.
    pub fn should_handoff(&self, _current: &NetworkEndpoint, candidate: &DiscoveredPeer) -> bool {
        candidate.score >= self.minimum_handoff_score
    }
}
