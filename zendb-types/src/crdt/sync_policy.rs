//! Replica-local synchronization boundaries.

use bincode::{Decode, Encode};

/// The resolved replication scope inherited by a cell from its parent.
///
/// Unlike [`SyncPolicy`], this has no `Inherit` state: a cell is always
/// effectively either shared or local.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncScope {
    Shared,
    Local,
}

impl SyncScope {
    pub const fn is_shared(self) -> bool {
        matches!(self, Self::Shared)
    }
}

/// Controls whether a cell participates in replication on this device.
///
/// The policy is local metadata. It is not a CRDT mutation, does not advance
/// an HLC, and must be removed before a cell is serialized for replication or
/// included in a replicated-state hash.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub enum SyncPolicy {
    /// Use the effective policy inherited from the parent cell or table.
    #[default]
    Inherit,
    /// Keep this cell and its descendants local.
    Local,
}

impl SyncPolicy {
    /// Resolve this cell's declared policy against its parent's effective
    /// scope. A local parent remains local for all descendants.
    pub const fn resolve(self, parent_scope: SyncScope) -> SyncScope {
        match self {
            Self::Inherit => parent_scope,
            Self::Local => SyncScope::Local,
        }
    }
}
