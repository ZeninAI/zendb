use bincode::{Decode, Encode};

/// Fixed, Workspace-wide role values held by a Device record.
///
/// Read access is implicit for every live device and therefore has no stored
/// role. The values are deliberately non-overlapping; an owner is a UI label
/// for a device that holds all three values, not a fourth protocol role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub enum WorkspaceRole {
    Contributor,
    Dispatcher,
    Manager,
}

/// The only authorization actions understood by the embedded control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum WorkspaceAction {
    Read,
    Contribute,
    Dispatch,
    Manage,
}

impl WorkspaceRole {
    pub const fn allows(self, action: WorkspaceAction) -> bool {
        matches!(
            (self, action),
            (_, WorkspaceAction::Read)
                | (Self::Contributor, WorkspaceAction::Contribute)
                | (Self::Dispatcher, WorkspaceAction::Dispatch)
                | (Self::Manager, WorkspaceAction::Manage)
        )
    }
}
