use bincode::{Decode, Encode};

/// Coarse-grained workspace role for the first generations of ZeninDB.
///
/// These roles are intentionally broad. The goal is not to express every
/// possible per-note or per-operator policy directly in the role itself.
/// Instead:
///
/// - the role answers "what is this member generally allowed to do?"
/// - finer-grained restrictions can later be added through workspace policy,
///   sensitivity labels, or operator permission checks
///
/// Current intended semantics:
///
/// - `Owner`
///   - full control of the workspace
///   - may manage membership, devices, and destructive/admin operations
/// - `Admin`
///   - may manage most workspace settings and membership
///   - not necessarily the ultimate owner for ownership transfer / hard delete
/// - `Editor`
///   - may create and modify normal workspace content
///   - may not manage membership or sensitive workspace-level configuration
/// - `Commenter`
///   - may annotate and comment, but should not freely edit canonical content
/// - `Viewer`
///   - read-only workspace access
/// - `OperatorAdmin`
///   - may manage operator definitions and automation settings
///   - may or may not be broader than `Editor`, depending on policy
///
/// The exact enforcement should eventually be driven by helper methods and
/// policy evaluation rather than by ad hoc string comparisons in higher layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum Role {
    Owner,
    Admin,
    Editor,
    Commenter,
    Viewer,
    OperatorAdmin,
}

impl Role {
    /// Whether the role should be treated as having read access to ordinary
    /// workspace content.
    pub const fn can_read(self) -> bool {
        true
    }

    /// Whether the role should be treated as having write access to canonical
    /// workspace content such as notes, graph nodes, or tasks.
    pub const fn can_write(self) -> bool {
        matches!(self, Self::Owner | Self::Admin | Self::Editor)
    }

    /// Whether the role should be treated as having comment/annotation rights.
    pub const fn can_comment(self) -> bool {
        matches!(
            self,
            Self::Owner | Self::Admin | Self::Editor | Self::Commenter | Self::OperatorAdmin
        )
    }

    /// Whether the role should be allowed to manage workspace membership,
    /// devices, invites, or similar administrative records.
    pub const fn can_administer_membership(self) -> bool {
        matches!(self, Self::Owner | Self::Admin)
    }

    /// Whether the role should be allowed to create, enable, disable, or
    /// approve operators at the workspace level.
    pub const fn can_manage_operators(self) -> bool {
        matches!(self, Self::Owner | Self::Admin | Self::OperatorAdmin)
    }
}
