//! Public installations facade backed by the owning workspace.

mod membership;

use std::sync::Arc;

use zendb_storage::InsertOutcome;
use zendb_types::{Edit, Event, EventId, Installation, InstallationId, Permission, PrimaryKey};

pub(crate) use membership::Membership;

use crate::{Result, core::WorkspaceCore, tables::OpenTable};

/// Installation operations for one open workspace.
pub struct Installations {
    core: Arc<WorkspaceCore>,
    table: Arc<OpenTable>,
}

impl Installations {
    pub(crate) fn new(core: Arc<WorkspaceCore>, table: Arc<OpenTable>) -> Self {
        Self { core, table }
    }

    pub fn local_installation_id(&self) -> InstallationId {
        self.core.membership.local_installation_id()
    }

    pub fn list(&self) -> Vec<(InstallationId, Installation)> {
        self.core.membership.list()
    }

    pub fn get(&self, installation_id: &InstallationId) -> Option<Installation> {
        self.core.membership.get(installation_id)
    }

    pub fn upsert(
        &self,
        installation_id: InstallationId,
        installation: Installation,
    ) -> Result<bool> {
        self.core.membership.require_permission(
            &self.core.membership.local_installation_id(),
            Permission::ManageInstallations,
        )?;
        if self.core.membership.get(&installation_id).as_ref() == Some(&installation) {
            return Ok(false);
        }
        // Registry changes are written through WorkspaceCore so membership,
        // replication routes, causal state, and listeners update together.
        let mut edit = Edit::empty();
        edit.typed::<Installation>()
            .set(installation)
            .expect("Installation set is infallible");
        let outcome = self.core.commit_change(
            &self.table,
            Event {
                id: EventId {
                    author: self.core.membership.local_installation_id(),
                    sequence: 0,
                },
                primary_key: PrimaryKey::from(installation_id),
                operations: edit.take_changes(),
            },
        )?;
        Ok(matches!(outcome, InsertOutcome::Applied(_)))
    }

    pub fn delete(&self, installation_id: InstallationId) -> Result<bool> {
        self.core.membership.require_permission(
            &self.core.membership.local_installation_id(),
            Permission::ManageInstallations,
        )?;
        let Some(mut installation) = self.core.membership.get(&installation_id) else {
            return Ok(false);
        };
        if installation.state == zendb_types::InstallationState::Rejected {
            return Ok(false);
        }
        installation.state = zendb_types::InstallationState::Rejected;
        let mut edit = Edit::empty();
        edit.typed::<Installation>()
            .set(installation)
            .expect("Installation set is infallible");
        let outcome = self.core.commit_change(
            &self.table,
            Event {
                id: EventId {
                    author: self.core.membership.local_installation_id(),
                    sequence: 0,
                },
                primary_key: PrimaryKey::from(installation_id),
                operations: edit.take_changes(),
            },
        )?;
        Ok(matches!(outcome, InsertOutcome::Applied(_)))
    }

    pub fn has_permission(&self, installation_id: &InstallationId, required: Permission) -> bool {
        self.core
            .membership
            .has_permission(installation_id, required)
    }
}
