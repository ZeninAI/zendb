//! Public installations facade backed by the owning workspace.

mod membership;

use std::sync::Arc;

use zendb_types::{Blob, Installation, InstallationId, Op, Path, Role, Value};

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
        self.core
            .membership
            .require_access(&self.core.membership.local_installation_id(), Role::Admin)?;
        if self.core.membership.get(&installation_id).as_ref() == Some(&installation) {
            return Ok(false);
        }
        // Registry changes are written through WorkspaceCore so membership,
        // replication routes, causal state, and listeners update together.
        self.core.commit_authorized_local_change(
            &self.table,
            installation_id.into(),
            Path::new(),
            Op::Upsert {
                value: Value::Blob(Blob::encode(&installation)?),
            },
        )?;
        Ok(true)
    }

    pub fn delete(&self, installation_id: InstallationId) -> Result<bool> {
        self.core
            .membership
            .require_access(&self.core.membership.local_installation_id(), Role::Admin)?;
        if self.core.membership.get(&installation_id).is_none() {
            return Ok(false);
        }
        self.core.commit_authorized_local_change(
            &self.table,
            installation_id.into(),
            Path::new(),
            Op::Delete,
        )?;
        Ok(true)
    }

    pub fn has_access(&self, installation_id: &InstallationId, required: Role) -> bool {
        self.core.membership.has_access(installation_id, required)
    }
}
