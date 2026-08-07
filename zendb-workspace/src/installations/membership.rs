//! Decoded installation membership and authorization state.

use std::{collections::BTreeMap, sync::Arc};

use arc_swap::ArcSwap;
use zendb_storage::{Change, ReadBackend, Table};
use zendb_types::{Installation, InstallationId, Op, Permission, PublicKey, TypeOp, Value};

use crate::{Error, Result};

#[derive(Clone)]
struct MembershipState {
    local_installation: Option<Arc<Installation>>,
    installations: Arc<BTreeMap<InstallationId, Arc<Installation>>>,
}

pub(crate) struct Membership {
    local_installation_id: InstallationId,
    state: ArcSwap<MembershipState>,
}

impl Membership {
    pub(crate) fn create(
        local_installation_id: InstallationId,
        installation: Installation,
    ) -> Self {
        let installation = Arc::new(installation);
        Self {
            local_installation_id,
            state: ArcSwap::from_pointee(MembershipState {
                local_installation: Some(Arc::clone(&installation)),
                installations: Arc::new(BTreeMap::from([(local_installation_id, installation)])),
            }),
        }
    }

    pub(crate) fn open(
        installations_table: &Table,
        local_installation_id: InstallationId,
        expected_public_key: &PublicKey,
    ) -> Result<Self> {
        let mut installations = BTreeMap::new();
        for (key, cell) in installations_table.entries() {
            let key = key.into_owned();
            let installation_id = InstallationId::try_from(&key).map_err(|error| {
                Error::CorruptInstallations(format!("invalid installation key: {error}"))
            })?;
            let installation = match cell.into_owned().value {
                Some(Value::Installation(installation)) => installation,
                None => continue,
                Some(_) => {
                    return Err(Error::CorruptInstallations(format!(
                        "installation {installation_id} has the wrong value type"
                    )));
                }
            };
            installations.insert(installation_id, Arc::new(installation));
        }
        let local = installations
            .get(&local_installation_id)
            .ok_or(Error::LocalInstallationNotEnrolled(local_installation_id))?;
        if &local.public_key != expected_public_key {
            return Err(Error::LocalInstallationKeyMismatch);
        }
        if !local.state.is_active() {
            return Err(Error::LocalInstallationNotActive(local_installation_id));
        }

        Ok(Self {
            local_installation_id,
            state: ArcSwap::from_pointee(MembershipState {
                local_installation: Some(Arc::clone(local)),
                installations: Arc::new(installations),
            }),
        })
    }

    pub(crate) const fn local_installation_id(&self) -> InstallationId {
        self.local_installation_id
    }

    pub(crate) fn list(&self) -> Vec<(InstallationId, Installation)> {
        self.state
            .load()
            .installations
            .iter()
            .map(|(id, installation)| (*id, installation.as_ref().clone()))
            .collect()
    }

    pub(crate) fn get(&self, installation_id: &InstallationId) -> Option<Installation> {
        let state = self.state.load();
        if installation_id == &self.local_installation_id {
            state.local_installation.as_deref().cloned()
        } else {
            state
                .installations
                .get(installation_id)
                .map(|installation| installation.as_ref().clone())
        }
    }

    pub(crate) fn has_permission(
        &self,
        installation_id: &InstallationId,
        required: Permission,
    ) -> bool {
        let state = self.state.load();
        let installation = if installation_id == &self.local_installation_id {
            state.local_installation.as_deref()
        } else {
            state.installations.get(installation_id).map(AsRef::as_ref)
        };
        installation
            .and_then(|installation| installation.state.permissions())
            .is_some_and(|permissions| permissions.allows(required))
    }

    pub(crate) fn require_permission(
        &self,
        installation_id: &InstallationId,
        required: Permission,
    ) -> Result<()> {
        if self.has_permission(installation_id, required) {
            Ok(())
        } else {
            Err(Error::PermissionDenied)
        }
    }

    pub(crate) fn apply_installation_change(&self, change: &Change) {
        let Ok(installation_id) = InstallationId::try_from(&change.event.primary_key) else {
            return;
        };
        if !change.event.path.is_empty() {
            return;
        }
        let installation = match &change.event.op {
            Op::Type(TypeOp::Installation(_)) => match &change.current {
                Some(cell) => match &cell.value {
                    Some(Value::Installation(installation)) => Arc::new(installation.clone()),
                    _ => return,
                },
                None => return,
            },
            _ => return,
        };
        // Readers use the old immutable snapshot until this whole projection is
        // ready; the separate local cache keeps authorization on the hot path.
        self.state.rcu(|state| {
            let mut next = (**state).clone();
            Arc::make_mut(&mut next.installations)
                .insert(installation_id, Arc::clone(&installation));
            if installation_id == self.local_installation_id {
                next.local_installation = Some(Arc::clone(&installation));
            }
            next
        });
    }
}
