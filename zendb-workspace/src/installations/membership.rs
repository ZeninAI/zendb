//! Decoded installation membership and authorization state.

use std::collections::BTreeMap;

use arc_swap::ArcSwap;
use zendb_storage::{Change, ReadBackend, Table};
use zendb_types::{Installation, InstallationId, Op, Permission, PublicKey, Value};

use crate::{Error, Result};

#[derive(Clone)]
struct MembershipState {
    local_installation: Option<Installation>,
    installations: BTreeMap<InstallationId, Installation>,
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
        Self {
            local_installation_id,
            state: ArcSwap::from_pointee(MembershipState {
                local_installation: Some(installation.clone()),
                installations: BTreeMap::from([(local_installation_id, installation)]),
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
            let blob = match cell.into_owned().value {
                Some(Value::Blob(blob)) => blob,
                None => continue,
                Some(_) => {
                    return Err(Error::CorruptInstallations(format!(
                        "installation {installation_id} is not stored as a Blob"
                    )));
                }
            };
            let installation: Installation = blob.decode().map_err(|error| {
                Error::CorruptInstallations(format!(
                    "installation {installation_id} cannot be decoded: {error}"
                ))
            })?;
            installations.insert(installation_id, installation);
        }
        // Publish one complete immutable snapshot only after the registry has
        // been decoded and the local installation has passed key validation.
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
                local_installation: Some(local.clone()),
                installations,
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
            .map(|(id, installation)| (*id, installation.clone()))
            .collect()
    }

    pub(crate) fn get(&self, installation_id: &InstallationId) -> Option<Installation> {
        let state = self.state.load();
        if installation_id == &self.local_installation_id {
            state.local_installation.clone()
        } else {
            state.installations.get(installation_id).cloned()
        }
    }

    pub(crate) fn has_permission(
        &self,
        installation_id: &InstallationId,
        required: Permission,
    ) -> bool {
        let state = self.state.load();
        let installation = if installation_id == &self.local_installation_id {
            state.local_installation.as_ref()
        } else {
            state.installations.get(installation_id)
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
            Op::Upsert {
                value: Value::Blob(blob),
            } => match blob.decode::<Installation>() {
                Ok(installation) => Some(installation),
                Err(_) => return,
            },
            Op::Delete => None,
            _ => return,
        };
        // Readers use the old immutable snapshot until this whole projection is
        // ready; the separate local cache keeps authorization on the hot path.
        self.state.rcu(|state| {
            let mut next = (**state).clone();
            match &installation {
                Some(installation) => {
                    next.installations
                        .insert(installation_id, installation.clone());
                }
                None => {
                    next.installations.remove(&installation_id);
                }
            }
            if installation_id == self.local_installation_id {
                next.local_installation = installation.clone();
            }
            next
        });
    }
}
