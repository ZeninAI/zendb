//! Authenticated envelope admission shared by Workspace and its network worker.

use libp2p_identity::PeerId;
use zendb_types::{Envelope, Event, EventId, EventStamp, Permission};

use crate::{
    Error,
    core::WorkspaceCore,
    system::{INSTALLATIONS_TABLE_NAME, TABLE_CATALOG_NAME},
};

#[derive(Debug)]
pub enum AdmitError {
    UnknownInstallation,
    AuthorMismatch,
    Unauthorized,
    Workspace(Error),
}

impl std::fmt::Display for AdmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownInstallation => formatter.write_str("the envelope author is not enrolled"),
            Self::AuthorMismatch => {
                formatter.write_str("the envelope author does not match its signed source")
            }
            Self::Unauthorized => formatter.write_str("the envelope author is not authorized"),
            Self::Workspace(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for AdmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Workspace(error) => Some(error),
            _ => None,
        }
    }
}

pub(crate) fn admit_event(
    core: &WorkspaceCore,
    envelope: Envelope,
    gossipsub_source: PeerId,
) -> Result<(), AdmitError> {
    let installation = core
        .membership
        .get(&envelope.author)
        .ok_or(AdmitError::UnknownInstallation)?;
    if installation.public_key.as_libp2p().to_peer_id() != gossipsub_source {
        return Err(AdmitError::AuthorMismatch);
    }
    let required = match envelope.table.as_str() {
        INSTALLATIONS_TABLE_NAME => Permission::ManageInstallations,
        TABLE_CATALOG_NAME => Permission::ManageCatalog,
        _ => Permission::WriteData,
    };
    if !installation
        .state
        .permissions()
        .is_some_and(|permissions| permissions.allows(required))
    {
        return Err(AdmitError::Unauthorized);
    }

    let table = core
        .table_store
        .get(&envelope.table)
        .map_err(AdmitError::Workspace)?;
    // The envelope author and signed source are authenticated once. Each
    // compact event then uses the shared commit path so remote projections and
    // causal receipts follow the same ordering as local changes.
    for compact in envelope.events {
        core.commit_admitted_event(
            &table,
            Event {
                primary_key: compact.primary_key,
                path: compact.path,
                op: compact.op,
                stamp: EventStamp {
                    id: EventId {
                        author: envelope.author,
                        sequence: compact.sequence,
                    },
                    time: compact.time,
                },
            },
        )
        .map_err(AdmitError::Workspace)?;
    }
    Ok(())
}
