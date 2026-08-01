//! Authenticated envelope admission shared by Workspace and its network worker.

use libp2p_identity::PeerId;
use zendb_types::{Envelope, Event, EventId, EventStamp, Role};

use crate::{Error, consts::is_system_table, installations::Installations, tables::Tables};

#[derive(Debug)]
pub enum AdmitError {
    UnknownPeer,
    AuthorMismatch,
    AmbiguousPeer,
    Unauthorized,
    Workspace(Error),
}

impl std::fmt::Display for AdmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownPeer => formatter.write_str("the envelope author is not enrolled"),
            Self::AuthorMismatch => {
                formatter.write_str("the envelope author does not match its signed source")
            }
            Self::AmbiguousPeer => {
                formatter.write_str("the signed source belongs to multiple installations")
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
    tables: &Tables,
    installations: &Installations,
    envelope: Envelope,
    gossipsub_source: PeerId,
) -> Result<(), AdmitError> {
    let installation = installations
        .get(&envelope.author)
        .ok_or(AdmitError::UnknownPeer)?;
    if installation.public_key.as_libp2p().to_peer_id() != gossipsub_source {
        return Err(AdmitError::AuthorMismatch);
    }
    if installations
        .list()
        .into_iter()
        .any(|(installation_id, installation)| {
            installation_id != envelope.author
                && installation.public_key.as_libp2p().to_peer_id() == gossipsub_source
        })
    {
        return Err(AdmitError::AmbiguousPeer);
    }

    let required = if is_system_table(&envelope.table) {
        Role::Admin
    } else {
        Role::Contributor
    };
    if !installation
        .role
        .is_some_and(|role| role.has_at_least(required))
    {
        return Err(AdmitError::Unauthorized);
    }

    let table = tables.get(&envelope.table).map_err(AdmitError::Workspace)?;
    for compact in envelope.events {
        table
            .insert_internal(Event {
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
            })
            .map_err(AdmitError::Workspace)?;
    }
    Ok(())
}
