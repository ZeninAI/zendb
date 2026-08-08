//! Public workspace errors and crate-local result alias.

use zendb_types::{InstallationId, WorkspaceId};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    WorkspaceExists,
    WorkspaceAlreadyOpen,
    WorkspaceIdMismatch {
        stored: WorkspaceId,
        configured: WorkspaceId,
    },
    TableNotFound(String),
    StateNotFound(String),
    PermissionDenied,
    SystemStateReadOnly(String),
    SystemTableReadOnly(String),
    StateTypeMismatch(String),
    CorruptTableCatalog(String),
    CorruptInstallations(String),
    ClockExhausted,
    WorkspaceClosed,
    LocalInstallationNotEnrolled(InstallationId),
    LocalInstallationNotActive(InstallationId),
    LocalInstallationKeyMismatch,
    KeyDerivationUnsupported,
    KeyDerivationFailed(String),
    Replication(String),
    TableInUse(String),
    StateInUse(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::WorkspaceExists => formatter.write_str("a workspace already exists at this path"),
            Self::WorkspaceAlreadyOpen => formatter.write_str("the workspace is already open"),
            Self::WorkspaceIdMismatch { stored, configured } => write!(
                formatter,
                "stored workspace ID {stored} does not match configured workspace ID {configured}"
            ),
            Self::TableNotFound(name) => write!(formatter, "table {name:?} was not found"),
            Self::StateNotFound(name) => write!(formatter, "state {name:?} was not found"),
            Self::PermissionDenied => formatter.write_str("permission denied"),
            Self::SystemStateReadOnly(name) => write!(
                formatter,
                "system state {name:?} is read-only through public APIs"
            ),
            Self::SystemTableReadOnly(name) => write!(
                formatter,
                "system table {name:?} is read-only through public APIs"
            ),
            Self::StateTypeMismatch(name) => {
                write!(formatter, "state {name:?} is open with other types")
            }
            Self::CorruptTableCatalog(message) => {
                write!(formatter, "corrupt table catalog: {message}")
            }
            Self::CorruptInstallations(message) => {
                write!(formatter, "corrupt installations table: {message}")
            }
            Self::ClockExhausted => formatter.write_str("installation clock exhausted"),
            Self::WorkspaceClosed => formatter.write_str("workspace is closed"),
            Self::LocalInstallationNotEnrolled(installation_id) => {
                write!(
                    formatter,
                    "local installation {installation_id:?} is not enrolled"
                )
            }
            Self::LocalInstallationNotActive(installation_id) => write!(
                formatter,
                "local installation {installation_id:?} is not active"
            ),
            Self::LocalInstallationKeyMismatch => formatter
                .write_str("the supplied identity does not match the local installation key"),
            Self::KeyDerivationUnsupported => {
                formatter.write_str("the supplied keypair does not support secret derivation")
            }
            Self::KeyDerivationFailed(message) => {
                write!(formatter, "workspace key derivation failed: {message}")
            }
            Self::Replication(message) => write!(formatter, "replication error: {message}"),
            Self::TableInUse(name) => write!(formatter, "table {name:?} is still in use"),
            Self::StateInUse(name) => write!(formatter, "state {name:?} is still in use"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
