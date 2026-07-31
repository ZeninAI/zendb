//! Public workspace errors and crate-local result alias.

use zendb_types::InstallationId;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    AlreadyExists(String),
    NotFound(String),
    PermissionDenied,
    SystemStateReadOnly(String),
    SystemTableReadOnly(String),
    TypeMismatch(String),
    CorruptCatalog(String),
    CorruptLocalState(String),
    CorruptDeviceRegistry(String),
    InvalidEventSequence,
    ClockExhausted,
    WorkspaceClosed,
    DeviceNotRegistered(InstallationId),
    LocalDeviceKeyMismatch,
    KeyDerivationUnsupported,
    Replication(String),
    ResourceBusy(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::AlreadyExists(name) => write!(formatter, "{name:?} already exists"),
            Self::NotFound(name) => write!(formatter, "{name:?} was not found"),
            Self::PermissionDenied => formatter.write_str("permission denied"),
            Self::SystemStateReadOnly(name) => write!(
                formatter,
                "system state {name:?} is read-only through public APIs"
            ),
            Self::SystemTableReadOnly(name) => write!(
                formatter,
                "system table {name:?} is read-only through public APIs"
            ),
            Self::TypeMismatch(name) => {
                write!(formatter, "state {name:?} is open with other types")
            }
            Self::CorruptCatalog(message) => write!(formatter, "corrupt catalog: {message}"),
            Self::CorruptLocalState(message) => {
                write!(formatter, "corrupt local state: {message}")
            }
            Self::CorruptDeviceRegistry(message) => {
                write!(formatter, "corrupt device registry: {message}")
            }
            Self::InvalidEventSequence => formatter.write_str("event sequence zero is reserved"),
            Self::ClockExhausted => formatter.write_str("device clock exhausted"),
            Self::WorkspaceClosed => formatter.write_str("workspace is closed"),
            Self::DeviceNotRegistered(installation_id) => {
                write!(
                    formatter,
                    "local installation {installation_id:?} is not registered as a device"
                )
            }
            Self::LocalDeviceKeyMismatch => {
                formatter.write_str("the supplied identity does not match the local device key")
            }
            Self::KeyDerivationUnsupported => {
                formatter.write_str("the supplied keypair does not support secret derivation")
            }
            Self::Replication(message) => write!(formatter, "replication error: {message}"),
            Self::ResourceBusy(name) => write!(formatter, "{name:?} is still in use"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
