//! Public workspace errors and crate-local result alias.

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    AlreadyExists(String),
    NotFound(String),
    PermissionDenied,
    TypeMismatch(String),
    CorruptCatalog(String),
    CorruptLocalState(String),
    CorruptDeviceRegistry(String),
    InvalidEventSequence,
    ClockExhausted,
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
