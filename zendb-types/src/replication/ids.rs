//! Opaque installation and workspace identifiers used by replication.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdFromPrimaryKeyError {
    ExpectedBlob,
    InvalidLength { expected: usize, actual: usize },
}

impl std::fmt::Display for IdFromPrimaryKeyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExpectedBlob => formatter.write_str("identifier primary key must be a Blob"),
            Self::InvalidLength { expected, actual } => write!(
                formatter,
                "identifier primary key must contain {expected} bytes, got {actual}"
            ),
        }
    }
}

impl std::error::Error for IdFromPrimaryKeyError {}

opaque_id!(InstallationId, InstallationIdParseError, 8);
opaque_id!(WorkspaceId, WorkspaceIdParseError, 8);
