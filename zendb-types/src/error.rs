//! Unified error type for all type-system operations.
//!
//! Uses tag-based error variants so adding a new type does not require
//! editing this file — only `register_types!` in `lib.rs`.

use crate::TypeTag;

/// Top-level error for the `zendb-types` crate.
#[derive(Debug)]
pub enum TypeError {
    /// An apply_op call failed for the given type.
    ApplyError { tag: TypeTag, message: String },

    /// A merge call failed for the given type.
    MergeError { tag: TypeTag, message: String },

    /// A descend / descend_or_create call failed.
    DescendError { tag: TypeTag, message: String },

    /// The requested TypeTag byte does not correspond to any registered type.
    UnknownTypeTag(u8),

    /// An operation's type does not match the target cell's type.
    TypeMismatch { expected: TypeTag, actual: TypeTag },

    /// Encoding failed.
    EncodeError(String),

    /// Decoding failed.
    DecodeError(String),

    /// Two values with different types cannot be merged.
    MergeConflict { local: TypeTag, remote: TypeTag },
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeError::ApplyError { tag, message } => {
                write!(f, "apply error in type {:?}: {}", tag, message)
            }
            TypeError::MergeError { tag, message } => {
                write!(f, "merge error in type {:?}: {}", tag, message)
            }
            TypeError::DescendError { tag, message } => {
                write!(f, "descend error in type {:?}: {}", tag, message)
            }
            TypeError::UnknownTypeTag(tag) => write!(f, "unknown type tag: {}", tag),
            TypeError::TypeMismatch { expected, actual } => {
                write!(
                    f,
                    "type mismatch: expected {:?}, got {:?}",
                    expected, actual
                )
            }
            TypeError::EncodeError(msg) => write!(f, "encode error: {}", msg),
            TypeError::DecodeError(msg) => write!(f, "decode error: {}", msg),
            TypeError::MergeConflict { local, remote } => {
                write!(
                    f,
                    "merge conflict: cannot merge {:?} with {:?}",
                    local, remote
                )
            }
        }
    }
}

impl std::error::Error for TypeError {}
