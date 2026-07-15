//! Macro definitions for identity type generation.
//!
//! The `define_id!` macro provides a consistent way to define string-based
//! identifier types with proper serialization, display, and conversion traits.

/// Generate a newtype wrapper around `String` for identity types.
///
/// # Generated Implementations
///
/// - `Debug`, `Clone`, `PartialEq`, `Eq`, `PartialOrd`, `Ord`, `Hash`
/// - `Encode`, `Decode` (via bincode)
/// - `From<String>`, `From<&str>`
/// - `Display`
///
/// # Example
///
/// ```ignore
/// define_id!(WorkspaceId);
/// define_id!(WorkspaceId);
/// ```
#[macro_export]
macro_rules! define_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, ::bincode::Encode, ::bincode::Decode)]
        pub struct $name(pub String);

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}
