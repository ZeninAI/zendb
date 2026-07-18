//! Identifier newtype generation.

/// Define a time-sortable, generated 128-bit entity identifier.
#[macro_export]
macro_rules! define_entity_id {
    ($name:ident) => {
        #[repr(transparent)]
        #[derive(
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            ::bincode::Encode,
            ::bincode::Decode,
            ::zendb_types::CellCodec,
        )]
        pub struct $name(pub [u8; 16]);

        impl $name {
            pub const ZERO: Self = Self([0; 16]);

            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            pub const fn from_u128(value: u128) -> Self {
                Self(value.to_be_bytes())
            }

            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            pub fn generate() -> ::std::io::Result<Self> {
                $crate::identity::ids::generate_entity_id().map(Self)
            }

            /// Approximate creator wall-clock time embedded by UUIDv7.
            ///
            /// This value is diagnostic metadata, not a trusted authorization,
            /// expiration, causal-ordering, or CRDT timestamp.
            pub const fn created_at_ms(&self) -> u64 {
                $crate::identity::ids::entity_id_timestamp_ms(&self.0)
            }

            pub fn parse(value: &str) -> Result<Self, $crate::identity::ids::IdParseError> {
                $crate::identity::ids::parse_entity_id(value).map(Self)
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                $crate::identity::ids::format_entity_id(&self.0, formatter)
            }
        }

        impl ::std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_string())
                    .finish()
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $crate::identity::ids::IdParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }
    };
}

/// Define a human-readable protocol label rather than a generated identity.
#[macro_export]
macro_rules! define_label {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            ::bincode::Encode,
            ::bincode::Decode,
            ::zendb_types::CellCodec,
        )]
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

        impl ::std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}
