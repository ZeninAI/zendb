//! Shared implementation generator for fixed-size random identifiers.

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

macro_rules! opaque_id {
    ($name:ident, $error:ident, $bytes:expr) => {
        #[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; $bytes]);

        impl $name {
            pub fn generate() -> Self {
                use rand::RngCore;

                let mut bytes = [0; $bytes];
                rand::rngs::OsRng.fill_bytes(&mut bytes);
                Self(bytes)
            }

            pub const fn from_bytes(bytes: [u8; $bytes]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; $bytes] {
                &self.0
            }

            pub const fn to_bytes(self) -> [u8; $bytes] {
                self.0
            }
        }

        impl From<$name> for $crate::PrimaryKey {
            fn from(value: $name) -> Self {
                Self::Blob(value.to_bytes().to_vec().into())
            }
        }

        impl TryFrom<&$crate::PrimaryKey> for $name {
            type Error = $crate::replication::IdFromPrimaryKeyError;

            fn try_from(value: &$crate::PrimaryKey) -> Result<Self, Self::Error> {
                let $crate::PrimaryKey::Blob(bytes) = value else {
                    return Err(Self::Error::ExpectedBlob);
                };
                let bytes: [u8; $bytes] =
                    bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| Self::Error::InvalidLength {
                            expected: $bytes,
                            actual: bytes.len(),
                        })?;
                Ok(Self::from_bytes(bytes))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

                let text_len = ($bytes * 8_usize).div_ceil(5);
                let padding = text_len * 5 - $bytes * 8;
                let mut encoded = String::with_capacity(text_len);
                for character in 0..text_len {
                    let mut digit = 0_u8;
                    for digit_bit in 0..5 {
                        digit <<= 1;
                        let padded_bit = character * 5 + digit_bit;
                        if padded_bit >= padding {
                            let source_bit = padded_bit - padding;
                            let byte = source_bit / 8;
                            let bit = 7 - source_bit % 8;
                            digit |= (self.0[byte] >> bit) & 1;
                        }
                    }
                    encoded.push(ALPHABET[usize::from(digit)] as char);
                }
                formatter.write_str(&encoded)
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_string())
                    .finish()
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $error {
            InvalidLength,
            InvalidCharacter,
            Overflow,
        }

        impl std::fmt::Display for $error {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::InvalidLength => write!(
                        formatter,
                        "{} must contain {} characters",
                        stringify!($name),
                        ($bytes * 8_usize).div_ceil(5)
                    ),
                    Self::InvalidCharacter => {
                        write!(
                            formatter,
                            "{} contains an invalid character",
                            stringify!($name)
                        )
                    }
                    Self::Overflow => {
                        write!(formatter, "{} exceeds {} bytes", stringify!($name), $bytes)
                    }
                }
            }
        }

        impl std::error::Error for $error {}

        impl std::str::FromStr for $name {
            type Err = $error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let text_len = ($bytes * 8_usize).div_ceil(5);
                if value.len() != text_len {
                    return Err($error::InvalidLength);
                }

                let padding = text_len * 5 - $bytes * 8;
                let mut bytes = [0_u8; $bytes];
                for (character, byte) in value.bytes().enumerate() {
                    let digit = match byte.to_ascii_uppercase() {
                        b'0' | b'O' => 0,
                        b'1' | b'I' | b'L' => 1,
                        b'2'..=b'9' => byte - b'0',
                        b'A' => 10,
                        b'B' => 11,
                        b'C' => 12,
                        b'D' => 13,
                        b'E' => 14,
                        b'F' => 15,
                        b'G' => 16,
                        b'H' => 17,
                        b'J' => 18,
                        b'K' => 19,
                        b'M' => 20,
                        b'N' => 21,
                        b'P' => 22,
                        b'Q' => 23,
                        b'R' => 24,
                        b'S' => 25,
                        b'T' => 26,
                        b'V' => 27,
                        b'W' => 28,
                        b'X' => 29,
                        b'Y' => 30,
                        b'Z' => 31,
                        _ => return Err($error::InvalidCharacter),
                    };
                    if character == 0 && padding != 0 && digit >= (1 << (5 - padding)) {
                        return Err($error::Overflow);
                    }
                    for digit_bit in 0..5 {
                        let padded_bit = character * 5 + digit_bit;
                        if padded_bit < padding {
                            continue;
                        }
                        let destination_bit = padded_bit - padding;
                        let destination_byte = destination_bit / 8;
                        let bit = 7 - destination_bit % 8;
                        bytes[destination_byte] |= ((digit >> (4 - digit_bit)) & 1) << bit;
                    }
                }
                Ok(Self(bytes))
            }
        }

        impl bincode::Encode for $name {
            fn encode<E: bincode::enc::Encoder>(
                &self,
                encoder: &mut E,
            ) -> Result<(), bincode::error::EncodeError> {
                bincode::Encode::encode(&self.0, encoder)
            }
        }

        impl<Context> bincode::Decode<Context> for $name {
            fn decode<D: bincode::de::Decoder<Context = Context>>(
                decoder: &mut D,
            ) -> Result<Self, bincode::error::DecodeError> {
                <[u8; $bytes] as bincode::Decode<Context>>::decode(decoder).map(Self)
            }
        }

        bincode::impl_borrow_decode!($name);
    };
}
