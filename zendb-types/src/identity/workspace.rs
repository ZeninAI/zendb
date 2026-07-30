//! Random workspace identity and Crockford Base32 representation.

use std::str::FromStr;

use bincode::{de::Decoder, enc::Encoder, Decode, Encode};
use rand::{rngs::OsRng, RngCore};

const WORKSPACE_ID_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const WORKSPACE_ID_TEXT_LEN: usize = 26;

/// A random 128-bit workspace identifier.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceId([u8; 16]);

impl WorkspaceId {
    pub fn generate() -> Self {
        let mut bytes = [0; 16];
        OsRng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl std::fmt::Display for WorkspaceId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = u128::from_be_bytes(self.0);
        let mut encoded = String::with_capacity(WORKSPACE_ID_TEXT_LEN);
        for index in 0..WORKSPACE_ID_TEXT_LEN {
            let shift = 125 - index * 5;
            encoded.push(WORKSPACE_ID_ALPHABET[((value >> shift) & 0x1f) as usize] as char);
        }
        formatter.write_str(&encoded)
    }
}

impl std::fmt::Debug for WorkspaceId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("WorkspaceId")
            .field(&self.to_string())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceIdParseError {
    InvalidLength,
    InvalidCharacter,
    Overflow,
}

impl std::fmt::Display for WorkspaceIdParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLength => formatter.write_str("workspace ID must contain 26 characters"),
            Self::InvalidCharacter => {
                formatter.write_str("workspace ID contains an invalid character")
            }
            Self::Overflow => formatter.write_str("workspace ID exceeds 128 bits"),
        }
    }
}

impl std::error::Error for WorkspaceIdParseError {}

impl FromStr for WorkspaceId {
    type Err = WorkspaceIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != WORKSPACE_ID_TEXT_LEN {
            return Err(WorkspaceIdParseError::InvalidLength);
        }

        let mut decoded = 0_u128;
        for (index, byte) in value.bytes().enumerate() {
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
                _ => return Err(WorkspaceIdParseError::InvalidCharacter),
            };
            if index == 0 {
                if digit > 7 {
                    return Err(WorkspaceIdParseError::Overflow);
                }
                decoded = u128::from(digit);
            } else {
                decoded = (decoded << 5) | u128::from(digit);
            }
        }
        Ok(Self(decoded.to_be_bytes()))
    }
}

impl Encode for WorkspaceId {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.encode(encoder)
    }
}

impl<Context> Decode<Context> for WorkspaceId {
    fn decode<D: Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        <[u8; 16]>::decode(decoder).map(Self)
    }
}

bincode::impl_borrow_decode!(WorkspaceId);
