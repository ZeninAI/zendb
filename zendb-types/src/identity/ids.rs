use std::{
    fmt, io,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

const RANDOM_BITS: u32 = 74;
const RANDOM_MASK: u128 = (1u128 << RANDOM_BITS) - 1;
const MAX_TIMESTAMP_MS: u64 = (1u64 << 48) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdParseError;

impl fmt::Display for IdParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("expected a canonical 128-bit UUID identifier")
    }
}

impl std::error::Error for IdParseError {}

#[derive(Debug, Clone, Copy)]
struct GeneratorState {
    timestamp_ms: u64,
    random: u128,
}

/// Process-local UUIDv7 generator with monotonic output during equal or
/// backwards-moving wall-clock milliseconds.
#[derive(Debug)]
pub struct EntityIdGenerator {
    state: Mutex<Option<GeneratorState>>,
}

impl Default for EntityIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl EntityIdGenerator {
    pub const fn new() -> Self {
        Self {
            state: Mutex::new(None),
        }
    }

    pub fn generate(&self) -> io::Result<[u8; 16]> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| io::Error::other(error.to_string()))?
            .as_millis()
            .try_into()
            .map_err(|_| io::Error::other("system time exceeds UUIDv7 timestamp range"))?;
        self.generate_at(now)
    }

    /// Generate using a caller-supplied wall clock, primarily for deterministic
    /// tests and embedders with an existing clock source.
    pub fn generate_at(&self, now_ms: u64) -> io::Result<[u8; 16]> {
        if now_ms > MAX_TIMESTAMP_MS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timestamp exceeds UUIDv7 48-bit millisecond range",
            ));
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("entity ID generator lock poisoned"))?;
        let next = match *state {
            Some(previous) if now_ms <= previous.timestamp_ms => {
                if previous.random == RANDOM_MASK {
                    if previous.timestamp_ms == MAX_TIMESTAMP_MS {
                        return Err(io::Error::other("UUIDv7 generator exhausted"));
                    }
                    GeneratorState {
                        timestamp_ms: previous.timestamp_ms + 1,
                        random: random_74_bits()?,
                    }
                } else {
                    GeneratorState {
                        timestamp_ms: previous.timestamp_ms,
                        random: previous.random + 1,
                    }
                }
            }
            _ => GeneratorState {
                timestamp_ms: now_ms,
                random: random_74_bits()?,
            },
        };
        *state = Some(next);
        Ok(encode_uuid_v7(next.timestamp_ms, next.random))
    }
}

fn random_74_bits() -> io::Result<u128> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
    Ok(u128::from_be_bytes(bytes) & RANDOM_MASK)
}

const fn encode_uuid_v7(timestamp_ms: u64, random: u128) -> [u8; 16] {
    let rand_a = (random >> 62) & 0x0fff;
    let rand_b = random & ((1u128 << 62) - 1);
    let encoded = ((timestamp_ms as u128) << 80)
        | (0x7u128 << 76)
        | (rand_a << 64)
        | (0b10u128 << 62)
        | rand_b;
    encoded.to_be_bytes()
}

pub(crate) fn generate_entity_id() -> io::Result<[u8; 16]> {
    static GENERATOR: OnceLock<EntityIdGenerator> = OnceLock::new();
    GENERATOR.get_or_init(EntityIdGenerator::new).generate()
}

pub const fn entity_id_timestamp_ms(bytes: &[u8; 16]) -> u64 {
    ((bytes[0] as u64) << 40)
        | ((bytes[1] as u64) << 32)
        | ((bytes[2] as u64) << 24)
        | ((bytes[3] as u64) << 16)
        | ((bytes[4] as u64) << 8)
        | bytes[5] as u64
}

pub fn format_entity_id(bytes: &[u8; 16], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            formatter.write_str("-")?;
        }
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

pub fn parse_entity_id(value: &str) -> Result<[u8; 16], IdParseError> {
    let canonical = value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        });
    let compact_form = value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    if !canonical && !compact_form {
        return Err(IdParseError);
    }

    let mut compact = [0u8; 32];
    let mut length = 0;
    for byte in value.bytes() {
        if byte == b'-' {
            continue;
        }
        if length == compact.len() || !byte.is_ascii_hexdigit() {
            return Err(IdParseError);
        }
        compact[length] = byte;
        length += 1;
    }
    if length != compact.len() {
        return Err(IdParseError);
    }

    let mut bytes = [0u8; 16];
    for (index, output) in bytes.iter_mut().enumerate() {
        let high = hex(compact[index * 2]).ok_or(IdParseError)?;
        let low = hex(compact[index * 2 + 1]).ok_or(IdParseError)?;
        *output = (high << 4) | low;
    }
    Ok(bytes)
}

const fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

define_entity_id!(DeviceId);
define_entity_id!(WorkspaceId);
define_entity_id!(OperatorId);
define_entity_id!(EnrollmentTicketId);
define_label!(CapabilityId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_monotonic_and_expose_the_timestamp() {
        let generator = EntityIdGenerator::new();
        let first = DeviceId(generator.generate_at(1_700_000_000_000).unwrap());
        let second = DeviceId(generator.generate_at(1_700_000_000_000).unwrap());
        let rollback = DeviceId(generator.generate_at(1_699_999_999_000).unwrap());

        assert!(first < second && second < rollback);
        assert_eq!(first.created_at_ms(), 1_700_000_000_000);
        assert_eq!(rollback.created_at_ms(), 1_700_000_000_000);
    }

    #[test]
    fn display_and_parse_round_trip() {
        let id = WorkspaceId::generate().unwrap();
        assert_eq!(WorkspaceId::parse(&id.to_string()).unwrap(), id);
        assert!(WorkspaceId::parse("0000000-00000-0000-0000-000000000000").is_err());
    }
}
