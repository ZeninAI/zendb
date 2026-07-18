//! Hybrid Logical Clock with the canonical installation `DeviceId`.
//!
//! ## Layout (big-endian)
//!
//! ```text
//! Bytes 0-5:    physical_ms   48-bit, milliseconds since UNIX epoch
//! Bytes 6-7:    logical       16-bit, monotonic counter per millisecond
//! Bytes 8-23:   device_id     canonical 128-bit replica identity
//! ```
//!
//! The full device identity is kept in the HLC. A shorter derived suffix would
//! create an avoidable collision domain for CRDT element identifiers.

use std::sync::OnceLock;

use bincode::{Decode, Encode};

use crate::{
    CellCodecError, CellCodecKey, CrdtCodec, DefaultCrdtCodec, DeviceId, PrimaryKey, Value,
};

/// Compatibility name for code that treats the HLC device component as a
/// separate type. It is intentionally an alias, not a second identity.
pub type HlcDeviceId = DeviceId;

// This fallback exists for old convenience APIs and tests. A real replica must
// load its persisted DeviceId and call `Hlc::with_device_id` instead.
static PROCESS_DEVICE_ID: OnceLock<DeviceId> = OnceLock::new();

/// Initialize the process fallback with a fresh CSPRNG-generated identity.
/// This is no longer derived from a hardware or OS machine identifier.
pub fn init_device_id() {
    PROCESS_DEVICE_ID
        .get_or_init(|| DeviceId::generate().expect("failed to generate process device id"));
}

/// Return the process fallback identity.
///
/// Production code should prefer an explicitly persisted replica identity.
pub fn device_id() -> DeviceId {
    *PROCESS_DEVICE_ID
        .get_or_init(|| DeviceId::generate().expect("failed to generate process device id"))
}

/// A 24-byte Hybrid Logical Clock.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct Hlc([u8; 24]);

pub struct HlcCodec;

impl CrdtCodec for HlcCodec {
    type Rust = Hlc;

    fn encode(value: &Hlc, _hlc: Hlc) -> Value {
        Value::Blob(value.as_bytes().to_vec().into())
    }

    fn decode(value: &Value) -> Result<Hlc, CellCodecError> {
        let Value::Blob(value) = value else {
            return Err(CellCodecError::expected("Blob"));
        };
        let bytes = value
            .as_slice()
            .try_into()
            .map_err(|_| CellCodecError::expected("24-byte HLC Blob"))?;
        Ok(Hlc::from_bytes(bytes))
    }
}

impl DefaultCrdtCodec for Hlc {
    type Codec = HlcCodec;
}

impl CellCodecKey for Hlc {
    fn to_primary_key(&self) -> PrimaryKey {
        PrimaryKey::Blob(self.as_bytes().to_vec().into())
    }

    fn from_primary_key(key: &PrimaryKey) -> Result<Self, CellCodecError> {
        let PrimaryKey::Blob(value) = key else {
            return Err(CellCodecError::expected("HLC Blob primary key"));
        };
        let bytes = value
            .as_slice()
            .try_into()
            .map_err(|_| CellCodecError::expected("24-byte HLC Blob primary key"))?;
        Ok(Hlc::from_bytes(bytes))
    }
}

impl Hlc {
    pub const ZERO: Hlc = Hlc([0u8; 24]);

    /// Construct an HLC using the process fallback identity.
    ///
    /// This method remains convenient for local tests. A database replica
    /// should use [`Self::with_device_id`] so multiple profiles in one process
    /// cannot accidentally share an identity.
    pub fn new(physical_ms: u64, logical: u16) -> Option<Hlc> {
        Self::with_device_id(physical_ms, logical, device_id())
    }

    /// Construct an HLC from explicit components. This is also used when
    /// decoding or testing remote events.
    pub const fn with_device_id(
        physical_ms: u64,
        logical: u16,
        device_id: HlcDeviceId,
    ) -> Option<Hlc> {
        if physical_ms > 0xFFFF_FFFF_FFFF {
            return None;
        }
        let p = physical_ms.to_be_bytes();
        let l = logical.to_be_bytes();
        let d = device_id.as_bytes();
        Some(Hlc([
            p[2], p[3], p[4], p[5], p[6], p[7], l[0], l[1], d[0], d[1], d[2], d[3], d[4], d[5],
            d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15],
        ]))
    }

    pub const fn as_bytes(&self) -> &[u8; 24] {
        &self.0
    }

    pub const fn from_bytes(bytes: [u8; 24]) -> Hlc {
        Hlc(bytes)
    }

    pub fn physical_ms(&self) -> u64 {
        let mut buf = [0u8; 8];
        buf[2..8].copy_from_slice(&self.0[0..6]);
        u64::from_be_bytes(buf)
    }

    pub fn logical(&self) -> u16 {
        let mut buf = [0u8; 2];
        buf.copy_from_slice(&self.0[6..8]);
        u16::from_be_bytes(buf)
    }

    pub fn device_id(&self) -> DeviceId {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&self.0[8..24]);
        DeviceId::from_bytes(bytes)
    }

    pub fn beats(&self, other: Hlc) -> bool {
        self > &other
    }
}

impl std::fmt::Debug for Hlc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hlc")
            .field("physical_ms", &self.physical_ms())
            .field("logical", &self.logical())
            .field("device_id", &self.device_id())
            .finish()
    }
}

impl std::fmt::Display for Hlc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Hlc({}:{}:{})",
            self.physical_ms(),
            self.logical(),
            self.device_id()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID_1: HlcDeviceId = DeviceId::from_bytes([1u8; 16]);
    const ID_2: HlcDeviceId = DeviceId::from_bytes([2u8; 16]);

    #[test]
    fn zero_is_sentinel() {
        assert_eq!(Hlc::ZERO.physical_ms(), 0);
        assert_eq!(Hlc::ZERO.logical(), 0);
        assert_eq!(Hlc::ZERO.device_id(), DeviceId::ZERO);
    }

    #[test]
    fn ordering_physical_dominates() {
        let a = Hlc::with_device_id(100, 0, ID_1).unwrap();
        let b = Hlc::with_device_id(200, 0, ID_1).unwrap();
        assert!(b.beats(a));
        assert!(!a.beats(b));
    }

    #[test]
    fn ordering_logical_tiebreaker() {
        let a = Hlc::with_device_id(100, 5, ID_1).unwrap();
        let b = Hlc::with_device_id(100, 10, ID_1).unwrap();
        assert!(b.beats(a));
    }

    #[test]
    fn ordering_device_id_tiebreaker() {
        let a = Hlc::with_device_id(100, 5, ID_1).unwrap();
        let b = Hlc::with_device_id(100, 5, ID_2).unwrap();
        assert!(b.beats(a));
    }

    #[test]
    fn roundtrip_bytes() {
        let id = DeviceId::from_bytes([0xBE; 16]);
        let h = Hlc::with_device_id(0x1234_5678_9ABC, 0xABCD, id).unwrap();
        let bytes = *h.as_bytes();
        let h2 = Hlc::from_bytes(bytes);
        assert_eq!(h, h2);
        assert_eq!(h.physical_ms(), 0x1234_5678_9ABC);
        assert_eq!(h.logical(), 0xABCD);
        assert_eq!(h.device_id(), id);
    }

    #[test]
    fn max_physical() {
        assert!(Hlc::with_device_id(0xFFFF_FFFF_FFFF, 0, ID_1).is_some());
        assert!(Hlc::with_device_id(0x1_0000_0000_0000, 0, ID_1).is_none());
    }

    #[test]
    fn generated_device_ids_are_not_zero() {
        let id = DeviceId::generate().unwrap();
        assert_ne!(id, DeviceId::ZERO);
    }
}
