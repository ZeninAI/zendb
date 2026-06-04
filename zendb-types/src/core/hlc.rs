//! Hybrid Logical Clock - 16-byte causal timestamp.
//!
//! ## Layout (big-endian)
//!
//! ```text
//! Bytes 0-5:   physical_ms   48-bit, milliseconds since UNIX epoch
//! Bytes 6-7:   logical       16-bit, monotonic counter per ms
//! Bytes 8-15:  device_id     64-bit, machine-derived node identity
//! ```
//!
//! Big-endian component encoding keeps raw byte comparison aligned with HLC
//! ordering: `physical_ms`, then `logical`, then `device_id`.

use bincode::{Decode, Encode};

/// A 64-bit device identifier derived from the OS machine UID.
pub type DeviceId = [u8; 8];

// Initialized once through `init_device_id`, then read directly by HLC
// construction. This intentionally avoids atomics on the hot path.
static mut DEVICE_ID: DeviceId = [0u8; 8];
static mut DEVICE_ID_INITIALIZED: bool = false;

/// Initialize the process-global device id from the OS machine UID.
///
/// The machine UID string is hashed with BLAKE3 and truncated to 64 bits.
/// This function is idempotent; later calls keep the first initialized value.
///
/// # Panics
///
/// Panics if the OS machine UID cannot be read on the first call.
pub fn init_device_id() {
    unsafe {
        if DEVICE_ID_INITIALIZED {
            return;
        }
    }

    let raw = machine_uid::get().expect("failed to read OS machine UID");
    let hash = blake3::hash(raw.as_bytes());
    let mut id = [0u8; 8];
    id.copy_from_slice(&hash.as_bytes()[..8]);

    unsafe {
        DEVICE_ID = id;
        DEVICE_ID_INITIALIZED = true;
    }
}

/// Return the initialized process-global device id.
///
/// # Panics
///
/// Panics if [`init_device_id`] has not been called.
pub fn device_id() -> DeviceId {
    unsafe {
        assert!(
            DEVICE_ID_INITIALIZED,
            "device_id() called before init_device_id()"
        );
        DEVICE_ID
    }
}

/// A 16-byte Hybrid Logical Clock.
///
/// Comparison is lexicographic on the big-endian byte representation,
/// which means greater = later = "beats".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct Hlc([u8; 16]);

impl Hlc {
    /// The all-zero sentinel. Every real HLC beats this.
    pub const ZERO: Hlc = Hlc([0u8; 16]);

    /// Build an HLC from physical/logical clock components and the global
    /// device id initialized through [`init_device_id`].
    ///
    /// Returns `None` if `physical_ms` exceeds 48 bits.
    ///
    /// # Panics
    ///
    /// Panics if [`init_device_id`] has not been called.
    pub fn new(physical_ms: u64, logical: u16) -> Option<Hlc> {
        Self::with_device_id(physical_ms, logical, device_id())
    }

    /// Build an HLC with an explicit device id. This is useful for tests and
    /// for reconstructing clocks from externally supplied components.
    pub const fn with_device_id(
        physical_ms: u64,
        logical: u16,
        device_id: DeviceId,
    ) -> Option<Hlc> {
        if physical_ms > 0xFFFF_FFFF_FFFF {
            return None;
        }
        let p = physical_ms.to_be_bytes();
        let l = logical.to_be_bytes();
        Some(Hlc([
            p[2],
            p[3],
            p[4],
            p[5],
            p[6],
            p[7],
            l[0],
            l[1],
            device_id[0],
            device_id[1],
            device_id[2],
            device_id[3],
            device_id[4],
            device_id[5],
            device_id[6],
            device_id[7],
        ]))
    }

    /// View as raw bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Construct from raw bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Hlc {
        Hlc(bytes)
    }

    /// Physical component: milliseconds since UNIX epoch.
    pub fn physical_ms(&self) -> u64 {
        let mut buf = [0u8; 8];
        buf[2..8].copy_from_slice(&self.0[0..6]);
        u64::from_be_bytes(buf)
    }

    /// Logical component: monotonic counter.
    pub fn logical(&self) -> u16 {
        let mut buf = [0u8; 2];
        buf.copy_from_slice(&self.0[6..8]);
        u16::from_be_bytes(buf)
    }

    /// Device component.
    pub fn device_id(&self) -> DeviceId {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.0[8..16]);
        buf
    }

    /// True if `self` is strictly later than `other`.
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
            "Hlc({}:{}:{:016x})",
            self.physical_ms(),
            self.logical(),
            u64::from_be_bytes(self.device_id())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID_1: DeviceId = [1u8; 8];
    const ID_2: DeviceId = [2u8; 8];

    #[test]
    fn zero_is_sentinel() {
        assert_eq!(Hlc::ZERO.physical_ms(), 0);
        assert_eq!(Hlc::ZERO.logical(), 0);
        assert_eq!(Hlc::ZERO.device_id(), [0u8; 8]);
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
    fn zero_beaten_by_anything() {
        let real = Hlc::with_device_id(1, 0, ID_1).unwrap();
        assert!(real.beats(Hlc::ZERO));
        assert!(!Hlc::ZERO.beats(real));
    }

    #[test]
    fn roundtrip_bytes() {
        let id = [0xBE, 0xEF, 0xCA, 0xFE, 0x12, 0x34, 0x56, 0x78];
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
        let h = Hlc::with_device_id(0xFFFF_FFFF_FFFF, 0, ID_1).unwrap();
        assert_eq!(h.physical_ms(), 0xFFFF_FFFF_FFFF);
    }

    #[test]
    fn overflow_physical_rejected() {
        assert!(Hlc::with_device_id(0x1_0000_0000_0000, 0, ID_1).is_none());
    }

    #[test]
    fn init_device_id_is_idempotent() {
        init_device_id();
        let first = device_id();
        init_device_id();
        let second = device_id();
        assert_eq!(first, second);
    }

    #[test]
    fn new_uses_initialized_device_id() {
        init_device_id();
        let id = device_id();
        let h = Hlc::new(100, 2).unwrap();
        assert_eq!(h.device_id(), id);
    }
}
