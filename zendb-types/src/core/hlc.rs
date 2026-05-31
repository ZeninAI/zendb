//! Hybrid Logical Clock — 10-byte causal timestamp.
//!
//! ## Layout (big-endian)
//!
//! ```text
//! Bytes 0–5:  physical_ms   48-bit, milliseconds since UNIX epoch
//! Bytes 6–7:  logical       16-bit, monotonic counter per ms
//! Bytes 8–9:  node_id       16-bit, unique node identifier
//! ```
//!
//! Big-endian so a raw `[u8]::cmp` yields correct HLC ordering:
//! `physical_ms` desc → `logical` desc → `node_id` desc.
//!
//! ## Sentinel
//!
//! `Hlc::ZERO` is the all-zero value. Used for dummy cells created during the
//! apply walk. Any real HLC beats `ZERO`. A real generator must never produce
//! `ZERO` (node_id of 0 is invalid).

use bincode::{Decode, Encode};

/// A 10-byte Hybrid Logical Clock.
///
/// Comparison is lexicographic on the big-endian byte representation,
/// which means greater = later = "beats".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct Hlc([u8; 10]);

impl Hlc {
    /// The all-zero sentinel. Every real HLC beats this.
    pub const ZERO: Hlc = Hlc([0u8; 10]);

    // --- constructors ---

    /// Build an HLC from its components.
    ///
    /// Returns `None` if `physical_ms` exceeds 48 bits (max ~8.9 million years).
    pub const fn new(physical_ms: u64, logical: u16, node_id: u16) -> Option<Hlc> {
        if physical_ms > 0xFFFF_FFFF_FFFF {
            return None;
        }
        let p = physical_ms.to_be_bytes(); // [u8; 8]
        let l = logical.to_be_bytes(); // [u8; 2]
        let n = node_id.to_be_bytes(); // [u8; 2]
        Some(Hlc([
            p[2], p[3], p[4], p[5], p[6], p[7], // low 6 bytes of physical_ms
            l[0], l[1], // logical
            n[0], n[1], // node_id
        ]))
    }

    /// View as raw bytes (big-endian).
    pub const fn as_bytes(&self) -> &[u8; 10] {
        &self.0
    }

    /// Construct from raw bytes (big-endian).
    pub const fn from_bytes(bytes: [u8; 10]) -> Hlc {
        Hlc(bytes)
    }

    // --- accessors ---

    /// Physical component: milliseconds since UNIX epoch (48-bit).
    pub fn physical_ms(&self) -> u64 {
        let mut buf = [0u8; 8];
        buf[2..8].copy_from_slice(&self.0[0..6]);
        u64::from_be_bytes(buf)
    }

    /// Logical component: monotonic counter (16-bit, 0–65535).
    pub fn logical(&self) -> u16 {
        let mut buf = [0u8; 2];
        buf.copy_from_slice(&self.0[6..8]);
        u16::from_be_bytes(buf)
    }

    /// Node identifier (16-bit). Uniqueness is the operator's responsibility.
    pub fn node_id(&self) -> u16 {
        let mut buf = [0u8; 2];
        buf.copy_from_slice(&self.0[8..10]);
        u16::from_be_bytes(buf)
    }

    // --- ordering helpers ---

    /// True if `self` is strictly later than `other`.
    ///
    /// This is equivalent to `self > other` but conveys intent more clearly.
    pub fn beats(&self, other: Hlc) -> bool {
        self > &other
    }
}

// --- formatting ---

impl std::fmt::Debug for Hlc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hlc")
            .field("physical_ms", &self.physical_ms())
            .field("logical", &self.logical())
            .field("node_id", &self.node_id())
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
            self.node_id()
        )
    }
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_sentinel() {
        assert_eq!(Hlc::ZERO.physical_ms(), 0);
        assert_eq!(Hlc::ZERO.logical(), 0);
        assert_eq!(Hlc::ZERO.node_id(), 0);
    }

    #[test]
    fn ordering_physical_dominates() {
        let a = Hlc::new(100, 0, 1).unwrap();
        let b = Hlc::new(200, 0, 1).unwrap();
        assert!(b.beats(a));
        assert!(!a.beats(b));
    }

    #[test]
    fn ordering_logical_tiebreaker() {
        let a = Hlc::new(100, 5, 1).unwrap();
        let b = Hlc::new(100, 10, 1).unwrap();
        assert!(b.beats(a));
    }

    #[test]
    fn ordering_node_id_tiebreaker() {
        let a = Hlc::new(100, 5, 1).unwrap();
        let b = Hlc::new(100, 5, 2).unwrap();
        assert!(b.beats(a));
    }

    #[test]
    fn zero_beaten_by_anything() {
        let real = Hlc::new(1, 0, 1).unwrap();
        assert!(real.beats(Hlc::ZERO));
        assert!(!Hlc::ZERO.beats(real));
    }

    #[test]
    fn roundtrip_bytes() {
        let h = Hlc::new(0x1234_5678_9ABC, 0xABCD, 0xBEEF).unwrap();
        let bytes = *h.as_bytes();
        let h2 = Hlc::from_bytes(bytes);
        assert_eq!(h, h2);
        assert_eq!(h.physical_ms(), 0x1234_5678_9ABC);
        assert_eq!(h.logical(), 0xABCD);
        assert_eq!(h.node_id(), 0xBEEF);
    }

    #[test]
    fn max_physical() {
        let h = Hlc::new(0xFFFF_FFFF_FFFF, 0, 1).unwrap();
        assert_eq!(h.physical_ms(), 0xFFFF_FFFF_FFFF);
    }

    #[test]
    fn overflow_physical_rejected() {
        assert!(Hlc::new(0x1_0000_0000_0000, 0, 1).is_none());
    }
}
