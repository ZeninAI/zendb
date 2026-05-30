//! Serialization / deserialization helpers backed by bincode 2.
//!
//! Three needs covered:
//!
//! 1. **Sizing** without allocating — [`serialized_size`] uses
//!    [`bincode::enc::write::SizeWriter`] to count bytes that would be
//!    written, no allocation, no I/O.
//! 2. **Direct-to-buffer writes** — [`serialize_into`] writes the encoded
//!    archive straight into a caller-supplied `&mut [u8]` (typically a
//!    slice of a memory-mapped file). One pass, no temporary buffer.
//! 3. **Owned roundtrip** — [`serialize_to_vec`] and [`deserialize_from`]
//!    for the cases that need a free-standing `Vec<u8>` or to materialize
//!    a typed value from raw bytes.
//!
//! All helpers use a single shared configuration ([`cfg`]) — little-endian,
//! fixed-int encoding, no decode limit — tuned for the hot inner loops of
//! the storage backends. Byte-level page helpers (`rd_u*` / `wr_u*` /
//! `read_u32_le`) live here too so the page-based formats share one
//! definition.

use std::{cell::RefCell, io};

use bincode::{
    config::{Configuration, Fixint, LittleEndian, NoLimit},
    enc::write::SizeWriter,
    error::{DecodeError, EncodeError},
    Decode, Encode,
};

// ---------------------------------------------------------------------------
// Shared bincode configuration — chosen for fastest encode/decode loops.
// Fixed-int encoding skips varint decoding cost; LE matches native; NoLimit
// avoids per-byte bounds checks on trusted internal data.
// ---------------------------------------------------------------------------

/// Bincode configuration used everywhere in storage. Returned as a typed
/// value (rather than a const) because `Configuration` builder methods
/// aren't const in bincode 2. Constructing it is free — the type itself
/// is zero-sized.
#[inline(always)]
pub fn cfg() -> Configuration<LittleEndian, Fixint, NoLimit> {
    bincode::config::standard()
        .with_little_endian()
        .with_fixed_int_encoding()
}

// ---------------------------------------------------------------------------
// Sizing / writing / reading helpers
// ---------------------------------------------------------------------------

/// Measure the byte length of `value`'s bincode encoding without
/// allocating. Uses [`SizeWriter`] — every byte that would be written is
/// counted and discarded.
pub fn serialized_size<T: Encode>(value: &T) -> io::Result<usize> {
    let mut sw = SizeWriter::default();
    bincode::encode_into_writer(value, &mut sw, cfg()).map_err(encode_err)?;
    Ok(sw.bytes_written)
}

/// Serialize `value` directly into `dst`, returning the number of bytes
/// written. Caller is responsible for sizing `dst` correctly — call
/// [`serialized_size`] first when the length isn't already known.
///
/// Used by backends to write straight into `mmap`, eliminating the
/// intermediate `Vec<u8>` + `copy_from_slice` step.
pub fn serialize_into<T: Encode>(value: &T, dst: &mut [u8]) -> io::Result<usize> {
    bincode::encode_into_slice(value, dst, cfg()).map_err(encode_err)
}

/// Serialize `value` into any [`io::Write`] destination, returning the
/// number of bytes written. Unlike [`serialize_into`], the destination
/// grows or streams as needed.
pub fn serialize_into_std<T: Encode, W: io::Write>(value: &T, dst: &mut W) -> io::Result<usize> {
    bincode::encode_into_std_write(value, dst, cfg()).map_err(encode_err)
}

/// Serialize `value` into a freshly-allocated `Vec<u8>`. Used when a
/// stand-alone byte buffer is needed (e.g., BPlusTree key navigation
/// scratch where the buffer's lifetime outlives the encode).
pub fn serialize_to_vec<T: Encode>(value: &T) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(value, cfg()).map_err(encode_err)
}

thread_local! {
    /// Per-thread scratch buffer used by [`with_scratch`]. The buffer
    /// retains its capacity across calls so the common case (point lookup,
    /// `put`, `delete`) avoids the per-call allocation that
    /// `serialize_to_vec` would otherwise perform.
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Encode a value into a thread-local scratch buffer and pass the resulting
/// byte slice to `f`. The scratch buffer is reused across calls, so the hot
/// path (`get`/`put`/`delete`/`contains`/`update`) avoids allocating a
/// fresh `Vec<u8>` on every invocation.
///
/// `f` must not recursively call `with_scratch` — the inner call would
/// panic on a double `RefCell::borrow_mut`. None of the storage hot paths
/// recurse, so this restriction is invisible in practice.
pub fn with_scratch<T, F, R>(value: &T, f: F) -> io::Result<R>
where
    T: Encode,
    F: FnOnce(&[u8]) -> io::Result<R>,
{
    SCRATCH.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        let written = serialize_into_std(value, &mut *buf)?;
        f(&buf[..written])
    })
}

/// Decode a value from `src`. Returns the decoded value, discarding the
/// trailing byte count.
pub fn deserialize_from<T: Decode<()>>(src: &[u8]) -> io::Result<T> {
    bincode::decode_from_slice(src, cfg())
        .map(|(value, _bytes_read)| value)
        .map_err(decode_err)
}

#[inline]
fn encode_err(e: EncodeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

#[inline]
fn decode_err(e: DecodeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

// ---------------------------------------------------------------------------
// Byte-level read/write helpers — shared across page-based formats.
// ---------------------------------------------------------------------------

#[inline]
pub(crate) fn rd_u16(b: &[u8]) -> u16 {
    u16::from_le_bytes([b[0], b[1]])
}

#[inline]
pub(crate) fn rd_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

#[inline]
pub(crate) fn rd_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

#[inline]
pub(crate) fn wr_u16(b: &mut [u8], v: u16) {
    b[..2].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub(crate) fn wr_u32(b: &mut [u8], v: u32) {
    b[..4].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub(crate) fn wr_u64(b: &mut [u8], v: u64) {
    b[..8].copy_from_slice(&v.to_le_bytes());
}

/// Read a little-endian u32 from `slice` at `pos`, returning `None` if the
/// slice doesn't have four bytes available at that offset.
#[inline]
pub(crate) fn read_u32_le(slice: &[u8], pos: usize) -> Option<u32> {
    slice
        .get(pos..pos + 4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
}
