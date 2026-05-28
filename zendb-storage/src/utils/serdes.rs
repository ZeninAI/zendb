//! Serialization / deserialization utilities shared across storage backends.
//!
//! The helpers here cover three needs:
//!
//! 1. **Sizing** a value's archived form without allocating ([`serialized_size`]).
//! 2. **Writing** a value's archive directly into a caller-provided byte slice
//!    ([`serialize_into`]). Backends use this to write straight into `mmap`.
//! 3. **Materializing** a small scratch buffer of serialized bytes when a
//!    backend needs the bytes for comparison/lookup ([`serialize_to_vec`]).
//!
//! Together they let backends keep K/V serialization to one pass with at most
//! one heap allocation — no AlignedVec, no grow-and-retry serialize loops.
//!
//! Byte-level page helpers (`rd_u*` / `wr_u*` / `read_u32_le`) also live here
//! so the page-based formats (B+Tree, KeyDir) can share a single source of
//! truth.

use std::{io, marker::PhantomData, ops::Deref};

use rkyv::{
    api::high::{to_bytes_in, HighDeserializer, HighSerializer},
    rancor::{Error as RkyvError, Fallible},
    ser::{allocator::ArenaHandle, writer::Buffer, Positional, Writer},
    Archive, Archived, Deserialize, Portable, Serialize,
};

// ---------------------------------------------------------------------------
// CountingWriter — measures serialized byte length without allocating.
// ---------------------------------------------------------------------------

/// A [`Writer`](rkyv::ser::Writer) that discards all data and only counts
/// how many bytes were written. Use with [`to_bytes_in`](rkyv::api::high::to_bytes_in)
/// to determine the serialized size of a value before writing it to mmap.
///
/// # Example
///
/// ```ignore
/// let mut cw = CountingWriter::new();
/// let cw = to_bytes_in::<_, RkyvError>(&my_value, cw)?;
/// let size = cw.count();
/// ```
pub struct CountingWriter {
    count: usize,
}

impl CountingWriter {
    pub fn new() -> Self {
        Self { count: 0 }
    }

    /// The total number of bytes written so far.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl Fallible for CountingWriter {
    type Error = RkyvError;
}

impl Positional for CountingWriter {
    fn pos(&self) -> usize {
        self.count
    }
}

impl Writer for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<(), RkyvError> {
        self.count += bytes.len();
        Ok(())
    }
}

impl Default for CountingWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ValueRef — borrowed view of an archived value living in backend storage.
// ---------------------------------------------------------------------------

/// A borrowed reference to an archived value living in the backend's
/// storage (mmap). Zero-copy on construction and on reads (via `Deref`).
///
/// - As an `&Archived<V>` directly — `ValueRef` derefs to it transparently.
/// - Call [`to_owned`](Self::to_owned) to materialize a real `V`.
/// - Call [`as_bytes`](Self::as_bytes) to expose the raw archive bytes
///   (replication, hashing, etc.).
pub struct ValueRef<'a, V: Archive> {
    bytes: &'a [u8],
    _marker: PhantomData<&'a Archived<V>>,
}

impl<'a, V: Archive> ValueRef<'a, V>
where
    V::Archived: Portable + 'static,
{
    /// Construct from a slice known to contain a valid archive of `V`.
    ///
    /// # Safety invariant
    /// The byte slice must be a complete, valid rkyv archive of `V`.
    /// Backends maintain this invariant for any slice they hand to
    /// `from_bytes`.
    pub(crate) fn from_bytes(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _marker: PhantomData,
        }
    }

    /// Zero-copy view of the archived value.
    pub fn archived(&self) -> &Archived<V> {
        // SAFETY: from_bytes invariant — `self.bytes` is a valid archive of V.
        unsafe { rkyv::access_unchecked::<Archived<V>>(self.bytes) }
    }

    /// Raw archive bytes. Useful for replication, hashing, or any other
    /// byte-level handling that doesn't need the typed view.
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes
    }

    /// Deserialize into an owned `V`. Allocates and traverses the archive.
    pub fn to_owned(&self) -> V
    where
        V::Archived: Deserialize<V, HighDeserializer<RkyvError>>,
    {
        rkyv::deserialize::<V, RkyvError>(self.archived())
            .expect("archive bytes produced by backend deserialize successfully")
    }
}

impl<'a, V: Archive> Deref for ValueRef<'a, V>
where
    V::Archived: Portable + 'static,
{
    type Target = Archived<V>;
    fn deref(&self) -> &Archived<V> {
        self.archived()
    }
}

// ---------------------------------------------------------------------------
// Sizing / writing helpers
// ---------------------------------------------------------------------------

/// Measure the rkyv-archived byte length of `value` without allocating.
///
/// Uses [`CountingWriter`] under the hood — every byte that would be written
/// is counted but discarded. Pair with [`serialize_into`] to write directly
/// into a pre-sized destination (mmap slot, extent page, etc.).
pub fn serialized_size<T>(value: &T) -> io::Result<usize>
where
    T: for<'a> Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
{
    let cw = to_bytes_in::<_, RkyvError>(value, CountingWriter::new())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(cw.count())
}

/// Serialize `value` directly into `dst`, returning the number of bytes
/// written. The caller is responsible for sizing `dst` correctly — call
/// [`serialized_size`] first when the length isn't already known.
///
/// Backends use this to write straight into `mmap`, eliminating the
/// intermediate `AlignedVec` + `copy_from_slice` step.
pub fn serialize_into<T>(value: &T, dst: &mut [u8]) -> io::Result<usize>
where
    T: for<'buf, 'a> Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
{
    let written = to_bytes_in::<_, RkyvError>(value, Buffer::from(dst))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(written.len())
}

/// Serialize `value` into a tight `Vec<u8>` — one allocation sized exactly
/// to the archive. Use only when raw archived bytes are needed transiently
/// (e.g. B+Tree key comparison during navigation); for storage, prefer
/// [`serialize_into`] writing straight into the destination.
pub fn serialize_to_vec<T>(value: &T) -> io::Result<Vec<u8>>
where
    T: for<'a> Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>
        + for<'buf, 'a> Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
{
    let size = serialized_size(value)?;
    let mut buf = vec![0u8; size];
    let written = serialize_into(value, &mut buf)?;
    debug_assert_eq!(written, size, "CountingWriter and Buffer disagreed on size");
    Ok(buf)
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
