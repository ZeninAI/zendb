//! Backend — the contract between the engine and the underlying storage.
//!
//! ## Design summary
//!
//! - **Generic** over `K` and `V`. Both are serialized through rkyv into the
//!   backend's byte storage (mmap, in our case).
//! - **Reads of values are zero-copy.** `get` and `values`/`entries` return
//!   [`ValueRef`] wrappers — pointer casts into the backend's mmap, no
//!   allocation, no deserialization.
//! - **Keys are returned owned** from iteration. Backends that hold `K` in
//!   memory (KeyDir's HashMap) clone; backends that don't (BPlusTree)
//!   deserialize per item. Either way the caller gets a plain `K` and
//!   doesn't have to think about the archived form.
//! - **Writes consume.** `put` takes `K` and `V` by value, matching
//!   `HashMap::insert`. Caller hands ownership over; backend serializes.
//! - **Read-modify-write** has a first-class [`Backend::update`] combinator
//!   that handles the deserialize → modify → serialize dance.
//! - **Ordered operations** (range scans) live on a separate
//!   [`OrderedBackend`] subtrait so unordered backends cannot silently
//!   fake them.
//!
//! ## Ownership grammar
//!
//! | Form | Meaning |
//! |------|---------|
//! | `&K`              | probe — look this up, caller keeps it |
//! | `K` (parameter)   | transfer — store this, caller is done with it |
//! | `K` (return)      | owned — backend hands you a fresh `K` |
//! | `ValueRef<'_, V>` | borrowed zero-copy view, lives as long as the backend isn't mutated |
//! | `V` (parameter)   | transfer — backend serializes and may drop it |
//!
//! ## Object safety
//!
//! The trait uses `impl Iterator` in return position (RPITIT). It is
//! therefore **not** object-safe — there is no `dyn Backend`. Code that
//! needs runtime dispatch over multiple backend kinds wraps them in a
//! concrete enum that itself implements `Backend`.

use std::{hash::Hash, io};

use rkyv::{
    api::high::{HighDeserializer, HighSerializer},
    rancor::Error as RkyvError,
    ser::{allocator::ArenaHandle, writer::Buffer},
    Archive, Archived, Deserialize, Portable, Serialize,
};

use crate::utils::serdes::{CountingWriter, ValueRef};

// ---------------------------------------------------------------------------
// Backend — semantically common subset across every backend kind.
// ---------------------------------------------------------------------------

/// The common contract every storage backend satisfies.
///
/// Generic over `K` and `V`. The bounds here are the **union** of what
/// every backend implementation needs: `Hash + Eq` (KeyDir's hash index),
/// `Clone` (owned-key iteration), and the full rkyv serialize/deserialize
/// suite for both `K` and `V`.
///
/// Note: no `Ord` on `K` or `Archived<K>`. The BPlusTree navigates by
/// lexicographic comparison of the **archived key bytes** in the mmap,
/// not via `K::cmp` — so the natural Rust ordering isn't needed (and
/// wouldn't be respected anyway). See the module docs in
/// [`crate::core::btree`] for the encoding implications.
///
/// The `Serialize` bound accepts any writer (both [`Buffer`] and
/// [`CountingWriter`]) so backends can measure serialized size before
/// committing bytes to mmap.
pub trait Backend<K, V>
where
    K: Archive + Hash + Eq + Clone,
    V: Archive,
    for<'buf, 'a> K: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> K: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    for<'buf, 'a> V: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> V: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    <V as Archive>::Archived: Portable + Deserialize<V, HighDeserializer<RkyvError>> + 'static,
{
    // ---- reads --------------------------------------------------------

    /// Look up `key`. Returns a [`ValueRef`] borrowing from the backend's
    /// storage; `None` if `key` is absent.
    fn get(&self, key: &K) -> Option<ValueRef<'_, V>>;

    /// Convenience: existence check without materializing a [`ValueRef`].
    fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    // ---- writes -------------------------------------------------------

    /// Insert or overwrite. Both `key` and `value` are consumed.
    fn put(&mut self, key: K, value: V) -> io::Result<()>;

    /// Remove `key`. Returns whether it existed. Does *not* return the
    /// removed value — if you need it, call [`get`](Self::get) first.
    fn delete(&mut self, key: &K) -> io::Result<bool>;

    /// Read-modify-write. Deserializes the value, passes it to `f`,
    /// and serializes the result back. Returns `true` if the key
    /// existed (and was modified), `false` otherwise. When the key is
    /// absent, `f` is never called and no insert happens.
    ///
    /// To insert-on-missing, call [`contains`](Self::contains) first
    /// (or use [`put`](Self::put) directly).
    fn update<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(V) -> V;

    /// In-place archived update. The closure receives `&mut Archived<V>` —
    /// direct mutation of the value's bytes in storage, no deserialize,
    /// no re-serialize. Returns whether the key existed; the closure
    /// only runs when it did.
    ///
    /// # Safety contract
    /// The closure **must not change the byte length** of the archive.
    /// Only size-stable mutations are sound: integer fields, flags,
    /// fixed-width numeric updates. Operations like growing an
    /// `ArchivedVec` or changing an `ArchivedString`'s length will
    /// corrupt the file. The trait can't enforce this — be careful.
    ///
    /// Use this when the cost of `update`'s deserialize/serialize round
    /// trip matters (hot counters, frequently-flipped flags). For
    /// anything structural, use `update`.
    fn update_in_place<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut Archived<V>);

    // ---- iteration (unspecified order) -------------------------------

    /// Iterate all keys. Order is backend-defined. Yields owned `K`.
    fn keys(&self) -> impl Iterator<Item = K> + '_;

    /// Iterate all values. Order is backend-defined. Yields zero-copy
    /// [`ValueRef`]s into the backend's storage.
    fn values(&self) -> impl Iterator<Item = ValueRef<'_, V>> + '_;

    /// Iterate all `(key, value)` pairs. Keys are owned; values are
    /// borrowed [`ValueRef`]s.
    fn entries(&self) -> impl Iterator<Item = (K, ValueRef<'_, V>)> + '_;

    // ---- bookkeeping --------------------------------------------------

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Make all writes durable.
    fn flush(&self) -> io::Result<()>;
}

// ---------------------------------------------------------------------------
// OrderedBackend — adds range and ordered-iteration guarantees.
// ---------------------------------------------------------------------------

/// Extension trait for backends whose iteration is in ascending key
/// order (e.g. B+ trees). Generic code that needs ordering should
/// constrain to `OrderedBackend` rather than `Backend`.
pub trait OrderedBackend<K, V>: Backend<K, V>
where
    K: Archive + Hash + Eq + Clone,
    V: Archive,
    for<'buf, 'a> K: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> K: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    for<'buf, 'a> V: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> V: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    <V as Archive>::Archived: Portable + Deserialize<V, HighDeserializer<RkyvError>> + 'static,
{
    /// Iterate entries with keys in `[start, end)`, ascending. Both bounds
    /// are inclusive-exclusive (like Rust's `..` range). A missing `start`
    /// key starts iteration at the first key ≥ `start`; `end` is a strict
    /// upper bound. The returned iterator yields owned keys and zero-copy
    /// [`ValueRef`] values.
    fn range(&self, start: &K, end: &K) -> impl Iterator<Item = (K, ValueRef<'_, V>)> + '_;
}
