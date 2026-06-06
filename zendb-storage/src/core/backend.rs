//! Backend — the contract between the engine and the underlying storage.
//!
//! ## Design summary
//!
//! - **Generic** over `K` and `V`. Both are serialized through bincode
//!   into the backend's byte storage (mmap'd files).
//! - **Reads return owned values.** `get` produces an `Option<V>` by
//!   deserializing from the backing bytes. Bincode is not a zero-copy
//!   format — every read materializes.
//! - **Writes consume.** `put` takes `K` and `V` by value, matching
//!   `HashMap::insert`. The backend serializes and may drop both.
//! - **Read-modify-write** has a first-class [`Backend::update`] combinator
//!   that handles the deserialize → modify → serialize dance. The trait
//!   provides a default implementation; backends with cheaper paths
//!   (e.g. KeyDir tracks dead bytes inline) should override.
//! - **Bulk operations** ([`bulk_put`](Backend::bulk_put),
//!   [`bulk_put_sorted`](Backend::bulk_put_sorted),
//!   [`bulk_delete`](Backend::bulk_delete)) default to looping the
//!   single-item variants; backends with optimized fast paths
//!   (BPlusTree's future bottom-up bulk-load) override the relevant one.
//! - **Writeback** is two-tier: [`flush`](Backend::flush) schedules OS
//!   writeback (async); [`sync`](Backend::sync) waits for it. This layer
//!   does not provide crash recovery, corruption repair, or multi-page
//!   atomicity.
//! - **Ordered operations** (range scans, `first`/`last`/`entries_rev`)
//!   live on the [`OrderedBackend`] subtrait so unordered backends can't
//!   silently fake them.
//!
//! ## Ownership grammar
//!
//! | Form | Meaning |
//! |------|---------|
//! | `&K`            | probe — look this up, caller keeps it |
//! | `K` (parameter) | transfer — store this, caller is done with it |
//! | `K` (return)    | owned — backend hands you a fresh `K` |
//! | `V` (return)    | owned — backend hands you a fresh `V` (deserialized) |
//! | `V` (parameter) | transfer — backend serializes and may drop it |
//!
//! ## Object safety
//!
//! The trait uses `impl Iterator` in return position (RPITIT). It is
//! therefore **not** object-safe — there is no `dyn Backend`. Code that
//! needs runtime dispatch over multiple backend kinds wraps them in a
//! concrete enum that itself implements `Backend`.

use std::{borrow::Cow, hash::Hash, io};

use bincode::{Decode, Encode};

// ---------------------------------------------------------------------------
// Backend — semantically common subset across every backend kind.
// ---------------------------------------------------------------------------

/// The common contract every storage backend satisfies.
///
/// Generic over `K` and `V`. The bounds are the union of what every
/// backend implementation needs: `Hash + Eq` (KeyDir's hash index),
/// `Clone` on both K and V (in-memory backends like OrderLog hold owned
/// `(K, V)` and need to hand cloned copies to callers; the `update`
/// default impl also clones the key on the insert path), plus bincode's
/// `Encode + Decode<()>` for both K and V.
///
/// No `Ord` on `K` for the common backend surface. `Ord` only appears on
/// [`OrderedBackend`], where each implementation documents what its
/// "ascending key order" means.
pub trait Backend<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone,
    V: Encode + Decode<()> + Clone,
{
    /// Cheap backend-specific metrics view. Implementations keep the
    /// underlying stats in the backend object and update them as state changes.
    type Stats<'a>
    where
        Self: 'a;

    /// Backend configuration values. Set once at construction, read
    /// through `config()`. Immutable after creation.
    type Config: Clone + Default + Encode + Decode<()>;

    // ---- reads --------------------------------------------------------

    /// Look up `key`. Returns the value (borrowed when the backend holds
    /// it in memory, owned when materialized from bytes), or `None` if
    /// absent.
    fn get(&self, key: &K) -> Option<Cow<'_, V>>;

    /// Existence check. Default implementation calls `get`; backends
    /// with a cheaper presence check (e.g., KeyDir's in-memory index)
    /// should override.
    fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    // ---- writes -------------------------------------------------------

    /// Insert or overwrite. Both `key` and `value` are consumed.
    fn put(&mut self, key: K, value: V) -> io::Result<()>;

    /// Insert iff `key` is currently absent. Returns `true` if the
    /// value was inserted, `false` if the key was already present (no
    /// change made). Default impl: `contains` then `put`; backends with
    /// a cheaper atomic primitive may override.
    fn put_if_absent(&mut self, key: K, value: V) -> io::Result<bool> {
        if self.contains(&key) {
            Ok(false)
        } else {
            self.put(key, value)?;
            Ok(true)
        }
    }

    /// Insert `(key, value)`, returning the previous value if the key
    /// existed (matching `HashMap::insert` semantics). For
    /// fire-and-forget overwrite use [`put`](Self::put); for
    /// insert-only-if-absent use [`put_if_absent`](Self::put_if_absent).
    ///
    /// Wraps the prior value in `Cow` for consistency with the other
    /// retrieval methods. In practice every backend's `replace` returns
    /// `Cow::Owned`: the old value is unconditionally evicted from the
    /// backend, so there's nothing left to borrow against.
    fn replace(&mut self, key: K, value: V) -> io::Result<Option<Cow<'_, V>>> {
        let old = self.get(&key).map(|c| Cow::Owned(c.into_owned()));
        self.put(key, value)?;
        Ok(old)
    }

    /// Insert every item from `items` (in iterator order). Default
    /// loops [`put`](Self::put); ordered backends with bulk-load fast
    /// paths should override [`bulk_put_sorted`](Self::bulk_put_sorted)
    /// instead and leave this as the unsorted fallback.
    fn bulk_put<I>(&mut self, items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        for (k, v) in items {
            self.put(k, v)?;
        }
        Ok(())
    }

    /// Insert every item from a **key-sorted** iterator. Caller
    /// guarantees ascending key order. Default loops [`put`](Self::put);
    /// backends that can build a tree bottom-up (BPlusTree's future
    /// bulk-load) override this for an O(n) construction instead of
    /// N × O(log n).
    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        self.bulk_put(sorted)
    }

    /// Remove `key`. Returns whether it existed. Does *not* return the
    /// removed value — if you need it, call [`get`](Self::get) first.
    fn delete(&mut self, key: &K) -> io::Result<bool>;

    /// Remove every key the iterator yields. Returns the number of
    /// keys that were actually present (and removed). Default loops
    /// [`delete`](Self::delete).
    fn bulk_delete<'a, I>(&mut self, keys: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let mut n = 0;
        for k in keys {
            if self.delete(k)? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Remove every key from a **key-sorted** iterator. Caller
    /// guarantees ascending key order. Returns the number of keys that
    /// were actually present (and removed). Default loops
    /// [`delete`](Self::delete); ordered backends with bottom-up
    /// bulk-delete fast paths (BPlusTree's range-trim) should override
    /// for an O(n) sweep instead of N × O(log n).
    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        self.bulk_delete(sorted)
    }

    /// Unified read-modify-write / insert / delete primitive.
    ///
    /// `f` receives the current value (`Some`) or `None` if the key is
    /// absent, and returns the desired new state:
    ///
    /// | input → output      | effect    |
    /// |---------------------|-----------|
    /// | `Some(v) → Some(v')`| overwrite |
    /// | `None    → Some(v)` | insert    |
    /// | `Some(v) → None`    | delete    |
    /// | `None    → None`    | no-op     |
    ///
    /// Because the closure runs **at most once** (`FnOnce`), it may
    /// move owned state in from the surrounding scope and consume it
    /// while computing the new value (`extras`, `Vec`, `String`, …).
    ///
    /// Default impl: `get` → `f` → `put`/`delete`/no-op. Backends that
    /// can fold the read-modify-write into a single index touch +
    /// single append/page-write should override.
    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let current = self.get(key).map(Cow::into_owned);
        let had_value = current.is_some();
        match (had_value, f(current)) {
            (_, Some(new_v)) => self.put(key.clone(), new_v),
            (true, None) => {
                self.delete(key)?;
                Ok(())
            }
            (false, None) => Ok(()),
        }
    }

    /// Remove every entry, returning the backend to an empty-but-open
    /// state. The on-disk file is reused (no path operations); only the
    /// logical state is reset.
    fn clear(&mut self) -> io::Result<()>;

    /// Reclaim backend-specific storage waste while preserving live
    /// entries. Backends that do not accumulate reclaimable space can
    /// keep the default no-op implementation.
    fn compact(&mut self) -> io::Result<()> {
        Ok(())
    }

    // ---- iteration (unspecified order) -------------------------------

    /// Iterate all keys. Order is backend-defined. Yields `Cow<K>` —
    /// borrowed when the backend holds them, owned when materialized.
    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, K>> + 'a
    where
        K: 'a;

    /// Iterate all values. Order is backend-defined. Yields `Cow<V>`.
    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, V>> + 'a
    where
        V: 'a;

    /// Iterate all `(key, value)` pairs as `Cow` per side.
    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a;

    // ---- bookkeeping --------------------------------------------------

    /// Number of live entries (not bytes).
    fn size(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Return the backend's current in-memory stats view.
    fn stats(&self) -> Self::Stats<'_>;

    /// Return the backend's configuration.
    fn config(&self) -> &Self::Config;

    // ---- durability ---------------------------------------------------

    /// Schedule pending writes for OS writeback. May return before the
    /// writeback has completed. Backed by `MmapMut::flush_async` on the
    /// mmap'd backends.
    fn flush(&self) -> io::Result<()>;

    /// Block until pending mmap writes have been flushed by the OS.
    /// Slower than [`flush`](Self::flush). This is a writeback boundary,
    /// not a crash-recovery or corruption-repair mechanism.
    fn sync(&self) -> io::Result<()>;
}

// ---------------------------------------------------------------------------
// OrderedBackend — adds range, reverse iteration, and endpoint accessors.
// ---------------------------------------------------------------------------

/// Extension trait for backends whose iteration has a stable ascending key
/// order (e.g. B+ trees, OrderLog). Generic code that needs ordering should
/// constrain to `OrderedBackend` rather than `Backend`.
///
/// The ordering is backend-defined:
/// - [`crate::core::btree::BPlusTree`] orders by lexicographic bincode key
///   bytes because tree navigation stores serialized keys.
/// - [`crate::core::orderlog::OrderLog`] orders by `K::Ord` because its
///   in-memory skip list stores decoded keys.
///
/// `Ord` lives **here**, not on [`Backend`]: only ordered backends
/// need it. Unordered backends (KeyDir's hash index) work without it.
pub trait OrderedBackend<K, V>: Backend<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    /// Iterate entries with keys in `[start, end)`, ascending.
    fn range<'a>(
        &'a self,
        start: &K,
        end: &K,
    ) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a;

    /// First `(key, value)` in ascending key order, or `None` if empty.
    /// Default implementation pulls the head of `entries()`; backends
    /// with O(1) leftmost-leaf access may override.
    fn first<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        self.entries().next()
    }

    /// Last `(key, value)` in ascending key order, or `None` if empty.
    /// Default implementation walks `entries()` to completion (O(n));
    /// backends with O(1) rightmost-leaf access may override.
    fn last<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        self.entries().last()
    }

    /// Iterate entries in **descending** key order. Default
    /// implementation materializes the forward iteration into a `Vec`
    /// then reverses (O(n) memory). Backends with prev-pointer support
    /// in their on-disk format may override for true streaming reverse
    /// iteration.
    ///
    /// The `'static` bound on `K` and `V` lets the default's owned
    /// `Vec::into_iter` satisfy the iterator's lifetime; all of our
    /// concrete K/V (Vec<u8>, primitives, derive-bincode structs) are
    /// `'static`.
    fn entries_rev(&self) -> impl Iterator<Item = (Cow<'_, K>, Cow<'_, V>)> + '_
    where
        K: 'static,
        V: 'static,
    {
        let mut v: Vec<(K, V)> = self
            .entries()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        v.reverse();
        v.into_iter().map(|(k, v)| (Cow::Owned(k), Cow::Owned(v)))
    }

    /// Iterate entries with keys in `[start, end)` in **descending** key
    /// order. Default implementation materializes the forward range into a
    /// `Vec` then reverses (O(n) memory). Backends with prev-pointer support
    /// may override for true streaming reverse iteration.
    fn range_rev(&self, start: &K, end: &K) -> impl Iterator<Item = (Cow<'_, K>, Cow<'_, V>)> + '_
    where
        K: 'static,
        V: 'static,
    {
        let mut v: Vec<(K, V)> = self
            .range(start, end)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        v.reverse();
        v.into_iter().map(|(k, v)| (Cow::Owned(k), Cow::Owned(v)))
    }
}
