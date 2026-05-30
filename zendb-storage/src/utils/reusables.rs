//! Thread-local pool of reusable byte buffers.
//!
//! Hot paths in the storage layer need scratch `Vec<u8>` buffers — to
//! encode keys for tree navigation, to stage a transaction's writes, to
//! materialize a snapshot during compaction. Allocating those vectors
//! fresh on every call is wasteful; they're released a few cycles later
//! and the same shape is needed again immediately after.
//!
//! [`PooledBuf`] is an RAII handle around a recycled `Vec<u8>`. The
//! thread-local pool is a LIFO stack: the most recently released buffer
//! is the first one handed back, so its capacity is the one most likely
//! to still be sitting in CPU cache.
//!
//! ## Thread-local by necessity
//!
//! The pool uses `thread_local!` + `RefCell`. `RefCell` is not `Sync`, so
//! it cannot live in a plain `static`. `thread_local!` sidesteps that
//! requirement by giving every thread its own instance, with no locks or
//! atomics.
//!
//! ## No size caps
//!
//! Neither the pool length nor individual buffer capacity is bounded —
//! callers are expected to use the pool for short-lived scratch work, not
//! to stash gigabytes in a buffer and drop it. If a workload genuinely
//! needs a huge buffer once, the cost of pooling it is paid by everyone
//! who acquires it next; that's a caller-side decision, not a pool-side
//! one.
//!
//! ## Recursion is fine
//!
//! Acquiring a second `PooledBuf` while one is already held just pops
//! the next slot (or allocates). There is no shared `RefCell::borrow_mut`
//! across the buffer's lifetime — the inner `Vec<u8>` is moved out of
//! the pool on acquire and moved back on drop.

use std::cell::RefCell;
use std::ops::{Deref, DerefMut};

thread_local! {
    /// Per-thread LIFO stack of recycled byte buffers. Each `Vec<u8>` is
    /// stored cleared (len = 0) but with its capacity intact.
    static POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

/// RAII handle around a recycled `Vec<u8>`. Behaves like a `Vec<u8>` via
/// `Deref` / `DerefMut`; returns its backing buffer to the thread-local
/// pool on drop.
pub struct PooledBuf {
    /// `Option` so `Drop` can move the `Vec` back into the pool without
    /// running the `Vec`'s own destructor.
    inner: Option<Vec<u8>>,
}

impl PooledBuf {
    /// Acquire a buffer from the thread-local pool. If the pool is
    /// empty, allocates a fresh empty `Vec`. The returned buffer always
    /// has `len() == 0`; its capacity is whatever the previous user left
    /// behind (zero on a fresh allocation).
    pub fn acquire() -> Self {
        let inner = POOL
            .with(|cell| cell.borrow_mut().pop())
            .unwrap_or_default();
        PooledBuf { inner: Some(inner) }
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(mut buf) = self.inner.take() {
            buf.clear();
            POOL.with(|cell| cell.borrow_mut().push(buf));
        }
    }
}

impl Deref for PooledBuf {
    type Target = Vec<u8>;
    fn deref(&self) -> &Vec<u8> {
        // Safe: `inner` is `Some` for the entire lifetime of the handle;
        // it is only taken in `Drop`.
        self.inner.as_ref().unwrap()
    }
}

impl DerefMut for PooledBuf {
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        self.inner.as_mut().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drain the pool so a test starts from a clean state. Otherwise
    /// the LIFO ordering checks below can be perturbed by buffers left
    /// behind by other tests on this thread.
    fn drain_pool() {
        POOL.with(|cell| cell.borrow_mut().clear());
    }

    fn pool_len() -> usize {
        POOL.with(|cell| cell.borrow().len())
    }

    #[test]
    fn acquired_buffer_is_empty() {
        drain_pool();
        let buf = PooledBuf::acquire();
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn drop_returns_buffer_to_pool() {
        drain_pool();
        assert_eq!(pool_len(), 0);
        {
            let _buf = PooledBuf::acquire();
        }
        assert_eq!(pool_len(), 1);
    }

    #[test]
    fn drop_clears_but_preserves_capacity() {
        drain_pool();
        {
            let mut buf = PooledBuf::acquire();
            buf.extend_from_slice(&[1, 2, 3, 4]);
            assert_eq!(buf.len(), 4);
            assert!(buf.capacity() >= 4);
        }
        let buf = PooledBuf::acquire();
        assert_eq!(buf.len(), 0);
        assert!(
            buf.capacity() >= 4,
            "capacity should be retained across pooling"
        );
    }

    #[test]
    fn lifo_returns_most_recent_buffer() {
        drain_pool();
        // Release two buffers with distinct capacities, then acquire and
        // check we get the most recently released one first.
        {
            let mut a = PooledBuf::acquire();
            a.reserve(64);
        }
        {
            let mut b = PooledBuf::acquire();
            b.reserve(256);
        }
        let top = PooledBuf::acquire();
        assert!(
            top.capacity() >= 256,
            "expected most-recently-released (256-cap) buffer first, got cap = {}",
            top.capacity()
        );
    }

    #[test]
    fn concurrent_acquires_yield_independent_buffers() {
        drain_pool();
        let mut a = PooledBuf::acquire();
        let mut b = PooledBuf::acquire();
        a.extend_from_slice(b"alpha");
        b.extend_from_slice(b"bravo");
        assert_eq!(&a[..], b"alpha");
        assert_eq!(&b[..], b"bravo");
    }
}
