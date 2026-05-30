//! Cross-cutting utilities shared across the storage backends.

use std::cell::Cell;

pub mod serdes;

// ---------------------------------------------------------------------------
// Cheap thread-local RNG — used by skip-list level coin flips and any other
// hot path that needs a fast non-cryptographic random u64. Per-thread state
// avoids the atomic load/store a process-global counter would pay on every
// call.
// ---------------------------------------------------------------------------

thread_local! {
    static RNG_SEED: Cell<u64> = const { Cell::new(0x9E37_79B9_7F4A_7C15) };
}

/// xorshift64 step on the per-thread seed. Not cryptographic — chosen for
/// the inner loops (skip-list level selection, etc.) where the only need
/// is a uniform-ish bit stream produced in a handful of ALU ops.
#[inline]
pub(crate) fn fast_rand() -> u64 {
    RNG_SEED.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            x = 0xDEAD_BEEF_CAFE_F00D;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        cell.set(x);
        x
    })
}
