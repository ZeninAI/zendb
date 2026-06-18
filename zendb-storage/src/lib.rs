//! # zendb-storage
//!
//! Storage subsystem for ZeninDB.
//!
//! Layer 1 — general-purpose data structures:
//! - **KeyDir** — persistent KV store with in-memory hash index + mmap'd data file (Bitcask model)
//! - **BPlusTree** — persistent ordered KV store (mmap, in-place mutation, bulk-merge)
//! - **SkipList** — entirely in-memory ordered KV store
//! - **Topic** — persistent segmented append-only log with consumer cursors
//!
//! Frontend - ZeninDB-aware storage facades:
//! - **State** - runtime-selected materialized-state backend
//! - **Table** - resolved table cache over State plus Topic-backed changes

pub mod core;
pub mod frontend;
pub mod utils;

#[cfg(test)]
mod benchmark;
