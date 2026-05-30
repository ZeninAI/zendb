//! # zendb-storage
//!
//! Storage subsystem for ZeninDB.
//!
//! Layer 1 — general-purpose data structures:
//! - **KeyDir** — persistent KV store with in-memory hash index + mmap'd data file (Bitcask model)
//! - **BPlusTree** — persistent ordered KV store (mmap, in-place mutation, bulk-merge)
//! - **OrderLog** — in-memory ordered KV store with a write-ahead log for durability
//!
//! Layer 2 — ZeninDB-aware storage (planned):
//! - DeltaBuffer, Compactor, StateHash, Storage

pub mod core;
pub mod utils;

#[cfg(test)]
mod benchmark;
