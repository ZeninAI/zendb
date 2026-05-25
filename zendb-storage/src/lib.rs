//! # zendb-storage
//!
//! Storage subsystem for ZeninDB.
//!
//! Layer 1 — general-purpose data structures:
//! - **WAL** — append-only write-ahead log
//! - **SkipList** — in-memory ordered map (arena-backed)
//! - **KeyDir** — persistent KV store with in-memory hash index + mmap'd data file (Bitcask model)
//! - **BPlusTree** — persistent ordered KV store (mmap, in-place mutation, bulk-merge)
//!
//! Layer 2 — ZeninDB-aware storage (planned):
//! - DeltaBuffer, Compactor, StateHash, Storage

pub mod core;
