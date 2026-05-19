//! # zendb-storage
//!
//! Storage subsystem for ZeninDB.
//!
//! Layer 1 — general-purpose data structures:
//! - **WAL** — append-only write-ahead log
//! - **SkipList** — in-memory ordered map (arena-backed)
//! - **HashTable** — persistent hash table (mmap-backed)
//! - **BPlusTree** — persistent ordered KV store (mmap, planned)
//!
//! Layer 2 — ZeninDB-aware storage (planned):
//! - DeltaBuffer, Compactor, StateHash, Storage

pub mod core;
