//! # zendb-engine
//!
//! Storage engine for ZeninDB — sits between the type system and the
//! storage primitives, providing:
//!
//! - **Table** — one logical table with WAL, delta buffer, and persistent tree
//! - **Database** — entry point managing multiple tables
//! - **MerkleTree** — anti-entropy primitive built from table row hashes
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │                   Database                       │
//! │  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
//! │  │ Table    │  │ Table    │  │ Table    │       │
//! │  │ "notes"  │  │ "todos"  │  │ "tags"   │       │
//! │  └──────────┘  └──────────┘  └──────────┘       │
//! └─────────────────────────────────────────────────┘
//!
//! Each Table:
//!   WAL ──▶ SkipList (delta buffer) ──▶ BPlusTree (persistent)
//!              ↑ flush() coalesces deltas
//! ```

pub mod config;
pub mod database;
pub mod merkle;
pub mod table;
