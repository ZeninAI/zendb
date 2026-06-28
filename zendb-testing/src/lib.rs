//! ZeninDB full-stack integration tests.
//!
//! Simulates a document search-engine pipeline:
//! - `Indexer` operator builds an inverted word→doc-id index
//! - `Archiver` operator periodically flushes stats to a reports table
//!
//! Validates operator lifecycle, state persistence, timer eviction,
//! and consumer cleanup across database reopen.

#![cfg(test)]

mod executor;
mod operators;
mod tests;
