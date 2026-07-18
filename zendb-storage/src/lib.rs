//! Generic persistent and in-memory storage mechanics for ZenDB.
//!
//! The crate contains raw key/value backends, a runtime-selected materialized
//! state backend, and a segmented append-only topic. It has no CRDT, device,
//! authorization, workspace, or networking policy.

pub mod backend;
pub mod topic;
pub mod utils;

#[cfg(test)]
mod benchmark;

pub use backend::{
    BPlusTree, BPlusTreeConfig, BPlusTreeStats, DurableStorage, KeyDir, KeyDirConfig, KeyDirStats,
    OrderedReadBackend, ReadBackend, SkipList, SkipListCapacity, SkipListConfig, SkipListStats,
    State, StateConfig, StateStats, Storage, WriteBackend,
};
pub use topic::{Topic, TopicConfig, TopicConsumer, TopicOffset, TopicStats};
