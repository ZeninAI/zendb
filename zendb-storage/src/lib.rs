//! Generic persistent and in-memory storage mechanics for ZenDB.
//!
//! The crate contains raw key/value backends, a runtime-selected materialized
//! state backend, and a segmented append-only topic. It has no CRDT, installation,
//! authorization, workspace, or networking policy.

pub mod backend;
pub mod table;
pub mod topic;

pub use backend::{
    BPlusTree, BPlusTreeConfig, BPlusTreeStats, DurableStorage, KeyDir, KeyDirConfig, KeyDirStats,
    OrderedReadBackend, ReadBackend, SkipList, SkipListCapacity, SkipListConfig, SkipListStats,
    State, StateConfig, StateStats, Storage, WriteBackend,
};
pub use table::{
    Change, InsertOutcome, Table, TableConfig, TableStats, DEFAULT_MAX_BUFFERED_RECORDS,
};
pub use topic::{Topic, TopicConfig, TopicConsumer, TopicOffset, TopicStats};
