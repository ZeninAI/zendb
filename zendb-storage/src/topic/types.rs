//! Shared topic configuration, cursor targets, and concurrent topic state.

use std::{marker::PhantomData, sync::Arc};

use arc_swap::ArcSwap;
use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::Mutex;
use zendb_types::EventStamp;

use crate::backend::keydir::{KeyDir, KeyDirConfig};

use super::segment::Segment;

pub type TopicOffset = u64;

pub(super) const OFFSETS_FILE: &str = "offsets";
pub(super) const SEGMENT_EXTENSION: &str = "log";
pub(super) const MAGIC: u32 = 0x4349_5054;
pub(super) const HEADER_SIZE: u64 = 4;
pub(super) const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const DEFAULT_SPARSE_INDEX_STRIDE: u64 = 256;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct TopicConfig {
    pub max_segment_bytes: u64,
    pub sparse_index_stride: u64,
    pub offsets: KeyDirConfig,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            sparse_index_stride: DEFAULT_SPARSE_INDEX_STRIDE,
            offsets: KeyDirConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct TopicStats {
    pub earliest_offset: TopicOffset,
    pub next_offset: TopicOffset,
    pub records: u64,
    pub retained_bytes: u64,
}

/// Target for positioning a topic cursor.
pub enum SeekTarget<'a> {
    /// Jump directly to a topic offset.
    Offset(TopicOffset),
    /// Scan from the earliest retained offset until the predicate returns true.
    StampPredicate(Box<dyn FnMut(&EventStamp) -> bool + 'a>),
    /// Jump to the earliest retained topic offset.
    Earliest,
    /// Jump to the current topic tail.
    Latest,
}

impl<'a> SeekTarget<'a> {
    /// Create a stamp-predicate target without explicitly boxing the closure.
    pub fn stamp_predicate(predicate: impl FnMut(&EventStamp) -> bool + 'a) -> Self {
        Self::StampPredicate(Box::new(predicate))
    }
}

pub(super) struct ConsumerRegistration {
    pub(super) committed: TopicOffset,
    pub(super) active: bool,
}

pub(super) struct TopicShared<T> {
    pub(super) segments: ArcSwap<Vec<Arc<Segment>>>,
    pub(super) next_offset: std::sync::atomic::AtomicU64,
    pub(super) consumers: Mutex<HashMap<String, ConsumerRegistration>>,
    pub(super) offsets: Mutex<KeyDir<String, TopicOffset>>,
    pub(super) _value: PhantomData<T>,
}
