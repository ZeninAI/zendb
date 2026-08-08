//! Unregistered topic readers and offset/stamp positioning.

use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    sync::{Arc, atomic::Ordering},
};

use bincode::Decode;
use zendb_types::EventStamp;
use zendb_types::utils::{reusables::PooledBuf, serdes::deserialize_from};

use super::{
    segment::Segment,
    types::{HEADER_SIZE, SeekTarget, TopicOffset, TopicShared},
};

pub(super) struct SegmentCursor {
    // The file must close before the final segment Arc can remove its path.
    file: File,
    segment: Arc<Segment>,
    logical_offset: TopicOffset,
}

/// Unregistered read cursor used for random access and prefix-based scans.
pub struct TopicReader<T> {
    pub(super) topic: Arc<TopicShared<T>>,
    pub(super) offset: TopicOffset,
    pub(super) current: Option<SegmentCursor>,
}

impl<T> TopicReader<T> {
    pub(super) fn set_offset(&mut self, offset: TopicOffset) {
        self.offset = offset;
        self.current = None;
    }

    /// Resolve a seek target to the next logical offset read by this reader.
    pub fn seek(&mut self, target: SeekTarget<'_>) -> io::Result<bool> {
        match target {
            SeekTarget::Offset(offset) => {
                self.set_offset(offset);
                Ok(offset < self.topic.next_offset.load(Ordering::Acquire))
            }
            SeekTarget::StampPredicate(predicate) => {
                let Some(offset) = scan_offset_by_stamp(&self.topic, predicate)? else {
                    self.set_offset(self.topic.next_offset.load(Ordering::Acquire));
                    return Ok(false);
                };
                self.set_offset(offset);
                Ok(true)
            }
            SeekTarget::Earliest => {
                let segments = self.topic.segments.load();
                let offset = segments
                    .first()
                    .map(|segment| segment.base_offset)
                    .unwrap_or(self.topic.next_offset.load(Ordering::Acquire));
                self.set_offset(offset);
                Ok(offset < self.topic.next_offset.load(Ordering::Acquire))
            }
            SeekTarget::Latest => {
                self.set_offset(self.topic.next_offset.load(Ordering::Acquire));
                Ok(false)
            }
        }
    }
}

impl<T> Iterator for TopicReader<T>
where
    T: Decode<()>,
{
    type Item = io::Result<(TopicOffset, T)>;

    fn next(&mut self) -> Option<Self::Item> {
        let needs_position = match &self.current {
            Some(cursor) => {
                if cursor.logical_offset < cursor.segment.end_offset.load(Ordering::Acquire) {
                    false
                } else if cursor.logical_offset >= self.topic.next_offset.load(Ordering::Acquire) {
                    return None;
                } else {
                    true
                }
            }
            None => true,
        };
        if needs_position {
            self.current = None;
            // Reconcile the requested offset with the segments retained now.
            let positioned = (|| -> io::Result<Option<SegmentCursor>> {
                let segments = self.topic.segments.load_full();
                let latest = self.topic.next_offset.load(Ordering::Acquire);
                let Some(first) = segments.first() else {
                    self.offset = latest;
                    return Ok(None);
                };
                if self.offset < first.base_offset {
                    self.offset = first.base_offset;
                }
                if self.offset >= latest {
                    self.offset = latest;
                    return Ok(None);
                }

                let segment = if let Some(segment) = segments.iter().find(|segment| {
                    self.offset >= segment.base_offset
                        && self.offset < segment.end_offset.load(Ordering::Acquire)
                }) {
                    segment
                } else if let Some(segment) = segments
                    .iter()
                    .find(|segment| segment.base_offset > self.offset)
                {
                    self.offset = segment.base_offset;
                    segment
                } else {
                    self.offset = latest;
                    return Ok(None);
                };
                let segment = Arc::clone(segment);
                let (indexed_offset, mut byte_offset) = segment
                    .sparse_index
                    .read()
                    .iter()
                    .rev()
                    .find(|(indexed, _)| *indexed <= self.offset)
                    .copied()
                    .unwrap_or((segment.base_offset, HEADER_SIZE));
                let mut file = File::open(&segment.path)?;
                for _ in indexed_offset..self.offset {
                    file.seek(SeekFrom::Start(byte_offset))?;
                    let mut size = [0; 4];
                    file.read_exact(&mut size)?;
                    byte_offset += 4 + u32::from_le_bytes(size) as u64;
                }
                file.seek(SeekFrom::Start(byte_offset))?;
                Ok(Some(SegmentCursor {
                    file,
                    segment,
                    logical_offset: self.offset,
                }))
            })();
            match positioned {
                Ok(Some(cursor)) => self.current = Some(cursor),
                Ok(None) => return None,
                Err(error) => return Some(Err(error)),
            }
        }

        let cursor = self.current.as_mut().unwrap();
        let offset = cursor.logical_offset;
        let mut size = [0; 4];
        let result = cursor.file.read_exact(&mut size).and_then(|_| {
            let value_size = u32::from_le_bytes(size) as usize;
            let mut bytes = PooledBuf::acquire();
            bytes.resize(value_size, 0);
            cursor.file.read_exact(&mut bytes[..])?;
            deserialize_from(&bytes[..])
        });
        if result.is_ok() {
            cursor.logical_offset += 1;
            self.offset = cursor.logical_offset;
        }
        Some(result.map(|value| (offset, value)))
    }
}

pub(super) fn scan_offset_by_stamp<T, F>(
    topic: &Arc<TopicShared<T>>,
    mut predicate: F,
) -> io::Result<Option<TopicOffset>>
where
    F: FnMut(&EventStamp) -> bool,
{
    let segments = topic.segments.load_full();
    for segment in segments.iter() {
        let end_offset = segment.end_offset.load(Ordering::Acquire);
        if segment.base_offset >= end_offset {
            continue;
        }

        let mut file = File::open(&segment.path)?;
        let (mut offset, mut byte_offset) = segment
            .sparse_index
            .read()
            .first()
            .copied()
            .unwrap_or((segment.base_offset, HEADER_SIZE));
        while offset < end_offset {
            file.seek(SeekFrom::Start(byte_offset))?;
            let mut size = [0; 4];
            file.read_exact(&mut size)?;
            let next_byte_offset = byte_offset + 4 + u32::from_le_bytes(size) as u64;
            let mut buf = [0; EventStamp::ENCODED_SIZE];
            file.read_exact(&mut buf)?;
            let stamp: EventStamp = deserialize_from(&buf)?;
            if predicate(&stamp) {
                return Ok(Some(offset));
            }
            byte_offset = next_byte_offset;
            offset += 1;
        }
    }
    Ok(None)
}
