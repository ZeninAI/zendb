//! Persistent single-writer, multiple-reader append-only topic.
//!
//! [`Topic`] owns segmented log files, provides exclusive mutable appends, and
//! creates independent [`TopicReader`] and [`TopicConsumer`] cursors.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arc_swap::ArcSwap;
use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::Mutex;

use crate::backend::{
    _traits::{DurableStorage, ReadBackend, Storage, WriteBackend},
    keydir::KeyDir,
};
use zendb_types::utils::reusables::PooledBuf;

use super::{
    consumer::TopicConsumer,
    reader::TopicReader,
    segment::{
        Segment, build_sparse_index, check_segment_magic, create_segment, list_segments,
        open_segment_writer, scan_active_segment,
    },
    types::{
        ConsumerRegistration, HEADER_SIZE, OFFSETS_FILE, TopicConfig, TopicOffset, TopicShared,
        TopicStats,
    },
};

/// Persistent single-writer, multiple-reader append-only topic.
pub struct Topic<T> {
    path: PathBuf,
    config: TopicConfig,
    segments: Vec<Arc<Segment>>,
    active: File,
    active_byte_len: u64,
    shared: Arc<TopicShared<T>>,
    stats: TopicStats,
}

impl<T> Topic<T> {
    fn persist(&mut self, barrier: zendb_types::Barrier) -> io::Result<()> {
        match barrier {
            zendb_types::Barrier::Flush => self.active.flush()?,
            zendb_types::Barrier::Sync => self.active.sync_all()?,
        }
        self.shared.offsets.lock().persist(barrier)
    }
}

impl<T> Topic<T>
where
    T: Encode + Decode<()>,
{
    /// Create an unregistered reader at the earliest retained offset.
    pub fn reader(&self) -> TopicReader<T> {
        TopicReader {
            topic: Arc::clone(&self.shared),
            offset: self.shared.segments.load()[0].base_offset,
            current: None,
        }
    }

    /// Create a named consumer, registering it at the current tail when first
    /// seen. A consumer name may have only one active handle.
    pub fn consumer(&self, consumer: &str) -> io::Result<TopicConsumer<T>> {
        let name = consumer.to_owned();
        let mut consumers = self.shared.consumers.lock();
        let committed = match consumers.get_mut(consumer) {
            Some(registration) => {
                if registration.active {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("consumer {consumer:?} already has an active reader"),
                    ));
                }
                registration.active = true;
                registration.committed
            }
            None => {
                let offset = self.shared.next_offset.load(Ordering::Acquire);
                self.shared.offsets.lock().put(name.clone(), offset)?;
                consumers.insert(
                    name.clone(),
                    ConsumerRegistration {
                        committed: offset,
                        active: true,
                    },
                );
                offset
            }
        };
        drop(consumers);

        Ok(TopicConsumer {
            reader: TopicReader {
                topic: Arc::clone(&self.shared),
                offset: committed,
                current: None,
            },
            name,
            committed,
        })
    }

    fn rotate_segment(&mut self) -> io::Result<()> {
        self.active.flush()?;

        let segment = create_segment(&self.path, self.stats.next_offset)?;
        self.active = open_segment_writer(&segment.path)?;
        self.active_byte_len = HEADER_SIZE;
        self.stats.retained_bytes += HEADER_SIZE;
        self.segments.push(segment);
        self.publish_segments();
        self.compact()?;
        Ok(())
    }

    fn publish_segments(&self) {
        self.shared.segments.store(Arc::new(self.segments.clone()));
    }

    pub fn append(&mut self, value: &T) -> io::Result<TopicOffset> {
        let mut encoded = PooledBuf::acquire();
        encoded.resize(4, 0);
        let payload_len = zendb_types::utils::serdes::serialize_into_std(value, &mut *encoded)?;
        let written = 4 + payload_len;
        encoded[..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
        let record_size = written as u64;

        {
            if self.active_byte_len != HEADER_SIZE
                && self.active_byte_len + record_size > self.config.max_segment_bytes
            {
                self.rotate_segment()?;
            }

            let offset = self.stats.next_offset;
            let segment = self.segments.last().unwrap();
            if offset != segment.base_offset
                && (offset - segment.base_offset).is_multiple_of(self.config.sparse_index_stride)
            {
                segment
                    .sparse_index
                    .write()
                    .push((offset, self.active_byte_len));
            }
            self.active.write_all(&encoded[..written])?;

            self.active_byte_len += record_size;
            segment
                .byte_len
                .store(self.active_byte_len, Ordering::Release);
            segment.end_offset.store(offset + 1, Ordering::Release);
            self.stats.next_offset = offset + 1;
            self.stats.records += 1;
            self.stats.retained_bytes += record_size;
            self.shared
                .next_offset
                .store(self.stats.next_offset, Ordering::Release);
            Ok(offset)
        }
    }
}

impl<T> Storage for Topic<T>
where
    T: Encode + Decode<()>,
{
    type Stats = TopicStats;
    type Config = TopicConfig;

    fn stats(&self) -> Self::Stats {
        self.stats
    }

    fn config(&self) -> Self::Config {
        self.config.clone()
    }
}

impl<T> DurableStorage for Topic<T>
where
    T: Encode + Decode<()>,
{
    fn create(path: &Path, config: Self::Config) -> io::Result<Self> {
        fs::create_dir_all(path)?;

        let offsets = KeyDir::create(&path.join(OFFSETS_FILE), config.offsets.clone())?;
        let segment = create_segment(path, 0)?;
        let active = open_segment_writer(&segment.path)?;
        let segments = vec![segment];
        let shared = Arc::new(TopicShared {
            segments: ArcSwap::from_pointee(segments.clone()),
            next_offset: AtomicU64::new(0),
            consumers: Mutex::new(HashMap::new()),
            offsets: Mutex::new(offsets),
            _value: PhantomData,
        });

        Ok(Self {
            path: path.to_path_buf(),
            config,
            segments,
            active,
            active_byte_len: HEADER_SIZE,
            shared,
            stats: TopicStats {
                earliest_offset: 0,
                next_offset: 0,
                records: 0,
                retained_bytes: HEADER_SIZE,
            },
        })
    }

    fn open(path: &Path, config: Self::Config) -> io::Result<Self> {
        let offsets: KeyDir<String, TopicOffset> =
            KeyDir::open(&path.join(OFFSETS_FILE), config.offsets.clone())?;
        let segment_files = list_segments(path)?;

        let active_index = segment_files.len() - 1;
        let active_base = segment_files[active_index].0;
        let active_path = segment_files[active_index].1.clone();
        let (active_end, active_len, file_len) = scan_active_segment(&active_path, active_base)?;
        if active_len < file_len {
            OpenOptions::new()
                .write(true)
                .open(&active_path)?
                .set_len(active_len)?;
        }

        let segments: Vec<_> = segment_files
            .iter()
            .enumerate()
            .map(|(index, (base_offset, path))| {
                let (end_offset, byte_len) = if index == active_index {
                    (active_end, active_len)
                } else {
                    check_segment_magic(path)?;
                    (segment_files[index + 1].0, fs::metadata(path)?.len())
                };
                Ok(Arc::new(Segment {
                    base_offset: *base_offset,
                    end_offset: AtomicU64::new(end_offset),
                    byte_len: AtomicU64::new(byte_len),
                    sparse_index: parking_lot::RwLock::new(build_sparse_index(
                        path,
                        *base_offset,
                        end_offset,
                        config.sparse_index_stride,
                    )?),
                    path: path.clone(),
                    delete_on_drop: AtomicBool::new(false),
                }))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let earliest_offset = segments[0].base_offset;
        let next_offset = segments.last().unwrap().end_offset.load(Ordering::Acquire);
        let retained_bytes = segments
            .iter()
            .map(|segment| segment.byte_len.load(Ordering::Acquire))
            .sum();
        let mut consumers = HashMap::new();
        for (consumer, committed) in offsets.entries() {
            consumers.insert(
                consumer.into_owned(),
                ConsumerRegistration {
                    committed: committed.into_owned(),
                    active: false,
                },
            );
        }

        let active = open_segment_writer(&segments.last().unwrap().path)?;
        let shared = Arc::new(TopicShared {
            segments: ArcSwap::from_pointee(segments.clone()),
            next_offset: AtomicU64::new(next_offset),
            consumers: Mutex::new(consumers),
            offsets: Mutex::new(offsets),
            _value: PhantomData,
        });
        Ok(Self {
            path: path.to_path_buf(),
            config,
            segments,
            active,
            active_byte_len: active_len,
            shared,
            stats: TopicStats {
                earliest_offset,
                next_offset,
                records: next_offset - earliest_offset,
                retained_bytes,
            },
        })
    }

    fn compact(&mut self) -> io::Result<()> {
        let Some(through) = ({
            let consumers = self.shared.consumers.lock();
            consumers
                .values()
                .map(|registration| registration.committed)
                .min()
        }) else {
            return Ok(());
        };

        let removable = self
            .segments
            .iter()
            .take(self.segments.len() - 1)
            .take_while(|segment| segment.end_offset.load(Ordering::Acquire) <= through)
            .count();
        if removable == 0 {
            return Ok(());
        }

        let removed: Vec<_> = self.segments.drain(..removable).collect();
        self.publish_segments();

        let removed_records: u64 = removed.iter().map(|segment| segment.record_count()).sum();
        let reclaimed_bytes: u64 = removed
            .iter()
            .map(|segment| segment.byte_len.load(Ordering::Acquire))
            .sum();
        for segment in removed {
            segment.delete_on_drop.store(true, Ordering::Release);
        }
        self.stats.earliest_offset = self.segments[0].base_offset;
        self.stats.records -= removed_records;
        self.stats.retained_bytes -= reclaimed_bytes;
        Ok(())
    }

    fn persist(&mut self, barrier: zendb_types::Barrier) -> io::Result<()> {
        Topic::persist(self, barrier)
    }
}

impl<T> Drop for Topic<T> {
    fn drop(&mut self) {
        let _ = Topic::persist(self, zendb_types::Barrier::Flush);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::keydir::KeyDirConfig;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::super::segment::segment_name;
    use super::super::types::{DEFAULT_SPARSE_INDEX_STRIDE, SeekTarget};

    /// RAII guard that removes the test directory when dropped.
    struct TmpDir(PathBuf);

    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    impl std::ops::Deref for TmpDir {
        type Target = std::path::Path;
        fn deref(&self) -> &std::path::Path {
            &self.0
        }
    }

    fn tmp(label: &str) -> TmpDir {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join("zendb_topic_tests").join(format!(
            "{label}_{}_{}",
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_dir_all(&path);
        TmpDir(path)
    }

    fn config(max_segment_bytes: u64) -> TopicConfig {
        TopicConfig {
            max_segment_bytes,
            sparse_index_stride: DEFAULT_SPARSE_INDEX_STRIDE,
            offsets: KeyDirConfig::default(),
        }
    }

    fn append_all(topic: &mut Topic<u64>, values: impl IntoIterator<Item = u64>) {
        for value in values {
            topic.append(&value).unwrap();
        }
    }

    #[test]
    fn reader_registers_at_current_tail() {
        let path = tmp("register_tail");
        let mut topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
        append_all(&mut topic, [1, 2]);
        let mut reader = topic.consumer("c").unwrap();
        topic.append(&3).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), 3);
        assert!(reader.next().is_none());
    }

    #[test]
    fn dropping_reader_resets_volatile_progress_to_committed() {
        let path = tmp("volatile");
        let mut topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
        let reader = topic.consumer("c").unwrap();
        drop(reader);
        append_all(&mut topic, [1, 2, 3]);

        {
            let mut reader = topic.consumer("c").unwrap();
            assert_eq!(reader.next().unwrap().unwrap(), 1);
        }
        {
            let mut reader = topic.consumer("c").unwrap();
            assert_eq!(reader.next().unwrap().unwrap(), 1);
            reader.commit().unwrap();
        }

        topic.persist(zendb_types::Barrier::Sync).unwrap();
        drop(topic);

        let topic = Topic::<u64>::open(&path, TopicConfig::default()).unwrap();
        let mut reader = topic.consumer("c").unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), 2);
    }

    #[test]
    fn reopen_discards_uncommitted_volatile_progress() {
        let path = tmp("reopen_volatile");
        {
            let mut topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
            let reader = topic.consumer("c").unwrap();
            drop(reader);
            append_all(&mut topic, [1, 2]);
            let mut reader = topic.consumer("c").unwrap();
            assert_eq!(reader.next().unwrap().unwrap(), 1);
        }

        let topic = Topic::<u64>::open(&path, TopicConfig::default()).unwrap();
        let mut reader = topic.consumer("c").unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), 1);
    }

    #[test]
    fn reset_returns_to_committed_offset() {
        let path = tmp("reset");
        let mut topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
        let reader = topic.consumer("c").unwrap();
        drop(reader);
        append_all(&mut topic, [1, 2]);

        let mut reader = topic.consumer("c").unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), 1);
        reader.reset();
        assert_eq!(reader.next().unwrap().unwrap(), 1);
    }

    #[test]
    fn reader_observes_appends_and_rotations_without_refresh() {
        let path = tmp("live");
        let mut topic = Topic::<u64>::create(&path, config(16)).unwrap();
        let mut reader = topic.consumer("c").unwrap();
        topic.append(&1).unwrap();
        topic.append(&2).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), 1);
        assert_eq!(reader.next().unwrap().unwrap(), 2);
        assert!(reader.next().is_none());
    }

    #[test]
    fn only_one_reader_per_consumer_is_allowed() {
        let path = tmp("single_reader");
        let topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
        let reader = topic.consumer("c").unwrap();
        assert_eq!(
            topic.consumer("c").err().unwrap().kind(),
            io::ErrorKind::AlreadyExists
        );
        drop(reader);
        assert!(topic.consumer("c").is_ok());
    }

    #[test]
    fn consumer_delete_unregisters_cursor() {
        let path = tmp("consumer_delete");
        let mut topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
        let consumer = topic.consumer("c").unwrap();
        drop(consumer);
        append_all(&mut topic, [1, 2]);

        let consumer = topic.consumer("c").unwrap();
        consumer.delete().unwrap();

        let mut consumer = topic.consumer("c").unwrap();
        assert!(consumer.next().is_none());
        topic.append(&3).unwrap();
        assert_eq!(consumer.next().unwrap().unwrap(), 3);
    }

    #[test]
    fn different_consumers_read_concurrently() {
        let path = tmp("concurrent_consumers");
        let mut topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
        let a = topic.consumer("a").unwrap();
        let b = topic.consumer("b").unwrap();
        drop((a, b));
        append_all(&mut topic, 0..100);

        let a = topic.consumer("a").unwrap();
        let b = topic.consumer("b").unwrap();
        let a = std::thread::spawn(move || a.collect::<io::Result<Vec<_>>>());
        let b = std::thread::spawn(move || b.collect::<io::Result<Vec<_>>>());
        assert_eq!(a.join().unwrap().unwrap(), (0..100).collect::<Vec<_>>());
        assert_eq!(b.join().unwrap().unwrap(), (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn seek_marks_records_through_offset_consumed() {
        let path = tmp("seek");
        let mut topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
        let first = topic.append(&1).unwrap();
        topic.append(&2).unwrap();

        let mut consumer = topic.consumer("c").unwrap();
        consumer.seek(SeekTarget::Offset(first + 1)).unwrap();
        consumer.commit().unwrap();
        drop(consumer);

        let mut consumer = topic.consumer("c").unwrap();
        assert_eq!(consumer.next().unwrap().unwrap(), 2);
    }

    #[test]
    fn single_writer_appends_while_reader_consumes() {
        let path = tmp("concurrent_writer_reader");
        let mut topic = Topic::<u64>::create(&path, config(64)).unwrap();
        let mut reader = topic.consumer("c").unwrap();

        let writer = std::thread::spawn(move || {
            for value in 0..100 {
                topic.append(&value).unwrap();
                std::thread::yield_now();
            }
            topic
        });
        let reader = std::thread::spawn(move || {
            let mut values = Vec::new();
            while values.len() < 100 {
                match reader.next() {
                    Some(value) => values.push(value.unwrap()),
                    None => std::thread::yield_now(),
                }
            }
            values
        });

        let writer = writer.join().unwrap();
        assert_eq!(reader.join().unwrap(), (0..100).collect::<Vec<_>>());
        assert_eq!(writer.stats().next_offset, 100);
    }

    #[test]
    fn compacted_segment_lives_until_reader_releases_it() {
        let path = tmp("reader_segment_lifetime");
        let mut topic = Topic::<u64>::create(&path, config(16)).unwrap();
        let mut reader = topic.consumer("c").unwrap();
        topic.append(&1).unwrap();
        topic.append(&2).unwrap();

        assert_eq!(reader.next().unwrap().unwrap(), 1);
        reader.commit().unwrap();
        let first_segment = path.join(segment_name(0));
        topic.compact().unwrap();
        assert!(first_segment.exists());

        assert_eq!(reader.next().unwrap().unwrap(), 2);
        assert!(!first_segment.exists());
    }

    #[test]
    fn rotation_auto_compacts_without_blocking_active_reader() {
        let path = tmp("auto_compact");
        let mut topic = Topic::<u64>::create(&path, config(16)).unwrap();
        let reader = topic.consumer("c").unwrap();
        drop(reader);
        append_all(&mut topic, 0..8);
        {
            let mut reader = topic.consumer("c").unwrap();
            assert_eq!(reader.by_ref().count(), 8);
            reader.commit().unwrap();
        }

        let reader = topic.consumer("c").unwrap();
        let before = topic.segments.len();
        topic.append(&8).unwrap();
        topic.append(&9).unwrap();
        assert!(topic.segments.len() < before + 1);
        drop(reader);
    }

    #[test]
    fn compacted_topic_survives_reopen() {
        let path = tmp("compact_reopen");
        let config = config(16);
        {
            let mut topic = Topic::<u64>::create(&path, config.clone()).unwrap();
            let reader = topic.consumer("c").unwrap();
            drop(reader);
            append_all(&mut topic, 0..8);
            {
                let mut reader = topic.consumer("c").unwrap();
                assert_eq!(reader.by_ref().count(), 8);
                reader.commit().unwrap();
            }
            let before = topic.segments.len();
            topic.compact().unwrap();
            assert!(topic.segments.len() < before);
            topic.persist(zendb_types::Barrier::Sync).unwrap();
        }

        let mut topic = Topic::<u64>::open(&path, config).unwrap();
        topic.append(&8).unwrap();
        let mut reader = topic.consumer("c").unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), 8);
    }

    #[test]
    fn open_scans_and_truncates_only_partial_active_record() {
        let path = tmp("partial");
        {
            let mut topic = Topic::<u64>::create(&path, TopicConfig::default()).unwrap();
            topic.append(&1).unwrap();
            topic.persist(zendb_types::Barrier::Sync).unwrap();
        }
        let active_path = path.join(segment_name(0));
        let mut file = OpenOptions::new().append(true).open(&active_path).unwrap();
        file.write_all(&8_u32.to_le_bytes()).unwrap();
        file.write_all(&[1, 2]).unwrap();
        drop(file);

        let topic = Topic::<u64>::open(&path, TopicConfig::default()).unwrap();
        assert_eq!(topic.stats().next_offset, 1);
        assert_eq!(fs::metadata(active_path).unwrap().len(), HEADER_SIZE + 12);
    }
}
