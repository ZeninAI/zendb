//! Persistent single-writer, multiple-reader append-only topic.
//!
//! [`Topic`] owns the topic files, provides exclusive mutable appends, and
//! creates independent [`TopicConsumer`] handles for reads. Consumers keep
//! only their current segment open and advance volatile offsets without
//! locking other consumers.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use arc_swap::ArcSwap;
use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::Mutex;

use crate::core::{
    keydir::{KeyDir, KeyDirConfig},
    traits::{Backend, DurableStorage, Storage},
};
use crate::utils::serdes::{deserialize_from, with_scratch};

pub type TopicOffset = u64;

const OFFSETS_FILE: &str = "offsets";
const SEGMENT_EXTENSION: &str = "log";
const MAGIC: u32 = 0x4349_5054;
const HEADER_SIZE: u64 = 4;
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Encode, Decode)]
pub struct TopicConfig {
    pub max_segment_bytes: u64,
    pub offsets: KeyDirConfig,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            offsets: KeyDirConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct TopicStats {
    pub earliest_offset: TopicOffset,
    pub next_offset: TopicOffset,
    pub records: u64,
    pub retained_bytes: u64,
}

struct ConsumerState {
    committed: AtomicU64,
    volatile: AtomicU64,
    reader_active: AtomicBool,
}

#[derive(Debug)]
struct Segment {
    base_offset: TopicOffset,
    end_offset: AtomicU64,
    byte_len: AtomicU64,
    path: PathBuf,
    delete_on_drop: AtomicBool,
}

impl Segment {
    fn record_count(&self) -> u64 {
        self.end_offset.load(Ordering::Acquire) - self.base_offset
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if self.delete_on_drop.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

struct TopicShared<T> {
    segments: ArcSwap<Vec<Arc<Segment>>>,
    next_offset: AtomicU64,
    consumers: Mutex<HashMap<String, Arc<ConsumerState>>>,
    offsets: Mutex<KeyDir<String, TopicOffset>>,
    _value: PhantomData<T>,
}

/// Persistent single-writer, multiple-reader append-only topic.
pub struct Topic<T> {
    path: PathBuf,
    config: TopicConfig,
    segments: Vec<Arc<Segment>>,
    active: File,
    shared: Arc<TopicShared<T>>,
    stats: TopicStats,
}

/// Named consumer handle over a topic.
pub struct TopicConsumer<T> {
    topic: Arc<TopicShared<T>>,
    name: String,
    consumer: Arc<ConsumerState>,
    current: Option<SegmentCursor>,
}

impl<T> Topic<T>
where
    T: Encode + Decode<()>,
{
    /// Create a named consumer, registering it at the current tail when first
    /// seen. A consumer name may have only one active handle.
    pub fn consumer(&self, consumer: &str) -> io::Result<TopicConsumer<T>> {
        let mut consumers = self.shared.consumers.lock();
        let state = match consumers.get(consumer) {
            Some(state) => Arc::clone(state),
            None => {
                let offset = self.shared.next_offset.load(Ordering::Acquire);
                self.shared
                    .offsets
                    .lock()
                    .put(consumer.to_owned(), offset)?;
                let state = Arc::new(ConsumerState {
                    committed: AtomicU64::new(offset),
                    volatile: AtomicU64::new(offset),
                    reader_active: AtomicBool::new(false),
                });
                consumers.insert(consumer.to_owned(), Arc::clone(&state));
                state
            }
        };
        if state.reader_active.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("consumer {consumer:?} already has an active reader"),
            ));
        }
        drop(consumers);

        Ok(TopicConsumer {
            topic: Arc::clone(&self.shared),
            name: consumer.to_owned(),
            consumer: state,
            current: None,
        })
    }

    fn active_segment(&self) -> &Arc<Segment> {
        self.segments.last().unwrap()
    }

    fn rotate_segment(&mut self) -> io::Result<()> {
        self.active.flush()?;

        let segment = create_segment(&self.path, self.stats.next_offset)?;
        self.active = open_segment_writer(&segment.path)?;
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
        with_scratch(value, |encoded| {
            let record_size = 4 + encoded.len() as u64;
            if self.active_segment().record_count() != 0
                && self.active_segment().byte_len.load(Ordering::Acquire) + record_size
                    > self.config.max_segment_bytes
            {
                self.rotate_segment()?;
            }

            let offset = self.stats.next_offset;
            self.active
                .write_all(&(encoded.len() as u32).to_le_bytes())?;
            self.active.write_all(encoded)?;

            let segment = self.active_segment();
            segment.byte_len.store(
                segment.byte_len.load(Ordering::Relaxed) + record_size,
                Ordering::Release,
            );
            segment.end_offset.store(offset + 1, Ordering::Release);
            self.stats.next_offset = offset + 1;
            self.stats.records += 1;
            self.stats.retained_bytes += record_size;
            self.shared
                .next_offset
                .store(self.stats.next_offset, Ordering::Release);
            Ok(offset)
        })
    }
}

impl<T> Storage for Topic<T>
where
    T: Encode + Decode<()>,
{
    type Stats = TopicStats;
    type Config = TopicConfig;

    fn stats(&self) -> Self::Stats {
        self.stats.clone()
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
            let committed = committed.into_owned();
            consumers.insert(
                consumer.into_owned(),
                Arc::new(ConsumerState {
                    committed: AtomicU64::new(committed),
                    volatile: AtomicU64::new(committed),
                    reader_active: AtomicBool::new(false),
                }),
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
        let through = {
            let consumers = self.shared.consumers.lock();
            let Some(through) = consumers
                .values()
                .map(|state| state.committed.load(Ordering::Acquire))
                .min()
            else {
                return Ok(());
            };
            through
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

    fn flush(&mut self) -> io::Result<()> {
        self.active.flush()?;
        self.shared.offsets.lock().flush()
    }

    fn sync(&mut self) -> io::Result<()> {
        self.active.sync_all()?;
        self.shared.offsets.lock().sync()
    }
}

impl<T> Drop for Topic<T> {
    fn drop(&mut self) {
        let _ = self.active.flush();
        let _ = self.shared.offsets.lock().flush();
    }
}

struct SegmentCursor {
    // The file must close before the final segment Arc can remove its path.
    file: File,
    segment: Arc<Segment>,
    logical_offset: TopicOffset,
}

impl<T> TopicConsumer<T> {
    /// Persist the current volatile cursor for this consumer.
    pub fn commit(&self) -> io::Result<()> {
        let volatile = self.consumer.volatile.load(Ordering::Acquire);
        self.topic.offsets.lock().put(self.name.clone(), volatile)?;
        self.consumer.committed.store(volatile, Ordering::Release);
        Ok(())
    }

    /// Move the volatile cursor to `offset`. The next read will start at
    /// that offset. Call [`commit`](Self::commit) to persist.
    pub fn seek(&mut self, offset: TopicOffset) {
        self.consumer.volatile.store(offset, Ordering::Release);
        self.current = None;
    }

    /// Rewind this consumer to its last committed cursor.
    pub fn reset(&mut self) -> io::Result<()> {
        self.consumer.volatile.store(
            self.consumer.committed.load(Ordering::Acquire),
            Ordering::Release,
        );
        self.current = None;
        Ok(())
    }

    /// Delete this consumer's persisted cursor and unregister its name.
    pub fn delete(mut self) -> io::Result<()> {
        self.current = None;
        let mut consumers = self.topic.consumers.lock();
        self.topic.offsets.lock().delete(&self.name)?;
        consumers.remove(&self.name);
        self.consumer.reader_active.store(false, Ordering::Release);
        Ok(())
    }

    fn position(&mut self) -> io::Result<bool> {
        let offset = self.consumer.volatile.load(Ordering::Acquire);
        if offset >= self.topic.next_offset.load(Ordering::Acquire) {
            return Ok(false);
        }

        let segments = self.topic.segments.load_full();
        let segment = Arc::clone(
            segments
                .iter()
                .find(|segment| {
                    offset >= segment.base_offset
                        && offset < segment.end_offset.load(Ordering::Acquire)
                })
                .unwrap(),
        );
        let mut file = File::open(&segment.path)?;
        let mut byte_offset = HEADER_SIZE;
        for _ in segment.base_offset..offset {
            file.seek(SeekFrom::Start(byte_offset))?;
            let mut size = [0; 4];
            file.read_exact(&mut size)?;
            byte_offset += 4 + u32::from_le_bytes(size) as u64;
        }
        file.seek(SeekFrom::Start(byte_offset))?;
        self.current = Some(SegmentCursor {
            file,
            segment,
            logical_offset: offset,
        });
        Ok(true)
    }
}

impl<T> Iterator for TopicConsumer<T>
where
    T: Decode<()>,
{
    type Item = io::Result<T>;

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
            match self.position() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => return Some(Err(error)),
            }
        }

        let cursor = self.current.as_mut().unwrap();
        let mut size = [0; 4];
        let result = cursor.file.read_exact(&mut size).and_then(|_| {
            let value_size = u32::from_le_bytes(size) as usize;
            let mut bytes = crate::utils::reusables::PooledBuf::acquire();
            bytes.resize(value_size, 0);
            cursor.file.read_exact(&mut bytes[..])?;
            deserialize_from(&bytes[..])
        });
        if result.is_ok() {
            cursor.logical_offset += 1;
            self.consumer
                .volatile
                .store(cursor.logical_offset, Ordering::Release);
        }
        Some(result)
    }
}

impl<T> Drop for TopicConsumer<T> {
    fn drop(&mut self) {
        self.consumer.reader_active.store(false, Ordering::Release);
    }
}

fn segment_name(base_offset: TopicOffset) -> String {
    format!("{base_offset:020}.{SEGMENT_EXTENSION}")
}

fn list_segments(path: &Path) -> io::Result<Vec<(TopicOffset, PathBuf)>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some(SEGMENT_EXTENSION) {
            continue;
        }
        let Some(base_offset) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse().ok())
        else {
            continue;
        };
        segments.push((base_offset, path));
    }
    segments.sort_by_key(|(base_offset, _)| *base_offset);
    Ok(segments)
}

fn check_segment_magic(path: &Path) -> io::Result<()> {
    let mut file = File::open(path)?;
    let mut magic = [0; 4];
    file.read_exact(&mut magic)?;
    if u32::from_le_bytes(magic) != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a topic segment",
        ));
    }
    Ok(())
}

fn create_segment(path: &Path, base_offset: TopicOffset) -> io::Result<Arc<Segment>> {
    let segment_path = path.join(segment_name(base_offset));
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&segment_path)?;
    file.write_all(&MAGIC.to_le_bytes())?;
    Ok(Arc::new(Segment {
        base_offset,
        end_offset: AtomicU64::new(base_offset),
        byte_len: AtomicU64::new(HEADER_SIZE),
        path: segment_path,
        delete_on_drop: AtomicBool::new(false),
    }))
}

fn open_segment_writer(path: &Path) -> io::Result<File> {
    let mut file = OpenOptions::new().read(true).append(true).open(path)?;
    file.seek(SeekFrom::End(0))?;
    Ok(file)
}

fn scan_active_segment(path: &Path, base_offset: TopicOffset) -> io::Result<(u64, u64, u64)> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut magic = [0; 4];
    file.read_exact(&mut magic)?;
    if u32::from_le_bytes(magic) != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a topic segment",
        ));
    }

    let mut records = 0;
    let mut cursor = HEADER_SIZE;
    while cursor + 4 <= file_len {
        file.seek(SeekFrom::Start(cursor))?;
        let mut size = [0; 4];
        file.read_exact(&mut size)?;
        let end = cursor + 4 + u32::from_le_bytes(size) as u64;
        if end > file_len {
            break;
        }
        records += 1;
        cursor = end;
    }
    Ok((base_offset + records, cursor, file_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    fn tmp(label: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join("zendb_topic_tests").join(format!(
            "{label}_{}_{}",
            std::process::id(),
            n
        ));
        let _ = fs::remove_dir_all(&path);
        path
    }

    fn config(max_segment_bytes: u64) -> TopicConfig {
        TopicConfig {
            max_segment_bytes,
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
    fn dropping_reader_preserves_volatile_progress_until_commit() {
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
            assert_eq!(reader.next().unwrap().unwrap(), 2);
            reader.commit().unwrap();
        }

        topic.sync().unwrap();
        drop(topic);

        let topic = Topic::<u64>::open(&path, TopicConfig::default()).unwrap();
        let mut reader = topic.consumer("c").unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), 3);
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
        reader.reset().unwrap();
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
        consumer.seek(first + 1);
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
            topic.sync().unwrap();
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
            topic.sync().unwrap();
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
