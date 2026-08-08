//! Topic segment files, rotation metadata, and sparse-index construction.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use parking_lot::RwLock;

use super::types::{HEADER_SIZE, MAGIC, SEGMENT_EXTENSION, TopicOffset};

#[derive(Debug)]
pub(super) struct Segment {
    pub(super) base_offset: TopicOffset,
    pub(super) end_offset: AtomicU64,
    pub(super) byte_len: AtomicU64,
    pub(super) sparse_index: RwLock<Vec<(TopicOffset, u64)>>,
    pub(super) path: PathBuf,
    pub(super) delete_on_drop: AtomicBool,
}

impl Segment {
    pub(super) fn record_count(&self) -> u64 {
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

pub(super) fn segment_name(base_offset: TopicOffset) -> String {
    format!("{base_offset:020}.{SEGMENT_EXTENSION}")
}

pub(super) fn list_segments(path: &Path) -> io::Result<Vec<(TopicOffset, PathBuf)>> {
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

pub(super) fn check_segment_magic(path: &Path) -> io::Result<()> {
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

pub(super) fn create_segment(path: &Path, base_offset: TopicOffset) -> io::Result<Arc<Segment>> {
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
        sparse_index: RwLock::new(vec![(base_offset, HEADER_SIZE)]),
        path: segment_path,
        delete_on_drop: AtomicBool::new(false),
    }))
}

pub(super) fn open_segment_writer(path: &Path) -> io::Result<File> {
    let mut file = OpenOptions::new().read(true).append(true).open(path)?;
    file.seek(SeekFrom::End(0))?;
    Ok(file)
}

pub(super) fn scan_active_segment(
    path: &Path,
    base_offset: TopicOffset,
) -> io::Result<(u64, u64, u64)> {
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

pub(super) fn build_sparse_index(
    path: &Path,
    base_offset: TopicOffset,
    end_offset: TopicOffset,
    stride: u64,
) -> io::Result<Vec<(TopicOffset, u64)>> {
    let mut file = File::open(path)?;
    let mut index = Vec::new();
    let mut offset = base_offset;
    let mut byte_offset = HEADER_SIZE;
    while offset < end_offset {
        if (offset - base_offset).is_multiple_of(stride) {
            index.push((offset, byte_offset));
        }
        file.seek(SeekFrom::Start(byte_offset))?;
        let mut size = [0; 4];
        file.read_exact(&mut size)?;
        byte_offset += 4 + u32::from_le_bytes(size) as u64;
        offset += 1;
    }
    Ok(index)
}
