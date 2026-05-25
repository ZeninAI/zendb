//! Write-Ahead Log — append-only, length-framed binary log.
//!
//! ```text
//! [entry_len: u32 LE][entry_bytes]
//! ```
//!
//! Entries are buffered in a **fixed-capacity** in-memory buffer and
//! flushed to disk when any of these conditions is met:
//!
//! 1. The next entry won't fit in the remaining buffer space.
//! 2. An explicit `sync()` call is made.
//! 3. `linger` has elapsed since the last flush (if configured).
//! 4. `flush_if_stale()` is called by the caller's event loop.
//!
//! Entries larger than `max_buf` itself bypass the buffer entirely and are
//! written directly to disk (the buffer is drained first to preserve
//! ordering).
//!
//! The caller **must** call `sync()` to guarantee durability.
//!
//! To consume, rotate (create new WAL, seal old), call `sync()` on the old
//! one, then call `into_iter`.  No seeks, no offsets, no cursor management —
//! the file cursor handles everything.

use std::{
    fs::{File, OpenOptions},
    io::{self, IoSlice, Read, Write},
    path::Path,
    time::{Duration, Instant},
};

/// Default buffer capacity (16 KiB).
const DEFAULT_MAX_BUF: usize = 16 * 1024;

/// Per-entry framing overhead: 4 bytes length.
const FRAMING: usize = 4;

/// Configuration for a WAL instance.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum buffer size before auto-flush.
    pub max_buf: usize,
    /// Flush the buffer if it has been sitting longer than this.
    /// `None` = never auto-flush on time.
    pub linger: Option<Duration>,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            max_buf: DEFAULT_MAX_BUF,
            linger: None,
        }
    }
}

impl WalConfig {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.max_buf as u64).to_le_bytes());
        match self.linger {
            Some(d) => {
                out.push(1);
                out.extend_from_slice(&d.as_millis().to_le_bytes());
            }
            None => out.push(0),
        }
    }

    pub fn decode(bytes: &[u8]) -> io::Result<(WalConfig, usize)> {
        if bytes.len() < 9 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated WalConfig",
            ));
        }
        let max_buf = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]) as usize;
        let linger = match bytes[8] {
            1 => {
                if bytes.len() < 25 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated WalConfig linger",
                    ));
                }
                let mut b = [0u8; 8];
                b.copy_from_slice(&bytes[9..17]);
                let ms = u64::from_le_bytes(b);
                // 8 bytes u64 for max_buf + 8 bytes u128 for millis
                Some(Duration::from_millis(ms))
            }
            _ => None,
        };
        let consumed = if bytes[8] == 1 { 25 } else { 9 };
        Ok((WalConfig { max_buf, linger }, consumed))
    }
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub data: Vec<u8>,
}

pub struct Wal {
    file: File,
    config: WalConfig,
    /// Fixed-capacity accumulation buffer.  Never grows beyond `config.max_buf`.
    buf: Vec<u8>,
    /// Timestamp of the most recent `flush_buf` call.
    last_flush: Instant,
}

impl Wal {
    // ---- constructors ----

    /// Create a new (or truncate an existing) WAL file at `path`.
    pub fn create(path: &Path, config: WalConfig) -> io::Result<Wal> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;
        Ok(Wal {
            file,
            buf: Vec::with_capacity(config.max_buf),
            config,
            last_flush: Instant::now(),
        })
    }

    /// Open an existing WAL file for appending and iteration.
    pub fn open(path: &Path, config: WalConfig) -> io::Result<Wal> {
        let file = OpenOptions::new().write(true).read(true).open(path)?;
        Ok(Wal {
            file,
            buf: Vec::with_capacity(config.max_buf),
            config,
            last_flush: Instant::now(),
        })
    }

    // ---- write path ----

    /// Append an entry.
    ///
    /// Three paths, tried in order:
    ///
    /// 1. **Direct write** — if `entry + framing > max_buf`, the buffer is
    ///    drained first, then the entry is written straight to disk in a
    ///    single syscall (preserving order).
    /// 2. **Flush-then-buffer** — if the entry fits in `max_buf` but not in
    ///    the remaining buffer space, the buffer is flushed first.
    /// 3. **Buffer** — otherwise the entry is serialised into the buffer.
    ///
    /// The entry is **not** durable until `sync()` is called.
    pub fn append(&mut self, entry: &[u8]) -> io::Result<()> {
        self.flush_if_stale_inner()?;

        let needed = entry.len() + FRAMING;

        // Path 1: entry larger than the entire buffer — direct write.
        if needed > self.config.max_buf {
            self.flush_buf()?; // drain to preserve ordering
            return self.write_direct(entry);
        }

        // Path 2: entry fits, but not enough headroom — flush first.
        if self.buf.len() + needed > self.config.max_buf {
            self.flush_buf()?;
        }

        // Path 3: serialize into buffer.
        let len = entry.len() as u32;
        self.buf.extend_from_slice(&len.to_le_bytes());
        self.buf.extend_from_slice(entry);
        Ok(())
    }

    /// Flush the buffer to the OS and `fsync` the file.
    ///
    /// After this returns the entries are durable (modulo drive write-cache
    /// lies — that's a hardware problem).
    pub fn sync(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        self.file.sync_all()
    }

    // ---- Approach B: caller-driven periodic flush ----

    /// Flush the buffer if `linger` has elapsed since the last flush.
    ///
    /// Returns `true` if a flush actually occurred.  Call this from your
    /// event loop / tick to guarantee timely flushes even when no writes
    /// are happening:
    ///
    /// ```ignore
    /// loop {
    ///     // ... handle commands, call wal.append() ...
    ///     wal.flush_if_stale()?;
    ///     std::thread::sleep(Duration::from_millis(10));
    /// }
    /// ```
    ///
    /// This is a cheap no-op when `linger` is not configured, the buffer
    /// is empty, or the linger hasn't expired.
    pub fn flush_if_stale(&mut self) -> io::Result<bool> {
        self.flush_if_stale_inner()
    }

    /// Shared logic for both append-triggered and caller-triggered checks.
    #[inline]
    fn flush_if_stale_inner(&mut self) -> io::Result<bool> {
        if let Some(linger) = self.config.linger {
            if !self.buf.is_empty() && self.last_flush.elapsed() >= linger {
                self.flush_buf()?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Write accumulated buffer to the file in a single syscall.
    fn flush_buf(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.buf)?;
        self.buf.clear();
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Write a single oversized entry directly to disk, bypassing the buffer.
    ///
    /// Caller must have already drained the buffer (to preserve ordering).
    /// Does *not* reset `last_flush` — the timer tracks buffer contents,
    /// and a direct write hasn't changed the buffer state.
    fn write_direct(&mut self, entry: &[u8]) -> io::Result<()> {
        debug_assert!(
            self.buf.is_empty(),
            "write_direct requires the buffer to be drained first"
        );
        let len_bytes = (entry.len() as u32).to_le_bytes();
        // Attempt a single syscall via vectored I/O (writev on Linux).
        // On Windows, write_vectored for files writes only the first IoSlice,
        // so we handle the short-write case by completing with write_all.
        let written = self
            .file
            .write_vectored(&[IoSlice::new(&len_bytes), IoSlice::new(entry)])?;
        let total = 4 + entry.len();
        if written < total {
            if written < 4 {
                self.file.write_all(&len_bytes[written..])?;
                self.file.write_all(entry)?;
            } else {
                self.file.write_all(&entry[written - 4..])?;
            }
        }
        Ok(())
    }

    // ---- read path ----

    /// Consume the WAL and iterate entries from the beginning.
    ///
    /// Any buffered-but-unflushed entries are flushed first (so that a
    /// dropped WAL without an explicit `sync()` doesn't silently lose
    /// data).
    pub fn into_iter(mut self) -> impl Iterator<Item = io::Result<WalEntry>> {
        use std::io::Seek;
        // Drain any remaining buffered data before reading.
        let _ = self.flush_buf();
        let _ = self.file.seek(std::io::SeekFrom::Start(0));
        WalIter { file: self.file }
    }
}

struct WalIter {
    file: File,
}

impl Iterator for WalIter {
    type Item = io::Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        // --- read length (4 bytes) ---
        let mut len_buf = [0u8; 4];
        if let Err(e) = self.file.read_exact(&mut len_buf) {
            return match e.kind() {
                io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(e)),
            };
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        // --- read entry bytes ---
        let mut data = vec![0u8; len];
        if let Err(e) = self.file.read_exact(&mut data) {
            return match e.kind() {
                io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(e)),
            };
        }

        Some(Ok(WalEntry { data: data }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, thread};

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("zendb_wal_{}", name))
    }

    fn default_cfg() -> WalConfig {
        WalConfig::default()
    }

    #[test]
    fn append_and_iterate() {
        let p = tmp("append_iter");
        let mut wal = Wal::create(&p, default_cfg()).unwrap();
        wal.append(b"hello").unwrap();
        wal.append(b"world").unwrap();
        wal.sync().unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries, vec![b"hello".to_vec(), b"world".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn reopen() {
        let p = tmp("reopen");
        let mut wal = Wal::create(&p, default_cfg()).unwrap();
        wal.append(b"persistent").unwrap();
        wal.sync().unwrap();
        let entries: Vec<_> = Wal::open(&p, default_cfg())
            .unwrap()
            .into_iter()
            .map(|e| e.unwrap().data)
            .collect();
        assert_eq!(entries, vec![b"persistent".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn empty() {
        let p = tmp("empty");
        let wal = Wal::create(&p, default_cfg()).unwrap();
        assert_eq!(wal.into_iter().count(), 0);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn auto_flush_on_threshold() {
        let p = tmp("auto_flush");
        let cfg = WalConfig {
            max_buf: 1,
            ..default_cfg()
        };
        let mut wal = Wal::create(&p, cfg).unwrap();
        wal.append(b"a").unwrap();
        wal.append(b"b").unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries, vec![b"a".to_vec(), b"b".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn into_iter_flushes_implicitly() {
        let p = tmp("implicit_flush");
        let mut wal = Wal::create(&p, default_cfg()).unwrap();
        wal.append(b"unsynced").unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries, vec![b"unsynced".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn linger_flushes_on_append() {
        let p = tmp("linger_append");
        let cfg = WalConfig {
            linger: Some(Duration::from_millis(10)),
            ..default_cfg()
        };
        let mut wal = Wal::create(&p, cfg).unwrap();
        wal.append(b"first").unwrap();
        thread::sleep(Duration::from_millis(20));
        wal.append(b"second").unwrap();
        wal.sync().unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries, vec![b"first".to_vec(), b"second".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn linger_does_not_flush_too_soon() {
        let p = tmp("linger_not_soon");
        let cfg = WalConfig {
            linger: Some(Duration::from_secs(10)),
            ..default_cfg()
        };
        let mut wal = Wal::create(&p, cfg).unwrap();
        wal.append(b"a").unwrap();
        wal.append(b"b").unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries, vec![b"a".to_vec(), b"b".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn flush_if_stale_idle() {
        let p = tmp("flush_stale_idle");
        let cfg = WalConfig {
            linger: Some(Duration::from_millis(10)),
            ..default_cfg()
        };
        let mut wal = Wal::create(&p, cfg).unwrap();
        wal.append(b"idle_data").unwrap();
        thread::sleep(Duration::from_millis(20));
        let flushed = wal.flush_if_stale().unwrap();
        assert!(flushed);
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries, vec![b"idle_data".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn flush_if_stale_noop() {
        let p = tmp("flush_stale_noop");
        let mut wal = Wal::create(&p, default_cfg()).unwrap();
        wal.append(b"data").unwrap();
        let flushed = wal.flush_if_stale().unwrap();
        assert!(!flushed, "should be no-op when no linger configured");
        fs::remove_file(&p).ok();
    }

    #[test]
    fn flush_if_stale_empty_buffer() {
        let p = tmp("flush_stale_empty");
        let cfg = WalConfig {
            linger: Some(Duration::from_millis(1)),
            ..default_cfg()
        };
        let mut wal = Wal::create(&p, cfg).unwrap();
        thread::sleep(Duration::from_millis(10));
        let flushed = wal.flush_if_stale().unwrap();
        assert!(!flushed, "empty buffer → nothing to flush");
        fs::remove_file(&p).ok();
    }

    #[test]
    fn direct_write_large_entry() {
        let p = tmp("direct_write");
        let cfg = WalConfig {
            max_buf: 64,
            ..default_cfg()
        };
        let mut wal = Wal::create(&p, cfg).unwrap();
        let large = vec![0xABu8; 100];
        wal.append(&large).unwrap();
        wal.sync().unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], large);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn ordering_mixed_small_and_large() {
        let p = tmp("mixed_ordering");
        let cfg = WalConfig {
            max_buf: 64,
            ..default_cfg()
        };
        let mut wal = Wal::create(&p, cfg).unwrap();
        wal.append(b"small1").unwrap();
        wal.append(b"small2").unwrap();
        let large = vec![0xCDu8; 200];
        wal.append(&large).unwrap();
        wal.append(b"small3").unwrap();
        wal.sync().unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0], b"small1".to_vec());
        assert_eq!(entries[1], b"small2".to_vec());
        assert_eq!(entries[2], large);
        assert_eq!(entries[3], b"small3".to_vec());
        fs::remove_file(&p).ok();
    }

    #[test]
    fn flush_before_direct_preserves_order() {
        let p = tmp("flush_before_direct");
        let cfg = WalConfig {
            max_buf: 64,
            ..default_cfg()
        };
        let mut wal = Wal::create(&p, cfg).unwrap();
        wal.append(b"a").unwrap();
        wal.append(b"b").unwrap();
        let large = vec![0xEFu8; 128];
        wal.append(&large).unwrap();
        wal.sync().unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries[0], b"a".to_vec());
        assert_eq!(entries[1], b"b".to_vec());
        assert_eq!(entries[2], large);
        fs::remove_file(&p).ok();
    }
}
