//! Write-Ahead Log — append-only, length-framed binary log.
//!
//! ```text
//! [entry_len: u32 LE][entry_bytes]
//! ```
//!
//! Writes go to the active WAL. To consume, rotate (create new WAL, seal old),
//! then call `into_iter` on the sealed one. No seeks, no offsets, no cursor
//! management — the file cursor handles everything.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub data: Vec<u8>,
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn create(path: &Path) -> io::Result<Wal> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;
        Ok(Wal { file })
    }

    pub fn open(path: &Path) -> io::Result<Wal> {
        let file = OpenOptions::new().write(true).read(true).open(path)?;
        Ok(Wal { file })
    }

    pub fn append(&mut self, entry: &[u8]) -> io::Result<()> {
        let len = entry.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(entry)?;
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_all()
    }

    /// Consume the WAL and iterate entries from the beginning.
    pub fn into_iter(mut self) -> impl Iterator<Item = io::Result<WalEntry>> {
        use std::io::Seek;
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
        let mut len_buf = [0u8; 4];
        if let Err(e) = self.file.read_exact(&mut len_buf) {
            return match e.kind() {
                io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(e)),
            };
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        // Read data
        let mut data = vec![0u8; len];
        if let Err(e) = self.file.read_exact(&mut data) {
            return match e.kind() {
                io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(e)),
            };
        }
        Some(Ok(WalEntry { data }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("zendb_wal_{}", name))
    }

    #[test]
    fn append_and_iterate() {
        let p = tmp("append_iter");
        let mut wal = Wal::create(&p).unwrap();
        wal.append(b"hello").unwrap();
        wal.append(b"world").unwrap();
        let entries: Vec<_> = wal.into_iter().map(|e| e.unwrap().data).collect();
        assert_eq!(entries, vec![b"hello".to_vec(), b"world".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn reopen() {
        let p = tmp("reopen");
        Wal::create(&p).unwrap().append(b"persistent").unwrap();
        let entries: Vec<_> = Wal::open(&p)
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
        assert_eq!(Wal::create(&p).unwrap().into_iter().count(), 0);
        fs::remove_file(&p).ok();
    }
}
