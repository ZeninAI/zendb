//! HashTable — persistent key-value store backed by an mmap'd file.
//!
//! ## File layout
//!
//! ```text
//! [directory: bucket_count × u64 LE]  — page offsets for each bucket
//! [bucket pages: 4KB each]
//! ```
//!
//! ## Bucket page
//!
//! ```text
//! [count: u16 LE] [overflow: u64 LE] [entry]*
//!
//! entry = [key_hash: u64 LE] [key_len: u16 LE] [value_len: u32 LE] [key] [value]
//! ```
//!
//! Collisions: linear probing within page, overflow pages if full.
//! Overwrites: compact the page (remove old entry, append new).
//! Deletes: compact the page (remove the entry).

use memmap2::MmapMut;
use std::{
    fs::OpenOptions,
    hash::{Hash, Hasher},
    io,
    path::Path,
};

const PAGE_SIZE: usize = 4096;
const DIR_ENTRY_SIZE: usize = 8;
const HEADER_SIZE: usize = 10; // count(u16) + overflow(u64)
const ENTRY_HEADER: usize = 14; // hash(u64) + key_len(u16) + val_len(u32)

fn hash_key(key: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

pub struct HashTable {
    mmap: MmapMut,
    bucket_count: u64,
    next_page: u64,
    total_pages: u64,
}

impl HashTable {
    pub fn create(path: &Path, bucket_count: u64, total_pages: u64) -> io::Result<HashTable> {
        assert!(bucket_count.is_power_of_two());
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.set_len(total_pages as u64 * PAGE_SIZE as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        // Directory at start: bucket_count entries of u64 LE.
        let dir_size = bucket_count as usize * DIR_ENTRY_SIZE;
        let first_bucket_page = dir_size.div_ceil(PAGE_SIZE);

        for i in 0..bucket_count as usize {
            let page_off = (first_bucket_page + i) * PAGE_SIZE;
            mmap[i * DIR_ENTRY_SIZE..][..DIR_ENTRY_SIZE]
                .copy_from_slice(&(page_off as u64).to_le_bytes());
        }

        for i in 0..bucket_count as usize {
            let off = (first_bucket_page + i) * PAGE_SIZE;
            mmap[off..off + 2].copy_from_slice(&0u16.to_le_bytes()); // count = 0
            mmap[off + 2..off + 10].copy_from_slice(&0u64.to_le_bytes()); // overflow = 0
        }

        Ok(HashTable {
            mmap,
            bucket_count,
            next_page: (first_bucket_page + bucket_count as usize) as u64,
            total_pages,
        })
    }

    pub fn open(path: &Path, bucket_count: u64) -> io::Result<HashTable> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let total_pages = file.metadata()?.len() as usize / PAGE_SIZE;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let dir_size = bucket_count as usize * DIR_ENTRY_SIZE;
        let first_bucket_page = dir_size.div_ceil(PAGE_SIZE);
        Ok(HashTable {
            mmap,
            bucket_count,
            next_page: (first_bucket_page + bucket_count as usize) as u64,
            total_pages: total_pages as u64,
        })
    }

    fn bucket_page(&self, idx: u64) -> u64 {
        let mut buf = [0u8; 8];
        let off = idx as usize * DIR_ENTRY_SIZE;
        buf.copy_from_slice(&self.mmap[off..off + 8]);
        u64::from_le_bytes(buf)
    }

    fn page(&self, off: u64) -> &[u8] {
        &self.mmap[off as usize..][..PAGE_SIZE]
    }

    fn page_mut(&mut self, off: u64) -> &mut [u8] {
        &mut self.mmap[off as usize..][..PAGE_SIZE]
    }

    fn alloc_page(&mut self) -> io::Result<u64> {
        if self.next_page >= self.total_pages {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "hash table full",
            ));
        }
        let off = self.next_page * PAGE_SIZE as u64;
        self.next_page += 1;
        self.page_mut(off).fill(0);
        Ok(off)
    }

    // --- read all entries from a bucket chain ---

    fn read_chain(&self, mut page_off: u64) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        let mut entries = Vec::new();
        while page_off != 0 {
            let p = self.page(page_off);
            let count = u16::from_le_bytes([p[0], p[1]]) as usize;
            let overflow = u64::from_le_bytes(p[2..10].try_into().unwrap());
            let mut pos = HEADER_SIZE;
            for _ in 0..count {
                let hash = u64::from_le_bytes(p[pos..pos + 8].try_into().unwrap());
                let klen = u16::from_le_bytes(p[pos + 8..pos + 10].try_into().unwrap()) as usize;
                let vlen = u32::from_le_bytes(p[pos + 10..pos + 14].try_into().unwrap()) as usize;
                let key = p[pos + 14..pos + 14 + klen].to_vec();
                let val = p[pos + 14 + klen..pos + 14 + klen + vlen].to_vec();
                entries.push((hash, key, val));
                pos += ENTRY_HEADER + klen + vlen;
            }
            page_off = overflow;
        }
        entries
    }

    fn write_chain(
        &mut self,
        mut page_off: u64,
        entries: &[(u64, &[u8], &[u8])],
    ) -> io::Result<()> {
        let mut entry_idx = 0;
        loop {
            // Read overflow before mutable borrow.
            let overflow = {
                let p = self.page(page_off);
                u64::from_le_bytes(p[2..10].try_into().unwrap())
            };

            let mut count = 0u16;
            {
                let p = self.page_mut(page_off);
                let mut pos = HEADER_SIZE;

                while entry_idx < entries.len() {
                    let (hash, key, val) = entries[entry_idx];
                    let sz = ENTRY_HEADER + key.len() + val.len();
                    if pos + sz > PAGE_SIZE {
                        break;
                    }
                    p[pos..pos + 8].copy_from_slice(&hash.to_le_bytes());
                    p[pos + 8..pos + 10].copy_from_slice(&(key.len() as u16).to_le_bytes());
                    p[pos + 10..pos + 14].copy_from_slice(&(val.len() as u32).to_le_bytes());
                    p[pos + 14..pos + 14 + key.len()].copy_from_slice(key);
                    p[pos + 14 + key.len()..pos + 14 + key.len() + val.len()].copy_from_slice(val);
                    pos += sz;
                    count += 1;
                    entry_idx += 1;
                }

                p[0..2].copy_from_slice(&count.to_le_bytes());

                if entry_idx >= entries.len() {
                    p[2..10].copy_from_slice(&0u64.to_le_bytes());
                    return Ok(());
                }

                if overflow != 0 {
                    // Drop p before continuing loop with new page_off
                    p[2..10].copy_from_slice(&overflow.to_le_bytes());
                }
            } // p dropped here

            if overflow != 0 {
                page_off = overflow;
            } else {
                let new_page = self.alloc_page()?;
                {
                    let p = self.page_mut(page_off);
                    p[2..10].copy_from_slice(&new_page.to_le_bytes());
                }
                page_off = new_page;
            }
        }
    }

    // --- public API ---

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let h = hash_key(key);
        let mut page_off = self.bucket_page(h & (self.bucket_count - 1));
        while page_off != 0 {
            let p = self.page(page_off);
            let count = u16::from_le_bytes([p[0], p[1]]) as usize;
            let overflow = u64::from_le_bytes(p[2..10].try_into().unwrap());
            let mut pos = HEADER_SIZE;
            for _ in 0..count {
                let eh = u64::from_le_bytes(p[pos..pos + 8].try_into().unwrap());
                let klen = u16::from_le_bytes(p[pos + 8..pos + 10].try_into().unwrap()) as usize;
                let vlen = u32::from_le_bytes(p[pos + 10..pos + 14].try_into().unwrap()) as usize;
                if eh == h && key.len() == klen && &p[pos + 14..pos + 14 + klen] == key {
                    return Some(p[pos + 14 + klen..pos + 14 + klen + vlen].to_vec());
                }
                pos += ENTRY_HEADER + klen + vlen;
            }
            page_off = overflow;
        }
        None
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let h = hash_key(key);
        let bucket = h & (self.bucket_count - 1);
        let page_off = self.bucket_page(bucket);

        // Read existing chain, replace or append.
        let mut entries: Vec<(u64, &[u8], &[u8])> = self
            .read_chain(page_off)
            .into_iter()
            .map(|(eh, k, v)| {
                if eh == h && k.as_slice() == key {
                    (eh, key, value)
                } else {
                    // Keep ownership in the Vec, borrow below.
                    let k: &[u8] = Box::leak(k.into_boxed_slice());
                    let v: &[u8] = Box::leak(v.into_boxed_slice());
                    (eh, k, v)
                }
            })
            .collect();

        // Check if key already exists (was replaced above).
        let exists = entries.iter().any(|(eh, k, _)| *eh == h && *k == key);
        if !exists {
            entries.push((h, key, value));
        }

        self.write_chain(page_off, &entries)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) {
        let h = hash_key(key);
        let bucket = h & (self.bucket_count - 1);
        let page_off = self.bucket_page(bucket);

        let entries: Vec<(u64, &[u8], &[u8])> = self
            .read_chain(page_off)
            .into_iter()
            .filter(|(eh, k, _)| *eh != h || k.as_slice() != key)
            .map(|(eh, k, v)| {
                let k: &[u8] = Box::leak(k.into_boxed_slice());
                let v: &[u8] = Box::leak(v.into_boxed_slice());
                (eh, k, v)
            })
            .collect();

        let _ = self.write_chain(page_off, &entries);
    }

    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("zendb_ht_{}", name))
    }

    #[test]
    fn put_and_get() {
        let p = tmp("put_get");
        let mut ht = HashTable::create(&p, 16, 64).unwrap();
        ht.put(b"hello", b"world").unwrap();
        assert_eq!(ht.get(b"hello").unwrap(), b"world");
        assert!(ht.get(b"missing").is_none());
        fs::remove_file(&p).ok();
    }

    #[test]
    fn delete() {
        let p = tmp("delete");
        let mut ht = HashTable::create(&p, 16, 64).unwrap();
        ht.put(b"x", b"y").unwrap();
        assert!(ht.get(b"x").is_some());
        ht.delete(b"x");
        assert!(ht.get(b"x").is_none());
        fs::remove_file(&p).ok();
    }

    #[test]
    fn overwrite() {
        let p = tmp("overwrite");
        let mut ht = HashTable::create(&p, 16, 64).unwrap();
        ht.put(b"k", b"v1").unwrap();
        ht.put(b"k", b"v2").unwrap();
        assert_eq!(ht.get(b"k").unwrap(), b"v2");
        fs::remove_file(&p).ok();
    }

    #[test]
    fn reopen() {
        let p = tmp("reopen");
        HashTable::create(&p, 16, 64)
            .unwrap()
            .put(b"persist", b"me")
            .unwrap();
        let ht = HashTable::open(&p, 16).unwrap();
        assert_eq!(ht.get(b"persist").unwrap(), b"me");
        fs::remove_file(&p).ok();
    }
}
