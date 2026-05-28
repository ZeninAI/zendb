//! Table configuration — persisted in the database metadata file.

use std::{io, iter, time::Duration};

use zendb_storage::core::{keydir::KeyDirConfig, wal::WalConfig};

/// What kind of storage backend the table uses.
///
/// `Unordered` carries the `KeyDirConfig` directly — ordered tables
/// (B+Tree) have no backend-specific configuration.
#[derive(Debug, Clone)]
pub enum TableKind {
    Ordered,
    Unordered(KeyDirConfig),
}

/// Configuration for a single table.
#[derive(Debug, Clone)]
pub struct TableConfig {
    pub name: String,
    pub sync_enabled: bool,
    pub kind: TableKind,
    /// WAL configuration used regardless of backend.
    pub wal: WalConfig,
}

impl Default for TableConfig {
    fn default() -> Self {
        TableConfig {
            name: String::new(),
            sync_enabled: true,
            kind: TableKind::Ordered,
            wal: WalConfig {
                ..WalConfig::default()
            },
        }
    }
}

impl TableConfig {
    pub fn new(name: &str) -> Self {
        TableConfig {
            name: name.to_string(),
            ..Default::default()
        }
    }

    pub fn ordered(name: &str) -> Self {
        TableConfig {
            name: name.to_string(),
            kind: TableKind::Ordered,
            ..Default::default()
        }
    }

    pub fn unordered(name: &str) -> Self {
        TableConfig {
            name: name.to_string(),
            kind: TableKind::Unordered(KeyDirConfig::default()),
            ..Default::default()
        }
    }

    pub fn with_sync(mut self, enabled: bool) -> Self {
        self.sync_enabled = enabled;
        self
    }

    /// Set the WAL linger duration in milliseconds.
    pub fn with_wal_linger(mut self, ms: u64) -> Self {
        self.wal.linger = Some(Duration::from_millis(ms));
        self
    }

    /// Set the WAL max buffer size.
    pub fn with_wal_max_buf(mut self, max_buf: usize) -> Self {
        self.wal.max_buf = max_buf;
        self
    }

    /// Set or override the KeyDir configuration (also sets kind to Unordered).
    pub fn with_keydir(mut self, kd: KeyDirConfig) -> Self {
        self.kind = TableKind::Unordered(kd);
        self
    }

    // --- wire format ---

    pub fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_string(out, &self.name);
        out.push(self.sync_enabled as u8);

        match &self.kind {
            TableKind::Ordered => out.push(0),
            TableKind::Unordered(kd) => {
                out.push(1);
                // Pad to 8-byte alignment for rkyv
                let pad = (8 - (out.len() % 8)) % 8;
                out.extend(iter::repeat_n(0u8, pad));
                let kd_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(kd)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                out.extend_from_slice(&(kd_bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&kd_bytes);
            }
        }

        // Pad to 8-byte alignment for rkyv
        let pad = (8 - (out.len() % 8)) % 8;
        out.extend(iter::repeat_n(0u8, pad));
        let wal_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.wal)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        out.extend_from_slice(&(wal_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&wal_bytes);
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> io::Result<(TableConfig, usize)> {
        let mut pos = 0;

        let (name, n) = decode_string(&bytes[pos..])?;
        pos += n;

        if bytes.len() < pos + 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated TableConfig",
            ));
        }
        let sync_enabled = bytes[pos] != 0;
        pos += 1;
        let kind = match bytes[pos] {
            0 => {
                pos += 1;
                TableKind::Ordered
            }
            1 => {
                pos += 1;
                // Skip alignment padding
                let pad = (8 - (pos % 8)) % 8;
                pos += pad;
                if bytes.len() < pos + 4 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated KeyDirConfig length",
                    ));
                }
                let kd_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if bytes.len() < pos + kd_len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated KeyDirConfig",
                    ));
                }
                let kd: KeyDirConfig = {
                    let aligned = bytes[pos..pos + kd_len].to_vec();
                    unsafe {
                        rkyv::from_bytes_unchecked::<KeyDirConfig, rkyv::rancor::Error>(&aligned)
                    }
                }
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                pos += kd_len;
                TableKind::Unordered(kd)
            }
            b => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown TableKind: {}", b),
                ))
            }
        };

        // Skip alignment padding
        let pad = (8 - (pos % 8)) % 8;
        pos += pad;
        if bytes.len() < pos + 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated WalConfig length",
            ));
        }
        let wal_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if bytes.len() < pos + wal_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated WalConfig",
            ));
        }
        let wal: WalConfig = {
            let aligned = bytes[pos..pos + wal_len].to_vec();
            unsafe { rkyv::from_bytes_unchecked::<WalConfig, rkyv::rancor::Error>(&aligned) }
        }
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        pos += wal_len;

        Ok((
            TableConfig {
                name,
                sync_enabled,
                kind,
                wal,
            },
            pos,
        ))
    }
}

// --- helpers ---

fn encode_string(out: &mut Vec<u8>, s: &str) {
    let len = s.len() as u32;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn decode_string(bytes: &[u8]) -> io::Result<(String, usize)> {
    if bytes.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated string len",
        ));
    }
    let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let end = 4 + len;
    if bytes.len() < end {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated string",
        ));
    }
    let s = String::from_utf8(bytes[4..end].to_vec())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok((s, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let cfg = TableConfig::ordered("notes")
            .with_sync(true)
            .with_wal_linger(200);
        let mut buf = Vec::new();
        cfg.encode(&mut buf).unwrap();
        let (decoded, n) = TableConfig::decode(&buf).unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(decoded.name, "notes");
        assert!(decoded.sync_enabled);
        assert!(matches!(decoded.kind, TableKind::Ordered));
        assert_eq!(decoded.wal.linger, Some(Duration::from_millis(200)));
    }

    #[test]
    fn unordered_with_keydir() {
        let kd = KeyDirConfig {
            compaction_ratio: 0.3,
            ..KeyDirConfig::default()
        };
        let cfg = TableConfig::unordered("cache")
            .with_sync(false)
            .with_keydir(kd);
        let mut buf = Vec::new();
        cfg.encode(&mut buf).unwrap();
        let (decoded, _) = TableConfig::decode(&buf).unwrap();
        assert!(!decoded.sync_enabled);
        match decoded.kind {
            TableKind::Unordered(dk) => assert_eq!(dk.compaction_ratio, 0.3),
            _ => panic!("expected Unordered"),
        }
    }

    #[test]
    fn ordered_no_keydir() {
        let cfg = TableConfig::ordered("test");
        assert!(matches!(cfg.kind, TableKind::Ordered));
        let mut buf = Vec::new();
        cfg.encode(&mut buf).unwrap();
        let (decoded, _) = TableConfig::decode(&buf).unwrap();
        assert!(matches!(decoded.kind, TableKind::Ordered));
    }
}
