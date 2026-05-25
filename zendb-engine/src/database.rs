//! Database — entry point for ZeninDB.  Owns multiple tables, routes
//! deltas to the correct table, and persists table configurations in a
//! metadata file backed by KeyDir (one key per table).

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use hashbrown::HashMap;
use zendb_storage::core::keydir::{KeyDir, KeyDirConfig};
use zendb_types::Delta;

use crate::config::TableConfig;
use crate::table::Table;

pub struct Database {
    path: PathBuf,
    meta: KeyDir,
    tables: HashMap<String, Table>,
}

impl Database {
    /// Open (or create) a database at `path`.  Recovers existing tables
    /// from the metadata file.
    pub fn open(path: &Path) -> io::Result<Database> {
        fs::create_dir_all(path)?;

        let meta_path = path.join("_meta");
        let meta = if meta_path.exists() {
            KeyDir::open(&meta_path, KeyDirConfig::default())?
        } else {
            KeyDir::create(&meta_path, KeyDirConfig::default())?
        };

        // Recover tables from metadata — each key is a table name.
        let mut tables = HashMap::new();
        for (key, value) in meta.entries() {
            let name = String::from_utf8(key.to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let (config, _) = TableConfig::decode(&value)?;
            let table = Table::open(path, &config)?;
            tables.insert(name, table);
        }

        Ok(Database {
            path: path.to_path_buf(),
            meta,
            tables,
        })
    }

    /// Get or create a table.  If the table doesn't exist, `config` is
    /// used to create it and is persisted to metadata.
    pub fn table_with_config(&mut self, config: TableConfig) -> io::Result<&mut Table> {
        let name = config.name.clone();
        if !self.tables.contains_key(&name) {
            let table = Table::open(&self.path, &config)?;
            self.tables.insert(name.clone(), table);

            // Persist under the table name.
            let mut buf = Vec::new();
            config.encode(&mut buf);
            self.meta.put(name.as_bytes(), &buf)?;
            self.meta.flush()?;
        }
        Ok(self.tables.get_mut(&name).unwrap())
    }

    /// Get or create a table with default ordered config.
    pub fn table(&mut self, name: &str) -> io::Result<&mut Table> {
        if self.tables.contains_key(name) {
            return Ok(self.tables.get_mut(name).unwrap());
        }
        self.table_with_config(TableConfig::ordered(name))
    }

    /// Apply a delta, routing it to the correct table.
    pub fn apply(&mut self, delta: Delta) -> io::Result<()> {
        let table_name = delta.table_id.0.clone();
        let table = self.table(&table_name)?;
        table.apply(delta)
    }

    /// Flush all tables.
    pub fn flush_all(&mut self) -> io::Result<()> {
        for table in self.tables.values_mut() {
            table.flush()?;
        }
        Ok(())
    }

    /// Sync all tables and metadata to disk.
    pub fn sync_all(&mut self) -> io::Result<()> {
        for table in self.tables.values_mut() {
            table.sync()?;
        }
        self.meta.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zendb_types::{AtomOp, AtomValue, Hlc, Op, Path as ZPath, PrimaryKey, TableId};

    fn tmp_dir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("zendb_db_{}", name));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn recover_tables_after_reopen() {
        let dir = tmp_dir("recover");
        {
            let mut db = Database::open(&dir).unwrap();
            db.table_with_config(TableConfig::ordered("notes").with_sync(true))
                .unwrap();
            db.table_with_config(TableConfig::unordered("cache").with_sync(false))
                .unwrap();
            db.sync_all().unwrap();
        }
        // Reopen — both tables should be recovered.
        let db = Database::open(&dir).unwrap();
        assert!(db.tables.contains_key("notes"));
        assert!(db.tables.contains_key("cache"));
    }

    #[test]
    fn multi_table_routing() {
        let dir = tmp_dir("multi");
        let mut db = Database::open(&dir).unwrap();

        let d1 = Delta {
            table_id: TableId("notes".into()),
            primary_key: PrimaryKey(AtomValue::String("n1".into())),
            path: ZPath::new(),
            op: Op::Atom(AtomOp::Set(AtomValue::String("hello".into()))),
            hlc: Hlc::new(100, 0, 1).unwrap(),
            sync: false,
            signature: zendb_types::Signature(vec![]),
        };

        let d2 = Delta {
            table_id: TableId("todos".into()),
            primary_key: PrimaryKey(AtomValue::String("t1".into())),
            path: ZPath::new(),
            op: Op::Atom(AtomOp::Set(AtomValue::String("buy milk".into()))),
            hlc: Hlc::new(200, 0, 1).unwrap(),
            sync: false,
            signature: zendb_types::Signature(vec![]),
        };

        db.apply(d1).unwrap();
        db.apply(d2).unwrap();
        db.flush_all().unwrap();
    }
}
