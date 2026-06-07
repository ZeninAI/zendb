//! Database — owner of multiple [`Table`]s, with a persistent catalog.
//!
//! Layout on disk:
//!
//! ```text
//! <db_path>/
//!     _meta              ← KeyDir<String, TableConfig>; the catalog
//!     <table_name>/      ← per-table directory (state/, events/)
//!     ...
//! ```
//!
//! The `Database` is a thin façade. It does not expose any read/write
//! methods of its own — those live on [`Table`]. The catalog persists
//! `(table_name → TableConfig)` so that `open_table` can rehydrate a
//! table on demand without the caller re-supplying its config.

use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::{Path, PathBuf},
};

use zendb_storage::core::{
    backend::Backend,
    keydir::{KeyDir, KeyDirConfig},
};

use crate::table::{Table, TableConfig};

/// Filename of the catalog KeyDir inside the database directory.
const META_FILE: &str = "_meta";

pub struct Database {
    path: PathBuf,
    catalog: KeyDir<String, TableConfig>,
    /// Mirror of the catalog's key set so we can hand out borrowed
    /// `&str`s in `all_tables()` without depending on what `Cow`
    /// variant `KeyDir::keys` happens to yield.
    names: HashSet<String>,
    tables: HashMap<String, Table>,
}

impl Database {
    /// Create a fresh database at `path`. Initializes an empty catalog
    /// at `<path>/_meta`. `path` is created if it doesn't exist.
    pub fn create(path: &Path) -> io::Result<Self> {
        fs::create_dir_all(path)?;
        let catalog = KeyDir::create(&path.join(META_FILE), KeyDirConfig::default())?;
        Ok(Self {
            path: path.to_path_buf(),
            catalog,
            names: HashSet::new(),
            tables: HashMap::new(),
        })
    }

    /// Open an existing database at `path`. The catalog is loaded but
    /// no tables are eagerly opened — call [`Database::open_table`] for
    /// each table you actually need.
    pub fn open(path: &Path) -> io::Result<Self> {
        let catalog: KeyDir<String, TableConfig> =
            KeyDir::open(&path.join(META_FILE), KeyDirConfig::default())?;
        let names = catalog.keys().map(|k| k.into_owned()).collect();
        Ok(Self {
            path: path.to_path_buf(),
            catalog,
            names,
            tables: HashMap::new(),
        })
    }

    /// Create a new table named `name` with the given config. Writes
    /// the config into the catalog and creates the table on disk under
    /// `<db_path>/<name>/`. Errors with `AlreadyExists` if a table with
    /// that name is already in the catalog.
    pub fn create_table(&mut self, name: &str, config: TableConfig) -> io::Result<&mut Table> {
        if self.tables.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("table {name:?} is already open"),
            ));
        }
        if !self
            .catalog
            .put_if_absent(name.to_string(), config.clone())?
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("table {name:?} already exists in catalog"),
            ));
        }
        self.catalog.sync()?;
        self.names.insert(name.to_string());

        let table = Table::create(&self.path.join(name), config)?;
        Ok(self.tables.entry(name.to_string()).or_insert(table))
    }

    /// Open the table named `name`. If already loaded in memory, returns
    /// the existing handle. Otherwise looks up the table's config in the
    /// catalog and opens the on-disk table. Errors with `NotFound` if no
    /// catalog entry exists for `name`.
    pub fn open_table(&mut self, name: &str) -> io::Result<&mut Table> {
        if !self.tables.contains_key(name) {
            let config = match self.catalog.get(&name.to_string()) {
                Some(c) => c.into_owned(),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("no table {name:?} in catalog"),
                    ));
                }
            };
            let table = Table::open(&self.path.join(name), config)?;
            self.tables.insert(name.to_string(), table);
        }
        Ok(self.tables.get_mut(name).unwrap())
    }

    /// Borrow an in-memory table handle, if it has been loaded.
    /// Does **not** consult the on-disk catalog.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// Mutably borrow an in-memory table handle, if it has been loaded.
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    /// Names of every table known to the catalog (loaded or not).
    pub fn all_tables(&self) -> impl Iterator<Item = &str> + '_ {
        self.names.iter().map(String::as_str)
    }

    /// Names of tables currently held in memory.
    pub fn all_open_tables(&self) -> impl Iterator<Item = &str> + '_ {
        self.tables.keys().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use zendb_storage::core::backend::Backend;
    use zendb_types::{Delta, Hlc, Op, Path as ValuePath, PrimaryKey, Value};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn tmp_db(name: &str) -> PathBuf {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("zendb_database_{name}_{id}"))
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, [1u8; 8]).unwrap()
    }

    fn delta(table: &str, key: &str, value: i64, hlc: Hlc) -> Delta {
        Delta {
            table_id: table.into(),
            primary_key: PrimaryKey::String(key.into()),
            path: ValuePath::new(),
            op: Op::Replace {
                value: Value::Int(value),
            },
            hlc,
            sync: false,
            signature: Vec::new(),
        }
    }

    #[test]
    fn create_initialises_empty_database() {
        let path = tmp_db("create_empty");
        let db = Database::create(&path).unwrap();
        assert!(db.tables.is_empty());
        assert_eq!(db.all_tables().count(), 0);
        assert_eq!(db.all_open_tables().count(), 0);
        assert!(path.join("_meta").exists());
    }

    #[test]
    fn create_table_persists_config_and_returns_handle() {
        let path = tmp_db("create_table");
        let mut db = Database::create(&path).unwrap();
        let table = db.create_table("users", TableConfig::default()).unwrap();
        // Round-trip a delta to confirm we got a real table.
        table
            .insert_delta(delta("users", "u1", 1, hlc(100)))
            .unwrap();
        let got = Backend::get(table, &PrimaryKey::String("u1".into())).unwrap();
        assert_eq!(got.into_owned().value, Some(Value::Int(1)));

        // Catalog now holds the entry.
        assert!(db.catalog.contains(&"users".to_string()));
        // On-disk table directory exists.
        assert!(path.join("users").is_dir());
    }

    #[test]
    fn create_table_rejects_duplicate() {
        let path = tmp_db("dup_table");
        let mut db = Database::create(&path).unwrap();
        db.create_table("t", TableConfig::default()).unwrap();
        let err = db.create_table("t", TableConfig::default()).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn open_database_then_open_table_recovers_data() {
        let path = tmp_db("reopen");
        {
            let mut db = Database::create(&path).unwrap();
            let table = db.create_table("t", TableConfig::default()).unwrap();
            table.insert_delta(delta("t", "k", 7, hlc(100))).unwrap();
            Backend::sync(table).unwrap();
        }

        let mut db = Database::open(&path).unwrap();
        assert!(db.get_table("t").is_none(), "tables open lazily");
        let table = db.open_table("t").unwrap();
        assert_eq!(
            Backend::get(table, &PrimaryKey::String("k".into()))
                .unwrap()
                .into_owned()
                .value,
            Some(Value::Int(7))
        );
    }

    #[test]
    fn open_table_returns_cached_handle_on_second_call() {
        let path = tmp_db("cached_handle");
        let mut db = Database::create(&path).unwrap();
        db.create_table("t", TableConfig::default()).unwrap();

        // Inserting via the second open_table call must observe the
        // first call's state — proving they returned the same handle.
        db.open_table("t")
            .unwrap()
            .insert_delta(delta("t", "k", 1, hlc(100)))
            .unwrap();
        let table = db.open_table("t").unwrap();
        let got = Backend::get(table, &PrimaryKey::String("k".into())).unwrap();
        assert_eq!(got.into_owned().value, Some(Value::Int(1)));
    }

    #[test]
    fn open_table_errors_on_missing_catalog_entry() {
        let path = tmp_db("missing");
        let mut db = Database::create(&path).unwrap();
        let err = db.open_table("ghost").err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn get_table_only_sees_loaded_tables() {
        let path = tmp_db("get_loaded");
        {
            let mut db = Database::create(&path).unwrap();
            db.create_table("t", TableConfig::default()).unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        assert!(db.get_table("t").is_none());
        db.open_table("t").unwrap();
        assert!(db.get_table("t").is_some());
        assert!(db.get_table_mut("t").is_some());
    }

    #[test]
    fn all_tables_lists_catalog_all_open_tables_lists_loaded() {
        let path = tmp_db("listers");
        {
            let mut db = Database::create(&path).unwrap();
            db.create_table("a", TableConfig::default()).unwrap();
            db.create_table("b", TableConfig::default()).unwrap();
            db.create_table("c", TableConfig::default()).unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        let mut all: Vec<&str> = db.all_tables().collect();
        all.sort();
        assert_eq!(all, vec!["a", "b", "c"]);
        assert_eq!(db.all_open_tables().count(), 0);

        db.open_table("b").unwrap();
        db.open_table("a").unwrap();
        let mut open: Vec<&str> = db.all_open_tables().collect();
        open.sort();
        assert_eq!(open, vec!["a", "b"]);

        // Catalog list is unchanged by what's open.
        let mut all: Vec<&str> = db.all_tables().collect();
        all.sort();
        assert_eq!(all, vec!["a", "b", "c"]);
    }
}
