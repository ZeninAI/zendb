//! Replication-aware table storage.
//!
//! A table owns one materialized Cell tree per row, a write-back cache, and a
//! durable change topic. Replica-local sync policy lives in that Cell tree;
//! there is no second shared state or overlay state.

use std::{borrow::Cow, collections::BTreeMap, fs, io, path::Path as FsPath};

use bincode::{Decode, Encode};
use zendb_types::{
    device_id, Cell, ContainerType, DeviceId, Event, Hlc, MergeClocks, Path, PrimaryKey,
    SyncPolicy, SyncScope,
};

use zendb_storage::{
    DurableStorage, ReadBackend, SkipList, SkipListCapacity, SkipListConfig, SkipListStats, State,
    StateConfig, StateStats, Storage, Topic, TopicConfig, TopicConsumer, TopicStats, WriteBackend,
};

pub const DEFAULT_MAX_BUFFERED_RECORDS: usize = 1_000;
const TABLE_RECOVERY_CONSUMER: &str = "__zendb_table_recovery";
const TABLE_CONFIG_FILE: &str = "table.config";

pub use zendb_types::Change;

/// Physical defaults needed to materialize a table on any device.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct TableConfig {
    pub state: StateConfig,
    pub max_buffered_records: usize,
    pub topic: TopicConfig,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            state: StateConfig::default(),
            max_buffered_records: DEFAULT_MAX_BUFFERED_RECORDS,
            topic: TopicConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TableStats {
    pub state: StateStats,
    pub cache: SkipListStats,
    pub topic: TopicStats,
}

pub struct Table {
    config: TableConfig,
    sync_policy: SyncPolicy,
    local_device_id: DeviceId,
    state: State<PrimaryKey, Cell>,
    cache: SkipList<PrimaryKey, (Cell, bool)>,
    novel_pending: usize,
    topic: Topic<Change>,
    recovery: TopicConsumer<Change>,
}

fn cache_cell(entry: Cow<'_, (Cell, bool)>) -> Cow<'_, Cell> {
    match entry {
        Cow::Borrowed((cell, _)) => Cow::Borrowed(cell),
        Cow::Owned((cell, _)) => Cow::Owned(cell),
    }
}

impl Table {
    pub fn create_with_device(
        path: &FsPath,
        config: TableConfig,
        local_device_id: DeviceId,
    ) -> io::Result<Self> {
        Self::create_with_policy(path, config, local_device_id, SyncPolicy::Local)
    }

    pub fn create_with_policy(
        path: &FsPath,
        config: TableConfig,
        local_device_id: DeviceId,
        sync_policy: SyncPolicy,
    ) -> io::Result<Self> {
        fs::create_dir_all(path)?;
        let state = State::create(&path.join("state"), config.state.clone())?;
        let cache = SkipList::new(SkipListConfig {
            capacity: SkipListCapacity::Bounded {
                max_entries: config.max_buffered_records,
            },
        });
        let topic = Topic::create(path, config.topic.clone())?;
        let recovery = topic.consumer(TABLE_RECOVERY_CONSUMER)?;
        let table = Self {
            config,
            sync_policy,
            local_device_id,
            state,
            cache,
            novel_pending: 0,
            topic,
            recovery,
        };
        persist_config(path, &table.config)?;
        Ok(table)
    }

    pub fn open_with_device(
        path: &FsPath,
        config: TableConfig,
        local_device_id: DeviceId,
    ) -> io::Result<Self> {
        Self::open_with_policy(path, config, local_device_id, SyncPolicy::Local)
    }

    pub fn open_with_policy(
        path: &FsPath,
        config: TableConfig,
        local_device_id: DeviceId,
        sync_policy: SyncPolicy,
    ) -> io::Result<Self> {
        let persisted = Self::persisted_config(path)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "table is missing its persisted physical configuration",
            )
        })?;
        if persisted != config {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "requested table configuration differs from persisted physical configuration",
            ));
        }
        let state = State::open(&path.join("state"), config.state.clone())?;
        let cache = SkipList::new(SkipListConfig {
            capacity: SkipListCapacity::Bounded {
                max_entries: config.max_buffered_records,
            },
        });
        let topic = Topic::open(path, config.topic.clone())?;
        let recovery = topic.consumer(TABLE_RECOVERY_CONSUMER)?;
        let mut table = Self {
            config,
            sync_policy,
            local_device_id,
            state,
            cache,
            novel_pending: 0,
            topic,
            recovery,
        };
        table.replay_recovery()?;
        Ok(table)
    }

    /// Read the physical configuration selected when this local table was
    /// materialized. Catalog configuration is a cross-device default; this
    /// file is the authoritative recipe for reopening the local files.
    pub fn persisted_config(path: &FsPath) -> io::Result<Option<TableConfig>> {
        let bytes = match fs::read(path.join(TABLE_CONFIG_FILE)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let (config, consumed) = bincode::decode_from_slice(&bytes, bincode::config::standard())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        if consumed != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "table configuration contains trailing bytes",
            ));
        }
        Ok(Some(config))
    }

    pub fn consumer(&self, consumer: &str) -> io::Result<TopicConsumer<Change>> {
        self.topic.consumer(consumer)
    }

    /// Apply an ordinary local event.
    ///
    /// Shared-path writes are reserved for the Workspace because it must
    /// authorize, sign, journal, and broadcast them. A fully local table and a
    /// local subtree use this same API and storage path.
    pub fn insert_event(&mut self, event: Event) -> io::Result<()> {
        if self.table_is_shared() && self.is_path_shared(&event.primary_key, &event.path) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "shared-table writes must pass through Workspace::mutate",
            ));
        }
        self.apply_and_publish(event, false)
    }

    /// Create or update a root-local row in an otherwise replicated table.
    /// This is primarily used by `_catalog` for device-local table entries.
    pub fn insert_local_root_event(&mut self, event: Event) -> io::Result<()> {
        if !event.path.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a local-root event must target the row root",
            ));
        }
        self.prepare_cache(&event.primary_key)?;
        let previous = self.get_resolved(&event.primary_key);
        let mut cell = previous.clone().unwrap_or_else(|| Cell::dummy(None));
        cell.sync = SyncPolicy::Local;
        if !cell
            .apply_routed(&event.op, event.hlc, &event.path)
            .map_err(io::Error::other)?
        {
            return Ok(());
        }
        let current = Some(cell.clone());
        self.cache_row(event.primary_key.clone(), cell, previous.is_some())?;
        let offset = self.topic.append(&Change {
            event,
            previous,
            current,
        })?;
        self.recovery.seek(offset + 1);
        Ok(())
    }

    /// Apply an event whose signature, membership, and journal identity were
    /// already verified by the Workspace replication layer.
    pub fn insert_shared_event(&mut self, event: Event) -> io::Result<()> {
        if !self.table_is_shared() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "cannot install a shared event into a local table",
            ));
        }
        // The event remains durable in the Workspace shared journal. This
        // replica deliberately does not materialize it below a local boundary.
        if !self.is_path_shared(&event.primary_key, &event.path) {
            return Ok(());
        }
        self.apply_and_publish(event, true)
    }

    pub fn is_path_shared(&self, primary_key: &PrimaryKey, path: &Path) -> bool {
        if !self.table_is_shared() {
            return false;
        }
        self.get_resolved(primary_key)
            .map(|cell| cell.effective_scope_at(SyncScope::Shared, path).is_shared())
            .unwrap_or(true)
    }

    /// Change local policy without emitting an Event or changing an HLC.
    pub fn set_sync_policy(
        &mut self,
        primary_key: &PrimaryKey,
        path: Path,
        policy: SyncPolicy,
    ) -> io::Result<bool> {
        if !self.table_is_shared() {
            return Ok(false);
        }
        self.prepare_cache(primary_key)?;
        let mut cell = self
            .get_resolved(primary_key)
            .unwrap_or_else(|| Cell::dummy(None));
        if !cell.set_sync_policy(&path, policy) {
            return Ok(false);
        }
        let had_previous = self.get_resolved(primary_key).is_some();
        self.cache_row(primary_key.clone(), cell, had_previous)?;
        // Sync policy is local durable metadata, not a CRDT event. Persist the
        // updated Cell directly so a crash cannot silently re-enable sharing.
        self.sync()?;
        Ok(true)
    }

    pub fn table_sync_policy(&self) -> SyncPolicy {
        self.sync_policy
    }

    pub fn set_table_sync_policy(&mut self, policy: SyncPolicy) -> bool {
        if self.sync_policy == policy {
            return false;
        }
        self.sync_policy = policy;
        true
    }

    /// Return the sanitized shared state rooted at one row path.
    pub fn shared_cell(&self, primary_key: &PrimaryKey, path: &Path) -> Option<Cell> {
        if !self.table_is_shared() || !self.is_path_shared(primary_key, path) {
            return None;
        }
        let cell = self.get_resolved(primary_key)?;
        cell.cell_at_path(path)?.shared_clone(SyncScope::Shared)
    }

    fn table_is_shared(&self) -> bool {
        self.sync_policy.resolve(SyncScope::Shared).is_shared()
    }

    /// Build a policy-free state projection for snapshots and Merkle hashing.
    pub fn shared_rows(&self) -> Vec<(PrimaryKey, Cell)> {
        if !self.table_is_shared() {
            return Vec::new();
        }
        self.resolved_entries()
            .into_iter()
            .filter_map(|(key, cell)| cell.shared_clone(SyncScope::Shared).map(|cell| (key, cell)))
            .collect()
    }

    /// Canonical Merkle root of this table's wholly shared materialized state.
    ///
    /// The root is derived from authoritative state on demand, so it cannot
    /// lag commits. A table containing any local boundary returns `None`
    /// because another replica cannot be expected to have identical bytes.
    pub fn shared_merkle_root(&self) -> io::Result<Option<[u8; 32]>> {
        if !self.table_is_shared() {
            return Ok(None);
        }
        let rows = self.resolved_entries();
        if rows
            .iter()
            .any(|(_, cell)| cell.contains_local_boundary(SyncScope::Shared))
        {
            return Ok(None);
        }
        let mut level: Vec<[u8; 32]> = rows
            .into_iter()
            .map(|row| {
                let bytes = bincode::encode_to_vec(row, bincode::config::standard())
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"zendb-table-leaf-v1");
                hasher.update(&bytes);
                Ok(*hasher.finalize().as_bytes())
            })
            .collect::<io::Result<_>>()?;
        if level.is_empty() {
            return Ok(Some(*blake3::hash(b"zendb-table-empty-v1").as_bytes()));
        }
        while level.len() > 1 {
            let mut parents = Vec::with_capacity(level.len().div_ceil(2));
            for pair in level.chunks(2) {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"zendb-table-node-v1");
                hasher.update(&pair[0]);
                hasher.update(pair.get(1).unwrap_or(&pair[0]));
                parents.push(*hasher.finalize().as_bytes());
            }
            level = parents;
        }
        Ok(level.pop())
    }

    /// Merge verified snapshot rows into the single materialized state.
    /// Existing local policies remain local while shared siblings converge.
    pub fn install_shared_rows(&mut self, rows: Vec<(PrimaryKey, Cell)>) -> io::Result<()> {
        if !self.table_is_shared() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot install shared rows into a local table",
            ));
        }
        for (key, remote) in rows {
            let mut local = self.get_resolved(&key).unwrap_or_else(|| Cell::dummy(None));
            if local
                .merge_shared(&remote, MergeClocks::ZERO, SyncScope::Shared)
                .map_err(io::Error::other)?
            {
                let had_previous = self.get_resolved(&key).is_some();
                self.cache_row(key, local, had_previous)?;
            }
        }
        self.sync()
    }

    /// Physical CRDT tombstone pruning remains disabled until pruning context
    /// is persisted. An HLC watermark alone cannot distinguish a compacted
    /// delete from a key that never existed.
    pub fn compact_shared_through(&mut self, _watermark: Hlc) -> io::Result<usize> {
        Ok(0)
    }

    fn apply_and_publish(&mut self, event: Event, shared: bool) -> io::Result<()> {
        self.prepare_cache(&event.primary_key)?;
        let previous = self.get_resolved(&event.primary_key);
        let mut cell = previous.clone().unwrap_or_else(|| Cell::dummy(None));
        let changed = if shared {
            cell.apply_event_from(&event, SyncScope::Shared, self.local_device_id)
        } else {
            cell.apply_routed(&event.op, event.hlc, &event.path)
        }
        .map_err(io::Error::other)?;
        if !changed {
            return Ok(());
        }

        let current = Some(cell.clone());
        self.cache_row(event.primary_key.clone(), cell, previous.is_some())?;
        let change = Change {
            event,
            previous,
            current,
        };
        let offset = self.topic.append(&change)?;
        self.recovery.seek(offset + 1);
        Ok(())
    }

    fn prepare_cache(&mut self, key: &PrimaryKey) -> io::Result<()> {
        if ReadBackend::size(&self.cache) >= self.config.max_buffered_records
            && !ReadBackend::contains(&self.cache, key)
        {
            self.drain_cache()?;
        }
        Ok(())
    }

    fn get_resolved(&self, key: &PrimaryKey) -> Option<Cell> {
        ReadBackend::get(&self.cache, key)
            .map(cache_cell)
            .or_else(|| ReadBackend::get(&self.state, key))
            .map(Cow::into_owned)
    }

    fn cache_row(&mut self, key: PrimaryKey, cell: Cell, had_previous: bool) -> io::Result<()> {
        WriteBackend::put(&mut self.cache, key, (cell, had_previous))?;
        if !had_previous {
            self.novel_pending += 1;
        }
        Ok(())
    }

    fn resolved_entries(&self) -> Vec<(PrimaryKey, Cell)> {
        let mut rows: BTreeMap<PrimaryKey, Cell> = ReadBackend::entries(&self.state)
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        rows.extend(
            ReadBackend::entries(&self.cache)
                .map(|(key, value)| (key.into_owned(), value.into_owned().0)),
        );
        rows.into_iter().collect()
    }

    fn replay_recovery(&mut self) -> io::Result<()> {
        while let Some(change) = self.recovery.next() {
            let change = change?;
            if let Some(cell) = change.current {
                WriteBackend::put(&mut self.state, change.event.primary_key, cell)?;
            } else {
                WriteBackend::delete(&mut self.state, &change.event.primary_key)?;
            }
        }
        self.recovery.commit()
    }

    fn drain_cache(&mut self) -> io::Result<()> {
        if ReadBackend::is_empty(&self.cache) {
            return Ok(());
        }
        let rows: Vec<_> = ReadBackend::entries(&self.cache)
            .map(|(key, value)| (key.into_owned(), value.into_owned().0))
            .collect();
        WriteBackend::bulk_put(&mut self.state, rows)?;
        WriteBackend::clear(&mut self.cache)?;
        self.novel_pending = 0;
        self.recovery.commit()
    }

    fn compact(&mut self) -> io::Result<()> {
        self.drain_cache()?;
        self.state.compact()?;
        self.topic.compact()
    }

    fn flush(&mut self) -> io::Result<()> {
        self.drain_cache()?;
        self.state.flush()?;
        self.topic.flush()
    }

    fn sync(&mut self) -> io::Result<()> {
        self.drain_cache()?;
        self.state.sync()?;
        self.topic.sync()
    }
}

fn persist_config(path: &FsPath, config: &TableConfig) -> io::Result<()> {
    let bytes = bincode::encode_to_vec(config, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))?;
    let config_path = path.join(TABLE_CONFIG_FILE);
    fs::write(&config_path, bytes)?;
    fs::File::options()
        .write(true)
        .open(config_path)?
        .sync_all()
}

impl Storage for Table {
    type Stats = TableStats;
    type Config = TableConfig;

    fn stats(&self) -> Self::Stats {
        TableStats {
            state: self.state.stats(),
            cache: self.cache.stats(),
            topic: self.topic.stats(),
        }
    }

    fn config(&self) -> Self::Config {
        self.config.clone()
    }
}

impl DurableStorage for Table {
    fn create(path: &FsPath, config: TableConfig) -> io::Result<Self> {
        Self::create_with_device(path, config, device_id())
    }

    fn open(path: &FsPath, config: TableConfig) -> io::Result<Self> {
        Self::open_with_device(path, config, device_id())
    }

    fn compact(&mut self) -> io::Result<()> {
        Table::compact(self)
    }

    fn flush(&mut self) -> io::Result<()> {
        Table::flush(self)
    }

    fn sync(&mut self) -> io::Result<()> {
        Table::sync(self)
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

impl zendb_storage::backend::_traits::ReadBackend<PrimaryKey, Cell> for Table {
    fn get(&self, key: &PrimaryKey) -> Option<Cow<'_, Cell>> {
        self.get_resolved(key).map(Cow::Owned)
    }

    fn contains(&self, key: &PrimaryKey) -> bool {
        self.get_resolved(key).is_some()
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, PrimaryKey>> + 'a
    where
        PrimaryKey: 'a,
    {
        self.resolved_entries()
            .into_iter()
            .map(|(key, _)| Cow::Owned(key))
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, Cell>> + 'a
    where
        Cell: 'a,
    {
        self.resolved_entries()
            .into_iter()
            .map(|(_, value)| Cow::Owned(value))
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        self.resolved_entries()
            .into_iter()
            .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value)))
    }

    fn size(&self) -> usize {
        self.resolved_entries().len()
    }
}

impl zendb_storage::backend::_traits::OrderedReadBackend<PrimaryKey, Cell> for Table {
    fn range<'a>(
        &'a self,
        start: &'a PrimaryKey,
        end: &'a PrimaryKey,
    ) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        self.resolved_entries()
            .into_iter()
            .filter(move |(key, _)| key >= start && key < end)
            .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value)))
    }

    fn first<'a>(&'a self) -> Option<(Cow<'a, PrimaryKey>, Cow<'a, Cell>)>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        self.resolved_entries()
            .into_iter()
            .next()
            .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value)))
    }

    fn last<'a>(&'a self) -> Option<(Cow<'a, PrimaryKey>, Cow<'a, Cell>)>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        self.resolved_entries()
            .into_iter()
            .next_back()
            .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value)))
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        self.resolved_entries()
            .into_iter()
            .rev()
            .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value)))
    }

    fn range_rev<'a>(
        &'a self,
        start: &'a PrimaryKey,
        end: &'a PrimaryKey,
    ) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        self.resolved_entries()
            .into_iter()
            .rev()
            .filter(move |(key, _)| key >= start && key < end)
            .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value)))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use zendb_types::{init_device_id, Op, TypeTag, Value};

    use super::*;
    use zendb_storage::backend::_traits::ReadBackend;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct TmpDir(PathBuf);

    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    impl std::ops::Deref for TmpDir {
        type Target = FsPath;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    fn tmp_path(name: &str) -> TmpDir {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        TmpDir(std::env::temp_dir().join(format!("zendb_table_{name}_{}_{id}", std::process::id())))
    }

    fn event(key: &str, value: i64, millis: u64) -> Event {
        init_device_id();
        Event {
            table_id: "test".into(),
            primary_key: PrimaryKey::String(key.into()),
            path: Path::new(),
            op: Op::Replace {
                value: Value::Int(value),
            },
            hlc: Hlc::with_device_id(millis, 0, device_id()).unwrap(),
        }
    }

    fn shared_table(path: &FsPath) -> Table {
        init_device_id();
        Table::create_with_policy(
            path,
            TableConfig::default(),
            device_id(),
            SyncPolicy::Inherit,
        )
        .unwrap()
    }

    #[test]
    fn local_table_uses_same_read_api_before_and_after_flush() {
        let path = tmp_path("local");
        let mut table = Table::create(&path, TableConfig::default()).unwrap();
        table.insert_event(event("a", 1, 100)).unwrap();
        assert_eq!(
            table.get(&PrimaryKey::String("a".into())).unwrap().value,
            Some(Value::Int(1))
        );
        table.sync().unwrap();
        assert_eq!(table.entries().count(), 1);
    }

    #[test]
    fn shared_table_rejects_direct_shared_writes() {
        let path = tmp_path("shared");
        let mut table = shared_table(&path);
        assert_eq!(
            table.insert_event(event("a", 1, 100)).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        table.insert_shared_event(event("a", 1, 100)).unwrap();
    }

    #[test]
    fn policy_change_is_hlc_neutral_and_durable() {
        let path = tmp_path("policy");
        let mut table = shared_table(&path);
        table.insert_shared_event(event("a", 1, 100)).unwrap();
        let key = PrimaryKey::String("a".into());
        let original_hlc = table.get(&key).unwrap().hlc;

        assert!(table
            .set_sync_policy(&key, Path::new(), SyncPolicy::Local)
            .unwrap());
        assert!(!table.is_path_shared(&key, &Path::new()));
        table.insert_event(event("a", 2, 200)).unwrap();
        assert!(table
            .set_sync_policy(&key, Path::new(), SyncPolicy::Inherit)
            .unwrap());

        assert_eq!(
            table.get(&key).unwrap().hlc,
            Hlc::with_device_id(200, 0, device_id()).unwrap()
        );
        assert_ne!(table.get(&key).unwrap().hlc, original_hlc);
    }

    #[test]
    fn table_does_not_implement_raw_backend_mutation() {
        fn assert_read_backend<T: ReadBackend<PrimaryKey, Cell>>() {}
        assert_read_backend::<Table>();
    }

    #[test]
    fn unsafe_tombstone_pruning_is_disabled() {
        let path = tmp_path("compact");
        let mut table = shared_table(&path);
        let mut delete = event("a", 1, 100);
        delete.op = Op::Delete;
        table.insert_shared_event(delete).unwrap();
        assert_eq!(
            table
                .compact_shared_through(Hlc::from_bytes([u8::MAX; 24]))
                .unwrap(),
            0
        );
    }

    #[test]
    fn ordered_reads_are_available_for_every_state_backend() {
        use zendb_storage::backend::_traits::OrderedReadBackend;

        let path = tmp_path("ordered-read");
        let mut table = Table::create(&path, TableConfig::default()).unwrap();
        table.insert_event(event("b", 2, 100)).unwrap();
        table.insert_event(event("a", 1, 101)).unwrap();
        assert_eq!(
            table.first().unwrap().0,
            Cow::Owned::<PrimaryKey>(PrimaryKey::String("a".into()))
        );
        assert_eq!(table.entries_rev().count(), 2);
        assert_eq!(
            TypeTag::Int,
            table
                .get(&PrimaryKey::String("a".into()))
                .unwrap()
                .type_tag()
                .unwrap()
        );
    }

    #[test]
    fn merkle_root_tracks_shared_state_and_withholds_local_boundaries() {
        let path = tmp_path("merkle-root");
        let mut table = shared_table(&path);
        let empty = table.shared_merkle_root().unwrap().unwrap();
        table.insert_shared_event(event("a", 1, 100)).unwrap();
        assert_ne!(table.shared_merkle_root().unwrap().unwrap(), empty);
        table
            .set_sync_policy(
                &PrimaryKey::String("a".into()),
                Path::new(),
                SyncPolicy::Local,
            )
            .unwrap();
        assert!(table.shared_merkle_root().unwrap().is_none());
    }

    #[test]
    fn physical_config_is_persisted_and_conflicts_require_migration() {
        let path = tmp_path("physical-config");
        let config = TableConfig::default();
        let table = Table::create(&path, config.clone()).unwrap();
        drop(table);
        assert_eq!(
            Table::persisted_config(&path).unwrap(),
            Some(config.clone())
        );
        Table::open(&path, config).unwrap();

        let mut incompatible = TableConfig::default();
        incompatible.max_buffered_records += 1;
        assert_eq!(
            Table::open(&path, incompatible).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
