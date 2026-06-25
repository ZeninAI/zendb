//! Table abstraction over materialized state, a resolved cache, and a change topic.

use std::{borrow::Cow, cmp::Ordering, fs, io, path::Path};

use bincode::{Decode, Encode};
use zendb_types::{Cell, Event, PrimaryKey};

use crate::core::{
    skiplist::{SkipList, SkipListCapacity, SkipListConfig, SkipListStats},
    topic::{Topic, TopicConfig, TopicConsumer, TopicStats},
    traits::{Backend, DurableStorage, OrderedBackend, Storage},
};
use crate::frontend::state::{State, StateConfig, StateStats};

pub const DEFAULT_MAX_BUFFERED_RECORDS: usize = 1_000;
const TABLE_RECOVERY_CONSUMER: &str = "__zendb_table_recovery";

pub use zendb_types::Change;

/// Complete configuration required to create or open a table.
#[derive(Debug, Clone, Encode, Decode)]
pub struct TableConfig {
    pub sync: bool,
    pub state: StateConfig,
    pub max_buffered_records: usize,
    pub topic: TopicConfig,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            sync: false,
            state: StateConfig::default(),
            max_buffered_records: DEFAULT_MAX_BUFFERED_RECORDS,
            topic: TopicConfig::default(),
        }
    }
}

/// Current stats view over the delegated backends.
#[derive(Debug, Clone, Encode, Decode)]
pub struct TableStats {
    pub state: StateStats,
    pub cache: SkipListStats,
    pub topic: TopicStats,
}

/// A table presents the resolved union of materialized state and its in-memory
/// skip-list cache. Cache entries shadow state, and successful changes are
/// durably appended to the table topic.
pub struct Table {
    config: TableConfig,
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

fn cache_entry<'a>(
    (key, entry): (Cow<'a, PrimaryKey>, Cow<'a, (Cell, bool)>),
) -> (Cow<'a, PrimaryKey>, Cow<'a, Cell>) {
    (key, cache_cell(entry))
}

impl Table {
    /// Apply an event, cache the resolved row, and publish real changes.
    pub fn insert_event(&mut self, event: Event) -> io::Result<()> {
        if self.cache.size() >= self.config.max_buffered_records
            && !self.cache.contains(&event.primary_key)
        {
            self.drain_cache()?;
        }

        let Some((previous, current)) = self.apply_event_to_cache(&event)? else {
            return Ok(());
        };
        let change = Change {
            event,
            previous,
            current,
        };
        let offset = self.topic.append(&change)?;
        self.recovery.seek(offset + 1);
        Ok(())
    }

    pub fn consumer(&self, consumer: &str) -> io::Result<TopicConsumer<Change>> {
        self.topic.consumer(consumer)
    }

    fn apply_event_to_cache(
        &mut self,
        event: &Event,
    ) -> io::Result<Option<(Option<Cell>, Option<Cell>)>> {
        let state = &self.state;
        let sync = self.config.sync;
        let mut previous = None;
        let mut current = None;
        let mut changed = false;
        let mut novel = false;

        self.cache.update(&event.primary_key, |cached| {
            let (mut cell, had_previous, visible) = match cached {
                Some((cell, had_previous)) => (cell, had_previous, true),
                None => match state.get(&event.primary_key) {
                    Some(cell) => (cell.into_owned(), true, true),
                    None => (Cell::dummy(None), false, false),
                },
            };

            previous = visible.then(|| cell.clone());
            match cell.apply_event(event, sync) {
                Ok(true) => {
                    current = Some(cell.clone());
                    changed = true;
                    novel = !visible;
                    Some((cell, had_previous))
                }
                Ok(false) => visible.then_some((cell, had_previous)),
                Err(error) => {
                    log::warn!(
                        "failed to apply event to table {:?}, key {:?}: {error}",
                        event.table_id,
                        event.primary_key
                    );
                    visible.then_some((cell, had_previous))
                }
            }
        })?;

        if changed {
            if novel {
                self.novel_pending += 1;
            }
            Ok(Some((previous, current)))
        } else {
            Ok(None)
        }
    }

    fn replay_recovery(&mut self) -> io::Result<()> {
        while let Some(change) = self.recovery.next() {
            let change = change?;
            if let Some(cell) = change.current {
                self.state.put(change.event.primary_key, cell)?;
            } else {
                self.state.delete(&change.event.primary_key)?;
            }
        }
        self.recovery.commit()?;
        Ok(())
    }

    fn drain_cache(&mut self) -> io::Result<()> {
        if self.cache.is_empty() {
            return Ok(());
        }
        for (pk, entry) in self.cache.entries() {
            self.state.put(pk.into_owned(), entry.into_owned().0)?;
        }
        self.cache.clear()?;
        self.novel_pending = 0;
        self.recovery.commit()?;
        Ok(())
    }
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
    fn create(path: &Path, config: TableConfig) -> io::Result<Self> {
        fs::create_dir_all(path)?;

        let state = State::create(&path.join("state"), config.state.clone())?;
        let cache = SkipList::new(SkipListConfig {
            capacity: SkipListCapacity::Bounded {
                max_entries: config.max_buffered_records,
            },
        });
        let topic = Topic::create(path, config.topic.clone())?;
        let recovery = topic.consumer(TABLE_RECOVERY_CONSUMER)?;

        Ok(Self {
            config,
            state,
            cache,
            novel_pending: 0,
            topic,
            recovery,
        })
    }

    fn open(path: &Path, config: TableConfig) -> io::Result<Self> {
        let state: State<PrimaryKey, Cell> =
            State::open(&path.join("state"), config.state.clone())?;
        let cache = SkipList::new(SkipListConfig {
            capacity: SkipListCapacity::Bounded {
                max_entries: config.max_buffered_records,
            },
        });
        let topic = Topic::open(path, config.topic.clone())?;
        let recovery = topic.consumer(TABLE_RECOVERY_CONSUMER)?;

        let mut table = Self {
            config,
            state,
            cache,
            novel_pending: 0,
            topic,
            recovery,
        };
        table.replay_recovery()?;
        Ok(table)
    }

    fn compact(&mut self) -> io::Result<()> {
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

impl Backend<PrimaryKey, Cell> for Table {
    // ---- reads --------------------------------------------------------

    fn get(&self, key: &PrimaryKey) -> Option<Cow<'_, Cell>> {
        if let Some(entry) = self.cache.get(key) {
            return Some(cache_cell(entry));
        }
        self.state.get(key)
    }

    fn contains(&self, key: &PrimaryKey) -> bool {
        if self.cache.contains(key) {
            return true;
        }
        self.state.contains(key)
    }

    // ---- writes (blocked) -------------------------------------------
    //
    // Direct backend writes are forbidden on Table. All mutations must
    // go through `insert_event` so the cache and topic stay consistent.

    fn put(&mut self, _key: PrimaryKey, _value: Cell) -> io::Result<()> {
        panic!("Table::put is disabled — use insert_event instead")
    }

    fn put_if_absent(&mut self, _key: &PrimaryKey, _value: Cell) -> io::Result<bool> {
        panic!("Table::put_if_absent is disabled — use insert_event instead")
    }

    fn replace(&mut self, _key: &PrimaryKey, _value: Cell) -> io::Result<Option<Cow<'_, Cell>>> {
        panic!("Table::replace is disabled — use insert_event instead")
    }

    fn bulk_put<I>(&mut self, _items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (PrimaryKey, Cell)>,
    {
        panic!("Table::bulk_put is disabled — use insert_event instead")
    }

    fn bulk_put_sorted<I>(&mut self, _sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (PrimaryKey, Cell)>,
    {
        panic!("Table::bulk_put_sorted is disabled — use insert_event instead")
    }

    fn delete(&mut self, _key: &PrimaryKey) -> io::Result<bool> {
        panic!("Table::delete is disabled — use insert_event with a tombstone op instead")
    }

    fn bulk_delete<'a, I>(&mut self, _keys: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a PrimaryKey>,
        PrimaryKey: 'a,
    {
        panic!("Table::bulk_delete is disabled — use insert_event with a tombstone op instead")
    }

    fn bulk_delete_sorted<'a, I>(&mut self, _sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a PrimaryKey>,
        PrimaryKey: 'a,
    {
        panic!(
            "Table::bulk_delete_sorted is disabled — use insert_event with a tombstone op instead"
        )
    }

    fn update<F>(&mut self, _key: &PrimaryKey, _f: F) -> io::Result<()>
    where
        F: FnOnce(Option<Cell>) -> Option<Cell>,
    {
        panic!("Table::update is disabled — use insert_event instead")
    }

    fn clear(&mut self) -> io::Result<()> {
        panic!("Table::clear is disabled — tables cannot be cleared directly")
    }

    // ---- iteration (resolved over state ⊕ cache) ---------------------

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, PrimaryKey>> + 'a
    where
        PrimaryKey: 'a,
    {
        // Same merge shape as `entries`, but on the state side we ask
        // for `keys()` only so the backend can skip deserializing Cell
        // values (when its `keys()` becomes a true key-only iterator).
        match &self.state {
            State::Ordered { backend: state, .. } => {
                let mut s_iter = state.keys().peekable();
                let mut c_iter = self.cache.keys().peekable();

                let it = std::iter::from_fn(move || {
                    let order = match (s_iter.peek(), c_iter.peek()) {
                        (None, None) => return None,
                        (Some(_), None) => Ordering::Less,
                        (None, Some(_)) => Ordering::Greater,
                        (Some(s_pk), Some(c_pk)) => s_pk.as_ref().cmp(c_pk),
                    };

                    match order {
                        Ordering::Less => s_iter.next(),
                        Ordering::Greater => c_iter.next(),
                        Ordering::Equal => {
                            s_iter.next();
                            c_iter.next()
                        }
                    }
                });

                Box::new(it) as Box<dyn Iterator<Item = Cow<'a, PrimaryKey>> + 'a>
            }
            State::Unordered { backend: state, .. } => {
                let cache_ref = &self.cache;
                let state_only = state
                    .keys()
                    .filter(move |k| !cache_ref.contains(k.as_ref()));
                let cache_keys = self.cache.keys();
                Box::new(state_only.chain(cache_keys))
                    as Box<dyn Iterator<Item = Cow<'a, PrimaryKey>> + 'a>
            }
            State::InMemory { backend: state, .. } => {
                let cache_ref = &self.cache;
                let state_only = state
                    .keys()
                    .filter(move |k| !cache_ref.contains(k.as_ref()));
                let cache_keys = self.cache.keys();
                Box::new(state_only.chain(cache_keys))
                    as Box<dyn Iterator<Item = Cow<'a, PrimaryKey>> + 'a>
            }
        }
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, Cell>> + 'a
    where
        Cell: 'a,
    {
        // We can't use `state.values()` directly: shadowing requires
        // knowing the key on the state side. So the ordered branch
        // walks `state.entries()` to do the key-driven merge but only
        // yields values; the unordered branch filters by key likewise.
        match &self.state {
            State::Ordered { backend: state, .. } => {
                let mut s_iter = state.entries().peekable();
                let mut c_iter = self.cache.entries().map(cache_entry).peekable();

                let it = std::iter::from_fn(move || {
                    let order = match (s_iter.peek(), c_iter.peek()) {
                        (None, None) => return None,
                        (Some(_), None) => Ordering::Less,
                        (None, Some(_)) => Ordering::Greater,
                        (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
                    };

                    match order {
                        Ordering::Less => s_iter.next().map(|(_, v)| v),
                        Ordering::Greater => {
                            let (_, cell) = c_iter.next().unwrap();
                            Some(cell)
                        }
                        Ordering::Equal => {
                            s_iter.next();
                            let (_, cell) = c_iter.next().unwrap();
                            Some(cell)
                        }
                    }
                });

                Box::new(it) as Box<dyn Iterator<Item = Cow<'a, Cell>> + 'a>
            }
            State::Unordered { backend: state, .. } => {
                let cache_ref = &self.cache;
                let state_only = state
                    .entries()
                    .filter_map(move |(k, v)| (!cache_ref.contains(k.as_ref())).then_some(v));
                let cache_vals = self.cache.values().map(cache_cell);
                Box::new(state_only.chain(cache_vals))
                    as Box<dyn Iterator<Item = Cow<'a, Cell>> + 'a>
            }
            State::InMemory { backend: state, .. } => {
                let cache_ref = &self.cache;
                let state_only = state
                    .entries()
                    .filter_map(move |(k, v)| (!cache_ref.contains(k.as_ref())).then_some(v));
                let cache_vals = self.cache.values().map(cache_cell);
                Box::new(state_only.chain(cache_vals))
                    as Box<dyn Iterator<Item = Cow<'a, Cell>> + 'a>
            }
        }
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        // Streaming merge of materialized state with the in-memory cache.
        // Cache entries always win on collision. The ordered branch
        // assumes the state backend iterates by `PrimaryKey: Ord`, which
        // is a standing system invariant for our B+Tree.
        match &self.state {
            State::Ordered { backend: state, .. } => {
                let mut s_iter = state.entries().peekable();
                let mut c_iter = self.cache.entries().map(cache_entry).peekable();

                let it = std::iter::from_fn(move || {
                    let order = match (s_iter.peek(), c_iter.peek()) {
                        (None, None) => return None,
                        (Some(_), None) => Ordering::Less,
                        (None, Some(_)) => Ordering::Greater,
                        (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
                    };

                    match order {
                        Ordering::Less => s_iter.next(),
                        Ordering::Greater => c_iter.next(),
                        Ordering::Equal => {
                            let _state_row = s_iter.next();
                            c_iter.next()
                        }
                    }
                });

                Box::new(it) as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
            }
            State::Unordered { backend: state, .. } => {
                let cache_ref = &self.cache;
                let state_only = state
                    .entries()
                    .filter(move |(k, _)| !cache_ref.contains(k.as_ref()));
                let cache_iter = self.cache.entries().map(cache_entry);
                Box::new(state_only.chain(cache_iter))
                    as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
            }
            State::InMemory { backend: state, .. } => {
                let cache_ref = &self.cache;
                let state_only = state
                    .entries()
                    .filter(move |(k, _)| !cache_ref.contains(k.as_ref()));
                let cache_iter = self.cache.entries().map(cache_entry);
                Box::new(state_only.chain(cache_iter))
                    as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
            }
        }
    }

    fn size(&self) -> usize {
        self.state.size() + self.novel_pending
    }

    fn is_empty(&self) -> bool {
        self.state.is_empty() && self.novel_pending == 0
    }
}

impl OrderedBackend<PrimaryKey, Cell> for Table {
    fn range<'a>(
        &'a self,
        start: &'a PrimaryKey,
        end: &'a PrimaryKey,
    ) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = &self.state;

        let mut s_iter = state.range(start, end).peekable();
        let mut c_iter = self.cache.range(start, end).map(cache_entry).peekable();

        let it = std::iter::from_fn(move || {
            let order = match (s_iter.peek(), c_iter.peek()) {
                (None, None) => return None,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
            };

            match order {
                Ordering::Less => s_iter.next(),
                Ordering::Greater => c_iter.next(),
                Ordering::Equal => {
                    let _state_row = s_iter.next();
                    c_iter.next()
                }
            }
        });

        Box::new(it) as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
    }

    fn first<'a>(&'a self) -> Option<(Cow<'a, PrimaryKey>, Cow<'a, Cell>)>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = &self.state;

        let s_first = state.first();
        let c_first = self.cache.first().map(cache_entry);

        match (s_first, c_first) {
            (None, None) => None,
            (Some(s), None) => Some(s),
            (None, Some(c)) => Some(c),
            (Some(s), Some(c)) => match s.0.as_ref().cmp(c.0.as_ref()) {
                Ordering::Less => Some(s),
                // Greater → cache key is smaller, yield it.
                // Equal   → cache always wins on collision.
                Ordering::Greater | Ordering::Equal => Some(c),
            },
        }
    }

    fn last<'a>(&'a self) -> Option<(Cow<'a, PrimaryKey>, Cow<'a, Cell>)>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = &self.state;

        let s_last = state.last();
        let c_last = self.cache.last().map(cache_entry);

        match (s_last, c_last) {
            (None, None) => None,
            (Some(s), None) => Some(s),
            (None, Some(c)) => Some(c),
            (Some(s), Some(c)) => match s.0.as_ref().cmp(c.0.as_ref()) {
                Ordering::Greater => Some(s),
                // Less  → cache key is greater, yield it.
                // Equal → cache always wins on collision.
                Ordering::Less | Ordering::Equal => Some(c),
            },
        }
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = &self.state;

        // Two-pointer streaming merge in reverse: pick the larger head
        // each step; cache wins on equality.
        let mut s_iter = state.entries_rev().peekable();
        let mut c_iter = self.cache.entries_rev().map(cache_entry).peekable();

        let it = std::iter::from_fn(move || {
            let order = match (s_iter.peek(), c_iter.peek()) {
                (None, None) => return None,
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
            };

            match order {
                Ordering::Greater => s_iter.next(),
                Ordering::Less => c_iter.next(),
                Ordering::Equal => {
                    s_iter.next();
                    c_iter.next()
                }
            }
        });

        Box::new(it) as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
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
        let state = &self.state;

        let mut s_iter = state.range_rev(start, end).peekable();
        let mut c_iter = self.cache.range_rev(start, end).map(cache_entry).peekable();

        let it = std::iter::from_fn(move || {
            let order = match (s_iter.peek(), c_iter.peek()) {
                (None, None) => return None,
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
            };

            match order {
                Ordering::Greater => s_iter.next(),
                Ordering::Less => c_iter.next(),
                Ordering::Equal => {
                    s_iter.next();
                    c_iter.next()
                }
            }
        });

        Box::new(it) as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::keydir::KeyDirConfig;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    use zendb_types::{device_id, init_device_id, Hlc, Op, Path, Value};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn hlc(ms: u64) -> Hlc {
        init_device_id();
        Hlc::with_device_id(ms, 0, device_id()).unwrap()
    }

    fn tmp_path(name: &str) -> PathBuf {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("zendb_engine_{name}_{}_{id}", std::process::id()))
    }

    fn event(key: &str, value: i64, hlc: Hlc) -> Event {
        Event {
            table_id: "ignored".into(),
            primary_key: PrimaryKey::String(key.into()),
            path: Path::new(),
            op: Op::Replace {
                value: Value::Int(value),
            },
            hlc,
            sync: false,
            signature: Vec::new(),
        }
    }

    fn materialize(table: &mut Table) {
        table.drain_cache().unwrap();
        table.state.flush().unwrap();
    }

    #[test]
    fn create_owns_configured_backends() {
        let path = tmp_path("create");
        let mut table = Table::create(
            &path,
            TableConfig {
                state: StateConfig::Unordered(KeyDirConfig::default()),
                ..TableConfig::default()
            },
        )
        .unwrap();

        let first_event = event("a", 1, hlc(100));
        table.insert_event(first_event.clone()).unwrap();
        table.insert_event(first_event).unwrap();
        let key = PrimaryKey::String("a".into());
        assert_eq!(
            Backend::get(&table, &key).unwrap().into_owned().value,
            Some(Value::Int(1))
        );
        let stats = Storage::stats(&table);
        assert!(matches!(&stats.state, StateStats::Unordered(_)));
        assert_eq!(stats.cache.entries, 1);
        assert_eq!(stats.topic.records, 1);
    }

    #[test]
    fn open_recovers_unflushed_topic_records() {
        let path = tmp_path("open");
        let config = TableConfig::default();
        {
            let mut table = Table::create(&path, config.clone()).unwrap();
            table.insert_event(event("a", 1, hlc(100))).unwrap();
        }

        let table = Table::open(&path, config).unwrap();
        let key = PrimaryKey::String("a".into());
        assert_eq!(
            Backend::get(&table, &key).unwrap().into_owned().value,
            Some(Value::Int(1))
        );
        assert_eq!(table.cache.size(), 0);
        assert_eq!(table.state.size(), 1);
        assert_eq!(table.topic.stats().records, 1);
    }

    fn manual_config() -> TableConfig {
        TableConfig::default()
    }

    #[test]
    fn cache_resolves_get_without_materialize() {
        let path = tmp_path("cache_get");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        table.insert_event(event("a", 2, hlc(200))).unwrap();

        let key = PrimaryKey::String("a".into());
        let got = Backend::get(&table, &key).unwrap();
        // Cache hit returns Cow::Borrowed (zero-clone).
        assert!(matches!(got, Cow::Borrowed(_)));
        assert_eq!(got.into_owned().value, Some(Value::Int(2)));
        assert_eq!(table.cache.size(), 1);
    }

    #[test]
    fn size_is_o1_and_matches_entries_count() {
        let path = tmp_path("size_o1");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        table.insert_event(event("b", 2, hlc(110))).unwrap();
        // Second event on "a" must not double-count.
        table.insert_event(event("a", 3, hlc(200))).unwrap();
        table.insert_event(event("c", 4, hlc(120))).unwrap();

        assert_eq!(Backend::size(&table), 3);
        assert_eq!(Backend::entries(&table).count(), 3);
        assert_eq!(table.cache.size(), 3);
        assert_eq!(table.novel_pending, 3);
        assert!(!Backend::is_empty(&table));
    }

    #[test]
    fn entries_yields_cache_over_state() {
        let path = tmp_path("entries_shadow");
        let mut table = Table::create(&path, manual_config()).unwrap();
        // Seed state via materialize.
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        materialize(&mut table);
        assert!(table.cache.is_empty());

        // Now shadow "a" with a newer event and add a fresh "b".
        table.insert_event(event("a", 9, hlc(200))).unwrap();
        table.insert_event(event("b", 7, hlc(210))).unwrap();
        assert_eq!(table.novel_pending, 1);

        let collected: Vec<(PrimaryKey, Cell)> = Backend::entries(&table)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(collected.len(), 2);
        let map: std::collections::HashMap<_, _> =
            collected.into_iter().map(|(k, v)| (k, v.value)).collect();
        assert_eq!(map[&PrimaryKey::String("a".into())], Some(Value::Int(9)));
        assert_eq!(map[&PrimaryKey::String("b".into())], Some(Value::Int(7)));
        assert_eq!(Backend::size(&table), 2);
    }

    #[test]
    fn duplicate_event_does_not_double_apply() {
        let path = tmp_path("dup");
        let mut table = Table::create(&path, manual_config()).unwrap();
        let d = event("a", 1, hlc(100));
        table.insert_event(d.clone()).unwrap();
        table.insert_event(d.clone()).unwrap();
        assert_eq!(table.cache.size(), 1);
        assert_eq!(Backend::size(&table), 1);
        let key = PrimaryKey::String("a".into());
        assert_eq!(
            Backend::get(&table, &key).unwrap().into_owned().value,
            Some(Value::Int(1))
        );
    }

    #[test]
    fn materialize_persists_and_clears_cache() {
        let path = tmp_path("mat");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        table.insert_event(event("b", 2, hlc(110))).unwrap();
        assert_eq!(table.cache.size(), 2);

        materialize(&mut table);
        assert!(table.cache.is_empty());
        assert_eq!(table.cache.size(), 0);
        assert_eq!(table.novel_pending, 0);
        assert_eq!(table.topic.stats().records, 2);

        // Reads now come straight from state.
        assert_eq!(Backend::size(&table), 2);
        let key = PrimaryKey::String("a".into());
        assert_eq!(
            Backend::get(&table, &key).unwrap().into_owned().value,
            Some(Value::Int(1))
        );
    }

    #[test]
    fn bounded_cache_materializes_at_capacity_without_clearing_topic() {
        let path = tmp_path("bounded_cache");
        let config = TableConfig {
            max_buffered_records: 2,
            ..TableConfig::default()
        };
        let mut table = Table::create(&path, config).unwrap();

        table.insert_event(event("a", 1, hlc(100))).unwrap();
        table.insert_event(event("b", 2, hlc(110))).unwrap();

        assert_eq!(table.cache.size(), 2);
        assert_eq!(table.state.size(), 0);

        table.insert_event(event("c", 3, hlc(120))).unwrap();

        assert_eq!(table.cache.size(), 1);
        assert_eq!(table.state.size(), 2);
        assert_eq!(table.novel_pending, 1);
        assert_eq!(table.topic.stats().records, 3);
    }

    #[test]
    fn sync_materializes_pending_events_before_open() {
        let path = tmp_path("open_cache");
        let config = manual_config();
        {
            let mut table = Table::create(&path, config.clone()).unwrap();
            table.insert_event(event("a", 1, hlc(100))).unwrap();
            table.insert_event(event("a", 2, hlc(200))).unwrap();
            table.insert_event(event("b", 5, hlc(150))).unwrap();
            DurableStorage::sync(&mut table).unwrap();
        }

        let table = Table::open(&path, config).unwrap();
        assert_eq!(table.cache.size(), 0);
        assert_eq!(Backend::size(&table), 2);
        let key_a = PrimaryKey::String("a".into());
        let key_b = PrimaryKey::String("b".into());
        assert_eq!(
            Backend::get(&table, &key_a).unwrap().into_owned().value,
            Some(Value::Int(2))
        );
        assert_eq!(
            Backend::get(&table, &key_b).unwrap().into_owned().value,
            Some(Value::Int(5))
        );
    }

    #[test]
    fn range_merges_cache_and_state() {
        let path = tmp_path("range_merge");
        let mut table = Table::create(&path, manual_config()).unwrap();
        // State rows: a, c, e.
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        table.insert_event(event("c", 3, hlc(101))).unwrap();
        table.insert_event(event("e", 5, hlc(102))).unwrap();
        materialize(&mut table);

        // Cache: shadow "c" + add "b" and "d".
        table.insert_event(event("c", 99, hlc(200))).unwrap();
        table.insert_event(event("b", 2, hlc(201))).unwrap();
        table.insert_event(event("d", 4, hlc(202))).unwrap();

        let start = PrimaryKey::String("a".into());
        let end = PrimaryKey::String("e".into()); // exclusive
        let got: Vec<(String, Option<Value>)> = OrderedBackend::range(&table, &start, &end)
            .map(|(k, v)| {
                let pk = match k.into_owned() {
                    PrimaryKey::String(s) => s,
                    _ => unreachable!(),
                };
                (pk, v.into_owned().value)
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("a".into(), Some(Value::Int(1))),
                ("b".into(), Some(Value::Int(2))),
                ("c".into(), Some(Value::Int(99))),
                ("d".into(), Some(Value::Int(4))),
            ]
        );
    }

    #[test]
    #[should_panic(expected = "requires an ordered state backend")]
    fn range_panics_on_unordered() {
        let path = tmp_path("range_panic");
        let table = Table::create(
            &path,
            TableConfig {
                state: StateConfig::Unordered(KeyDirConfig::default()),
                ..TableConfig::default()
            },
        )
        .unwrap();
        let start = PrimaryKey::String("a".into());
        let end = PrimaryKey::String("z".into());
        let _ = OrderedBackend::range(&table, &start, &end).count();
    }

    /// Build a table with state rows {a, c, e} and cache rows
    /// {b, c-shadowed, d}. Used by ordered-iteration tests.
    fn shadowed_table(name: &str) -> Table {
        let path = tmp_path(name);
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        table.insert_event(event("c", 3, hlc(101))).unwrap();
        table.insert_event(event("e", 5, hlc(102))).unwrap();
        materialize(&mut table);
        table.insert_event(event("c", 99, hlc(200))).unwrap();
        table.insert_event(event("b", 2, hlc(201))).unwrap();
        table.insert_event(event("d", 4, hlc(202))).unwrap();
        table
    }

    fn pk(s: &str) -> PrimaryKey {
        PrimaryKey::String(s.into())
    }

    fn collect_pks<'a>(
        it: impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)>,
    ) -> Vec<String> {
        it.map(|(k, _)| match k.into_owned() {
            PrimaryKey::String(s) => s,
            _ => unreachable!(),
        })
        .collect()
    }

    #[test]
    fn first_picks_smallest_across_state_and_cache() {
        let table = shadowed_table("first_smallest");
        let (k, v) = OrderedBackend::first(&table).unwrap();
        assert_eq!(k.into_owned(), pk("a"));
        assert_eq!(v.into_owned().value, Some(Value::Int(1)));
    }

    #[test]
    fn first_prefers_cache_when_smaller() {
        let path = tmp_path("first_cache_smaller");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_event(event("m", 1, hlc(100))).unwrap();
        materialize(&mut table);
        // Cache key "a" < state key "m" → cache wins.
        table.insert_event(event("a", 9, hlc(200))).unwrap();

        let (k, _) = OrderedBackend::first(&table).unwrap();
        assert_eq!(k.into_owned(), pk("a"));
    }

    #[test]
    fn first_cache_wins_on_equal_key() {
        let path = tmp_path("first_eq");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        materialize(&mut table);
        table.insert_event(event("a", 99, hlc(200))).unwrap();

        let (k, v) = OrderedBackend::first(&table).unwrap();
        assert_eq!(k.into_owned(), pk("a"));
        assert_eq!(v.into_owned().value, Some(Value::Int(99)));
    }

    #[test]
    fn last_picks_largest_across_state_and_cache() {
        let table = shadowed_table("last_largest");
        let (k, v) = OrderedBackend::last(&table).unwrap();
        assert_eq!(k.into_owned(), pk("e"));
        assert_eq!(v.into_owned().value, Some(Value::Int(5)));
    }

    #[test]
    fn last_prefers_cache_when_larger() {
        let path = tmp_path("last_cache_larger");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        materialize(&mut table);
        // Cache key "z" > state key "a" → cache wins.
        table.insert_event(event("z", 9, hlc(200))).unwrap();

        let (k, _) = OrderedBackend::last(&table).unwrap();
        assert_eq!(k.into_owned(), pk("z"));
    }

    #[test]
    fn last_cache_wins_on_equal_key() {
        let path = tmp_path("last_eq");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_event(event("a", 1, hlc(100))).unwrap();
        materialize(&mut table);
        table.insert_event(event("a", 99, hlc(200))).unwrap();

        let (k, v) = OrderedBackend::last(&table).unwrap();
        assert_eq!(k.into_owned(), pk("a"));
        assert_eq!(v.into_owned().value, Some(Value::Int(99)));
    }

    #[test]
    fn first_and_last_empty_table() {
        let path = tmp_path("first_last_empty");
        let table = Table::create(&path, manual_config()).unwrap();
        assert!(OrderedBackend::first(&table).is_none());
        assert!(OrderedBackend::last(&table).is_none());
    }

    #[test]
    fn entries_rev_streams_in_descending_order_with_dedup() {
        let table = shadowed_table("entries_rev");
        assert_eq!(
            collect_pks(OrderedBackend::entries_rev(&table)),
            vec!["e", "d", "c", "b", "a"]
        );
        // Verify cache wins on 'c'.
        let cell_for_c = OrderedBackend::entries_rev(&table)
            .find(|(k, _)| k.as_ref() == &pk("c"))
            .unwrap()
            .1
            .into_owned();
        assert_eq!(cell_for_c.value, Some(Value::Int(99)));
    }

    #[test]
    fn range_rev_streams_in_descending_order_with_dedup() {
        let table = shadowed_table("range_rev");
        let start = pk("a");
        let end = pk("e"); // exclusive
        assert_eq!(
            collect_pks(OrderedBackend::range_rev(&table, &start, &end)),
            vec!["d", "c", "b", "a"]
        );
    }

    #[test]
    #[should_panic(expected = "requires an ordered state backend")]
    fn first_panics_on_unordered() {
        let path = tmp_path("first_panic");
        let table = Table::create(
            &path,
            TableConfig {
                state: StateConfig::Unordered(KeyDirConfig::default()),
                ..TableConfig::default()
            },
        )
        .unwrap();
        let _ = OrderedBackend::first(&table);
    }
}
