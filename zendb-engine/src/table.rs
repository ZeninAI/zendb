//! Table abstraction over materialized state and an ordered delta log.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::Path,
};

use bincode::{Decode, Encode};
use zendb_storage::core::{
    backend::{Backend, OrderedBackend},
    btree::{BPlusTree, BPlusTreeConfig, BPlusTreeStats},
    keydir::{KeyDir, KeyDirConfig, KeyDirStats},
    orderlog::{OrderLog, OrderLogConfig, OrderLogStats},
};
use zendb_types::{Cell, Delta, Hlc, Path as ValuePath, PrimaryKey};

type OrderedState = BPlusTree<PrimaryKey, Cell>;
type UnorderedState = KeyDir<PrimaryKey, Cell>;
type EventLog = OrderLog<EventKey, Delta>;

/// Controls when in-flight deltas are materialized into table state.
#[derive(Debug, Clone, Encode, Decode)]
pub enum FlushConfig {
    Manual,
    EventCount { max_events: usize },
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self::Manual
    }
}

/// Configures the table's materialized-state backend.
#[derive(Debug, Clone, Encode, Decode)]
pub enum StateConfig {
    Ordered(BPlusTreeConfig),
    Unordered(KeyDirConfig),
}

impl Default for StateConfig {
    fn default() -> Self {
        Self::Ordered(BPlusTreeConfig::default())
    }
}

/// Declares a derived index that should be owned by the table.
///
/// Index implementations are intentionally deferred; the configuration is
/// part of the table contract now so creation/opening remains stable later.
#[derive(Debug, Clone, Encode, Decode)]
pub enum IndexConfig {
    Value {
        name: String,
        path: ValuePath,
    },
    FullText {
        name: String,
        paths: Vec<ValuePath>,
    },
    Vector {
        name: String,
        path: ValuePath,
        dimensions: usize,
    },
}

/// Complete configuration required to create or open a table.
#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct TableConfig {
    pub sync: bool,
    pub flush: FlushConfig,
    pub state: StateConfig,
    pub events: OrderLogConfig,
    pub indexes: Vec<IndexConfig>,
}

/// Stats from the configured materialized-state backend.
#[derive(Debug)]
pub enum StateStats<'a> {
    Ordered(&'a BPlusTreeStats),
    Unordered(&'a KeyDirStats),
}

/// Current stats view over the delegated backends.
#[derive(Debug)]
pub struct TableStats<'a> {
    pub state: StateStats<'a>,
    pub events: &'a OrderLogStats,
}

/// Position of an event within one row's ordered event range.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub enum EventPosition {
    Start,
    Event(Hlc),
    End,
}

/// OrderLog key that groups events by row and orders each row by HLC.
///
/// HLC includes the originating device ID and acts as the event identity.
/// The path stays in the delta value because ordering by path could reorder
/// parent and descendant operations within a row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventKey {
    pub primary_key: PrimaryKey,
    pub position: EventPosition,
}

impl EventKey {
    pub fn from_delta(delta: &Delta) -> Self {
        Self {
            primary_key: delta.primary_key.clone(),
            position: EventPosition::Event(delta.hlc),
        }
    }

    pub fn bounds(primary_key: &PrimaryKey) -> (Self, Self) {
        (
            Self {
                primary_key: primary_key.clone(),
                position: EventPosition::Start,
            },
            Self {
                primary_key: primary_key.clone(),
                position: EventPosition::End,
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertResult {
    Inserted,
    Duplicate,
    InsertedAndMaterialized,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaterializeResult {
    pub events: usize,
    pub rows: usize,
}

enum State {
    Ordered(OrderedState),
    Unordered(UnorderedState),
}

/// A table presents a resolved `PrimaryKey -> Cell` backend while owning the
/// materialized backend and durable in-flight event log used to produce it.
pub struct Table {
    config: TableConfig,
    state: State,
    events: EventLog,
}

impl Table {
    pub fn create(path: &Path, config: TableConfig) -> io::Result<Self> {
        fs::create_dir_all(path)?;

        let state = match &config.state {
            StateConfig::Ordered(state_config) => State::Ordered(BPlusTree::create(
                &path.join("state"),
                state_config.clone(),
            )?),
            StateConfig::Unordered(state_config) => {
                State::Unordered(KeyDir::create(&path.join("state"), state_config.clone())?)
            }
        };
        let events = OrderLog::create(&path.join("events"), config.events.clone())?;

        Ok(Self {
            config,
            state,
            events,
        })
    }

    pub fn open(path: &Path, config: TableConfig) -> io::Result<Self> {
        let state = match &config.state {
            StateConfig::Ordered(state_config) => {
                State::Ordered(BPlusTree::open(&path.join("state"), state_config.clone())?)
            }
            StateConfig::Unordered(state_config) => {
                State::Unordered(KeyDir::open(&path.join("state"), state_config.clone())?)
            }
        };
        let events = OrderLog::open(&path.join("events"), config.events.clone())?;

        Ok(Self {
            config,
            state,
            events,
        })
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    pub fn insert_delta(&mut self, delta: Delta) -> io::Result<InsertResult> {
        let key = EventKey::from_delta(&delta);
        if !self.events.put_if_absent(key, delta)? {
            return Ok(InsertResult::Duplicate);
        }

        if self.should_materialize() {
            self.materialize()?;
            Ok(InsertResult::InsertedAndMaterialized)
        } else {
            Ok(InsertResult::Inserted)
        }
    }

    pub fn insert_deltas(
        &mut self,
        deltas: impl IntoIterator<Item = Delta>,
    ) -> io::Result<Vec<InsertResult>> {
        deltas
            .into_iter()
            .map(|delta| self.insert_delta(delta))
            .collect()
    }

    pub fn events_for(&self, key: &PrimaryKey) -> Vec<Delta> {
        let (start, end) = EventKey::bounds(key);
        self.events
            .range(&start, &end)
            .map(|(_, delta)| delta.into_owned())
            .collect()
    }

    pub fn materialize(&mut self) -> io::Result<MaterializeResult> {
        let mut grouped: BTreeMap<PrimaryKey, Vec<Delta>> = BTreeMap::new();
        for (event_key, delta) in self.events.entries() {
            grouped
                .entry(event_key.primary_key.clone())
                .or_default()
                .push(delta.into_owned());
        }

        let event_count = grouped.values().map(Vec::len).sum();
        let row_count = grouped.len();
        let mut rows = Vec::with_capacity(row_count);

        for (primary_key, deltas) in grouped {
            let mut cell = match &self.state {
                State::Ordered(state) => state.get(&primary_key).map(Cow::into_owned),
                State::Unordered(state) => state.get(&primary_key).map(Cow::into_owned),
            }
            .unwrap_or_else(|| Cell::new(None, Hlc::ZERO, None));
            for delta in &deltas {
                cell.apply(delta);
            }
            rows.push((primary_key, cell));
        }

        match &mut self.state {
            State::Ordered(state) => state.bulk_put(rows)?,
            State::Unordered(state) => state.bulk_put(rows)?,
        }
        match &self.state {
            State::Ordered(state) => state.sync()?,
            State::Unordered(state) => state.sync()?,
        }
        self.events.clear()?;
        self.events.sync()?;

        Ok(MaterializeResult {
            events: event_count,
            rows: row_count,
        })
    }

    fn should_materialize(&self) -> bool {
        match self.config.flush {
            FlushConfig::Manual => false,
            FlushConfig::EventCount { max_events } => self.events.size() >= max_events,
        }
    }
}

impl Backend<PrimaryKey, Cell> for Table {
    type Stats<'a>
        = TableStats<'a>
    where
        Self: 'a;
    type Config = TableConfig;

    fn get(&self, key: &PrimaryKey) -> Option<Cow<'_, Cell>> {
        let previous = match &self.state {
            State::Ordered(state) => state.get(key).map(Cow::into_owned),
            State::Unordered(state) => state.get(key).map(Cow::into_owned),
        };
        let mut current = previous
            .clone()
            .unwrap_or_else(|| Cell::new(None, Hlc::ZERO, None));

        let mut changed = false;
        let (start, end) = EventKey::bounds(key);
        for (_, delta) in self.events.range(&start, &end) {
            changed |= current.apply(&delta);
        }

        if previous.is_none() && !changed {
            None
        } else {
            Some(Cow::Owned(current))
        }
    }

    fn contains(&self, key: &PrimaryKey) -> bool {
        Backend::get(self, key).is_some()
    }

    fn put(&mut self, key: PrimaryKey, value: Cell) -> io::Result<()> {
        let (start, end) = EventKey::bounds(&key);
        let event_keys: Vec<EventKey> = self
            .events
            .range(&start, &end)
            .map(|(event_key, _)| event_key.into_owned())
            .collect();
        self.events.bulk_delete_sorted(event_keys.iter())?;

        match &mut self.state {
            State::Ordered(state) => state.put(key, value)?,
            State::Unordered(state) => state.put(key, value)?,
        }

        Ok(())
    }

    fn delete(&mut self, key: &PrimaryKey) -> io::Result<bool> {
        let existed = Backend::contains(self, key);
        let (start, end) = EventKey::bounds(key);
        let event_keys: Vec<EventKey> = self
            .events
            .range(&start, &end)
            .map(|(event_key, _)| event_key.into_owned())
            .collect();
        self.events.bulk_delete_sorted(event_keys.iter())?;

        match &mut self.state {
            State::Ordered(state) => {
                state.delete(key)?;
            }
            State::Unordered(state) => {
                state.delete(key)?;
            }
        }

        Ok(existed)
    }

    fn clear(&mut self) -> io::Result<()> {
        match &mut self.state {
            State::Ordered(state) => state.clear()?,
            State::Unordered(state) => state.clear()?,
        }
        self.events.clear()?;

        Ok(())
    }

    fn compact(&mut self) -> io::Result<()> {
        match &mut self.state {
            State::Ordered(state) => state.compact()?,
            State::Unordered(state) => state.compact()?,
        }
        self.events.compact()?;

        Ok(())
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, PrimaryKey>> + 'a
    where
        PrimaryKey: 'a,
    {
        Backend::entries(self).map(|(key, _)| key)
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, Cell>> + 'a
    where
        Cell: 'a,
    {
        Backend::entries(self).map(|(_, cell)| cell)
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let mut keys = BTreeSet::new();
        match &self.state {
            State::Ordered(state) => {
                keys.extend(state.keys().map(Cow::into_owned));
            }
            State::Unordered(state) => {
                keys.extend(state.keys().map(Cow::into_owned));
            }
        }
        keys.extend(self.events.keys().map(|key| key.primary_key.clone()));

        keys.into_iter()
            .filter_map(|key| Backend::get(self, &key).map(|cell| (Cow::Owned(key), cell)))
    }

    fn size(&self) -> usize {
        Backend::entries(self).count()
    }

    fn stats(&self) -> Self::Stats<'_> {
        TableStats {
            state: match &self.state {
                State::Ordered(state) => StateStats::Ordered(state.stats()),
                State::Unordered(state) => StateStats::Unordered(state.stats()),
            },
            events: self.events.stats(),
        }
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn flush(&self) -> io::Result<()> {
        match &self.state {
            State::Ordered(state) => state.flush()?,
            State::Unordered(state) => state.flush()?,
        }
        self.events.flush()
    }

    fn sync(&self) -> io::Result<()> {
        match &self.state {
            State::Ordered(state) => state.sync()?,
            State::Unordered(state) => state.sync()?,
        }
        self.events.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    use zendb_types::{Op, Path, Value};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn hlc(ms: u64) -> Hlc {
        Hlc::with_device_id(ms, 0, [1u8; 8]).unwrap()
    }

    fn tmp_path(name: &str) -> PathBuf {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("zendb_engine_{name}_{id}"))
    }

    fn delta(key: &str, value: i64, hlc: Hlc) -> Delta {
        Delta {
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

    #[test]
    fn event_keys_group_rows_and_order_by_hlc() {
        let early_delta = delta("a", 1, hlc(100));
        let late_delta = delta("a", 2, hlc(200));
        let next_row_delta = delta("b", 3, hlc(50));
        let (start, end) = EventKey::bounds(&early_delta.primary_key);

        let early = EventKey::from_delta(&early_delta);
        let late = EventKey::from_delta(&late_delta);
        let next_row = EventKey::from_delta(&next_row_delta);

        assert!(start < early);
        assert!(early < late);
        assert!(late < end);
        assert!(end < next_row);
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

        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        let key = PrimaryKey::String("a".into());
        assert_eq!(
            Backend::get(&table, &key).unwrap().into_owned().value,
            Some(Value::Int(1))
        );
        let stats = Backend::stats(&table);
        assert!(matches!(&stats.state, StateStats::Unordered(_)));
        assert!(stats.events.data_size > 0);
    }

    #[test]
    fn open_recovers_state_and_events() {
        let path = tmp_path("open");
        let config = TableConfig::default();
        {
            let mut table = Table::create(&path, config.clone()).unwrap();
            table.insert_delta(delta("a", 1, hlc(100))).unwrap();
            Backend::sync(&table).unwrap();
        }

        let table = Table::open(&path, config).unwrap();
        let key = PrimaryKey::String("a".into());
        assert_eq!(
            Backend::get(&table, &key).unwrap().into_owned().value,
            Some(Value::Int(1))
        );
    }
}
