//! A table couples materialized row state with its durable change topic.

use std::{borrow::Cow, cmp::Ordering, fs, io, path::Path};

use bincode::{Decode, Encode};
use zendb_types::{Cell, ContainerType, Event, EventStamp, MergeStamps, PrimaryKey};

use crate::{
    DurableStorage, OrderedReadBackend, ReadBackend, SkipList, SkipListCapacity, SkipListConfig,
    SkipListStats, State, StateConfig, StateStats, Storage, Topic, TopicConfig, TopicConsumer,
    TopicStats, WriteBackend,
};

use super::{
    change::Change,
    iter::{IterationOrder, MergedEntries},
};

pub const DEFAULT_MAX_BUFFERED_RECORDS: usize = 1_000;
const RECOVERY_CONSUMER: &str = "__zendb_table_recovery";

type TableEntry<'a> = (Cow<'a, PrimaryKey>, Cow<'a, Cell>);

#[derive(Debug, Clone)]
pub enum InsertOutcome {
    Ignored,
    Applied(Box<Change>),
}

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
    state: State<PrimaryKey, Cell>,
    cache: SkipList<PrimaryKey, Cell>,
    novel_pending: usize,
    topic: Topic<Change>,
    recovery: TopicConsumer<Change>,
}

impl Table {
    pub fn consumer(&self, name: &str) -> io::Result<TopicConsumer<Change>> {
        self.topic.consumer(name)
    }

    /// Apply one fully stamped event through the table's only mutation path.
    pub fn insert(&mut self, event: Event) -> io::Result<InsertOutcome> {
        let previous = self.get(&event.primary_key).map(Cow::into_owned);
        let mut current = previous.clone().unwrap_or_else(|| Cell::dummy(None));
        let current_stamp = previous
            .as_ref()
            .map_or_else(EventStamp::zero, |cell| cell.stamp);
        let changed = current
            .apply_walk(
                &event.op,
                MergeStamps::new(current_stamp, event.stamp),
                &event.path,
            )
            .map_err(io::Error::other)?;
        if !changed {
            return Ok(InsertOutcome::Ignored);
        }

        self.prepare_cache(&event.primary_key)?;
        let change = Change {
            event,
            previous,
            current: Some(current.clone()),
        };
        let offset = self.topic.append(&change)?;
        WriteBackend::put(&mut self.cache, change.event.primary_key.clone(), current)?;
        if change.previous.is_none() {
            self.novel_pending += 1;
        }
        self.recovery.seek(offset + 1);
        Ok(InsertOutcome::Applied(Box::new(change)))
    }

    fn prepare_cache(&mut self, key: &PrimaryKey) -> io::Result<()> {
        let max_buffered_records = match self.cache.config().capacity {
            SkipListCapacity::Unbounded => usize::MAX,
            SkipListCapacity::Bounded { max_entries } => max_entries,
        };
        if ReadBackend::size(&self.cache) >= max_buffered_records
            && !ReadBackend::contains(&self.cache, key)
        {
            self.drain_cache()?;
        }
        Ok(())
    }

    fn replay_recovery(&mut self) -> io::Result<()> {
        for change in self.recovery.by_ref() {
            let change = change?;
            match change.current {
                Some(cell) => {
                    WriteBackend::put(&mut self.state, change.event.primary_key, cell)?;
                }
                None => {
                    WriteBackend::delete(&mut self.state, &change.event.primary_key)?;
                }
            }
        }
        self.state.sync()?;
        self.commit_recovery()?;
        self.topic.sync()
    }

    fn drain_cache(&mut self) -> io::Result<()> {
        if ReadBackend::is_empty(&self.cache) {
            return Ok(());
        }
        for (key, value) in ReadBackend::entries(&self.cache) {
            WriteBackend::put(&mut self.state, key.into_owned(), value.into_owned())?;
        }
        WriteBackend::clear(&mut self.cache)?;
        self.novel_pending = 0;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.drain_cache()?;
        self.state.flush()?;
        self.commit_recovery()?;
        self.topic.flush()
    }

    fn commit_recovery(&self) -> io::Result<()> {
        if matches!(self.state, State::InMemory { .. }) {
            return Ok(());
        }
        self.recovery.commit()
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
        let max_buffered_records = match self.cache.config().capacity {
            SkipListCapacity::Unbounded => usize::MAX,
            SkipListCapacity::Bounded { max_entries } => max_entries,
        };
        TableConfig {
            state: self.state.config(),
            max_buffered_records,
            topic: self.topic.config(),
        }
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
        let topic = Topic::create(&path.join("topic"), config.topic.clone())?;
        let recovery = topic.consumer(RECOVERY_CONSUMER)?;
        Ok(Self {
            state,
            cache,
            novel_pending: 0,
            topic,
            recovery,
        })
    }

    fn open(path: &Path, config: TableConfig) -> io::Result<Self> {
        let state = State::open(&path.join("state"), config.state.clone())?;
        let cache = SkipList::new(SkipListConfig {
            capacity: SkipListCapacity::Bounded {
                max_entries: config.max_buffered_records,
            },
        });
        let topic = Topic::open(&path.join("topic"), config.topic.clone())?;
        let recovery = topic.consumer(RECOVERY_CONSUMER)?;
        let mut table = Self {
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
        Table::flush(self)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.drain_cache()?;
        self.state.sync()?;
        self.commit_recovery()?;
        self.topic.sync()
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let _ = Table::flush(self);
    }
}

impl ReadBackend<PrimaryKey, Cell> for Table {
    fn get(&self, key: &PrimaryKey) -> Option<Cow<'_, Cell>> {
        ReadBackend::get(&self.cache, key).or_else(|| ReadBackend::get(&self.state, key))
    }

    fn contains(&self, key: &PrimaryKey) -> bool {
        ReadBackend::contains(&self.cache, key) || ReadBackend::contains(&self.state, key)
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, PrimaryKey>> + 'a
    where
        PrimaryKey: 'a,
    {
        Box::new(ReadBackend::entries(self).map(|(key, _)| key))
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, Cell>> + 'a
    where
        Cell: 'a,
    {
        Box::new(ReadBackend::entries(self).map(|(_, value)| value))
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let entries: Box<dyn Iterator<Item = TableEntry<'a>> + 'a> = match &self.state {
            State::Ordered { .. } | State::InMemory { .. } => Box::new(MergedEntries::new(
                ReadBackend::entries(&self.state),
                ReadBackend::entries(&self.cache),
                IterationOrder::Ascending,
            )),
            State::Unordered { .. } => {
                let state = ReadBackend::entries(&self.state)
                    .filter(|(key, _)| !ReadBackend::contains(&self.cache, key.as_ref()));
                Box::new(state.chain(ReadBackend::entries(&self.cache)))
            }
        };
        entries
    }

    fn size(&self) -> usize {
        ReadBackend::size(&self.state) + self.novel_pending
    }

    fn is_empty(&self) -> bool {
        ReadBackend::is_empty(&self.state) && self.novel_pending == 0
    }
}

impl OrderedReadBackend<PrimaryKey, Cell> for Table {
    fn range<'a>(
        &'a self,
        start: &'a PrimaryKey,
        end: &'a PrimaryKey,
    ) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        MergedEntries::new(
            OrderedReadBackend::range(&self.state, start, end),
            OrderedReadBackend::range(&self.cache, start, end),
            IterationOrder::Ascending,
        )
    }

    fn first<'a>(&'a self) -> Option<TableEntry<'a>>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = OrderedReadBackend::first(&self.state);
        let cache = OrderedReadBackend::first(&self.cache);
        match (state, cache) {
            (None, None) => None,
            (Some(row), None) => Some(row),
            (None, Some(row)) => Some(row),
            (Some(state), Some(cache)) => match state.0.as_ref().cmp(cache.0.as_ref()) {
                Ordering::Less => Some(state),
                Ordering::Equal | Ordering::Greater => Some(cache),
            },
        }
    }

    fn last<'a>(&'a self) -> Option<TableEntry<'a>>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = OrderedReadBackend::last(&self.state);
        let cache = OrderedReadBackend::last(&self.cache);
        match (state, cache) {
            (None, None) => None,
            (Some(row), None) => Some(row),
            (None, Some(row)) => Some(row),
            (Some(state), Some(cache)) => match state.0.as_ref().cmp(cache.0.as_ref()) {
                Ordering::Greater => Some(state),
                Ordering::Equal | Ordering::Less => Some(cache),
            },
        }
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = TableEntry<'a>> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        MergedEntries::new(
            OrderedReadBackend::entries_rev(&self.state),
            OrderedReadBackend::entries_rev(&self.cache),
            IterationOrder::Descending,
        )
    }

    fn range_rev<'a>(
        &'a self,
        start: &'a PrimaryKey,
        end: &'a PrimaryKey,
    ) -> impl Iterator<Item = TableEntry<'a>> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        MergedEntries::new(
            OrderedReadBackend::range_rev(&self.state, start, end),
            OrderedReadBackend::range_rev(&self.cache, start, end),
            IterationOrder::Descending,
        )
    }
}
