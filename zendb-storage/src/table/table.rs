//! A table couples materialized row state, per-installation causal state, and
//! its durable materialized-change topic.

use std::{borrow::Cow, fs, io, path::Path};

use bincode::{Decode, Encode};
use zendb_types::{
    Cell, ContainerType, Event, EventId, EventStamp, InstallationId, MergeStamps, PrimaryKey,
};

use crate::{
    DurableStorage, OrderedReadBackend, ReadBackend, SeekTarget, State, StateConfig, StateStats,
    Storage, Topic, TopicConfig, TopicConsumer, TopicReader, TopicStats, WriteBackend,
};

use super::change::Change;
use super::receipt::ReceiptWindow;

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
    pub causal: StateConfig,
    pub topic: TopicConfig,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            state: StateConfig::default(),
            causal: StateConfig::Unordered(crate::KeyDirConfig::default()),
            topic: TopicConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TableStats {
    pub state: StateStats,
    pub causal: StateStats,
    pub topic: TopicStats,
}

pub struct Table {
    config: TableConfig,
    state: State<PrimaryKey, Cell>,
    causal: State<InstallationId, ReceiptWindow>,
    topic: Topic<Change>,
    recovery: TopicConsumer<Change>,
}

impl Table {
    /// Create an unregistered reader over this table's durable changes.
    pub fn reader(&self) -> TopicReader<Change> {
        self.topic.reader()
    }

    /// Create a named durable consumer over this table's changes.
    pub fn consumer(&self, consumer: &str) -> io::Result<TopicConsumer<Change>> {
        self.topic.consumer(consumer)
    }

    /// Assign this table's next local sequence and apply a local event.
    ///
    /// A sequence is committed only when the event changes materialized state.
    /// A CRDT no-op therefore remains invisible to the topic and causal state.
    pub fn insert(&mut self, mut event: Event) -> io::Result<InsertOutcome> {
        let author = event.stamp.id.author;
        let mut receipt = self
            .causal
            .get(&author)
            .map(Cow::into_owned)
            .unwrap_or_default();
        event.stamp.id.sequence = receipt.max_seen.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "table installation sequence exhausted",
            )
        })?;

        let Some(change) = self.prepare_change(event)? else {
            return Ok(InsertOutcome::Ignored);
        };

        self.topic.append(&change)?;
        WriteBackend::put(
            &mut self.state,
            change.event.primary_key.clone(),
            change
                .current
                .clone()
                .expect("applied changes always carry current state"),
        )?;
        receipt.observe(change.event.stamp.id.sequence)?;
        self.causal.put(author, receipt)?;
        Ok(InsertOutcome::Applied(Box::new(change)))
    }

    /// Observe a remotely authored event using its existing sequence number.
    ///
    /// A duplicate is ignored before CRDT evaluation. A novel event that does
    /// not change this table still advances the receipt window, but is not
    /// appended to the materialized-change topic.
    pub fn observe(&mut self, event: Event) -> io::Result<InsertOutcome> {
        let author = event.stamp.id.author;
        let sequence = event.stamp.id.sequence;
        if sequence == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "event sequence zero is reserved",
            ));
        }
        let mut receipt = self
            .causal
            .get(&author)
            .map(Cow::into_owned)
            .unwrap_or_default();
        if receipt.contains(sequence) {
            return Ok(InsertOutcome::Ignored);
        }

        let Some(change) = self.prepare_change(event)? else {
            receipt.observe(sequence)?;
            self.causal.put(author, receipt)?;
            return Ok(InsertOutcome::Ignored);
        };
        {
            self.topic.append(&change)?;
            WriteBackend::put(
                &mut self.state,
                change.event.primary_key.clone(),
                change
                    .current
                    .clone()
                    .expect("applied changes always carry current state"),
            )?;
        }

        receipt.observe(sequence)?;
        self.causal.put(author, receipt)?;
        Ok(InsertOutcome::Applied(Box::new(change)))
    }

    /// Return whether this table has already observed an event identity.
    pub fn contains_event(&self, event_id: EventId) -> bool {
        self.causal
            .get(&event_id.author)
            .is_some_and(|receipt| receipt.contains(event_id.sequence))
    }

    /// Export receipt windows for this table's anti-entropy summary.
    pub fn receipt_summaries(&self) -> Vec<(InstallationId, ReceiptWindow)> {
        self.causal
            .entries()
            .map(|(author, receipt)| (author.into_owned(), receipt.into_owned()))
            .collect()
    }

    fn prepare_change(&self, event: Event) -> io::Result<Option<Change>> {
        let previous = self.get(&event.primary_key).map(Cow::into_owned);
        let mut current = previous.clone().unwrap_or_else(|| Cell::dummy(None));
        let current_stamp = previous
            .as_ref()
            .map_or_else(EventStamp::default, |cell| cell.stamp);
        let changed = current
            .apply_walk(
                &event.op,
                MergeStamps::new(current_stamp, event.stamp),
                &event.path,
            )
            .map_err(io::Error::other)?;
        if !changed {
            return Ok(None);
        }
        Ok(Some(Change {
            event,
            previous,
            current: Some(current),
        }))
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
        self.state.persist(zendb_types::Barrier::Sync)?;
        self.commit_recovery()?;
        self.topic.persist(zendb_types::Barrier::Sync)
    }

    fn persist(&mut self, barrier: zendb_types::Barrier) -> io::Result<()> {
        self.state.persist(barrier)?;
        self.causal.persist(barrier)?;
        self.commit_recovery()?;
        self.topic.persist(barrier)
    }

    fn commit_recovery(&mut self) -> io::Result<()> {
        if matches!(self.state, State::InMemory { .. }) {
            return Ok(());
        }
        self.recovery.seek(SeekTarget::Latest)?;
        self.recovery.commit()
    }
}

impl Storage for Table {
    type Stats = TableStats;
    type Config = TableConfig;

    fn stats(&self) -> Self::Stats {
        TableStats {
            state: self.state.stats(),
            causal: self.causal.stats(),
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
        let causal = State::create(&path.join("causal"), config.causal.clone())?;
        let topic = Topic::create(&path.join("topic"), config.topic.clone())?;
        let recovery = topic.consumer(RECOVERY_CONSUMER)?;
        Ok(Self {
            config,
            state,
            causal,
            topic,
            recovery,
        })
    }

    fn open(path: &Path, config: TableConfig) -> io::Result<Self> {
        let state = State::open(&path.join("state"), config.state.clone())?;
        let causal = State::open(&path.join("causal"), config.causal.clone())?;
        let topic: Topic<Change> = Topic::open(&path.join("topic"), config.topic.clone())?;
        let recovery = topic.consumer(RECOVERY_CONSUMER)?;
        let mut table = Self {
            config,
            state,
            causal,
            topic,
            recovery,
        };
        table.replay_recovery()?;
        Ok(table)
    }

    fn compact(&mut self) -> io::Result<()> {
        // Change topics are the replication log. Their retention watermark is
        // workspace-wide and cannot be inferred from local consumers alone.
        self.state.compact()?;
        self.causal.compact()
    }

    fn persist(&mut self, barrier: zendb_types::Barrier) -> io::Result<()> {
        Table::persist(self, barrier)
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let _ = Table::persist(self, zendb_types::Barrier::Flush);
    }
}

impl ReadBackend<PrimaryKey, Cell> for Table {
    fn get(&self, key: &PrimaryKey) -> Option<Cow<'_, Cell>> {
        ReadBackend::get(&self.state, key)
    }

    fn contains(&self, key: &PrimaryKey) -> bool {
        ReadBackend::contains(&self.state, key)
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
        Box::new(ReadBackend::entries(&self.state))
    }

    fn size(&self) -> usize {
        ReadBackend::size(&self.state)
    }

    fn is_empty(&self) -> bool {
        ReadBackend::is_empty(&self.state)
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
        OrderedReadBackend::range(&self.state, start, end)
    }

    fn first<'a>(&'a self) -> Option<TableEntry<'a>>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        OrderedReadBackend::first(&self.state)
    }

    fn last<'a>(&'a self) -> Option<TableEntry<'a>>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        OrderedReadBackend::last(&self.state)
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = TableEntry<'a>> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        OrderedReadBackend::entries_rev(&self.state)
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
        OrderedReadBackend::range_rev(&self.state, start, end)
    }
}
