//! A table couples materialized row state, per-installation causal state, and
//! its durable materialized-change topic.

use std::{borrow::Cow, fs, io, path::Path};

use bincode::{Decode, Encode};
use zendb_types::{Event, EventId, InstallationId, OpDispatcher, PrimaryKey, Value};

use crate::{
    DurableStorage, OrderedReadBackend, ReadBackend, SeekTarget, State, StateConfig, StateStats,
    Storage, Topic, TopicConfig, TopicConsumer, TopicReader, TopicStats, WriteBackend,
};

use super::change::Change;
use super::receipt::ReceiptWindow;

const RECOVERY_CONSUMER: &str = "__zendb_table_recovery";

type TableEntry<'a> = (Cow<'a, PrimaryKey>, Cow<'a, Value>);

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
    state: State<PrimaryKey, Value>,
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
        let author = event.id.author;
        let mut receipt = self
            .causal
            .get(&author)
            .map(Cow::into_owned)
            .unwrap_or_default();
        event.id.sequence = receipt.max_seen.checked_add(1).ok_or_else(|| {
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
        receipt.observe(change.event.id.sequence)?;
        self.causal.put(author, receipt)?;
        Ok(InsertOutcome::Applied(Box::new(change)))
    }

    /// Observe a remotely authored event using its existing sequence number.
    ///
    /// A duplicate is ignored before CRDT evaluation. A novel event that does
    /// not change this table still advances the receipt window, but is not
    /// appended to the materialized-change topic.
    pub fn observe(&mut self, event: Event) -> io::Result<InsertOutcome> {
        let author = event.id.author;
        let sequence = event.id.sequence;
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
        let mut current = previous.clone();
        let mut changed = false;
        for operation in &event.operations {
            zendb_types::global_clock().observe(operation.time);
            changed |= current
                .apply_path(operation.time, &operation.path, &operation.op)
                .map_err(io::Error::other)?;
        }
        if !changed {
            return Ok(None);
        }
        Ok(Some(Change {
            event,
            previous,
            current,
        }))
    }

    fn replay_recovery(&mut self) -> io::Result<()> {
        for change in self.recovery.by_ref() {
            let change = change?;
            match change.current {
                Some(value) => {
                    WriteBackend::put(&mut self.state, change.event.primary_key, value)?;
                }
                None => {
                    WriteBackend::delete(&mut self.state, &change.event.primary_key)?;
                }
            }
        }
        self.state.persist(crate::backend::Barrier::Sync)?;
        self.commit_recovery()?;
        self.topic.persist(crate::backend::Barrier::Sync)
    }

    fn persist(&mut self, barrier: crate::backend::Barrier) -> io::Result<()> {
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

    fn persist(&mut self, barrier: crate::backend::Barrier) -> io::Result<()> {
        Table::persist(self, barrier)
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let _ = Table::persist(self, crate::backend::Barrier::Flush);
    }
}

impl ReadBackend<PrimaryKey, Value> for Table {
    fn get(&self, key: &PrimaryKey) -> Option<Cow<'_, Value>> {
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

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, Value>> + 'a
    where
        Value: 'a,
    {
        Box::new(ReadBackend::entries(self).map(|(_, value)| value))
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Value>)> + 'a
    where
        PrimaryKey: 'a,
        Value: 'a,
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

impl OrderedReadBackend<PrimaryKey, Value> for Table {
    fn range<'a>(
        &'a self,
        start: &'a PrimaryKey,
        end: &'a PrimaryKey,
    ) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Value>)> + 'a
    where
        PrimaryKey: 'a,
        Value: 'a,
    {
        OrderedReadBackend::range(&self.state, start, end)
    }

    fn first<'a>(&'a self) -> Option<TableEntry<'a>>
    where
        PrimaryKey: 'a,
        Value: 'a,
    {
        OrderedReadBackend::first(&self.state)
    }

    fn last<'a>(&'a self) -> Option<TableEntry<'a>>
    where
        PrimaryKey: 'a,
        Value: 'a,
    {
        OrderedReadBackend::last(&self.state)
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = TableEntry<'a>> + 'a
    where
        PrimaryKey: 'a,
        Value: 'a,
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
        Value: 'a,
    {
        OrderedReadBackend::range_rev(&self.state, start, end)
    }
}
