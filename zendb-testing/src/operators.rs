//! Document indexing pipeline operators and helpers.

use std::collections::HashSet;
use std::io;
use std::time::{Duration, Instant};

use bincode::{Decode, Encode};
use zendb_engine::{
    define_operator_set, BoxFuture, Change, DispatchOperator, Operator, OperatorContext,
    OperatorDirective, OperatorRuntimeConfig, RetryConfig, StateHandle, Subscription, TableConfig,
    TableHandle,
};
use zendb_storage::{core::traits::Backend, frontend::state::StateConfig};
use zendb_types::{
    device_id, init_device_id, Event, Hlc, Op, Path as ValuePath, PrimaryKey, Value,
};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

pub(crate) fn hlc(ms: u64) -> Hlc {
    init_device_id();
    Hlc::with_device_id(ms, 0, device_id()).unwrap()
}

pub(crate) fn doc_event(doc_id: &str, content: &str, ms: u64) -> Event {
    Event {
        table_id: "documents".into(),
        primary_key: PrimaryKey::String(doc_id.into()),
        path: ValuePath::new(),
        op: Op::Replace {
            value: Value::String(content.into()),
        },
        hlc: hlc(ms),
        sync: false,
        signature: Vec::new(),
    }
}

pub(crate) fn wait_until(condition: impl Fn() -> bool, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "condition was not reached within {:?}",
            timeout
        );
        std::thread::yield_now();
    }
}

/// Simple tokenizer: lowercase, split on non-alphanumeric, filter short tokens.
pub(crate) fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.len() >= 2)
        .map(|t| t.to_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// Operator set
// ---------------------------------------------------------------------------

define_operator_set! {
    pub mod doc_operators {
        Indexer(IndexerOp),
        Archiver(ArchiverOp),
    }
}

pub(crate) use doc_operators::{OperatorConfig, OperatorConfigVariant};

// ---------------------------------------------------------------------------
// Indexer operator
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct IndexerConfig;

impl Default for IndexerConfig {
    fn default() -> Self {
        Self
    }
}

pub(crate) struct IndexerOp {
    index: Option<StateHandle<String, HashSet<String>>>,
    stats: Option<StateHandle<String, u64>>,
}

impl Operator for IndexerOp {
    type Config = IndexerConfig;
    type Timer = ();

    fn new(_config: &Self::Config) -> io::Result<Self> {
        Ok(Self {
            index: None,
            stats: None,
        })
    }

    fn open<'a, D>(
        &'a mut self,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            self.index = Some(ctx.state("index", Some(StateConfig::default()))?);
            self.stats = Some(ctx.state("doc_stats", Some(StateConfig::default()))?);
            Ok(OperatorDirective::Continue)
        })
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        _ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        let index = self.index.as_ref().expect("index state not open");
        let stats = self.stats.as_ref().expect("stats state not open");

        Box::pin(async move {
            for change in &changes {
                let doc_id = match &change.event.primary_key {
                    PrimaryKey::String(s) => s.clone(),
                    _ => continue,
                };

                match &change.event.op {
                    Op::Replace { value } => {
                        if let Value::String(content) = value {
                            let words = tokenize(content);
                            let word_count = words.len() as u64;

                            {
                                let handle = stats.get()?;
                                let mut state = handle.write();
                                let current =
                                    state.get(&doc_id).map(|v| v.into_owned()).unwrap_or(0);
                                state.put(doc_id.clone(), current.max(word_count))?;
                            }

                            {
                                let handle = index.get()?;
                                let mut state = handle.write();
                                for word in words {
                                    let mut entry = state
                                        .get(&word)
                                        .map(|v| v.into_owned())
                                        .unwrap_or_default();
                                    entry.insert(doc_id.clone());
                                    state.put(word, entry)?;
                                }
                            }
                        }
                    }
                    Op::Delete => {
                        {
                            let handle = stats.get()?;
                            let mut state = handle.write();
                            state.delete(&doc_id)?;
                        }
                        {
                            let handle = index.get()?;
                            let state = handle.read();
                            let mut words_to_update: Vec<String> = Vec::new();
                            for item in state.entries() {
                                let (word, docs) = (item.0.into_owned(), item.1.into_owned());
                                if docs.contains(&doc_id) {
                                    words_to_update.push(word);
                                }
                            }
                            drop(state);
                            let mut state = handle.write();
                            for word in words_to_update {
                                if let Some(mut docs) = state.get(&word).map(|v| v.into_owned()) {
                                    docs.remove(&doc_id);
                                    if docs.is_empty() {
                                        state.delete(&word)?;
                                    } else {
                                        state.put(word, docs)?;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(OperatorDirective::Continue)
        })
    }

    fn finish<'a, D>(
        &'a mut self,
        _ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<()>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            self.index = None;
            self.stats = None;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Archiver operator
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct ArchiverConfig {
    pub(crate) max_reports: u64,
}

pub(crate) struct ArchiverOp {
    reports_written: u64,
    max_reports: u64,
    output: Option<TableHandle>,
    source_stats: Option<StateHandle<String, u64>>,
}

impl Operator for ArchiverOp {
    type Config = ArchiverConfig;
    type Timer = ();

    fn new(config: &Self::Config) -> io::Result<Self> {
        Ok(Self {
            reports_written: 0,
            max_reports: config.max_reports,
            output: None,
            source_stats: None,
        })
    }

    fn open<'a, D>(
        &'a mut self,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            self.output = Some(ctx.table("reports", Some(TableConfig::default()))?);
            self.source_stats = Some(ctx.state("doc_stats", None)?);

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            ctx.register_timer(now + 50, &())?;

            Ok(OperatorDirective::Continue)
        })
    }

    fn process<'a, D>(
        &'a mut self,
        _changes: Vec<Change>,
        _ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async { Ok(OperatorDirective::Continue) })
    }

    fn handle_timer<'a, D>(
        &'a mut self,
        _payload: (),
        _fire_at_ms: u64,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        let output = self.output.as_ref().expect("output table not open").clone();
        let source_stats = self
            .source_stats
            .as_ref()
            .expect("source stats not open")
            .clone();

        Box::pin(async move {
            self.reports_written += 1;

            let stats_snapshot: Vec<(String, u64)> = {
                let state = source_stats.get()?;
                let guard = state.read();
                guard
                    .entries()
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect()
            };

            let report_key = format!("report_{}", self.reports_written);
            let total_words: u64 = stats_snapshot.iter().map(|(_, c)| c).sum();
            let report_content =
                format!("docs={} total_words={}", stats_snapshot.len(), total_words);

            output.get()?.write().insert_event(Event {
                table_id: "reports".into(),
                primary_key: PrimaryKey::String(report_key),
                path: ValuePath::new(),
                op: Op::Replace {
                    value: Value::String(report_content),
                },
                hlc: hlc(1000 + self.reports_written),
                sync: false,
                signature: Vec::new(),
            })?;

            if self.reports_written >= self.max_reports {
                Ok(OperatorDirective::Finish)
            } else {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                ctx.register_timer(now + 50, &())?;
                Ok(OperatorDirective::Continue)
            }
        })
    }

    fn finish<'a, D>(
        &'a mut self,
        _ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<()>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            self.output = None;
            self.source_stats = None;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Config builders
// ---------------------------------------------------------------------------

pub(crate) fn indexer_config() -> OperatorConfig {
    OperatorConfig {
        operator: OperatorConfigVariant::Indexer(IndexerConfig),
        runtime: OperatorRuntimeConfig {
            subscriptions: vec![Subscription::pattern("documents")],
            retry: RetryConfig::default(),
            poll_size: 128,
        },
    }
}

pub(crate) fn archiver_config(max_reports: u64) -> OperatorConfig {
    OperatorConfig {
        operator: OperatorConfigVariant::Archiver(ArchiverConfig { max_reports }),
        runtime: OperatorRuntimeConfig {
            subscriptions: vec![Subscription::pattern("documents")],
            retry: RetryConfig::default(),
            poll_size: 128,
        },
    }
}
