//! Document indexing pipeline operators and helpers.

use std::collections::HashSet;
use std::sync::Arc;
use std::io;
use std::time::{Duration, Instant};

use bincode::{Decode, Encode};
use zendb_engine::{
    define_operator_set, BoxFuture, Change, Database, DispatchOperator, Operator,
    OperatorDirective, OperatorRuntimeConfig, StateHandle, Subscription, TableConfig, TableHandle,
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
    index: StateHandle<String, HashSet<String>>,
    stats: StateHandle<String, u64>,
}

impl Operator for IndexerOp {
    type Config = IndexerConfig;
    type Timer = ();

    fn create<'a, D>(
        db: &'a Arc<Database<D>>, name: &'a str, config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<Self>>
    where
        D: DispatchOperator,
        Self: Sized,
    {
        Box::pin(async move {
            let index = db.state("index", Some(StateConfig::default()))?;
            let stats = db.state("doc_stats", Some(StateConfig::default()))?;
            Ok(Self { index, stats })
        })
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        _db: &'a Arc<Database<D>>, _name: &'a str, _config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
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
                                let handle = self.stats.get()?;
                                let mut state = handle.write();
                                let current =
                                    state.get(&doc_id).map(|v| v.into_owned()).unwrap_or(0);
                                state.put(doc_id.clone(), current.max(word_count))?;
                            }

                            {
                                let handle = self.index.get()?;
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
                            let handle = self.stats.get()?;
                            let mut state = handle.write();
                            state.delete(&doc_id)?;
                        }
                        {
                            let handle = self.index.get()?;
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
    output: TableHandle,
    source_stats: StateHandle<String, u64>,
}

impl Operator for ArchiverOp {
    type Config = ArchiverConfig;
    type Timer = ();

    fn create<'a, D>(
        db: &'a Arc<Database<D>>, name: &'a str, config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<Self>>
    where
        D: DispatchOperator,
        Self: Sized,
    {
        Box::pin(async move {
            let output = db.table("reports", Some(TableConfig::default()))?;
            let source_stats = db.state("doc_stats", None)?;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            db.register_timer(name, now + 50, &())?;

            Ok(Self {
                reports_written: 0,
                max_reports: config.max_reports,
                output,
                source_stats,
            })
        })
    }

    fn process<'a, D>(
        &'a mut self,
        _changes: Vec<Change>,
        _db: &'a Arc<Database<D>>, _name: &'a str, _config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async { Ok(OperatorDirective::Continue) })
    }

    fn on_timer<'a, D>(
        &'a mut self,
        _payload: (),
        _fire_at_ms: u64,
        db: &'a Arc<Database<D>>, name: &'a str, config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            self.reports_written += 1;

            let stats_snapshot: Vec<(String, u64)> = {
                let state = self.source_stats.get()?;
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

            self.output.get()?.write().insert_event(Event {
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
                db.register_timer(name, now + 50, &())?;
                Ok(OperatorDirective::Continue)
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Config builders
// ---------------------------------------------------------------------------

pub(crate) fn indexer_config() -> IndexerConfig {
    IndexerConfig
}

pub(crate) fn indexer_runtime_config() -> OperatorRuntimeConfig {
    OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("documents")],
        poll_size: 128,
    }
}

pub(crate) fn archiver_config(max_reports: u64) -> ArchiverConfig {
    ArchiverConfig { max_reports }
}

pub(crate) fn archiver_runtime_config() -> OperatorRuntimeConfig {
    OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("documents")],
        poll_size: 128,
    }
}
