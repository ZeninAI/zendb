use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::sync::Arc;

use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::RwLock;
use zendb_storage::backend::_traits::{ReadBackend, WriteBackend};
use zendb_storage::backend::keydir::KeyDirConfig;
use zendb_types::{Cell, PrimaryKey, Value};

use crate::{
    Change, DispatchOperator, Operator, OperatorDirective, OperatorHost, StateConfig, StateHandle,
};

/// Configuration for the full-text index operator.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct FullTextIndexConfig {
    /// Base state namespace used to persist per-table indexes.
    pub state: String,
    /// Minimum token length to index (shorter tokens are ignored).
    pub min_token_len: usize,
}

impl Default for FullTextIndexConfig {
    fn default() -> Self {
        Self {
            state: "operator/prelude/full-text-index".to_owned(),
            min_token_len: 2,
        }
    }
}

/// A single search hit with its score.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub table: String,
    pub key: PrimaryKey,
    /// Number of query tokens matched by this entry.
    pub matched_tokens: usize,
    /// TF score: sum of (occurrences of matched token in this entry's token set / total tokens).
    pub score: f64,
}

/// Per-table index state handles.
struct TableIndex {
    /// Posting lists: token → set of primary keys containing that token.
    posting: StateHandle<String, HashSet<PrimaryKey>>,
    /// Forward index: primary key → set of tokens for that entry.
    forward: StateHandle<PrimaryKey, HashSet<String>>,
}

impl Clone for TableIndex {
    fn clone(&self) -> Self {
        Self {
            posting: self.posting.clone(),
            forward: self.forward.clone(),
        }
    }
}

/// Full-text index operator.
///
/// Maintains a token-level inverted index (posting lists) and a forward index
/// (per-entry token set) for all subscribed tables. Text is extracted from
/// `Value::String` cells; other value types are ignored.
pub struct FullTextIndexOperator {
    base_state: String,
    indexes: Arc<RwLock<HashMap<String, TableIndex>>>,
    min_token_len: usize,
}

/// Public query interface for the full-text index operator.
///
/// Obtained via [`Workspace::facet`] while the operator is running.
#[derive(Clone)]
pub struct FullTextIndexFacet {
    indexes: Arc<RwLock<HashMap<String, TableIndex>>>,
}

impl FullTextIndexFacet {
    /// Search for entries matching the query string within one indexed table.
    ///
    /// Tokenizes the query, reads posting lists, and returns hits ranked by
    /// number of matched tokens (descending), limited to `limit` results.
    pub fn search(&self, table: &str, query: &str, limit: usize) -> io::Result<Vec<SearchHit>> {
        let tokens = tokenize(query, 1);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        self.search_tokens(table, &tokens, limit)
    }

    /// Return all entries in `table` containing the exact token.
    pub fn lookup_token(&self, table: &str, token: &str) -> io::Result<HashSet<PrimaryKey>> {
        let Some(index) = self.table_index(table) else {
            return Ok(HashSet::new());
        };
        let state = index.posting.get()?;
        let guard = state.read();
        let key = token.to_lowercase();
        Ok(guard.get(&key).map(|v| v.into_owned()).unwrap_or_default())
    }

    /// Return the set of tokens indexed for a particular entry.
    pub fn tokens_for_entry(&self, table: &str, key: &PrimaryKey) -> io::Result<HashSet<String>> {
        let Some(index) = self.table_index(table) else {
            return Ok(HashSet::new());
        };
        let state = index.forward.get()?;
        let guard = state.read();
        Ok(guard.get(key).map(|v| v.into_owned()).unwrap_or_default())
    }

    fn table_index(&self, table: &str) -> Option<TableIndex> {
        self.indexes.read().get(table).cloned()
    }

    fn search_tokens(
        &self,
        table: &str,
        tokens: &[String],
        limit: usize,
    ) -> io::Result<Vec<SearchHit>> {
        let Some(index) = self.table_index(table) else {
            return Ok(Vec::new());
        };
        let state = index.posting.get()?;
        let guard = state.read();

        let mut hit_counts: HashMap<PrimaryKey, usize> = HashMap::new();
        let num_query_tokens = tokens.len();

        for token in tokens {
            if let Some(postings) = guard.get(token) {
                for pk in postings.into_owned() {
                    *hit_counts.entry(pk).or_insert(0) += 1;
                }
            }
        }

        let mut hits: Vec<SearchHit> = hit_counts
            .into_iter()
            .map(|(key, matched)| {
                let score = matched as f64 / num_query_tokens as f64;
                SearchHit {
                    table: table.to_owned(),
                    key,
                    matched_tokens: matched,
                    score,
                }
            })
            .collect();

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.matched_tokens.cmp(&a.matched_tokens))
        });
        hits.truncate(limit);
        Ok(hits)
    }
}

/// State config for FTI: unordered (KeyDir) since we only do exact lookups.
fn state_config() -> StateConfig {
    StateConfig::Unordered(KeyDirConfig::default())
}

impl Operator for FullTextIndexOperator {
    type Config = FullTextIndexConfig;
    type Timer = ();
    type Facet = FullTextIndexFacet;

    fn create<'a, D>(
        db: &'a Arc<OperatorHost<D>>,
        _name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: DispatchOperator,
        Self: Sized,
    {
        async move {
            let mut op = Self {
                base_state: config.state.clone(),
                indexes: Arc::new(RwLock::new(HashMap::new())),
                min_token_len: config.min_token_len,
            };
            op.reconcile_orphaned_tables(db)?;
            Ok(op)
        }
    }

    fn facet(&self) -> FullTextIndexFacet {
        FullTextIndexFacet {
            indexes: Arc::clone(&self.indexes),
        }
    }

    fn on_input_opened<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<OperatorHost<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            // Only rebuild if we don't already have index state for this table.
            // The operator processes changes incrementally via process(), so
            // a rebuild is only needed on first open or after state was deleted.
            let needs_rebuild = {
                let indexes = self.indexes.read();
                !indexes.contains_key(&table)
            };

            if needs_rebuild {
                self.rebuild_table(db, &table)?;
            }
            Ok(OperatorDirective::Continue)
        }
    }

    fn on_input_closed<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<OperatorHost<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            // If the table was deleted, clean up its index state.
            if !db.contains_table(&table) {
                self.indexes.write().remove(&table);
                let _ = db.delete_state(&posting_state_name(&self.base_state, &table));
                let _ = db.delete_state(&forward_state_name(&self.base_state, &table));
            }
            Ok(OperatorDirective::Continue)
        }
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        db: &'a Arc<OperatorHost<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            // Group changes by table to reduce state handle lookups.
            let mut by_table: HashMap<String, Vec<(PrimaryKey, Option<Cell>, Option<Cell>)>> =
                HashMap::new();

            for change in changes {
                let table = change.event.table_id;
                let pk = change.event.primary_key;
                by_table
                    .entry(table)
                    .or_default()
                    .push((pk, change.previous, change.current));
            }

            for (table, table_changes) in by_table {
                let index = self.table_index(db, &table)?;
                let posting_state = index.posting.get()?;
                let forward_state = index.forward.get()?;

                // Batch updates: collect all posting list modifications.
                let mut posting_removals: HashMap<String, Vec<PrimaryKey>> = HashMap::new();
                let mut posting_additions: HashMap<String, Vec<PrimaryKey>> = HashMap::new();

                for (pk, _previous, current) in table_changes {
                    // Get old tokens from forward index.
                    let old_tokens: HashSet<String> = forward_state
                        .read()
                        .get(&pk)
                        .map(|v| v.into_owned())
                        .unwrap_or_default();

                    // Extract new tokens from current cell.
                    let new_tokens = current
                        .as_ref()
                        .map(|cell| extract_tokens(cell, self.min_token_len))
                        .unwrap_or_default();

                    // Compute diff.
                    for token in old_tokens.difference(&new_tokens) {
                        posting_removals
                            .entry(token.clone())
                            .or_default()
                            .push(pk.clone());
                    }
                    for token in new_tokens.difference(&old_tokens) {
                        posting_additions
                            .entry(token.clone())
                            .or_default()
                            .push(pk.clone());
                    }

                    // Update forward index.
                    let mut forward_write = forward_state.write();
                    if new_tokens.is_empty() {
                        forward_write.delete(&pk)?;
                    } else {
                        forward_write.put(pk, new_tokens)?;
                    }
                }

                // Apply batched posting list updates.
                let mut posting_write = posting_state.write();

                for (token, pks_to_remove) in posting_removals {
                    if let Some(mut set) = posting_write.get(&token).map(|v| v.into_owned()) {
                        for pk in pks_to_remove {
                            set.remove(&pk);
                        }
                        if set.is_empty() {
                            posting_write.delete(&token)?;
                        } else {
                            posting_write.put(token, set)?;
                        }
                    }
                }

                for (token, pks_to_add) in posting_additions {
                    let mut set = posting_write
                        .get(&token)
                        .map(|v| v.into_owned())
                        .unwrap_or_default();
                    for pk in pks_to_add {
                        set.insert(pk);
                    }
                    posting_write.put(token, set)?;
                }
            }

            Ok(OperatorDirective::Continue)
        }
    }
}

impl FullTextIndexOperator {
    fn table_index<D>(&mut self, db: &Arc<OperatorHost<D>>, table: &str) -> io::Result<TableIndex>
    where
        D: DispatchOperator,
    {
        if let Some(index) = self.indexes.read().get(table).cloned() {
            return Ok(index);
        }

        let posting = db.state(
            &posting_state_name(&self.base_state, table),
            Some(state_config()),
        )?;
        let forward = db.state(
            &forward_state_name(&self.base_state, table),
            Some(state_config()),
        )?;
        let index = TableIndex { posting, forward };
        self.indexes.write().insert(table.to_owned(), index.clone());
        Ok(index)
    }

    /// Full rebuild: scan all entries in the table and reindex from scratch.
    fn rebuild_table<D>(&mut self, db: &Arc<OperatorHost<D>>, table: &str) -> io::Result<()>
    where
        D: DispatchOperator,
    {
        let table_handle = db.table(table).open()?;
        let table_guard = table_handle.get()?;
        let table_read = table_guard.read();

        let index = self.table_index(db, table)?;
        let posting_state = index.posting.get()?;
        let forward_state = index.forward.get()?;

        // Clear existing index data.
        posting_state.write().clear()?;
        forward_state.write().clear()?;

        // Build posting lists in memory first, then write all at once before
        // writing forward entries. This ensures posting lists are available
        // when forward index is queried.
        let mut posting_lists: HashMap<String, HashSet<PrimaryKey>> = HashMap::new();
        let mut forward_entries: Vec<(PrimaryKey, HashSet<String>)> = Vec::new();

        for (pk, cell) in table_read.entries() {
            let pk = pk.into_owned();
            let tokens = extract_tokens(cell.as_ref(), self.min_token_len);
            if tokens.is_empty() {
                continue;
            }

            for token in &tokens {
                posting_lists
                    .entry(token.clone())
                    .or_default()
                    .insert(pk.clone());
            }

            forward_entries.push((pk, tokens));
        }

        // Write posting lists FIRST.
        {
            let mut posting_write = posting_state.write();
            for (token, pks) in posting_lists {
                posting_write.put(token, pks)?;
            }
        }

        // Then write forward entries. This is what wait_until checks,
        // so posting lists must be complete before this.
        {
            let mut forward_write = forward_state.write();
            for (pk, tokens) in forward_entries {
                forward_write.put(pk, tokens)?;
            }
        }

        Ok(())
    }

    /// Remove index entries for tables that no longer exist in the catalog.
    fn reconcile_orphaned_tables<D>(&mut self, db: &Arc<OperatorHost<D>>) -> io::Result<()>
    where
        D: DispatchOperator,
    {
        let posting_prefix = format!("{}/posting/", self.base_state.trim_end_matches('/'));
        let forward_prefix = format!("{}/forward/", self.base_state.trim_end_matches('/'));

        for state_name in db.list_states() {
            let table = if let Some(t) = state_name.strip_prefix(&posting_prefix) {
                t
            } else if let Some(t) = state_name.strip_prefix(&forward_prefix) {
                t
            } else {
                continue;
            };

            if db.contains_table(table) {
                // Load existing index handles.
                let posting = db.state(
                    &posting_state_name(&self.base_state, table),
                    Some(state_config()),
                )?;
                let forward = db.state(
                    &forward_state_name(&self.base_state, table),
                    Some(state_config()),
                )?;
                self.indexes
                    .write()
                    .insert(table.to_owned(), TableIndex { posting, forward });
            } else {
                // Orphaned — delete.
                let _ = db.delete_state(&posting_state_name(&self.base_state, table));
                let _ = db.delete_state(&forward_state_name(&self.base_state, table));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn posting_state_name(base: &str, table: &str) -> String {
    format!("{}/posting/{}", base.trim_end_matches('/'), table)
}

fn forward_state_name(base: &str, table: &str) -> String {
    format!("{}/forward/{}", base.trim_end_matches('/'), table)
}

/// Tokenize text: lowercase, split on non-alphanumeric, filter by min length.
fn tokenize(text: &str, min_len: usize) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.len() >= min_len)
        .map(|t| t.to_owned())
        .collect()
}

/// Extract indexable tokens from a cell value.
fn extract_tokens(cell: &Cell, min_token_len: usize) -> HashSet<String> {
    let mut tokens = HashSet::new();
    if let Some(value) = &cell.value {
        extract_tokens_from_value(value, min_token_len, &mut tokens);
    }
    tokens
}

fn extract_tokens_from_value(value: &Value, min_len: usize, tokens: &mut HashSet<String>) {
    match value {
        Value::String(s) => {
            for token in tokenize(s, min_len) {
                tokens.insert(token);
            }
        }
        Value::Text(t) => {
            let s = t.string();
            for token in tokenize(&s, min_len) {
                tokens.insert(token);
            }
        }
        Value::Record(r) => {
            for (_, cell) in r.fields() {
                if let Some(v) = &cell.value {
                    extract_tokens_from_value(v, min_len, tokens);
                }
            }
        }
        Value::List(l) => {
            for cell in l.cells() {
                if let Some(v) = &cell.value {
                    extract_tokens_from_value(v, min_len, tokens);
                }
            }
        }
        Value::MvRegister(mv) => {
            for v in mv.values() {
                extract_tokens_from_value(v, min_len, tokens);
            }
        }
        _ => {}
    }
}
