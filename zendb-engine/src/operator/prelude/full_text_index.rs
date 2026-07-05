use std::borrow::Cow;
use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::sync::Arc;

use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::RwLock;
use zendb_storage::core::traits::Backend;
use zendb_types::{Cell, PrimaryKey, Value};

use crate::{
    Change, Database, DispatchOperator, Operator, OperatorDirective, StateConfig,
    StateHandle,
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

/// Key for the full-text index state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub enum FtiStateKey {
    /// Posting list for a token: maps token → set of (table, primary_key).
    Posting(String),
    /// Forward index: maps (table, primary_key) → set of tokens for that entry.
    /// Used for efficient deletion/update without scanning all posting lists.
    Forward(PrimaryKey),
}

/// Value for the full-text index state.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum FtiStateValue {
    /// Set of (table, primary_key) pairs that contain this token.
    PostingList(HashSet<PrimaryKey>),
    /// Set of tokens extracted from a particular entry.
    TokenSet(HashSet<String>),
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

/// Full-text index operator.
///
/// Maintains a token-level inverted index (posting lists) and a forward index
/// (per-entry token set) for all subscribed tables. Text is extracted from
/// `Value::String` cells; other value types are ignored.
pub struct FullTextIndexOperator {
    base_state: String,
    states: Arc<RwLock<HashMap<String, StateHandle<FtiStateKey, FtiStateValue>>>>,
    min_token_len: usize,
}

/// Public query interface for the full-text index operator.
///
/// Obtained via [`Database::facet`] while the operator is running.
#[derive(Clone)]
pub struct FullTextIndexFacet {
    states: Arc<RwLock<HashMap<String, StateHandle<FtiStateKey, FtiStateValue>>>>,
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
        let Some(state) = self.table_state(table) else {
            return Ok(HashSet::new());
        };
        let state = state.get()?;
        let guard = state.read();
        let key = FtiStateKey::Posting(token.to_lowercase());
        Ok(match guard.get(&key) {
            Some(value) => match value.into_owned() {
                FtiStateValue::PostingList(set) => set,
                _ => HashSet::new(),
            },
            None => HashSet::new(),
        })
    }

    /// Return the set of tokens indexed for a particular entry.
    pub fn tokens_for_entry(&self, table: &str, key: &PrimaryKey) -> io::Result<HashSet<String>> {
        let Some(state) = self.table_state(table) else {
            return Ok(HashSet::new());
        };
        let state = state.get()?;
        let guard = state.read();
        let fwd_key = FtiStateKey::Forward(key.clone());
        Ok(match guard.get(&fwd_key) {
            Some(value) => match value.into_owned() {
                FtiStateValue::TokenSet(set) => set,
                _ => HashSet::new(),
            },
            None => HashSet::new(),
        })
    }

    fn table_state(&self, table: &str) -> Option<StateHandle<FtiStateKey, FtiStateValue>> {
        self.states.read().get(table).cloned()
    }

    fn search_tokens(
        &self,
        table: &str,
        tokens: &[String],
        limit: usize,
    ) -> io::Result<Vec<SearchHit>> {
        let Some(state) = self.table_state(table) else {
            return Ok(Vec::new());
        };
        let state = state.get()?;
        let guard = state.read();

        // Collect posting lists for each query token.
        let mut hit_counts: HashMap<PrimaryKey, usize> = HashMap::new();
        let num_query_tokens = tokens.len();

        for token in tokens {
            let key = FtiStateKey::Posting(token.clone());
            if let Some(value) = guard.get(&key) {
                if let FtiStateValue::PostingList(postings) = value.into_owned() {
                    for pk in postings {
                        *hit_counts.entry(pk).or_insert(0) += 1;
                    }
                }
            }
        }

        // Score and rank.
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

        // Sort by score descending, then by matched_tokens descending.
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

impl Operator for FullTextIndexOperator {
    type Config = FullTextIndexConfig;
    type Timer = ();
    type Facet = FullTextIndexFacet;

    fn create<'a, D>(
        db: &'a Arc<Database<D>>,
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
                states: Arc::new(RwLock::new(HashMap::new())),
                min_token_len: config.min_token_len,
            };
            op.reconcile_orphaned_tables(db)?;
            Ok(op)
        }
    }

    fn facet(&self) -> FullTextIndexFacet {
        FullTextIndexFacet {
            states: Arc::clone(&self.states),
        }
    }

    fn on_input_opened<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            self.rebuild_table(db, &table)?;
            Ok(OperatorDirective::Continue)
        }
    }

    fn on_input_closed<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            // If the table was deleted (not just closed), clean up its index entries.
            // The table is already removed from the catalog by this point.
            // For now, we keep the index entries alive — they can be cleaned up
            // on the next rebuild if the table reappears, or removed explicitly.
            if !db.contains_table(&table) {
                self.states.write().remove(&table);
                let _ = db.delete_state(&table_state_name(&self.base_state, &table))?;
            }
            Ok(OperatorDirective::Continue)
        }
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            for change in changes {
                let table = change.event.table_id.clone();
                let pk = change.event.primary_key.clone();
                let state = self.table_state(db, &table)?;
                let state = state.get()?;
                let mut state = state.write();

                let old_tokens = get_forward_tokens(&state, &pk);
                let new_tokens = change
                    .current
                    .as_ref()
                    .map(|cell| extract_tokens(cell, self.min_token_len))
                    .unwrap_or_default();

                // Compute tokens to remove and add.
                let to_remove: Vec<String> = old_tokens.difference(&new_tokens).cloned().collect();
                let to_add: Vec<String> = new_tokens.difference(&old_tokens).cloned().collect();

                if to_remove.is_empty() && to_add.is_empty() {
                    continue;
                }

                // Remove from old posting lists.
                for token in &to_remove {
                    let posting_key = FtiStateKey::Posting(token.clone());
                    update_posting_list(&mut state, &posting_key, |set| {
                        set.remove(&pk);
                    })?;
                }

                // Add to new posting lists.
                for token in &to_add {
                    let posting_key = FtiStateKey::Posting(token.clone());
                    update_posting_list(&mut state, &posting_key, |set| {
                        set.insert(pk.clone());
                    })?;
                }

                // Update forward index.
                let fwd_key = FtiStateKey::Forward(pk);
                if new_tokens.is_empty() {
                    state.delete(&fwd_key)?;
                } else {
                    state.put(fwd_key, FtiStateValue::TokenSet(new_tokens))?;
                }
            }

            Ok(OperatorDirective::Continue)
        }
    }
}

impl FullTextIndexOperator {
    fn table_state<D>(
        &mut self,
        db: &Arc<Database<D>>,
        table: &str,
    ) -> io::Result<StateHandle<FtiStateKey, FtiStateValue>>
    where
        D: DispatchOperator,
    {
        if let Some(state) = self.states.read().get(table).cloned() {
            return Ok(state);
        }

        let state = db.state(
            &table_state_name(&self.base_state, table),
            Some(StateConfig::default()),
        )?;
        self.states.write().insert(table.to_owned(), state.clone());
        Ok(state)
    }

    /// Full rebuild: scan all entries in the table and reindex from scratch.
    fn rebuild_table<D>(&mut self, db: &Arc<Database<D>>, table: &str) -> io::Result<()>
    where
        D: DispatchOperator,
    {
        let table_handle = db.table(table, None)?;
        let table_guard = table_handle.get()?;
        let table_read = table_guard.read();

        let state = self.table_state(db, table)?;
        let state = state.get()?;
        let mut state = state.write();
        state.clear()?;

        // Rebuild from table contents.
        for (pk, cell) in table_read.entries() {
            let pk = pk.into_owned();
            let tokens = extract_tokens(cell.as_ref(), self.min_token_len);
            if tokens.is_empty() {
                continue;
            }

            for token in &tokens {
                let posting_key = FtiStateKey::Posting(token.clone());
                update_posting_list(&mut state, &posting_key, |set| {
                    set.insert(pk.clone());
                })?;
            }

            state.put(FtiStateKey::Forward(pk), FtiStateValue::TokenSet(tokens))?;
        }

        Ok(())
    }

    /// Remove index entries for tables that no longer exist in the catalog.
    /// Called once during `create()` to handle tables deleted while the
    /// operator was suspended.
    fn reconcile_orphaned_tables<D>(&mut self, db: &Arc<Database<D>>) -> io::Result<()>
    where
        D: DispatchOperator,
    {
        let prefix = format!("{}/", self.base_state.trim_end_matches('/'));
        for state_name in db.list_states() {
            let Some(table) = state_name.strip_prefix(&prefix) else {
                continue;
            };
            if db.contains_table(table) {
                let state = db.state(&state_name, Some(StateConfig::default()))?;
                self.states.write().insert(table.to_owned(), state);
            } else {
                let _ = db.delete_state(&state_name)?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn table_state_name(base: &str, table: &str) -> String {
    format!("{}/{}", base.trim_end_matches('/'), table)
}

/// Tokenize text: lowercase, split on non-alphanumeric, filter by min length.
fn tokenize(text: &str, min_len: usize) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.len() >= min_len)
        .map(|t| t.to_owned())
        .collect()
}

/// Recursively collect all text fragments from a value.
fn collect_text(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => {
            if !s.is_empty() {
                out.push(s.clone());
            }
        }
        Value::Text(t) => {
            let s = t.string();
            if !s.is_empty() {
                out.push(s);
            }
        }
        Value::Record(r) => {
            for (_, cell) in r.fields() {
                if let Some(v) = &cell.value {
                    collect_text(v, out);
                }
            }
        }
        Value::List(l) => {
            for cell in l.cells() {
                if let Some(v) = &cell.value {
                    collect_text(v, out);
                }
            }
        }
        Value::MvRegister(mv) => {
            for v in mv.values() {
                collect_text(v, out);
            }
        }
        _ => {}
    }
}

/// Extract indexable tokens from a cell value by recursively walking containers.
fn extract_tokens(cell: &Cell, min_token_len: usize) -> HashSet<String> {
    let mut texts = Vec::new();
    if let Some(value) = &cell.value {
        collect_text(value, &mut texts);
    }
    let mut tokens = HashSet::new();
    for text in texts {
        for token in tokenize(&text, min_token_len) {
            tokens.insert(token);
        }
    }
    tokens
}

/// Read the forward token set for an entry from state.
fn get_forward_tokens(
    state: &crate::State<FtiStateKey, FtiStateValue>,
    key: &PrimaryKey,
) -> HashSet<String> {
    let fwd_key = FtiStateKey::Forward(key.clone());
    match state.get(&fwd_key) {
        Some(value) => match value.into_owned() {
            FtiStateValue::TokenSet(set) => set,
            _ => HashSet::new(),
        },
        None => HashSet::new(),
    }
}

/// Update a posting list in-place: read, apply mutation, write back (or delete if empty).
fn update_posting_list(
    state: &mut crate::State<FtiStateKey, FtiStateValue>,
    key: &FtiStateKey,
    f: impl FnOnce(&mut HashSet<PrimaryKey>),
) -> io::Result<()> {
    let mut set = match state.get(key).map(Cow::into_owned) {
        Some(FtiStateValue::PostingList(s)) => s,
        _ => HashSet::new(),
    };
    f(&mut set);
    if set.is_empty() {
        state.delete(key)?;
    } else {
        state.put(key.clone(), FtiStateValue::PostingList(set))?;
    }
    Ok(())
}
