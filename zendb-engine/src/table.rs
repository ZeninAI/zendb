//! Table abstraction over materialized state and an ordered delta log.

use std::{borrow::Cow, cmp::Ordering, collections::BTreeMap, fs, io, path::Path};

use bincode::{Decode, Encode};
use zendb_storage::core::{
    backend::{Backend, OrderedBackend},
    btree::{BPlusTree, BPlusTreeConfig, BPlusTreeStats},
    keydir::{KeyDir, KeyDirConfig, KeyDirStats},
    orderlog::{OrderLog, OrderLogConfig, OrderLogStats},
};
use zendb_types::{Cell, Delta, Hlc, PrimaryKey};

type OrderedState = BPlusTree<PrimaryKey, Cell>;
type UnorderedState = KeyDir<PrimaryKey, Cell>;
type EventLog = OrderLog<EventKey, Delta>;

pub const DEFAULT_MAX_EVENTS: usize = 1_000;

/// Controls when in-flight deltas are materialized into table state.
#[derive(Debug, Clone, Encode, Decode)]
pub enum FlushConfig {
    Manual,
    EventCount { max_events: usize },
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self::EventCount {
            max_events: DEFAULT_MAX_EVENTS,
        }
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

/// Complete configuration required to create or open a table.
#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct TableConfig {
    pub sync: bool,
    pub flush: FlushConfig,
    pub state: StateConfig,
    pub events: OrderLogConfig,
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

/// OrderLog key that groups events by row and orders each row by HLC.
///
/// HLC includes the originating device ID and acts as the event identity.
/// The path stays in the delta value because ordering by path could reorder
/// parent and descendant operations within a row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventKey {
    pub primary_key: PrimaryKey,
    pub hlc: Hlc,
}

impl EventKey {
    pub fn from_delta(delta: &Delta) -> Self {
        Self {
            primary_key: delta.primary_key.clone(),
            hlc: delta.hlc,
        }
    }
}

enum State {
    Ordered(OrderedState),
    Unordered(UnorderedState),
}

/// A table presents a resolved `PrimaryKey -> Cell` backend while owning the
/// materialized backend and durable in-flight event log used to produce it.
///
/// The `cache` is an in-memory map from primary key to the **fully resolved**
/// `Cell` (i.e. `state[pk] ⊔ fold(deltas-for-pk-in-events-log)`). It is a
/// transparent read accelerator over the events log, not a parallel store of
/// truth — on crash it is dropped and rebuilt from the events log on `open`.
///
/// Invariant: for every `pk ∈ cache`, `cache[pk].0` equals what the legacy
/// "state.get + apply events.range" path would compute, and `cache[pk].1`
/// (the `had_previous_in_state` bit) reflects whether the materialized state
/// contained `pk` at the time the cache entry was first created.
///
/// `novel_pending` is the count of cache entries with `had_previous == false`,
/// maintained incrementally so that `Backend::size()` is `state.size() +
/// novel_pending` in O(1).
pub struct Table {
    config: TableConfig,
    state: State,
    events: EventLog,
    cache: BTreeMap<PrimaryKey, (Cell, bool)>,
    novel_pending: usize,
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
            cache: BTreeMap::new(),
            novel_pending: 0,
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
        let events: EventLog = OrderLog::open(&path.join("events"), config.events.clone())?;

        // Replay the events log into the in-memory cache. `events.entries()`
        // yields in `EventKey` order, which groups by primary_key and orders
        // each row's events by HLC, so we walk one row at a time and commit
        // it on the next-row transition.
        let mut cache: BTreeMap<PrimaryKey, (Cell, bool)> = BTreeMap::new();
        let mut novel_pending: usize = 0;
        let mut group: Option<(PrimaryKey, Cell, bool, bool)> = None;
        for (event_key, delta) in events.entries() {
            let same_row = group
                .as_ref()
                .is_some_and(|(pk, _, _, _)| pk == &event_key.primary_key);

            if !same_row {
                if let Some((pk, cell, had_previous, changed)) = group.take() {
                    if had_previous || changed {
                        if !had_previous {
                            novel_pending += 1;
                        }
                        cache.insert(pk, (cell, had_previous));
                    }
                }
                let pk = event_key.primary_key.clone();
                let (cell, had_previous) = match &state {
                    State::Ordered(s) => match s.get(&pk) {
                        Some(c) => (c.into_owned(), true),
                        None => (Cell::dummy(None), false),
                    },
                    State::Unordered(s) => match s.get(&pk) {
                        Some(c) => (c.into_owned(), true),
                        None => (Cell::dummy(None), false),
                    },
                };
                group = Some((pk, cell, had_previous, false));
            }

            let g = group.as_mut().unwrap();
            g.3 |= g.1.apply(&delta);
        }
        if let Some((pk, cell, had_previous, changed)) = group {
            if had_previous || changed {
                if !had_previous {
                    novel_pending += 1;
                }
                cache.insert(pk, (cell, had_previous));
            }
        }

        Ok(Self {
            config,
            state,
            events,
            cache,
            novel_pending,
        })
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    /// Insert a delta if it is not already present (by `EventKey`).
    ///
    /// Returns whether the delta was inserted.
    pub fn insert_delta(&mut self, delta: Delta) -> io::Result<bool> {
        let key = EventKey::from_delta(&delta);
        if !self.events.put_if_absent(key, delta.clone())? {
            return Ok(false);
        }

        // Update the resolved-Cell cache. Either fold the delta into the
        // existing cache entry, or create a fresh entry seeded from state.
        // Skip fresh-entry creation if both the row was absent from state
        // AND the delta did not change anything (preserves "absent + only
        // no-op events ⇒ row invisible" semantics).
        if let Some((cell, _)) = self.cache.get_mut(&delta.primary_key) {
            cell.apply(&delta);
        } else {
            let (mut cell, had_previous) = match &self.state {
                State::Ordered(s) => match s.get(&delta.primary_key) {
                    Some(c) => (c.into_owned(), true),
                    None => (Cell::dummy(None), false),
                },
                State::Unordered(s) => match s.get(&delta.primary_key) {
                    Some(c) => (c.into_owned(), true),
                    None => (Cell::dummy(None), false),
                },
            };
            let changed = cell.apply(&delta);
            if had_previous || changed {
                if !had_previous {
                    self.novel_pending += 1;
                }
                self.cache
                    .insert(delta.primary_key.clone(), (cell, had_previous));
            }
        }

        self.maybe_materialize()?;

        Ok(true)
    }

    /// Insert deltas one-by-one in sorted order, returning the count that
    /// were not duplicates.
    ///
    /// Note: we intentionally do **not** use [`OrderLog::bulk_put_sorted`]
    /// here. That primitive has overwrite (last-write-wins) semantics for
    /// duplicate keys, but `insert_delta` is `put_if_absent` — a delta whose
    /// `EventKey` already exists in the journal must be a no-op, not a
    /// silent overwrite, otherwise the cache would double-apply it. So we
    /// route through `events.put_if_absent` per item.
    pub fn bulk_insert_delta(
        &mut self,
        deltas: impl IntoIterator<Item = Delta>,
    ) -> io::Result<usize> {
        let mut pairs: Vec<(EventKey, Delta)> = deltas
            .into_iter()
            .map(|delta| (EventKey::from_delta(&delta), delta))
            .collect();

        pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
        pairs.dedup_by(|(a, _), (b, _)| a == b);

        let mut inserted = 0;
        for (key, delta) in pairs {
            if !self.events.put_if_absent(key, delta.clone())? {
                continue;
            }

            if let Some((cell, _)) = self.cache.get_mut(&delta.primary_key) {
                cell.apply(&delta);
            } else {
                let (mut cell, had_previous) = match &self.state {
                    State::Ordered(s) => match s.get(&delta.primary_key) {
                        Some(c) => (c.into_owned(), true),
                        None => (Cell::dummy(None), false),
                    },
                    State::Unordered(s) => match s.get(&delta.primary_key) {
                        Some(c) => (c.into_owned(), true),
                        None => (Cell::dummy(None), false),
                    },
                };
                let changed = cell.apply(&delta);
                if had_previous || changed {
                    if !had_previous {
                        self.novel_pending += 1;
                    }
                    self.cache
                        .insert(delta.primary_key.clone(), (cell, had_previous));
                }
            }

            inserted += 1;
        }

        if inserted > 0 {
            self.maybe_materialize()?;
        }
        Ok(inserted)
    }

    pub fn materialize(&mut self) -> io::Result<()> {
        // Iterate by reference and only clear at the end so that a
        // mid-loop I/O failure leaves the in-memory cache intact —
        // otherwise reads in this process would no longer see pending
        // edits even though the events log still contains them.
        match &mut self.state {
            State::Ordered(state) => {
                for (pk, (cell, _)) in &self.cache {
                    state.put(pk.clone(), cell.clone())?;
                }
                state.sync()?;
            }
            State::Unordered(state) => {
                for (pk, (cell, _)) in &self.cache {
                    state.put(pk.clone(), cell.clone())?;
                }
                state.sync()?;
            }
        }
        self.events.clear()?;
        self.events.sync()?;
        self.cache.clear();
        self.novel_pending = 0;

        Ok(())
    }

    fn maybe_materialize(&mut self) -> io::Result<()> {
        let should = match self.config.flush {
            FlushConfig::Manual => false,
            FlushConfig::EventCount { max_events } => self.events.size() >= max_events,
        };
        if should {
            self.materialize()?;
        }
        Ok(())
    }
}

impl Backend<PrimaryKey, Cell> for Table {
    type Stats<'a>
        = TableStats<'a>
    where
        Self: 'a;
    type Config = TableConfig;

    // ---- reads --------------------------------------------------------

    fn get(&self, key: &PrimaryKey) -> Option<Cow<'_, Cell>> {
        if let Some((cell, _)) = self.cache.get(key) {
            return Some(Cow::Borrowed(cell));
        }
        match &self.state {
            State::Ordered(state) => state.get(key),
            State::Unordered(state) => state.get(key),
        }
    }

    fn contains(&self, key: &PrimaryKey) -> bool {
        if self.cache.contains_key(key) {
            return true;
        }
        match &self.state {
            State::Ordered(state) => state.contains(key),
            State::Unordered(state) => state.contains(key),
        }
    }

    // ---- writes (delegated straight to the state backend) ------------
    //
    // These bypass the events log and the resolved-Cell cache, so the
    // cache may become stale for keys touched here. This is intentional
    // for now; callers who mix direct writes with `insert_delta` are
    // responsible for ordering / consistency.

    fn put(&mut self, key: PrimaryKey, value: Cell) -> io::Result<()> {
        match &mut self.state {
            State::Ordered(state) => state.put(key, value),
            State::Unordered(state) => state.put(key, value),
        }
    }

    fn put_if_absent(&mut self, key: PrimaryKey, value: Cell) -> io::Result<bool> {
        match &mut self.state {
            State::Ordered(state) => state.put_if_absent(key, value),
            State::Unordered(state) => state.put_if_absent(key, value),
        }
    }

    fn replace(&mut self, key: PrimaryKey, value: Cell) -> io::Result<Option<Cow<'_, Cell>>> {
        match &mut self.state {
            State::Ordered(state) => state.replace(key, value),
            State::Unordered(state) => state.replace(key, value),
        }
    }

    fn bulk_put<I>(&mut self, items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (PrimaryKey, Cell)>,
    {
        match &mut self.state {
            State::Ordered(state) => state.bulk_put(items),
            State::Unordered(state) => state.bulk_put(items),
        }
    }

    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (PrimaryKey, Cell)>,
    {
        match &mut self.state {
            State::Ordered(state) => state.bulk_put_sorted(sorted),
            State::Unordered(state) => state.bulk_put_sorted(sorted),
        }
    }

    fn delete(&mut self, key: &PrimaryKey) -> io::Result<bool> {
        match &mut self.state {
            State::Ordered(state) => state.delete(key),
            State::Unordered(state) => state.delete(key),
        }
    }

    fn bulk_delete<'a, I>(&mut self, keys: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a PrimaryKey>,
        PrimaryKey: 'a,
    {
        match &mut self.state {
            State::Ordered(state) => state.bulk_delete(keys),
            State::Unordered(state) => state.bulk_delete(keys),
        }
    }

    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a PrimaryKey>,
        PrimaryKey: 'a,
    {
        match &mut self.state {
            State::Ordered(state) => state.bulk_delete_sorted(sorted),
            State::Unordered(state) => state.bulk_delete_sorted(sorted),
        }
    }

    fn update<F>(&mut self, key: &PrimaryKey, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<Cell>) -> Option<Cell>,
    {
        match &mut self.state {
            State::Ordered(state) => state.update(key, f),
            State::Unordered(state) => state.update(key, f),
        }
    }

    fn clear(&mut self) -> io::Result<()> {
        match &mut self.state {
            State::Ordered(state) => state.clear()?,
            State::Unordered(state) => state.clear()?,
        }
        self.events.clear()?;
        self.cache.clear();
        self.novel_pending = 0;
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

    // ---- iteration (resolved over state ⊕ cache) ---------------------

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, PrimaryKey>> + 'a
    where
        PrimaryKey: 'a,
    {
        // Same merge shape as `entries`, but on the state side we ask
        // for `keys()` only so the backend can skip deserializing Cell
        // values (when its `keys()` becomes a true key-only iterator).
        match &self.state {
            State::Ordered(state) => {
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
                        Ordering::Greater => c_iter.next().map(Cow::Borrowed),
                        Ordering::Equal => {
                            s_iter.next();
                            c_iter.next().map(Cow::Borrowed)
                        }
                    }
                });

                Box::new(it) as Box<dyn Iterator<Item = Cow<'a, PrimaryKey>> + 'a>
            }
            State::Unordered(state) => {
                let cache_ref = &self.cache;
                let state_only = state
                    .keys()
                    .filter(move |k| !cache_ref.contains_key(k.as_ref()));
                let cache_keys = self.cache.keys().map(Cow::Borrowed);
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
            State::Ordered(state) => {
                let mut s_iter = state.entries().peekable();
                let mut c_iter = self.cache.iter().peekable();

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
                            let (_, (cell, _)) = c_iter.next().unwrap();
                            Some(Cow::Borrowed(cell))
                        }
                        Ordering::Equal => {
                            s_iter.next();
                            let (_, (cell, _)) = c_iter.next().unwrap();
                            Some(Cow::Borrowed(cell))
                        }
                    }
                });

                Box::new(it) as Box<dyn Iterator<Item = Cow<'a, Cell>> + 'a>
            }
            State::Unordered(state) => {
                let cache_ref = &self.cache;
                let state_only = state
                    .entries()
                    .filter_map(move |(k, v)| (!cache_ref.contains_key(k.as_ref())).then_some(v));
                let cache_vals = self.cache.values().map(|(v, _)| Cow::Borrowed(v));
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
            State::Ordered(state) => {
                let mut s_iter = state.entries().peekable();
                let mut c_iter = self.cache.iter().peekable();

                let it = std::iter::from_fn(move || {
                    let order = match (s_iter.peek(), c_iter.peek()) {
                        (None, None) => return None,
                        (Some(_), None) => Ordering::Less,
                        (None, Some(_)) => Ordering::Greater,
                        (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
                    };

                    match order {
                        Ordering::Less => s_iter.next(),
                        Ordering::Greater => {
                            let (pk, (cell, _)) = c_iter.next().unwrap();
                            Some((Cow::Borrowed(pk), Cow::Borrowed(cell)))
                        }
                        Ordering::Equal => {
                            let _state_row = s_iter.next();
                            let (pk, (cell, _)) = c_iter.next().unwrap();
                            Some((Cow::Borrowed(pk), Cow::Borrowed(cell)))
                        }
                    }
                });

                Box::new(it) as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
            }
            State::Unordered(state) => {
                let cache_ref = &self.cache;
                let state_only = state
                    .entries()
                    .filter(move |(k, _)| !cache_ref.contains_key(k.as_ref()));
                let cache_iter = self
                    .cache
                    .iter()
                    .map(|(k, (v, _))| (Cow::Borrowed(k), Cow::Borrowed(v)));
                Box::new(state_only.chain(cache_iter))
                    as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
            }
        }
    }

    fn size(&self) -> usize {
        let state_size = match &self.state {
            State::Ordered(state) => state.size(),
            State::Unordered(state) => state.size(),
        };
        state_size + self.novel_pending
    }

    fn is_empty(&self) -> bool {
        // A cache entry with `had_previous == true` requires a corresponding
        // state row, so `state.is_empty()` implies every cache entry has
        // `had_previous == false`, i.e. they all count toward `novel_pending`.
        // Therefore `state.is_empty() && novel_pending == 0` is the exact
        // emptiness predicate.
        let state_empty = match &self.state {
            State::Ordered(state) => state.is_empty(),
            State::Unordered(state) => state.is_empty(),
        };
        state_empty && self.novel_pending == 0
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

impl OrderedBackend<PrimaryKey, Cell> for Table {
    fn range<'a>(
        &'a self,
        start: &PrimaryKey,
        end: &PrimaryKey,
    ) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = match &self.state {
            State::Ordered(state) => state,
            State::Unordered(_) => panic!(
                "Table::range requires an ordered state backend; configure StateConfig::Ordered"
            ),
        };

        // BTreeMap::range uses an exclusive upper bound, matching the
        // [start, end) contract of `OrderedBackend::range`.
        let mut s_iter = state.range(start, end).peekable();
        let mut c_iter = self.cache.range(start.clone()..end.clone()).peekable();

        let it = std::iter::from_fn(move || {
            let order = match (s_iter.peek(), c_iter.peek()) {
                (None, None) => return None,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
            };

            match order {
                Ordering::Less => s_iter.next(),
                Ordering::Greater => {
                    let (pk, (cell, _)) = c_iter.next().unwrap();
                    Some((Cow::Borrowed(pk), Cow::Borrowed(cell)))
                }
                Ordering::Equal => {
                    let _state_row = s_iter.next();
                    let (pk, (cell, _)) = c_iter.next().unwrap();
                    Some((Cow::Borrowed(pk), Cow::Borrowed(cell)))
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
        let state = match &self.state {
            State::Ordered(state) => state,
            State::Unordered(_) => panic!(
                "Table::first requires an ordered state backend; configure StateConfig::Ordered"
            ),
        };

        let s_first = state.first();
        let c_first = self.cache.iter().next();

        match (s_first, c_first) {
            (None, None) => None,
            (Some(s), None) => Some(s),
            (None, Some((pk, (cell, _)))) => Some((Cow::Borrowed(pk), Cow::Borrowed(cell))),
            (Some(s), Some((c_pk, (c_cell, _)))) => match s.0.as_ref().cmp(c_pk) {
                Ordering::Less => Some(s),
                // Greater → cache key is smaller, yield it.
                // Equal   → cache always wins on collision.
                Ordering::Greater | Ordering::Equal => {
                    Some((Cow::Borrowed(c_pk), Cow::Borrowed(c_cell)))
                }
            },
        }
    }

    fn last<'a>(&'a self) -> Option<(Cow<'a, PrimaryKey>, Cow<'a, Cell>)>
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = match &self.state {
            State::Ordered(state) => state,
            State::Unordered(_) => panic!(
                "Table::last requires an ordered state backend; configure StateConfig::Ordered"
            ),
        };

        let s_last = state.last();
        let c_last = self.cache.iter().next_back();

        match (s_last, c_last) {
            (None, None) => None,
            (Some(s), None) => Some(s),
            (None, Some((pk, (cell, _)))) => Some((Cow::Borrowed(pk), Cow::Borrowed(cell))),
            (Some(s), Some((c_pk, (c_cell, _)))) => match s.0.as_ref().cmp(c_pk) {
                Ordering::Greater => Some(s),
                // Less  → cache key is greater, yield it.
                // Equal → cache always wins on collision.
                Ordering::Less | Ordering::Equal => {
                    Some((Cow::Borrowed(c_pk), Cow::Borrowed(c_cell)))
                }
            },
        }
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = match &self.state {
            State::Ordered(state) => state,
            State::Unordered(_) => panic!(
                "Table::entries_rev requires an ordered state backend; configure StateConfig::Ordered"
            ),
        };

        // Two-pointer streaming merge in reverse: pick the larger head
        // each step; cache wins on equality.
        let mut s_iter = state.entries_rev().peekable();
        let mut c_iter = self.cache.iter().rev().peekable();

        let it = std::iter::from_fn(move || {
            let order = match (s_iter.peek(), c_iter.peek()) {
                (None, None) => return None,
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
            };

            match order {
                Ordering::Greater => s_iter.next(),
                Ordering::Less => {
                    let (pk, (cell, _)) = c_iter.next().unwrap();
                    Some((Cow::Borrowed(pk), Cow::Borrowed(cell)))
                }
                Ordering::Equal => {
                    s_iter.next();
                    let (pk, (cell, _)) = c_iter.next().unwrap();
                    Some((Cow::Borrowed(pk), Cow::Borrowed(cell)))
                }
            }
        });

        Box::new(it) as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
    }

    fn range_rev<'a>(
        &'a self,
        start: &PrimaryKey,
        end: &PrimaryKey,
    ) -> impl Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a
    where
        PrimaryKey: 'a,
        Cell: 'a,
    {
        let state = match &self.state {
            State::Ordered(state) => state,
            State::Unordered(_) => panic!(
                "Table::range_rev requires an ordered state backend; configure StateConfig::Ordered"
            ),
        };

        // Same reverse two-pointer merge as `entries_rev`, bounded to
        // `[start, end)`. `BTreeMap::range` is double-ended, so its
        // `.rev()` walks the slice from high to low without buffering.
        let mut s_iter = state.range_rev(start, end).peekable();
        let mut c_iter = self.cache.range(start.clone()..end.clone()).rev().peekable();

        let it = std::iter::from_fn(move || {
            let order = match (s_iter.peek(), c_iter.peek()) {
                (None, None) => return None,
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (Some((s_pk, _)), Some((c_pk, _))) => s_pk.as_ref().cmp(c_pk),
            };

            match order {
                Ordering::Greater => s_iter.next(),
                Ordering::Less => {
                    let (pk, (cell, _)) = c_iter.next().unwrap();
                    Some((Cow::Borrowed(pk), Cow::Borrowed(cell)))
                }
                Ordering::Equal => {
                    s_iter.next();
                    let (pk, (cell, _)) = c_iter.next().unwrap();
                    Some((Cow::Borrowed(pk), Cow::Borrowed(cell)))
                }
            }
        });

        Box::new(it) as Box<dyn Iterator<Item = (Cow<'a, PrimaryKey>, Cow<'a, Cell>)> + 'a>
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
        let early = EventKey::from_delta(&delta("a", 1, hlc(100)));
        let late = EventKey::from_delta(&delta("a", 2, hlc(200)));
        let next_row = EventKey::from_delta(&delta("b", 3, hlc(50)));

        assert!(early < late);
        assert!(late < next_row);
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

        let event = delta("a", 1, hlc(100));
        assert!(table.insert_delta(event.clone()).unwrap());
        assert!(!table.insert_delta(event).unwrap());
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

    fn manual_config() -> TableConfig {
        TableConfig {
            flush: FlushConfig::Manual,
            ..TableConfig::default()
        }
    }

    #[test]
    fn cache_resolves_get_without_materialize() {
        let path = tmp_path("cache_get");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.insert_delta(delta("a", 2, hlc(200))).unwrap();

        let key = PrimaryKey::String("a".into());
        let got = Backend::get(&table, &key).unwrap();
        // Cache hit returns Cow::Borrowed (zero-clone).
        assert!(matches!(got, Cow::Borrowed(_)));
        assert_eq!(got.into_owned().value, Some(Value::Int(2)));
        assert_eq!(table.novel_pending, 1);
    }

    #[test]
    fn size_is_o1_and_matches_entries_count() {
        let path = tmp_path("size_o1");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.insert_delta(delta("b", 2, hlc(110))).unwrap();
        // Second delta on "a" must not double-count.
        table.insert_delta(delta("a", 3, hlc(200))).unwrap();
        table.insert_delta(delta("c", 4, hlc(120))).unwrap();

        assert_eq!(Backend::size(&table), 3);
        assert_eq!(Backend::entries(&table).count(), 3);
        assert_eq!(table.novel_pending, 3);
        assert!(!Backend::is_empty(&table));
    }

    #[test]
    fn entries_yields_cache_over_state() {
        let path = tmp_path("entries_shadow");
        let mut table = Table::create(&path, manual_config()).unwrap();
        // Seed state via materialize.
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.materialize().unwrap();
        assert!(table.cache.is_empty());

        // Now shadow "a" with a newer delta and add a fresh "b".
        table.insert_delta(delta("a", 9, hlc(200))).unwrap();
        table.insert_delta(delta("b", 7, hlc(210))).unwrap();

        let collected: Vec<(PrimaryKey, Cell)> = Backend::entries(&table)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(collected.len(), 2);
        let map: std::collections::HashMap<_, _> = collected
            .into_iter()
            .map(|(k, v)| (k, v.value))
            .collect();
        assert_eq!(map[&PrimaryKey::String("a".into())], Some(Value::Int(9)));
        assert_eq!(map[&PrimaryKey::String("b".into())], Some(Value::Int(7)));
        assert_eq!(Backend::size(&table), 2);
    }

    #[test]
    fn duplicate_delta_does_not_double_apply() {
        let path = tmp_path("dup");
        let mut table = Table::create(&path, manual_config()).unwrap();
        let d = delta("a", 1, hlc(100));
        assert!(table.insert_delta(d.clone()).unwrap());
        assert!(!table.insert_delta(d.clone()).unwrap());
        assert_eq!(table.novel_pending, 1);
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
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.insert_delta(delta("b", 2, hlc(110))).unwrap();
        assert_eq!(table.novel_pending, 2);

        table.materialize().unwrap();
        assert!(table.cache.is_empty());
        assert_eq!(table.novel_pending, 0);
        assert_eq!(table.events.size(), 0);

        // Reads now come straight from state.
        assert_eq!(Backend::size(&table), 2);
        let key = PrimaryKey::String("a".into());
        assert_eq!(
            Backend::get(&table, &key).unwrap().into_owned().value,
            Some(Value::Int(1))
        );
    }

    #[test]
    fn open_rebuilds_cache_from_events() {
        let path = tmp_path("open_cache");
        let config = manual_config();
        {
            let mut table = Table::create(&path, config.clone()).unwrap();
            table.insert_delta(delta("a", 1, hlc(100))).unwrap();
            table.insert_delta(delta("a", 2, hlc(200))).unwrap();
            table.insert_delta(delta("b", 5, hlc(150))).unwrap();
            Backend::sync(&table).unwrap();
            // Intentionally do NOT materialize.
        }

        let table = Table::open(&path, config).unwrap();
        assert_eq!(table.novel_pending, 2);
        assert_eq!(table.cache.len(), 2);
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
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.insert_delta(delta("c", 3, hlc(101))).unwrap();
        table.insert_delta(delta("e", 5, hlc(102))).unwrap();
        table.materialize().unwrap();

        // Cache: shadow "c" + add "b" and "d".
        table.insert_delta(delta("c", 99, hlc(200))).unwrap();
        table.insert_delta(delta("b", 2, hlc(201))).unwrap();
        table.insert_delta(delta("d", 4, hlc(202))).unwrap();

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
                flush: FlushConfig::Manual,
                ..TableConfig::default()
            },
        )
        .unwrap();
        let start = PrimaryKey::String("a".into());
        let end = PrimaryKey::String("z".into());
        let _ = OrderedBackend::range(&table, &start, &end).count();
    }

    #[test]
    fn direct_put_writes_to_state() {
        let path = tmp_path("direct_put");
        let mut table = Table::create(&path, manual_config()).unwrap();
        let key = PrimaryKey::String("a".into());
        Backend::put(&mut table, key.clone(), Cell::dummy(Some(Value::Int(42)))).unwrap();
        assert_eq!(
            Backend::get(&table, &key).unwrap().into_owned().value,
            Some(Value::Int(42))
        );
    }

    #[test]
    fn clear_wipes_state_events_and_cache() {
        let path = tmp_path("clear");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.insert_delta(delta("b", 2, hlc(110))).unwrap();
        table.materialize().unwrap();
        table.insert_delta(delta("c", 3, hlc(120))).unwrap();

        assert!(!Backend::is_empty(&table));
        assert!(table.events.size() > 0);
        assert!(!table.cache.is_empty());

        Backend::clear(&mut table).unwrap();
        assert!(Backend::is_empty(&table));
        assert_eq!(Backend::size(&table), 0);
        assert_eq!(table.events.size(), 0);
        assert!(table.cache.is_empty());
        assert_eq!(table.novel_pending, 0);
    }

    /// Build a table with state rows {a, c, e} and cache rows
    /// {b, c-shadowed, d}. Used by ordered-iteration tests.
    fn shadowed_table(name: &str) -> Table {
        let path = tmp_path(name);
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.insert_delta(delta("c", 3, hlc(101))).unwrap();
        table.insert_delta(delta("e", 5, hlc(102))).unwrap();
        table.materialize().unwrap();
        table.insert_delta(delta("c", 99, hlc(200))).unwrap();
        table.insert_delta(delta("b", 2, hlc(201))).unwrap();
        table.insert_delta(delta("d", 4, hlc(202))).unwrap();
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
        table.insert_delta(delta("m", 1, hlc(100))).unwrap();
        table.materialize().unwrap();
        // Cache key "a" < state key "m" → cache wins.
        table.insert_delta(delta("a", 9, hlc(200))).unwrap();

        let (k, _) = OrderedBackend::first(&table).unwrap();
        assert_eq!(k.into_owned(), pk("a"));
    }

    #[test]
    fn first_cache_wins_on_equal_key() {
        let path = tmp_path("first_eq");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.materialize().unwrap();
        table.insert_delta(delta("a", 99, hlc(200))).unwrap();

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
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.materialize().unwrap();
        // Cache key "z" > state key "a" → cache wins.
        table.insert_delta(delta("z", 9, hlc(200))).unwrap();

        let (k, _) = OrderedBackend::last(&table).unwrap();
        assert_eq!(k.into_owned(), pk("z"));
    }

    #[test]
    fn last_cache_wins_on_equal_key() {
        let path = tmp_path("last_eq");
        let mut table = Table::create(&path, manual_config()).unwrap();
        table.insert_delta(delta("a", 1, hlc(100))).unwrap();
        table.materialize().unwrap();
        table.insert_delta(delta("a", 99, hlc(200))).unwrap();

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
                flush: FlushConfig::Manual,
                ..TableConfig::default()
            },
        )
        .unwrap();
        let _ = OrderedBackend::first(&table);
    }
}
