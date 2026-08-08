//! Runtime-selected materialized-state backend.

use std::{borrow::Cow, hash::Hash, io, path::Path};

use bincode::{Decode, Encode};

use crate::backend::{
    _traits::{DurableStorage, OrderedReadBackend, ReadBackend, Storage, WriteBackend},
    btree::{BPlusTree, BPlusTreeConfig, BPlusTreeStats},
    keydir::{KeyDir, KeyDirConfig, KeyDirStats},
    skiplist::{SkipList, SkipListConfig, SkipListStats},
};

/// Configures the materialized-state backend.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum StateConfig {
    Ordered(BPlusTreeConfig),
    Unordered(KeyDirConfig),
    InMemory(SkipListConfig),
}

impl Default for StateConfig {
    fn default() -> Self {
        Self::Ordered(BPlusTreeConfig::default())
    }
}

/// Stats from the selected materialized-state backend.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum StateStats {
    Ordered(BPlusTreeStats),
    Unordered(KeyDirStats),
    InMemory(SkipListStats),
}

/// Runtime dispatch between ordered B+ tree, unordered KeyDir, and in-memory
/// SkipList state.
#[expect(
    clippy::large_enum_variant,
    reason = "backend values stay inline to preserve the existing State representation"
)]
pub enum State<K: Ord, V> {
    Ordered {
        backend: BPlusTree<K, V>,
        config: StateConfig,
    },
    Unordered {
        backend: KeyDir<K, V>,
        config: StateConfig,
    },
    InMemory {
        backend: SkipList<K, V>,
        config: StateConfig,
    },
}

impl<K: Ord, V> State<K, V> {
    fn persist(&mut self, barrier: zendb_types::Barrier) -> io::Result<()> {
        match self {
            Self::Ordered { backend, .. } => BPlusTree::persist(backend, barrier),
            Self::Unordered { backend, .. } => KeyDir::persist(backend, barrier),
            Self::InMemory { .. } => Ok(()),
        }
    }
}

impl<K: Ord, V> Drop for State<K, V> {
    fn drop(&mut self) {
        let _ = State::persist(self, zendb_types::Barrier::Flush);
    }
}

impl<K, V> Storage for State<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    type Stats = StateStats;
    type Config = StateConfig;

    fn stats(&self) -> Self::Stats {
        match self {
            Self::Ordered { backend, .. } => StateStats::Ordered(backend.stats()),
            Self::Unordered { backend, .. } => StateStats::Unordered(backend.stats()),
            Self::InMemory { backend, .. } => StateStats::InMemory(backend.stats()),
        }
    }

    fn config(&self) -> Self::Config {
        match self {
            Self::Ordered { config, .. }
            | Self::Unordered { config, .. }
            | Self::InMemory { config, .. } => config.clone(),
        }
    }
}

impl<K, V> DurableStorage for State<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn create(path: &Path, config: Self::Config) -> io::Result<Self> {
        match config {
            StateConfig::Ordered(backend_config) => Ok(Self::Ordered {
                backend: BPlusTree::create(path, backend_config.clone())?,
                config: StateConfig::Ordered(backend_config),
            }),
            StateConfig::Unordered(backend_config) => Ok(Self::Unordered {
                backend: KeyDir::create(path, backend_config.clone())?,
                config: StateConfig::Unordered(backend_config),
            }),
            StateConfig::InMemory(backend_config) => Ok(Self::InMemory {
                backend: SkipList::new(backend_config.clone()),
                config: StateConfig::InMemory(backend_config),
            }),
        }
    }

    fn open(path: &Path, config: Self::Config) -> io::Result<Self> {
        match config {
            StateConfig::Ordered(backend_config) => Ok(Self::Ordered {
                backend: BPlusTree::open(path, backend_config.clone())?,
                config: StateConfig::Ordered(backend_config),
            }),
            StateConfig::Unordered(backend_config) => Ok(Self::Unordered {
                backend: KeyDir::open(path, backend_config.clone())?,
                config: StateConfig::Unordered(backend_config),
            }),
            StateConfig::InMemory(backend_config) => Ok(Self::InMemory {
                backend: SkipList::new(backend_config.clone()),
                config: StateConfig::InMemory(backend_config),
            }),
        }
    }

    fn compact(&mut self) -> io::Result<()> {
        match self {
            Self::Ordered { backend, .. } => backend.compact(),
            Self::Unordered { backend, .. } => backend.compact(),
            Self::InMemory { .. } => Ok(()),
        }
    }

    fn persist(&mut self, barrier: zendb_types::Barrier) -> io::Result<()> {
        State::persist(self, barrier)
    }
}

impl<K, V> ReadBackend<K, V> for State<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn get(&self, key: &K) -> Option<Cow<'_, V>> {
        match self {
            Self::Ordered { backend, .. } => backend.get(key),
            Self::Unordered { backend, .. } => backend.get(key),
            Self::InMemory { backend, .. } => backend.get(key),
        }
    }

    fn contains(&self, key: &K) -> bool {
        match self {
            Self::Ordered { backend, .. } => backend.contains(key),
            Self::Unordered { backend, .. } => backend.contains(key),
            Self::InMemory { backend, .. } => backend.contains(key),
        }
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, K>> + 'a
    where
        K: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => {
                Box::new(backend.keys()) as Box<dyn Iterator<Item = _>>
            }
            Self::Unordered { backend, .. } => Box::new(backend.keys()),
            Self::InMemory { backend, .. } => Box::new(backend.keys()),
        }
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, V>> + 'a
    where
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => {
                Box::new(backend.values()) as Box<dyn Iterator<Item = _>>
            }
            Self::Unordered { backend, .. } => Box::new(backend.values()),
            Self::InMemory { backend, .. } => Box::new(backend.values()),
        }
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => {
                Box::new(backend.entries()) as Box<dyn Iterator<Item = _>>
            }
            Self::Unordered { backend, .. } => Box::new(backend.entries()),
            Self::InMemory { backend, .. } => Box::new(backend.entries()),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Ordered { backend, .. } => backend.size(),
            Self::Unordered { backend, .. } => backend.size(),
            Self::InMemory { backend, .. } => backend.size(),
        }
    }
}

impl<K, V> OrderedReadBackend<K, V> for State<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn range<'a>(
        &'a self,
        start: &'a K,
        end: &'a K,
    ) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => {
                Box::new(backend.range(start, end)) as Box<dyn Iterator<Item = _>>
            }
            Self::Unordered { backend, .. } => {
                let mut rows: Vec<_> = backend
                    .entries()
                    .filter(|(key, _)| key.as_ref() >= start && key.as_ref() < end)
                    .map(|(key, value)| (key.into_owned(), value.into_owned()))
                    .collect();
                rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                Box::new(
                    rows.into_iter()
                        .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value))),
                )
            }
            Self::InMemory { backend, .. } => Box::new(backend.range(start, end)),
        }
    }

    fn first<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => backend.first(),
            Self::Unordered { backend, .. } => backend
                .entries()
                .min_by(|left, right| left.0.as_ref().cmp(right.0.as_ref())),
            Self::InMemory { backend, .. } => backend.first(),
        }
    }

    fn last<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => backend.last(),
            Self::Unordered { backend, .. } => backend
                .entries()
                .max_by(|left, right| left.0.as_ref().cmp(right.0.as_ref())),
            Self::InMemory { backend, .. } => backend.last(),
        }
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => {
                Box::new(backend.entries_rev()) as Box<dyn Iterator<Item = _>>
            }
            Self::Unordered { backend, .. } => {
                let mut rows: Vec<_> = backend
                    .entries()
                    .map(|(key, value)| (key.into_owned(), value.into_owned()))
                    .collect();
                rows.sort_unstable_by(|left, right| right.0.cmp(&left.0));
                Box::new(
                    rows.into_iter()
                        .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value))),
                )
            }
            Self::InMemory { backend, .. } => Box::new(backend.entries_rev()),
        }
    }

    fn range_rev<'a>(
        &'a self,
        start: &'a K,
        end: &'a K,
    ) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => {
                Box::new(backend.range_rev(start, end)) as Box<dyn Iterator<Item = _>>
            }
            Self::Unordered { backend, .. } => {
                let mut rows: Vec<_> = backend
                    .entries()
                    .filter(|(key, _)| key.as_ref() >= start && key.as_ref() < end)
                    .map(|(key, value)| (key.into_owned(), value.into_owned()))
                    .collect();
                rows.sort_unstable_by(|left, right| right.0.cmp(&left.0));
                Box::new(
                    rows.into_iter()
                        .map(|(key, value)| (Cow::Owned(key), Cow::Owned(value))),
                )
            }
            Self::InMemory { backend, .. } => Box::new(backend.range_rev(start, end)),
        }
    }
}

impl<K, V> WriteBackend<K, V> for State<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        match self {
            Self::Ordered { backend, .. } => backend.put(key, value),
            Self::Unordered { backend, .. } => backend.put(key, value),
            Self::InMemory { backend, .. } => backend.put(key, value),
        }
    }

    fn put_if_absent(&mut self, key: &K, value: V) -> io::Result<bool> {
        match self {
            Self::Ordered { backend, .. } => backend.put_if_absent(key, value),
            Self::Unordered { backend, .. } => backend.put_if_absent(key, value),
            Self::InMemory { backend, .. } => backend.put_if_absent(key, value),
        }
    }

    fn replace(&mut self, key: &K, value: V) -> io::Result<Option<Cow<'_, V>>> {
        match self {
            Self::Ordered { backend, .. } => backend.replace(key, value),
            Self::Unordered { backend, .. } => backend.replace(key, value),
            Self::InMemory { backend, .. } => backend.replace(key, value),
        }
    }

    fn bulk_put<I>(&mut self, items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        match self {
            Self::Ordered { backend, .. } => backend.bulk_put(items),
            Self::Unordered { backend, .. } => backend.bulk_put(items),
            Self::InMemory { backend, .. } => backend.bulk_put(items),
        }
    }

    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        match self {
            Self::Ordered { backend, .. } => backend.bulk_put_sorted(sorted),
            Self::Unordered { backend, .. } => backend.bulk_put_sorted(sorted),
            Self::InMemory { backend, .. } => backend.bulk_put_sorted(sorted),
        }
    }

    fn delete(&mut self, key: &K) -> io::Result<bool> {
        match self {
            Self::Ordered { backend, .. } => backend.delete(key),
            Self::Unordered { backend, .. } => backend.delete(key),
            Self::InMemory { backend, .. } => backend.delete(key),
        }
    }

    fn bulk_delete<'a, I>(&mut self, keys: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => backend.bulk_delete(keys),
            Self::Unordered { backend, .. } => backend.bulk_delete(keys),
            Self::InMemory { backend, .. } => backend.bulk_delete(keys),
        }
    }

    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => backend.bulk_delete_sorted(sorted),
            Self::Unordered { backend, .. } => backend.bulk_delete_sorted(sorted),
            Self::InMemory { backend, .. } => backend.bulk_delete_sorted(sorted),
        }
    }

    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        match self {
            Self::Ordered { backend, .. } => backend.update(key, f),
            Self::Unordered { backend, .. } => backend.update(key, f),
            Self::InMemory { backend, .. } => backend.update(key, f),
        }
    }

    fn clear(&mut self) -> io::Result<()> {
        match self {
            Self::Ordered { backend, .. } => backend.clear(),
            Self::Unordered { backend, .. } => backend.clear(),
            Self::InMemory { backend, .. } => backend.clear(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct TmpFile(PathBuf);

    impl Drop for TmpFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    impl std::ops::Deref for TmpFile {
        type Target = std::path::Path;
        fn deref(&self) -> &std::path::Path {
            &self.0
        }
    }

    fn tmp(label: &str) -> TmpFile {
        let unique = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "zendb-state-{label}-{}-{unique}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        TmpFile(path)
    }

    #[test]
    fn in_memory_state_uses_skiplist_without_creating_a_file() {
        let path = std::env::temp_dir().join("zendb-state-in-memory-unused");
        let _ = std::fs::remove_file(&path);

        let mut state =
            State::<u64, u64>::create(&path, StateConfig::InMemory(SkipListConfig::default()))
                .unwrap();
        state.put(2, 20).unwrap();
        state.put(1, 10).unwrap();

        assert!(matches!(state, State::InMemory { .. }));
        assert_eq!(state.keys().map(|key| *key).collect::<Vec<_>>(), vec![1, 2]);
        assert!(!path.exists());
    }

    #[test]
    fn drop_flushes_ordered_state() {
        let path = tmp("ordered-drop");
        {
            let mut state =
                State::<u64, u64>::create(&path, StateConfig::Ordered(BPlusTreeConfig::default()))
                    .unwrap();
            state.put(1, 10).unwrap();
        }

        let state =
            State::<u64, u64>::open(&path, StateConfig::Ordered(BPlusTreeConfig::default()))
                .unwrap();
        assert_eq!(state.get(&1).map(|value| *value), Some(10));
    }

    #[test]
    fn drop_flushes_unordered_state() {
        let path = tmp("unordered-drop");
        {
            let mut state =
                State::<u64, u64>::create(&path, StateConfig::Unordered(KeyDirConfig::default()))
                    .unwrap();
            state.put(1, 10).unwrap();
        }

        let state = State::<u64, u64>::open(&path, StateConfig::Unordered(KeyDirConfig::default()))
            .unwrap();
        assert_eq!(state.get(&1).map(|value| *value), Some(10));
    }

    #[test]
    fn ordered_reads_dispatch_to_ordered_state_backends() {
        fn assert_ordered_read_backend<T: OrderedReadBackend<u64, u64>>() {}
        assert_ordered_read_backend::<State<u64, u64>>();

        let mut state = State::InMemory {
            backend: SkipList::new(SkipListConfig::default()),
            config: StateConfig::InMemory(SkipListConfig::default()),
        };
        state.put(2, 20).unwrap();
        state.put(1, 10).unwrap();
        state.put(3, 30).unwrap();

        assert_eq!(state.first().map(|(key, _)| *key), Some(1));
        assert_eq!(state.last().map(|(key, _)| *key), Some(3));
        assert_eq!(
            state
                .range(&1, &3)
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            vec![(1, 10), (2, 20)]
        );
        assert_eq!(
            state.entries_rev().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
        assert_eq!(
            state
                .range_rev(&1, &3)
                .map(|(key, _)| *key)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
    }

    #[test]
    fn unordered_state_materializes_order_only_for_ordered_reads() {
        let path = tmp("unordered-ordered-read");
        let mut state: State<u64, u64> = State::Unordered {
            backend: KeyDir::create(&path, KeyDirConfig::default()).unwrap(),
            config: StateConfig::Unordered(KeyDirConfig::default()),
        };
        state.put(2, 20).unwrap();
        state.put(1, 10).unwrap();
        state.put(3, 30).unwrap();

        assert_eq!(state.first().map(|(key, _)| *key), Some(1));
        assert_eq!(state.last().map(|(key, _)| *key), Some(3));
        assert_eq!(
            state
                .range(&1, &3)
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            vec![(1, 10), (2, 20)]
        );
        assert_eq!(
            state.entries_rev().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
        assert_eq!(
            state
                .range_rev(&1, &3)
                .map(|(key, _)| *key)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
    }
}
