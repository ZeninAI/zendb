//! Runtime-selected materialized-state backend.

use std::{borrow::Cow, hash::Hash, io, path::Path};

use bincode::{Decode, Encode};

use crate::core::{
    backend::{Backend, FileBackedBackend, OrderedBackend},
    btree::{BPlusTree, BPlusTreeConfig, BPlusTreeStats},
    keydir::{KeyDir, KeyDirConfig, KeyDirStats},
    skiplist::{SkipList, SkipListConfig, SkipListStats},
};

/// Configures the materialized-state backend.
#[derive(Debug, Clone, Encode, Decode)]
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
#[derive(Debug, Clone, PartialEq)]
pub enum StateStats {
    Ordered(BPlusTreeStats),
    Unordered(KeyDirStats),
    InMemory(SkipListStats),
}

/// Runtime dispatch between ordered B+ tree, unordered KeyDir, and in-memory
/// SkipList state.
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

impl<K, V> FileBackedBackend<K, V> for State<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
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
}

impl<K, V> Backend<K, V> for State<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    type Stats<'a>
        = StateStats
    where
        Self: 'a;
    type Config = StateConfig;

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

    fn compact(&mut self) -> io::Result<()> {
        match self {
            Self::Ordered { backend, .. } => backend.compact(),
            Self::Unordered { backend, .. } => backend.compact(),
            Self::InMemory { backend, .. } => backend.compact(),
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

    fn is_empty(&self) -> bool {
        match self {
            Self::Ordered { backend, .. } => backend.is_empty(),
            Self::Unordered { backend, .. } => backend.is_empty(),
            Self::InMemory { backend, .. } => backend.is_empty(),
        }
    }

    fn stats(&self) -> Self::Stats<'_> {
        match self {
            Self::Ordered { backend, .. } => StateStats::Ordered(backend.stats().clone()),
            Self::Unordered { backend, .. } => StateStats::Unordered(backend.stats().clone()),
            Self::InMemory { backend, .. } => StateStats::InMemory(backend.stats().clone()),
        }
    }

    fn config(&self) -> &Self::Config {
        match self {
            Self::Ordered { config, .. }
            | Self::Unordered { config, .. }
            | Self::InMemory { config, .. } => config,
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Ordered { backend, .. } => backend.flush(),
            Self::Unordered { backend, .. } => backend.flush(),
            Self::InMemory { backend, .. } => backend.flush(),
        }
    }

    fn sync(&mut self) -> io::Result<()> {
        match self {
            Self::Ordered { backend, .. } => backend.sync(),
            Self::Unordered { backend, .. } => backend.sync(),
            Self::InMemory { backend, .. } => backend.sync(),
        }
    }
}

impl<K, V> OrderedBackend<K, V> for State<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
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
            Self::InMemory { backend, .. } => Box::new(backend.range(start, end)),
            Self::Unordered { .. } => {
                panic!("ordered operation requires an ordered state backend")
            }
        }
    }

    fn first<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => backend.first(),
            Self::InMemory { backend, .. } => backend.first(),
            Self::Unordered { .. } => {
                panic!("ordered operation requires an ordered state backend")
            }
        }
    }

    fn last<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        match self {
            Self::Ordered { backend, .. } => backend.last(),
            Self::InMemory { backend, .. } => backend.last(),
            Self::Unordered { .. } => {
                panic!("ordered operation requires an ordered state backend")
            }
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
            Self::InMemory { backend, .. } => Box::new(backend.entries_rev()),
            Self::Unordered { .. } => {
                panic!("ordered operation requires an ordered state backend")
            }
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
            Self::InMemory { backend, .. } => Box::new(backend.range_rev(start, end)),
            Self::Unordered { .. } => {
                panic!("ordered operation requires an ordered state backend")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
