//! Contracts implemented by ZenDB key/value storage backends.
//!
//! Read and write authority are deliberately separate. Raw storage engines
//! implement both; invariant-preserving facades such as a CRDT `Table`
//! expose reads without exposing writes that bypass their mutation pipeline.

use std::{borrow::Cow, fmt::Debug, hash::Hash, io, path::Path};

use bincode::{Decode, Encode};

pub trait Storage {
    type Stats: Clone + Encode + Decode<()> + Debug;
    type Config: Clone + Debug;

    fn stats(&self) -> Self::Stats;
    fn config(&self) -> Self::Config;
}

/// Lifecycle and writeback operations for filesystem-backed storage.
///
/// This remains independent from `WriteBackend`: append-only logs are durable
/// storage but are not key/value backends.
pub trait DurableStorage: Storage {
    fn create(path: &Path, config: Self::Config) -> io::Result<Self>
    where
        Self: Sized;

    fn open(path: &Path, config: Self::Config) -> io::Result<Self>
    where
        Self: Sized;

    fn compact(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
}

/// Read-only key/value access shared by raw backends and higher-level tables.
pub trait ReadBackend<K, V>: Storage
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn get(&self, key: &K) -> Option<Cow<'_, V>>;

    fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, K>> + 'a
    where
        K: 'a;

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, V>> + 'a
    where
        V: 'a;

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a;

    fn size(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

/// Raw key/value mutation. A writer is always readable, so default conditional
/// and read-modify-write operations use the single inherited read vocabulary.
pub trait WriteBackend<K, V>: ReadBackend<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn put(&mut self, key: K, value: V) -> io::Result<()>;

    fn put_if_absent(&mut self, key: &K, value: V) -> io::Result<bool> {
        if self.contains(key) {
            Ok(false)
        } else {
            self.put(key.clone(), value)?;
            Ok(true)
        }
    }

    fn replace(&mut self, key: &K, value: V) -> io::Result<Option<Cow<'_, V>>> {
        let previous = self.get(key).map(|value| Cow::Owned(value.into_owned()));
        self.put(key.clone(), value)?;
        Ok(previous)
    }

    fn bulk_put<I>(&mut self, items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        for (key, value) in items {
            self.put(key, value)?;
        }
        Ok(())
    }

    /// Insert a key-sorted input. Unordered backends may use the default path;
    /// ordered backends may override it with a bulk-load optimization.
    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        self.bulk_put(sorted)
    }

    fn delete(&mut self, key: &K) -> io::Result<bool>;

    fn bulk_delete<'a, I>(&mut self, keys: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let mut removed = 0;
        for key in keys {
            if self.delete(key)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        self.bulk_delete(sorted)
    }

    fn update<F>(&mut self, key: &K, update: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let current = self.get(key).map(Cow::into_owned);
        let existed = current.is_some();
        match (existed, update(current)) {
            (_, Some(value)) => self.put(key.clone(), value),
            (true, None) => {
                self.delete(key)?;
                Ok(())
            }
            (false, None) => Ok(()),
        }
    }

    fn clear(&mut self) -> io::Result<()>;
}

/// Ordered reads for backends with native ordering or a documented ordered
/// fallback. The observed order must remain stable for one key encoding.
pub trait OrderedReadBackend<K, V>: ReadBackend<K, V>
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
        V: 'a;

    fn first<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        self.entries().next()
    }

    fn last<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        self.entries().last()
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a;

    fn range_rev<'a>(
        &'a self,
        start: &'a K,
        end: &'a K,
    ) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a;
}
