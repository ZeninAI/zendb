//! Entirely in-memory arena-backed skip-list backend.

use std::{borrow::Cow, hash::Hash, io};

use bincode::{Decode, Encode};

use super::traits::{Backend, OrderedBackend, Storage};
use crate::utils::fast_rand;

const MAX_LEVEL: usize = 16;
type Links = [Option<usize>; MAX_LEVEL];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub enum SkipListCapacity {
    #[default]
    Unbounded,
    /// Preallocate space for `max_entries` nodes and reject new keys once all
    /// slots are occupied. Existing keys can still be overwritten.
    Bounded { max_entries: usize },
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct SkipListConfig {
    pub capacity: SkipListCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SkipListStats {
    pub entries: usize,
}

struct Node<K, V> {
    key: K,
    value: Option<V>,
    next: Links,
    prev: Option<usize>,
    level: usize,
}

/// An entirely in-memory ordered backend. Construct it with [`SkipList::new`].
///
/// Bounded instances reserve all node and free-list slots up front and never
/// grow beyond their configured entry count.
pub struct SkipList<K: Ord, V> {
    arena: Vec<Node<K, V>>,
    free: Vec<usize>,
    heads: Links,
    tail: Option<usize>,
    height: usize,
    config: SkipListConfig,
    stats: SkipListStats,
}

impl<K: Ord, V> SkipList<K, V> {
    pub fn new(config: SkipListConfig) -> Self {
        let initial_capacity = match config.capacity {
            SkipListCapacity::Unbounded => 0,
            SkipListCapacity::Bounded { max_entries } => max_entries,
        };
        Self {
            arena: Vec::with_capacity(initial_capacity),
            free: Vec::with_capacity(initial_capacity),
            heads: [None; MAX_LEVEL],
            tail: None,
            height: 0,
            config,
            stats: SkipListStats { entries: 0 },
        }
    }

    fn search(&self, key: &K) -> (Links, Option<usize>) {
        let mut update = [None; MAX_LEVEL];
        let mut previous: Option<usize> = None;
        let mut found = None;
        for level in (0..self.height).rev() {
            let mut current = previous
                .map(|index| self.arena[index].next[level])
                .unwrap_or(self.heads[level]);
            while let Some(index) = current {
                match self.arena[index].key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        previous = Some(index);
                        current = self.arena[index].next[level];
                    }
                    std::cmp::Ordering::Equal => {
                        found = Some(index);
                        break;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
            update[level] = previous;
        }
        (update, found)
    }

    fn alloc(&mut self, key: K, value: V, level: usize) -> io::Result<usize> {
        let node = Node {
            key,
            value: Some(value),
            next: [None; MAX_LEVEL],
            prev: None,
            level,
        };
        if let Some(index) = self.free.pop() {
            self.arena[index] = node;
            return Ok(index);
        }

        if let SkipListCapacity::Bounded { max_entries } = self.config.capacity {
            if self.arena.len() >= max_entries {
                return Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    format!("skip list capacity of {max_entries} entries reached"),
                ));
            }
        }

        self.arena.push(node);
        Ok(self.arena.len() - 1)
    }

    fn insert_new(&mut self, key: K, value: V, update: Links) -> io::Result<usize> {
        let mut level = 1;
        while level < MAX_LEVEL && fast_rand() & 1 == 0 {
            level += 1;
        }
        let index = self.alloc(key, value, level)?;
        self.height = self.height.max(level);
        self.arena[index].prev = update[0];
        for (level, previous) in update.iter().copied().enumerate().take(level) {
            let next = previous
                .map(|previous| self.arena[previous].next[level])
                .unwrap_or(self.heads[level]);
            self.arena[index].next[level] = next;
            if let Some(previous) = previous {
                self.arena[previous].next[level] = Some(index);
            } else {
                self.heads[level] = Some(index);
            }
        }
        if let Some(next) = self.arena[index].next[0] {
            self.arena[next].prev = Some(index);
        } else {
            self.tail = Some(index);
        }
        self.stats.entries += 1;
        Ok(index)
    }

    fn remove_found(&mut self, update: Links, index: usize) {
        let previous = self.arena[index].prev;
        let next = self.arena[index].next[0];
        for (level, predecessor) in update
            .iter()
            .copied()
            .enumerate()
            .take(self.arena[index].level)
        {
            let successor = self.arena[index].next[level];
            if let Some(predecessor) = predecessor {
                self.arena[predecessor].next[level] = successor;
            } else {
                self.heads[level] = successor;
            }
        }
        if let Some(next) = next {
            self.arena[next].prev = previous;
        } else {
            self.tail = previous;
        }
        while self.height > 0 && self.heads[self.height - 1].is_none() {
            self.height -= 1;
        }
        self.arena[index].next = [None; MAX_LEVEL];
        self.arena[index].prev = None;
        self.arena[index].value = None;
        self.free.push(index);
        self.stats.entries -= 1;
    }

    fn first_at_or_after(&self, key: &K) -> Option<usize> {
        let (update, found) = self.search(key);
        found.or_else(|| {
            update[0]
                .map(|previous| self.arena[previous].next[0])
                .unwrap_or(self.heads[0])
        })
    }

    fn last_index(&self) -> Option<usize> {
        self.tail
    }
}

impl<K, V> Storage for SkipList<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    type Stats = SkipListStats;
    type Config = SkipListConfig;

    fn stats(&self) -> Self::Stats {
        self.stats.clone()
    }

    fn config(&self) -> Self::Config {
        self.config.clone()
    }
}

impl<K, V> Backend<K, V> for SkipList<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn get(&self, key: &K) -> Option<Cow<'_, V>> {
        self.search(key).1.map(|index| {
            Cow::Borrowed(
                self.arena[index]
                    .value
                    .as_ref()
                    .expect("live skip-list node has a value"),
            )
        })
    }
    fn contains(&self, key: &K) -> bool {
        self.search(key).1.is_some()
    }
    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let (update, found) = self.search(&key);
        if let Some(index) = found {
            self.arena[index].value = Some(value);
        } else {
            self.insert_new(key, value, update)?;
        }
        Ok(())
    }
    fn put_if_absent(&mut self, key: &K, value: V) -> io::Result<bool> {
        let (update, found) = self.search(key);
        if found.is_some() {
            return Ok(false);
        }
        self.insert_new(key.clone(), value, update)?;
        Ok(true)
    }
    fn replace(&mut self, key: &K, value: V) -> io::Result<Option<Cow<'_, V>>> {
        let (update, found) = self.search(key);
        if let Some(index) = found {
            Ok(Some(Cow::Owned(
                self.arena[index]
                    .value
                    .replace(value)
                    .expect("live skip-list node has a value"),
            )))
        } else {
            self.insert_new(key.clone(), value, update)?;
            Ok(None)
        }
    }
    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let mut update = [None; MAX_LEVEL];
        let mut current = self.heads[0];

        for (key, value) in sorted {
            while let Some(index) = current {
                if self.arena[index].key >= key {
                    break;
                }
                for level in 0..self.arena[index].level {
                    update[level] = Some(index);
                }
                current = self.arena[index].next[0];
            }

            if let Some(index) = current {
                if self.arena[index].key == key {
                    self.arena[index].value = Some(value);
                    continue;
                }
            }

            current = Some(self.insert_new(key, value, update)?);
        }
        Ok(())
    }
    fn delete(&mut self, key: &K) -> io::Result<bool> {
        let (update, found) = self.search(key);
        let Some(index) = found else {
            return Ok(false);
        };
        self.remove_found(update, index);
        Ok(true)
    }
    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let mut removed = 0;
        let mut update = [None; MAX_LEVEL];
        let mut current = self.heads[0];

        for key in sorted {
            while let Some(index) = current {
                if self.arena[index].key >= *key {
                    break;
                }
                for level in 0..self.arena[index].level {
                    update[level] = Some(index);
                }
                current = self.arena[index].next[0];
            }

            let Some(index) = current else {
                break;
            };
            if self.arena[index].key == *key {
                current = self.arena[index].next[0];
                self.remove_found(update, index);
                removed += 1;
            }
        }
        Ok(removed)
    }
    fn update<F>(&mut self, key: &K, update_value: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let (update, found) = self.search(key);
        let current = found.and_then(|index| self.arena[index].value.take());
        match update_value(current) {
            Some(value) => {
                if let Some(index) = found {
                    self.arena[index].value = Some(value);
                } else {
                    self.insert_new(key.clone(), value, update)?;
                }
            }
            None => {
                if let Some(index) = found {
                    self.remove_found(update, index);
                }
            }
        }
        Ok(())
    }
    fn clear(&mut self) -> io::Result<()> {
        self.arena.clear();
        self.free.clear();
        self.heads = [None; MAX_LEVEL];
        self.tail = None;
        self.height = 0;
        self.stats.entries = 0;
        Ok(())
    }
    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, K>> + 'a
    where
        K: 'a,
    {
        Iter::new(self).map(|(key, _)| Cow::Borrowed(key))
    }
    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, V>> + 'a
    where
        V: 'a,
    {
        Iter::new(self).map(|(_, value)| Cow::Borrowed(value))
    }
    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        Iter::new(self).map(|(key, value)| (Cow::Borrowed(key), Cow::Borrowed(value)))
    }
    fn size(&self) -> usize {
        self.stats.entries
    }
}

impl<K, V> OrderedBackend<K, V> for SkipList<K, V>
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
        RangeIter {
            list: self,
            current: self.first_at_or_after(start),
            end,
        }
        .map(|(key, value)| (Cow::Borrowed(key), Cow::Borrowed(value)))
    }
    fn first<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        let node = &self.arena[self.heads[0]?];
        Some((
            Cow::Borrowed(&node.key),
            Cow::Borrowed(
                node.value
                    .as_ref()
                    .expect("live skip-list node has a value"),
            ),
        ))
    }
    fn last<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        let node = &self.arena[self.last_index()?];
        Some((
            Cow::Borrowed(&node.key),
            Cow::Borrowed(
                node.value
                    .as_ref()
                    .expect("live skip-list node has a value"),
            ),
        ))
    }
    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        RevIter {
            list: self,
            current: self.last_index(),
        }
        .map(|(key, value)| (Cow::Borrowed(key), Cow::Borrowed(value)))
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
        let current = self
            .first_at_or_after(end)
            .and_then(|index| self.arena[index].prev)
            .or_else(|| self.tail.filter(|index| self.arena[*index].key < *end));
        RevRangeIter {
            list: self,
            current,
            start,
        }
        .map(|(key, value)| (Cow::Borrowed(key), Cow::Borrowed(value)))
    }
}

struct Iter<'a, K: Ord, V> {
    list: &'a SkipList<K, V>,
    current: Option<usize>,
}
impl<'a, K: Ord, V> Iter<'a, K, V> {
    fn new(list: &'a SkipList<K, V>) -> Self {
        Self {
            list,
            current: list.heads[0],
        }
    }
}
impl<'a, K: Ord, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = &self.list.arena[self.current?];
        self.current = node.next[0];
        Some((
            &node.key,
            node.value
                .as_ref()
                .expect("live skip-list node has a value"),
        ))
    }
}
struct RangeIter<'a, K: Ord, V> {
    list: &'a SkipList<K, V>,
    current: Option<usize>,
    end: &'a K,
}
impl<'a, K: Ord, V> Iterator for RangeIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = &self.list.arena[self.current?];
        if node.key >= *self.end {
            return None;
        }
        self.current = node.next[0];
        Some((
            &node.key,
            node.value
                .as_ref()
                .expect("live skip-list node has a value"),
        ))
    }
}
struct RevIter<'a, K: Ord, V> {
    list: &'a SkipList<K, V>,
    current: Option<usize>,
}
impl<'a, K: Ord, V> Iterator for RevIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = &self.list.arena[self.current?];
        self.current = node.prev;
        Some((
            &node.key,
            node.value
                .as_ref()
                .expect("live skip-list node has a value"),
        ))
    }
}
struct RevRangeIter<'a, K: Ord, V> {
    list: &'a SkipList<K, V>,
    current: Option<usize>,
    start: &'a K,
}
impl<'a, K: Ord, V> Iterator for RevRangeIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = &self.list.arena[self.current?];
        if node.key < *self.start {
            return None;
        }
        self.current = node.prev;
        Some((
            &node.key,
            node.value
                .as_ref()
                .expect("live skip-list node has a value"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static UPDATE_CLONES: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Encode, Decode)]
    struct CloneTracked(u64);

    impl Clone for CloneTracked {
        fn clone(&self) -> Self {
            UPDATE_CLONES.fetch_add(1, Ordering::Relaxed);
            Self(self.0)
        }
    }

    fn list() -> SkipList<u64, u64> {
        SkipList::new(SkipListConfig::default())
    }

    fn bounded(max_entries: usize) -> SkipList<u64, u64> {
        SkipList::new(SkipListConfig {
            capacity: SkipListCapacity::Bounded { max_entries },
        })
    }

    #[test]
    fn inserts_overwrites_and_iterates_in_order() {
        let mut list = list();
        list.put(3, 30).unwrap();
        list.put(1, 10).unwrap();
        list.put(2, 20).unwrap();
        list.put(2, 200).unwrap();

        let entries: Vec<_> = list.entries().map(|(key, value)| (*key, *value)).collect();
        assert_eq!(entries, vec![(1, 10), (2, 200), (3, 30)]);
        assert_eq!(list.stats().entries, 3);
    }

    #[test]
    fn delete_and_reinsert_preserve_links() {
        let mut list = list();
        list.bulk_put([(1, 10), (2, 20), (3, 30), (4, 40)]).unwrap();

        assert!(list.delete(&2).unwrap());
        assert!(list.delete(&4).unwrap());
        assert!(!list.delete(&9).unwrap());
        list.put(2, 200).unwrap();

        let keys: Vec<_> = list.keys().map(|key| *key).collect();
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(*list.get(&2).unwrap(), 200);
    }

    #[test]
    fn ordered_operations_use_half_open_ranges() {
        let mut list = list();
        list.bulk_put([(1, 10), (2, 20), (3, 30), (4, 40)]).unwrap();

        let forward: Vec<_> = list.range(&2, &4).map(|(key, _)| *key).collect();
        let reverse: Vec<_> = list.range_rev(&2, &4).map(|(key, _)| *key).collect();
        let all_reverse: Vec<_> = list.entries_rev().map(|(key, _)| *key).collect();

        assert_eq!(forward, vec![2, 3]);
        assert_eq!(reverse, vec![3, 2]);
        assert_eq!(all_reverse, vec![4, 3, 2, 1]);
        assert_eq!(list.first().map(|(key, _)| *key), Some(1));
        assert_eq!(list.last().map(|(key, _)| *key), Some(4));
    }

    #[test]
    fn bulk_put_sorted_merges_and_overwrites_in_order() {
        let mut list = list();
        list.bulk_put([(1, 10), (3, 30), (5, 50)]).unwrap();

        Backend::bulk_put_sorted(&mut list, [(2, 20), (3, 300), (3, 333), (4, 40), (6, 60)])
            .unwrap();

        let entries: Vec<_> = list.entries().map(|(key, value)| (*key, *value)).collect();
        assert_eq!(
            entries,
            vec![(1, 10), (2, 20), (3, 333), (4, 40), (5, 50), (6, 60)]
        );
        assert_eq!(list.size(), 6);
    }

    #[test]
    fn bulk_delete_sorted_sweeps_hits_and_misses() {
        let mut list = list();
        list.bulk_put_sorted([(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)])
            .unwrap();

        let keys = [0, 2, 4, 4, 7];
        let removed = Backend::bulk_delete_sorted(&mut list, keys.iter()).unwrap();

        assert_eq!(removed, 2);
        let entries: Vec<_> = list.entries().map(|(key, value)| (*key, *value)).collect();
        assert_eq!(entries, vec![(1, 10), (3, 30), (5, 50), (6, 60)]);
        assert_eq!(
            list.entries_rev().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![6, 5, 3, 1]
        );
    }

    #[test]
    fn cached_tail_tracks_mutations() {
        let mut list = list();
        list.bulk_put_sorted([(1, 10), (2, 20), (3, 30)]).unwrap();

        assert_eq!(list.last().map(|(key, _)| *key), Some(3));
        assert!(list.delete(&3).unwrap());
        assert_eq!(list.last().map(|(key, _)| *key), Some(2));

        list.put(4, 40).unwrap();
        assert_eq!(
            list.entries_rev().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![4, 2, 1]
        );

        let keys = [4];
        assert_eq!(list.bulk_delete_sorted(keys.iter()).unwrap(), 1);
        assert_eq!(list.last().map(|(key, _)| *key), Some(2));

        list.clear().unwrap();
        assert_eq!(list.last().map(|(key, _)| *key), None);
        list.put(9, 90).unwrap();
        assert_eq!(list.last().map(|(key, _)| *key), Some(9));
    }

    #[test]
    fn update_existing_moves_value_without_cloning() {
        let mut list: SkipList<u64, CloneTracked> = SkipList::new(SkipListConfig::default());
        list.put(1, CloneTracked(10)).unwrap();

        UPDATE_CLONES.store(0, Ordering::Relaxed);
        list.update(&1, |value| {
            assert_eq!(value, Some(CloneTracked(10)));
            Some(CloneTracked(11))
        })
        .unwrap();

        assert_eq!(UPDATE_CLONES.load(Ordering::Relaxed), 0);
        assert_eq!(*list.get(&1).unwrap(), CloneTracked(11));
    }

    #[test]
    fn bounded_bulk_put_sorted_allows_overwrites_but_rejects_new_overflow() {
        let mut list = bounded(2);
        list.bulk_put_sorted([(1, 10), (2, 20)]).unwrap();

        list.bulk_put_sorted([(1, 100), (2, 200)]).unwrap();
        assert_eq!(*list.get(&1).unwrap(), 100);
        assert_eq!(*list.get(&2).unwrap(), 200);

        let error = list.bulk_put_sorted([(3, 30)]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::StorageFull);
        assert!(!list.contains(&3));
    }

    #[test]
    fn stats_and_config_are_owned_views() {
        let mut list = list();
        list.put(1, 10).unwrap();

        assert_eq!(list.stats().entries, 1);
        assert_eq!(list.config().capacity, SkipListCapacity::Unbounded);
    }

    #[test]
    fn bounded_preallocates_its_slots() {
        let list = bounded(16);

        assert_eq!(list.arena.capacity(), 16);
        assert_eq!(list.free.capacity(), 16);
    }

    #[test]
    fn bounded_rejects_only_new_keys_when_full() {
        let mut list = bounded(2);
        list.put(1, 10).unwrap();
        list.put(2, 20).unwrap();

        list.put(2, 200).unwrap();
        assert!(!list.put_if_absent(&2, 300).unwrap());
        let error = list.put(3, 30).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::StorageFull);
        assert_eq!(*list.get(&2).unwrap(), 200);
        assert!(!list.contains(&3));
    }

    #[test]
    fn bounded_reuses_deleted_slots() {
        let mut list = bounded(2);
        list.put(1, 10).unwrap();
        list.put(2, 20).unwrap();
        list.delete(&1).unwrap();

        list.put(3, 30).unwrap();

        assert_eq!(list.size(), 2);
        assert_eq!(list.arena.len(), 2);
        assert_eq!(list.keys().map(|key| *key).collect::<Vec<_>>(), vec![2, 3]);
    }

    #[test]
    fn bounded_replace_and_update_enforce_capacity() {
        let mut list = bounded(1);
        list.put(1, 10).unwrap();

        assert_eq!(*list.replace(&1, 100).unwrap().unwrap(), 10);
        list.update(&1, |value| value.map(|value| value + 1))
            .unwrap();
        assert_eq!(*list.get(&1).unwrap(), 101);

        assert_eq!(
            list.replace(&2, 20).unwrap_err().kind(),
            io::ErrorKind::StorageFull
        );
        assert_eq!(
            list.update(&2, |_| Some(20)).unwrap_err().kind(),
            io::ErrorKind::StorageFull
        );
        assert!(!list.contains(&2));
    }

    #[test]
    fn zero_capacity_rejects_inserts() {
        let mut list = bounded(0);

        let error = list.put(1, 10).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::StorageFull);
        assert!(list.is_empty());
    }
}
