//! SkipList — in-memory ordered map backed by a contiguous arena.
//!
//! Nodes live in a `Vec<Node<K, V>>` and are referenced by index.
//! No raw pointers, no `unsafe`. Cache-friendly iteration in key order.

use std::cmp::Ordering;

const MAX_LEVEL: usize = 16;

struct Node<K, V> {
    key: K,
    value: V,
    /// `next[i]` = index of next node at level i, or `None`.
    next: [Option<usize>; MAX_LEVEL],
    /// Actual height of this node (1..MAX_LEVEL).
    level: usize,
}

/// Head pointers. `heads[i]` is the first node at level `i`.
type Heads = [Option<usize>; MAX_LEVEL];

/// In-memory ordered map.
pub struct SkipList<K: Ord, V> {
    arena: Vec<Node<K, V>>,
    heads: Heads,
    /// Highest level currently in use.
    height: usize,
    len: usize,
}

impl<K: Ord, V> SkipList<K, V> {
    pub fn new() -> SkipList<K, V> {
        SkipList {
            arena: Vec::new(),
            heads: [None; MAX_LEVEL],
            height: 0,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    // --- random level ---

    fn random_level(&self) -> usize {
        // 50% chance per level. Level is always >= 1.
        let mut level = 1;
        while level < MAX_LEVEL && fast_rand() & 1 == 0 {
            level += 1;
        }
        level
    }

    // --- search ---

    /// Internal: walk from heads down to find position of `key`.
    /// Returns `update[i]` = the node whose `next[i]` should point to the
    /// target (or where a new node would be inserted).
    /// Also returns the found node index if key exists.
    fn search(&self, key: &K) -> (Heads, Option<usize>) {
        let mut update: Heads = [None; MAX_LEVEL];
        let mut found: Option<usize> = None;

        for i in (0..self.height).rev() {
            // Start from the highest level's head or from the node
            // we descended from at the level above.
            let start = if i == self.height - 1 || found.is_none() {
                self.heads[i]
            } else {
                self.arena[found.unwrap()].next[i]
            };
            let mut prev: Option<usize> = None;
            let mut x = start;

            while let Some(idx) = x {
                match self.arena[idx].key.cmp(key) {
                    Ordering::Less => {
                        prev = Some(idx);
                        x = self.arena[idx].next[i];
                    }
                    Ordering::Equal => {
                        found = Some(idx);
                        x = None;
                    }
                    Ordering::Greater => {
                        x = None;
                    }
                }
            }
            update[i] = prev;
        }

        (update, found)
    }

    // --- public API ---

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.is_empty() {
            return None;
        }
        let (_update, found) = self.search(key);
        found.map(|idx| &self.arena[idx].value)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let (update, found) = self.search(&key);

        if let Some(idx) = found {
            // Key exists — replace in place.
            let old = std::mem::replace(&mut self.arena[idx].value, value);
            return Some(old);
        }

        // New key — allocate node.
        let level = self.random_level();
        if level > self.height {
            self.height = level;
        }

        let idx = self.arena.len();
        self.arena.push(Node {
            key,
            value,
            next: [None; MAX_LEVEL],
            level,
        });

        // Splice into each level.
        for i in 0..level {
            if let Some(prev) = update[i] {
                self.arena[idx].next[i] = self.arena[prev].next[i];
                self.arena[prev].next[i] = Some(idx);
            } else {
                self.arena[idx].next[i] = self.heads[i];
                self.heads[i] = Some(idx);
            }
        }

        self.len += 1;
        None
    }

    pub fn remove(&mut self, key: &K) -> bool {
        if self.is_empty() {
            return false;
        }
        let (update, found) = self.search(key);
        let Some(idx) = found else { return false };

        // Unlink from each level.
        for i in 0..self.arena[idx].level {
            if let Some(prev) = update[i] {
                self.arena[prev].next[i] = self.arena[idx].next[i];
            } else {
                self.heads[i] = self.arena[idx].next[i];
            }
        }

        // Shrink height if needed.
        while self.height > 0 && self.heads[self.height - 1].is_none() {
            self.height -= 1;
        }

        self.len -= 1;
        true
    }

    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    // --- iteration ---

    /// Iterate over all entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        SkipListIter {
            arena: &self.arena,
            current: self.heads[0],
        }
    }

    /// Iterate over entries with keys in [start, end) (inclusive start, exclusive end).
    pub fn range<'a>(&'a self, start: &K, end: &'a K) -> impl Iterator<Item = (&'a K, &'a V)> + 'a {
        // Walk to the first key >= start.
        let mut x = self.heads[0];
        while let Some(idx) = x {
            if self.arena[idx].key >= *start {
                break;
            }
            x = self.arena[idx].next[0];
        }
        RangeIter {
            arena: &self.arena,
            current: x,
            end,
        }
    }
}

struct SkipListIter<'a, K, V> {
    arena: &'a Vec<Node<K, V>>,
    current: Option<usize>,
}

impl<'a, K, V> Iterator for SkipListIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        self.current = node.next[0];
        Some((&node.key, &node.value))
    }
}

struct RangeIter<'a, K: Ord, V> {
    arena: &'a Vec<Node<K, V>>,
    current: Option<usize>,
    end: &'a K,
}

impl<'a, K: Ord, V> Iterator for RangeIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        if node.key >= *self.end {
            return None;
        }
        self.current = node.next[0];
        Some((&node.key, &node.value))
    }
}

// --- cheap random for level generation ---

fn fast_rand() -> u64 {
    use std::cell::Cell;
    thread_local! { static SEED: Cell<u64> = const { Cell::new(0xDEAD_BEEF_CAFE_BABE) }; }
    SEED.with(|s| {
        let val = s.get();
        // SplitMix64
        let val = val.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = val;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        s.set(val);
        z ^ (z >> 31)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut sl = SkipList::new();
        sl.insert("a", 1);
        sl.insert("b", 2);
        assert_eq!(sl.get(&"a"), Some(&1));
        assert_eq!(sl.get(&"b"), Some(&2));
        assert_eq!(sl.get(&"c"), None);
    }

    #[test]
    fn insert_replace() {
        let mut sl = SkipList::new();
        assert_eq!(sl.insert("x", 1), None);
        assert_eq!(sl.insert("x", 2), Some(1));
        assert_eq!(sl.get(&"x"), Some(&2));
        assert_eq!(sl.len(), 1);
    }

    #[test]
    fn remove() {
        let mut sl = SkipList::new();
        sl.insert("a", 1);
        sl.insert("b", 2);
        assert!(sl.remove(&"a"));
        assert_eq!(sl.get(&"a"), None);
        assert_eq!(sl.len(), 1);
        assert!(!sl.remove(&"a")); // already removed
    }

    #[test]
    fn iteration_ordered() {
        let mut sl = SkipList::new();
        sl.insert("c", 3);
        sl.insert("a", 1);
        sl.insert("b", 2);
        let keys: Vec<_> = sl.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn range() {
        let mut sl = SkipList::new();
        for k in ["a", "b", "c", "d", "e"] {
            sl.insert(k, ());
        }
        let keys: Vec<_> = sl.range(&"b", &"d").map(|(k, _)| *k).collect();
        assert_eq!(keys, vec!["b", "c"]);
    }

    #[test]
    fn large_insert() {
        let mut sl = SkipList::new();
        for i in 0..1000 {
            sl.insert(i, i * 2);
        }
        assert_eq!(sl.len(), 1000);
        assert_eq!(sl.get(&500), Some(&1000));
    }

    #[test]
    fn empty_ops() {
        let sl: SkipList<i32, i32> = SkipList::new();
        assert!(sl.is_empty());
        assert_eq!(sl.get(&1), None);
        assert_eq!(sl.iter().count(), 0);
    }
}
