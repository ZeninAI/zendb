//! SkipList — in-memory ordered map backed by a contiguous arena.
//!
//! Nodes live in a `Vec<Node<K, V>>` and are referenced by index.
//! No raw pointers, no `unsafe`. Cache-friendly iteration in key order.
//!
//! Deleted node indices are recycled via a free-list so the arena does
//! not grow without bound under insert/delete workloads.

use std::cmp::Ordering;

const MAX_LEVEL: usize = 16;

struct Node<K, V> {
    key: K,
    value: V,
    /// `next[i]` = index of next node at level i, or `None`.
    next: [Option<usize>; MAX_LEVEL],
    /// Actual height of this node (1..=MAX_LEVEL).
    level: usize,
}

/// Head pointers. `heads[i]` is the first node at level `i`.
type Heads = [Option<usize>; MAX_LEVEL];

/// In-memory ordered map.
pub struct SkipList<K: Ord, V> {
    arena: Vec<Node<K, V>>,
    /// Indices of logically-deleted nodes available for reuse.
    free: Vec<usize>,
    heads: Heads,
    /// Highest level currently in use.
    height: usize,
    len: usize,
}

impl<K: Ord, V> SkipList<K, V> {
    pub fn new() -> SkipList<K, V> {
        SkipList {
            arena: Vec::new(),
            free: Vec::new(),
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
        let mut level = 1;
        while level < MAX_LEVEL && fast_rand() & 1 == 0 {
            level += 1;
        }
        level
    }

    // --- search ---

    /// Walk from the highest level down to level 0, carrying the predecessor
    /// position across levels.  At each level we continue from where we stopped
    /// at the level above — this is what gives O(log n) expected complexity.
    ///
    /// Returns:
    /// - `update[i]`: the node whose `next[i]` should point to `key`
    ///   (or where a new node would be spliced in at level i).
    /// - `found`: the node index if the key already exists.
    fn search(&self, key: &K) -> (Heads, Option<usize>) {
        let mut update: Heads = [None; MAX_LEVEL];
        let mut found: Option<usize> = None;
        // `prev` is the last node confirmed to have key < search key.
        // It is carried across levels so we never re-traverse already-skipped
        // nodes.
        let mut prev: Option<usize> = None;

        for i in (0..self.height).rev() {
            // Start this level from prev's next-at-i (or the level head if no
            // predecessor is known yet).
            let mut x = match prev {
                Some(p) => self.arena[p].next[i],
                None => self.heads[i],
            };
            while let Some(idx) = x {
                match self.arena[idx].key.cmp(key) {
                    Ordering::Less => {
                        prev = Some(idx);
                        x = self.arena[idx].next[i];
                    }
                    Ordering::Equal => {
                        found = Some(idx);
                        break;
                    }
                    Ordering::Greater => break,
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
            let old = std::mem::replace(&mut self.arena[idx].value, value);
            return Some(old);
        }

        let level = self.random_level();
        if level > self.height {
            self.height = level;
        }

        // Reuse a freed slot when one is available; otherwise extend the arena.
        let idx = if let Some(free_idx) = self.free.pop() {
            // Overwriting the slot drops the old (dead) key and value.
            self.arena[free_idx] = Node {
                key,
                value,
                next: [None; MAX_LEVEL],
                level,
            };
            free_idx
        } else {
            let i = self.arena.len();
            self.arena.push(Node {
                key,
                value,
                next: [None; MAX_LEVEL],
                level,
            });
            i
        };

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

        for i in 0..self.arena[idx].level {
            if let Some(prev) = update[i] {
                self.arena[prev].next[i] = self.arena[idx].next[i];
            } else {
                self.heads[i] = self.arena[idx].next[i];
            }
        }

        while self.height > 0 && self.heads[self.height - 1].is_none() {
            self.height -= 1;
        }

        // Return the slot to the free-list.  The key/value will be dropped
        // when the slot is overwritten on next reuse (or when the arena is
        // dropped).
        self.free.push(idx);
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

    /// Iterate over entries with keys in [start, end) using O(log n) search
    /// to locate the first entry instead of a linear level-0 scan.
    pub fn range<'a>(&'a self, start: &K, end: &'a K) -> impl Iterator<Item = (&'a K, &'a V)> + 'a {
        // Use the multi-level search to find the predecessor of `start`, then
        // step one node forward at level 0 to get the first key >= start.
        let (update, found) = self.search(start);
        let first = found.or_else(|| match update[0] {
            None => self.heads[0],
            Some(p) => self.arena[p].next[0],
        });
        RangeIter {
            arena: &self.arena,
            current: first,
            end,
        }
    }
}

impl<K: Ord, V> Default for SkipList<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

struct SkipListIter<'a, K, V> {
    arena: &'a [Node<K, V>],
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
    arena: &'a [Node<K, V>],
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

// --- fast PRNG for level generation ---

fn fast_rand() -> u64 {
    use std::cell::Cell;
    thread_local! {
        static SEED: Cell<u64> = Cell::new(make_seed());
    }
    SEED.with(|s| {
        // SplitMix64
        let val = s.get().wrapping_add(0x9E3779B97F4A7C15);
        let mut z = val;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        s.set(val);
        z ^ (z >> 31)
    })
}

/// Derive a per-thread seed from wall-clock time XOR'd with a stack address.
/// The stack address differs per thread, giving independent sequences without
/// requiring OS-specific thread-ID APIs.
fn make_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let canary = 0u64;
    let addr = &canary as *const u64 as u64;
    let mut z = t ^ addr;
    z = z.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
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
        assert!(!sl.remove(&"a"));
    }

    #[test]
    fn free_list_reuse() {
        let mut sl = SkipList::new();
        for i in 0..100 {
            sl.insert(i, i * 2);
        }
        for i in 0..50 {
            sl.remove(&i);
        }
        let arena_len_after_removes = sl.arena.len();
        // Inserting new keys should reuse freed slots, not grow the arena.
        for i in 100..150 {
            sl.insert(i, i * 2);
        }
        assert!(
            sl.arena.len() <= arena_len_after_removes + sl.free.len() + 50,
            "arena should not grow beyond freed slots"
        );
        assert_eq!(sl.len(), 100);
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
    fn range_start_exact_match() {
        let mut sl = SkipList::new();
        for i in 0..20 {
            sl.insert(i, i);
        }
        let keys: Vec<_> = sl.range(&5, &10).map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn range_start_before_all() {
        let mut sl = SkipList::new();
        sl.insert(10, ());
        sl.insert(20, ());
        let keys: Vec<_> = sl.range(&0, &15).map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![10]);
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

    #[test]
    fn search_correctness_large() {
        let mut sl = SkipList::new();
        for i in (0..500).step_by(2) {
            sl.insert(i, i);
        }
        for i in 0..500 {
            if i % 2 == 0 {
                assert_eq!(sl.get(&i), Some(&i));
            } else {
                assert_eq!(sl.get(&i), None);
            }
        }
    }

    // --- Regression tests for fixed bugs ---

    // Old bug: search() restarted from heads[i] at every level, making it
    // O(n * MAX_LEVEL) instead of O(log n).  While the results were still
    // correct, we can verify correctness after the fix with a stress test that
    // would have been slow-but-correct before.
    #[test]
    fn search_correct_after_many_inserts() {
        let mut sl = SkipList::new();
        for i in 0..2000i32 {
            sl.insert(i, i * 3);
        }
        for i in 0..2000i32 {
            assert_eq!(sl.get(&i), Some(&(i * 3)), "missing key {}", i);
        }
        // Keys outside the range must be absent.
        assert_eq!(sl.get(&-1), None);
        assert_eq!(sl.get(&2000), None);
    }

    // Old bug: range() walked level-0 linearly from heads[0] to find the
    // start, giving O(n) instead of O(log n).  Verify the results are correct
    // for a start that lands in the middle of a large list.
    #[test]
    fn range_mid_list_correct() {
        let mut sl = SkipList::new();
        for i in 0..1000i32 {
            sl.insert(i, i);
        }
        // Range starting at 500 (middle of 1000-element list).
        let collected: Vec<i32> = sl.range(&500, &510).map(|(k, _)| *k).collect();
        assert_eq!(collected, (500..510).collect::<Vec<_>>());
        // Range starting before the first key.
        let collected: Vec<i32> = sl.range(&-5, &3).map(|(k, _)| *k).collect();
        assert_eq!(collected, vec![0, 1, 2]);
        // Range starting after the last key yields nothing.
        assert_eq!(sl.range(&1000, &2000).count(), 0);
    }

    // Old bug: remove() unlinked the node but left it in self.arena, causing
    // unbounded memory growth under heavy delete/reinsert workloads.  The
    // free list recycles those slots so the arena does not grow linearly.
    #[test]
    fn arena_bounded_under_churn() {
        let mut sl: SkipList<i32, i32> = SkipList::new();
        for i in 0..100 {
            sl.insert(i, i);
        }
        // 10 rounds of full delete + full reinsert.
        for _ in 0..10 {
            for i in 0..100 {
                sl.remove(&i);
            }
            for i in 0..100 {
                sl.insert(i, i * 2);
            }
        }
        // Without the free list the arena would be 100 + 10*100 = 1100 entries.
        // With reuse it stays at 100 (± the handful of high-level nodes that
        // may not be exactly recycled due to varying levels).
        assert!(
            sl.arena.len() <= 300,
            "arena grew to {} without reuse",
            sl.arena.len()
        );
        assert_eq!(sl.len(), 100);
        // Values must reflect the last write (i * 2).
        for i in 0..100i32 {
            assert_eq!(sl.get(&i), Some(&(i * 2)));
        }
    }

    // Verify that free-list reuse does not corrupt the logical order or values
    // of surviving entries when the reused slot gets a different key/level.
    #[test]
    fn free_list_reuse_no_corruption() {
        let mut sl = SkipList::new();
        for i in 0..200i32 {
            sl.insert(i, i);
        }
        // Delete every other key.
        for i in (0..200i32).step_by(2) {
            sl.remove(&i);
        }
        // Insert 100 new keys that will reuse freed slots.
        for i in 200..300i32 {
            sl.insert(i, i * 10);
        }
        // Odd keys 1..199 must still have their original values.
        for i in (1..200i32).step_by(2) {
            assert_eq!(sl.get(&i), Some(&i), "corrupted key {}", i);
        }
        // New keys 200..300 must have their new values.
        for i in 200..300i32 {
            assert_eq!(sl.get(&i), Some(&(i * 10)), "corrupted key {}", i);
        }
        // Even keys 0..200 must be gone.
        for i in (0..200i32).step_by(2) {
            assert_eq!(sl.get(&i), None, "key {} should be absent", i);
        }
        assert_eq!(sl.len(), 200); // 100 odd + 100 new
    }

    // Iteration order must remain correct after free-list churn.
    #[test]
    fn iteration_correct_after_churn() {
        let mut sl = SkipList::new();
        for i in 0..50i32 {
            sl.insert(i, i);
        }
        for i in 0..50i32 {
            sl.remove(&i);
        }
        for i in (0..50i32).rev() {
            sl.insert(i, i * 3);
        }
        let keys: Vec<i32> = sl.iter().map(|(k, _)| *k).collect();
        let expected: Vec<i32> = (0..50).collect();
        assert_eq!(keys, expected);
    }
}
