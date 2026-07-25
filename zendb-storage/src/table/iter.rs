//! Lazy merge iteration over materialized state and the pending table cache.

use std::{borrow::Cow, cmp::Ordering, iter::Peekable};

#[derive(Clone, Copy)]
pub(super) enum IterationOrder {
    Ascending,
    Descending,
}

pub(super) struct MergedEntries<S, C>
where
    S: Iterator,
    C: Iterator<Item = S::Item>,
{
    state: Peekable<S>,
    cache: Peekable<C>,
    order: IterationOrder,
}

impl<S, C> MergedEntries<S, C>
where
    S: Iterator,
    C: Iterator<Item = S::Item>,
{
    pub(super) fn new(state: S, cache: C, order: IterationOrder) -> Self {
        Self {
            state: state.peekable(),
            cache: cache.peekable(),
            order,
        }
    }
}

impl<'a, K, V, S, C> Iterator for MergedEntries<S, C>
where
    K: Ord + Clone + 'a,
    V: Clone + 'a,
    S: Iterator<Item = (Cow<'a, K>, Cow<'a, V>)>,
    C: Iterator<Item = (Cow<'a, K>, Cow<'a, V>)>,
{
    type Item = (Cow<'a, K>, Cow<'a, V>);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.state.peek(), self.cache.peek()) {
            (None, None) => None,
            (Some(_), None) => self.state.next(),
            (None, Some(_)) => self.cache.next(),
            (Some((state_key, _)), Some((cache_key, _))) => {
                let ordering = state_key.as_ref().cmp(cache_key.as_ref());
                let ordering = match self.order {
                    IterationOrder::Ascending => ordering,
                    IterationOrder::Descending => ordering.reverse(),
                };
                match ordering {
                    Ordering::Less => self.state.next(),
                    Ordering::Greater => self.cache.next(),
                    Ordering::Equal => {
                        self.state.next();
                        self.cache.next()
                    }
                }
            }
        }
    }
}
