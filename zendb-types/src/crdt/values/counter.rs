//! PN-Counter — a replicated integer that converges under concurrent increments
//! and decrements.
//!
//! ## Semantics
//!
//! Each replica accumulates positive and negative deltas independently. The
//! value of the counter is Σ(positive) − Σ(negative) across all replicas.
//! Merge takes the element-wise maximum per replica, so concurrent operations
//! from different replicas are never lost.
//!
//! ## Reference
//!
//! Shapiro, Preguiça, Baquero & Zawirski. "A comprehensive study of Convergent
//! and Commutative Replicated Data Types." INRIA RR-7506, 2011. §3.3 (PN-Counter).

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{PeerId, Type};

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct Counter {
    entries: BTreeMap<PeerId, (u64, u64)>,
}

impl Counter {
    /// Compute the current value by summing all peer contributions.
    pub fn value(&self) -> i128 {
        self.entries
            .values()
            .map(|(pos, neg)| i128::from(*pos) - i128::from(*neg))
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum CounterOp {
    Add(i64),
}

#[derive(Debug)]
pub enum CounterError {
    Overflow,
}

impl std::fmt::Display for CounterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CounterError::Overflow => f.write_str("counter component overflow"),
        }
    }
}

impl std::error::Error for CounterError {}

impl Type for Counter {
    type Op = CounterOp;
    type Error = CounterError;

    fn apply(&mut self, op: &CounterOp, stamps: crate::MergeStamps) -> Result<bool, CounterError> {
        let stamps = stamps.incoming;
        let CounterOp::Add(delta) = op;
        if *delta == 0 {
            return Ok(false);
        }
        let peer = stamps.peer_id();
        let entry = self.entries.entry(peer).or_default();
        if *delta > 0 {
            entry.0 = entry
                .0
                .checked_add(delta.unsigned_abs())
                .ok_or(CounterError::Overflow)?;
        } else {
            entry.1 = entry
                .1
                .checked_add(delta.unsigned_abs())
                .ok_or(CounterError::Overflow)?;
        }

        Ok(true)
    }

    fn merge(
        &mut self,
        remote: &Counter,
        _stamps: crate::MergeStamps,
    ) -> Result<bool, CounterError> {
        let mut changed = false;

        for (device, (remote_pos, remote_neg)) in &remote.entries {
            match self.entries.get_mut(device) {
                Some((local_pos, local_neg)) => {
                    if remote_pos > local_pos {
                        *local_pos = *remote_pos;
                        changed = true;
                    }
                    if remote_neg > local_neg {
                        *local_neg = *remote_neg;
                        changed = true;
                    }
                }
                None => {
                    self.entries.insert(*device, (*remote_pos, *remote_neg));
                    changed = true;
                }
            }
        }

        Ok(changed)
    }
}
