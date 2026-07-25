//! Per-peer receipt windows and missing-sequence range maintenance.

use std::{cmp::Ordering, ops::RangeInclusive};

use bincode::{Decode, Encode};

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserveOutcome {
    New,
    Duplicate,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct ReceiptWindow {
    pub max_seen: u64,
    pub missing: Vec<RangeInclusive<u64>>,
}

impl ReceiptWindow {
    pub fn has_received(&self, sequence: u64) -> bool {
        sequence != 0 && sequence <= self.max_seen && self.missing_index(sequence).is_err()
    }

    pub fn observe(&mut self, sequence: u64) -> Result<ObserveOutcome> {
        if sequence == 0 {
            return Err(Error::InvalidEventSequence);
        }
        if sequence > self.max_seen {
            if self.max_seen < u64::MAX && sequence > self.max_seen + 1 {
                self.missing.push((self.max_seen + 1)..=(sequence - 1));
            }
            self.max_seen = sequence;
            return Ok(ObserveOutcome::New);
        }

        let Ok(index) = self.missing_index(sequence) else {
            return Ok(ObserveOutcome::Duplicate);
        };
        let range = self.missing.remove(index);
        let start = *range.start();
        let end = *range.end();
        if start < sequence {
            self.missing.insert(index, start..=(sequence - 1));
        }
        if sequence < end {
            let index = index + usize::from(start < sequence);
            self.missing.insert(index, (sequence + 1)..=end);
        }
        Ok(ObserveOutcome::New)
    }

    fn missing_index(&self, sequence: u64) -> std::result::Result<usize, usize> {
        self.missing.binary_search_by(|range| {
            if sequence < *range.start() {
                Ordering::Greater
            } else if sequence > *range.end() {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        })
    }
}
