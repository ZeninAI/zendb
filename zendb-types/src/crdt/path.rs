//! Recursive addressing into the cell tree.
//!
//! A `Path` is a sequence of container segments from a row root to any cell.
//! An empty path refers to the row root itself.

use crate::Segment;

/// A sequence of container segments from a row root to a target cell.
pub type Path = Vec<Segment>;
