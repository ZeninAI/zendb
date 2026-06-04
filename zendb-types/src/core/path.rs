//! Path — recursive addressing into the cell tree.
//!
//! A `Path` is a sequence of `PathStep`s from a row root to any cell.
//! An empty path refers to the row root itself.
//!
//! Each `PathStep` carries a `container_tag` so the apply walk knows
//! what type to expect at each depth — this is what enables self-healing
//! when intermediate containers don't yet exist locally.

use bincode::{Decode, Encode};

use crate::{Segment, TypeTag};

/// One step in a Path: the expected container type and how to descend.
#[derive(Debug, Clone, Encode, Decode)]
pub struct PathStep {
    pub container_tag: TypeTag,
    pub segment: Segment,
}

impl PathStep {
    pub fn new(container_tag: TypeTag, segment: Segment) -> PathStep {
        PathStep {
            container_tag,
            segment,
        }
    }
}

/// A sequence of path steps from a row root to a target cell.
#[derive(Debug, Clone, Encode, Decode)]
pub struct Path {
    pub steps: Vec<PathStep>,
}

impl Path {
    pub fn new() -> Path {
        Path { steps: Vec::new() }
    }
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    pub fn step(mut self, container_tag: TypeTag, segment: Segment) -> Path {
        self.steps.push(PathStep::new(container_tag, segment));
        self
    }
}

impl Default for Path {
    fn default() -> Path {
        Path::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_path_is_root() {
        let path = Path::new();
        assert!(path.is_empty());
        assert_eq!(path.len(), 0);
    }
}
