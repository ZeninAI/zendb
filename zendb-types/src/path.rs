//! Path — recursive addressing into the cell tree.
//!
//! A `Path` is a sequence of `PathStep`s from a row root to any cell.
//! An empty path refers to the row root itself.
//!
//! Each `PathStep` carries a `container_tag` so the apply walk knows
//! what type to expect at each depth — this is what enables self-healing
//! when intermediate containers don't yet exist locally.

use crate::{Segment, TypeTag};

/// One step in a Path: the expected container type and how to descend.
#[derive(Debug, Clone)]
pub struct PathStep {
    /// The expected `TypeTag` of the container at this depth.
    pub container_tag: TypeTag,
    /// How to descend from that container.
    pub segment: Segment,
}

impl PathStep {
    /// Create a new path step.
    pub fn new(container_tag: TypeTag, segment: Segment) -> PathStep {
        PathStep {
            container_tag,
            segment,
        }
    }
}

/// A sequence of path steps from a row root to a target cell.
///
/// An empty path (`steps.is_empty()`) refers to the row root itself.
#[derive(Debug, Clone)]
pub struct Path {
    /// Steps from root to target. Empty = root cell.
    pub steps: Vec<PathStep>,
}

impl Path {
    /// An empty path targeting the row root.
    pub fn new() -> Path {
        Path { steps: Vec::new() }
    }

    /// True if this path refers to the root cell.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Number of steps in the path.
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// Append a step, consuming `self` (builder pattern).
    pub fn step(mut self, container_tag: TypeTag, segment: Segment) -> Path {
        self.steps.push(PathStep::new(container_tag, segment));
        self
    }

    /// Return the parent path (all steps except the last).
    pub fn parent(&self) -> Option<Path> {
        if self.steps.is_empty() {
            return None;
        }
        let mut parent_steps = self.steps.clone();
        parent_steps.pop();
        Some(Path {
            steps: parent_steps,
        })
    }

    /// Return a new path with an additional step appended.
    pub fn child(&self, container_tag: TypeTag, segment: Segment) -> Path {
        let mut steps = self.steps.clone();
        steps.push(PathStep::new(container_tag, segment));
        Path { steps }
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
    use crate::types::record::RecordSegment;

    #[test]
    fn empty_path_is_root() {
        let path = Path::new();
        assert!(path.is_empty());
        assert_eq!(path.len(), 0);
        assert!(path.parent().is_none());
    }

    #[test]
    fn parent_returns_path_without_last_step() {
        let path = Path::new()
            .step(TypeTag::Record, Segment::Record(RecordSegment {
                field_name: "a".into(),
            }))
            .step(TypeTag::Record, Segment::Record(RecordSegment {
                field_name: "b".into(),
            }));
        assert_eq!(path.len(), 2);
        let parent = path.parent().unwrap();
        assert_eq!(parent.len(), 1);
    }
}
