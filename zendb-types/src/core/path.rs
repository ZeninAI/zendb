//! Path — recursive addressing into the cell tree.
//!
//! A `Path` is a sequence of `PathStep`s from a row root to any cell.
//! An empty path refers to the row root itself.
//!
//! Each `PathStep` carries a `container_tag` so the apply walk knows
//! what type to expect at each depth — this is what enables self-healing
//! when intermediate containers don't yet exist locally.

use crate::{Segment, TypeError, TypeTag};

/// One step in a Path: the expected container type and how to descend.
#[derive(Debug, Clone)]
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

    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), TypeError> {
        out.push(self.container_tag as u8);
        self.segment.encode(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<(PathStep, usize), TypeError> {
        if bytes.is_empty() {
            return Err(TypeError::DecodeError("empty input".into()));
        }
        let tag = TypeTag::from_u8(bytes[0])?;
        let (seg, n) = Segment::decode(&bytes[1..])?;
        Ok((
            PathStep {
                container_tag: tag,
                segment: seg,
            },
            1 + n,
        ))
    }
}

/// A sequence of path steps from a row root to a target cell.
#[derive(Debug, Clone)]
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

    pub fn parent(&self) -> Option<Path> {
        if self.steps.is_empty() {
            return None;
        }
        let mut steps = self.steps.clone();
        steps.pop();
        Some(Path { steps })
    }

    pub fn child(&self, container_tag: TypeTag, segment: Segment) -> Path {
        let mut steps = self.steps.clone();
        steps.push(PathStep::new(container_tag, segment));
        Path { steps }
    }

    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), TypeError> {
        crate::codec::encode_varint(out, self.steps.len() as u64);
        for step in &self.steps {
            step.encode(out)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<(Path, usize), TypeError> {
        let (count, mut n) = crate::codec::decode_varint(bytes)
            .ok_or_else(|| TypeError::DecodeError("truncated path".into()))?;
        let mut steps = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let (step, m) = PathStep::decode(&bytes[n..])?;
            n += m;
            steps.push(step);
        }
        Ok((Path { steps }, n))
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
            .step(
                TypeTag::Record,
                Segment::Record(RecordSegment {
                    field_name: "a".into(),
                }),
            )
            .step(
                TypeTag::Record,
                Segment::Record(RecordSegment {
                    field_name: "b".into(),
                }),
            );
        assert_eq!(path.len(), 2);
        let parent = path.parent().unwrap();
        assert_eq!(parent.len(), 1);
    }
}
