//! Merge — type-directed cell merging for convergence.
//!
//! `merge_cells` combines two `Cell` values at the same path into a single
//! converged cell. It dispatches to type-specific merge logic via the
//! `Type::merge` trait method.
//!
//! # Merge contracts
//!
//! 1. **Commutative**: `merge_cells(a, b) == merge_cells(b, a)`
//! 2. **Idempotent**: `merge_cells(a, a) == a`
//! 3. **Associative**: `merge_cells(a, merge_cells(b, c)) == merge_cells(merge_cells(a, b), c)`

use crate::Cell;

/// Merge two cells into one.
///
/// - If types differ: LWW on Cell HLC decides the winner.
/// - If types match: delegate to type-specific `Type::merge`.
/// - Sync flags merge with `Some(true)` dominating (OR semantics).
pub fn merge_cells(local: Cell, remote: Cell) -> Cell {
    // Type mismatch — LWW on cell HLC.
    if local.type_tag() != remote.type_tag() {
        if remote.hlc.beats(local.hlc) {
            return remote;
        }
        return local;
    }

    // Types match. Use the max HLC.
    let new_hlc = if remote.hlc.beats(local.hlc) {
        remote.hlc
    } else {
        local.hlc
    };

    // Merge sync: Some(true) dominates everything.
    let new_sync = merge_sync(local.sync, remote.sync);

    // Dispatch to type-specific merge.
    let local_hlc_val = local.hlc;
    let remote_hlc_val = remote.hlc;
    let local_val = local.value.clone();
    let remote_val = remote.value.clone();
    let new_value = crate::merge_dispatch(local_val, local_hlc_val, remote_val, remote_hlc_val)
        .unwrap_or_else(|_| {
            // Fallback: LWW on cell HLC.
            if remote.hlc.beats(local.hlc) {
                remote.value
            } else {
                local.value
            }
        });

    Cell {
        value: new_value,
        hlc: new_hlc,
        sync: new_sync,
    }
}

/// Merge two sync `Option<bool>` values.
///
/// `Some(true)` dominates everything (once synced, always synced).
/// `None` + `Some(false)` = `Some(false)` (explicit local wins over no opinion).
/// `None` + `None` = `None`.
fn merge_sync(local: Option<bool>, remote: Option<bool>) -> Option<bool> {
    match (local, remote) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), _) | (_, Some(false)) => Some(false),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::Type;
    use crate::types::atom::AtomValue;
    use crate::types::record::RecordType;
    use crate::Hlc;
    use crate::Value;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, 1).unwrap()
    }

    #[test]
    fn merge_type_mismatch_lww() {
        let local = Cell::new(Value::Atom(AtomValue::Int(1)), hlc(100), None);
        let remote = Cell::new(Value::Record(RecordType::empty()), hlc(200), None);
        let merged = merge_cells(local, remote);
        // Remote has higher HLC and different type → remote wins.
        assert_eq!(merged.type_tag(), crate::TypeTag::Record);
        assert_eq!(merged.hlc, hlc(200));
    }

    #[test]
    fn merge_sync_true_dominates() {
        assert_eq!(merge_sync(Some(true), Some(false)), Some(true));
        assert_eq!(merge_sync(Some(false), Some(true)), Some(true));
        assert_eq!(merge_sync(None, Some(true)), Some(true));
    }

    #[test]
    fn merge_sync_none_plus_false() {
        assert_eq!(merge_sync(None, Some(false)), Some(false));
        assert_eq!(merge_sync(None, None), None);
    }

    #[test]
    fn merge_atoms_lww() {
        let local = Cell::new(
            Value::Atom(AtomValue::String("local".into())),
            hlc(100),
            None,
        );
        let remote = Cell::new(
            Value::Atom(AtomValue::String("remote".into())),
            hlc(200),
            None,
        );
        let merged = merge_cells(local, remote);
        if let Value::Atom(v) = &merged.value {
            assert_eq!(*v, AtomValue::String("remote".into()));
        } else {
            panic!("expected Atom");
        }
    }
}
