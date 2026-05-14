//! Delta — the unit of mutation.
//!
//! Every write produces a `Delta`. It contains everything needed to apply the
//! write locally and (if `sync = true`) replicate it to peers.

use crate::{types::atom::AtomValue, Hlc, Op, Path};

/// Identifies a table. Opaque string for now; may become u64 or UUID later.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId(pub String);

/// A primary key value. Must be of a `KEYABLE` type (currently just `AtomValue`).
#[derive(Debug, Clone)]
pub struct PrimaryKey(pub AtomValue);

/// A cryptographic signature. Raw bytes (64 bytes for Ed25519).
/// Verification is handled by `zendb-replication`, not this crate.
#[derive(Debug, Clone)]
pub struct Signature(pub Vec<u8>);

/// The unit produced by every write.
///
/// Contains the target location, operation, timestamp, sync decision,
/// and a cryptographic signature for authenticity.
#[derive(Debug, Clone)]
pub struct Delta {
    /// Which table this write targets.
    pub table_id: TableId,

    /// Primary key of the row.
    pub primary_key: PrimaryKey,

    /// Path from the row root to the target cell.
    pub path: Path,

    /// The operation to apply.
    pub op: Op,

    /// HLC timestamp of this write.
    pub hlc: Hlc,

    /// Whether this write should replicate.
    ///
    /// Always a plain `bool`. Resolved at creation time from `Cell.sync`
    /// (`Option<bool>`) via inheritance, or set explicitly via `.synced()`
    /// or `.local()` on the write API.
    pub sync: bool,

    /// Cryptographic signature over the delta payload.
    pub signature: Signature,
}
