//! Event — the unit of mutation.
//!
//! Every write produces an `Event`. It contains the CRDT mutation. The local
//! routing layer decides whether that mutation enters a private or shared
//! journal; callers must not treat `sync` as an authorization decision.

use bincode::{Decode, Encode};

use crate::{Hlc, Op, Path, PrimaryKey};

pub type TableId = String;
pub type Signature = Vec<u8>;

/// The unit produced by every write.
#[derive(Debug, Clone, Encode, Decode)]
pub struct Event {
    pub table_id: TableId,
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
    pub hlc: Hlc,
    /// Transitional local routing hint. Shared identity allocation happens
    /// only after the router has resolved the effective Cell boundary.
    pub sync: bool,
    pub signature: Signature,
}
