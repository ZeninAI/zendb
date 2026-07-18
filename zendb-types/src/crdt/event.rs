//! Event - the unit of CRDT mutation.
//!
//! Every write produces an `Event`. It contains the CRDT mutation. The local
//! routing layer decides whether that mutation enters a local table topic or
//! a signed shared journal.

use bincode::{Decode, Encode};

use crate::{Hlc, Op, Path, PrimaryKey};

pub type TableId = String;

/// The unit produced by every write.
#[derive(Debug, Clone, Encode, Decode)]
pub struct Event {
    pub table_id: TableId,
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
    pub hlc: Hlc,
}
