//! Delta — the unit of mutation.
//!
//! Every write produces a `Delta`. It contains everything needed to apply the
//! write locally and (if `sync = true`) replicate it to peers.

use bincode::{Decode, Encode};

use crate::{types::atom::AtomValue, Hlc, Op, Path};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct TableId(pub String);

impl TableId {}

#[derive(Debug, Clone, Encode, Decode)]
pub struct PrimaryKey(pub AtomValue);

#[derive(Debug, Clone, Encode, Decode)]
pub struct Signature(pub Vec<u8>);

/// The unit produced by every write.
#[derive(Debug, Clone, Encode, Decode)]
pub struct Delta {
    pub table_id: TableId,
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
    pub hlc: Hlc,
    pub sync: bool,
    pub signature: Signature,
}
