//! Delta — the unit of mutation.
//!
//! Every write produces a `Delta`. It contains everything needed to apply the
//! write locally and (if `sync = true`) replicate it to peers.

use crate::{
    codec::encode_string, core::traits::TypedValue, types::atom::AtomValue, Hlc, Op, Path,
    TypeError,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId(pub String);

impl TableId {
    pub fn encode(&self, out: &mut Vec<u8>) {
        encode_string(out, &self.0);
    }
    pub fn decode(bytes: &[u8]) -> Option<(TableId, usize)> {
        crate::codec::decode_string(bytes).map(|(s, n)| (TableId(s), n))
    }
}

#[derive(Debug, Clone)]
pub struct PrimaryKey(pub AtomValue);

#[derive(Debug, Clone)]
pub struct Signature(pub Vec<u8>);

/// The unit produced by every write.
#[derive(Debug, Clone)]
pub struct Delta {
    pub table_id: TableId,
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
    pub hlc: Hlc,
    pub sync: bool,
    pub signature: Signature,
}

impl Delta {
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), TypeError> {
        self.table_id.encode(out);
        self.primary_key
            .0
            .encode(out)
            .map_err(|e| TypeError::DecodeError(e.to_string()))?;
        self.path.encode(out)?;
        self.op.encode(out)?;
        out.extend_from_slice(self.hlc.as_bytes());
        out.push(if self.sync { 0x01 } else { 0x00 });
        crate::codec::encode_varint(out, self.signature.0.len() as u64);
        out.extend_from_slice(&self.signature.0);
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<(Delta, usize), TypeError> {
        let (table_id, mut n) = TableId::decode(bytes)
            .ok_or_else(|| TypeError::DecodeError("truncated table_id".into()))?;
        let (pk, m) =
            AtomValue::decode(&bytes[n..]).map_err(|e| TypeError::DecodeError(e.to_string()))?;
        n += m;
        let (path, m) = Path::decode(&bytes[n..])?;
        n += m;
        let (op, m) = Op::decode(&bytes[n..])?;
        n += m;
        if bytes.len() < n + 11 {
            return Err(TypeError::DecodeError("truncated delta".into()));
        }
        let mut hb = [0u8; 10];
        hb.copy_from_slice(&bytes[n..n + 10]);
        let hlc = Hlc::from_bytes(hb);
        n += 10;
        let sync = bytes[n] != 0;
        n += 1;
        let (sig_len, m) = crate::codec::decode_varint(&bytes[n..])
            .ok_or_else(|| TypeError::DecodeError("truncated signature".into()))?;
        n += m;
        let sig = bytes[n..n + sig_len as usize].to_vec();
        n += sig_len as usize;
        Ok((
            Delta {
                table_id,
                primary_key: PrimaryKey(pk),
                path,
                op,
                hlc,
                sync,
                signature: Signature(sig),
            },
            n,
        ))
    }
}
