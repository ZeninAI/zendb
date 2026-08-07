//! Zenin `/zenin/1` wire protocol: message definitions and length-prefixed
//! bincode framing.
//!
//! The format is a 4-byte little-endian length prefix followed by a bincode
//! payload. Any language with a bincode reader can participate.

use bincode::{Decode, Encode};
use zendb_types::{Event, InstallationId, Multiaddr, PublicKey, WorkspaceId};

// ─── Batching ────────────────────────────────────────────────────────────────

/// A batch of events belonging to one table, sent in order.
#[derive(Debug, Clone, Encode, Decode)]
pub struct TableBatch {
    pub table: String,
    pub events: Vec<Event>,
}

// ─── Receipt types for anti-entropy ──────────────────────────────────────────

/// Summary of what we have observed from one author.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ReceiptSummary {
    pub author: InstallationId,
    pub max_seen: u64,
    pub missing: Vec<(u64, u64)>,
}

/// A contiguous range of sequences we need from one author.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct EventRange {
    pub author: InstallationId,
    pub start: u64,
    pub end: u64,
}

// ─── Protocol errors ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Encode, Decode)]
pub enum ProtocolError {
    /// Peer's public key does not match the one we have on file.
    IdentityMismatch,
    /// Peer is not admitted into this workspace.
    NotAdmitted,
    /// Peer is not authorized for this operation.
    Unauthorized,
    /// Malformed request.
    InvalidRequest,
    /// Internal error on the responder side.
    Internal,
}

// ─── Session messages ────────────────────────────────────────────────────────

/// A single frame exchanged on a `/zenin/1` session.
///
/// Both sides send `Handshake` first. After mutual handshake validation, either
/// side may send any other variant freely.
#[derive(Debug, Clone, Encode, Decode)]
pub enum Message {
    /// Identity announcement. Must be the first frame on any new session.
    Handshake {
        workspace_id: WorkspaceId,
        installation_id: InstallationId,
        display_name: String,
        public_key: PublicKey,
        addresses: Vec<Multiaddr>,
    },
    /// Fire-and-forget event push to mesh neighbours.
    Push { batches: Vec<TableBatch> },
    /// Request inclusion in the sender's forwarding mesh.
    Graft,
    /// Withdraw from the sender's forwarding mesh.
    Prune,
    /// Anti-entropy: local receipt summaries. Peer responds with SummaryResponse.
    Summary { receipts: Vec<ReceiptSummary> },
    /// Anti-entropy: peer's receipt summaries in reply to Summary.
    SummaryResponse { receipts: Vec<ReceiptSummary> },
    /// Request specific event ranges by author and sequence.
    Fetch { ranges: Vec<EventRange>, max_bytes: u32 },
    /// Response to a Fetch request.
    FetchResponse { batches: Vec<TableBatch> },
    /// Protocol-level error. The sender may close the session after this.
    Error(ProtocolError),
}

// ─── Framing ─────────────────────────────────────────────────────────────────

/// Read one length-prefixed bincode message from an async reader.
pub async fn read_message<R>(io: &mut R, max_bytes: usize) -> std::io::Result<Message>
where
    R: futures::AsyncRead + Unpin + Send,
{
    use futures::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds max_bytes",
        ));
    }
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    zendb_types::utils::serdes::deserialize_from(&buf)
}

/// Write one message as a length-prefixed bincode frame.
pub async fn write_message<W>(io: &mut W, msg: &Message, max_bytes: usize) -> std::io::Result<()>
where
    W: futures::AsyncWrite + Unpin + Send,
{
    use futures::AsyncWriteExt;
    let buf = zendb_types::utils::serdes::serialize_to_vec(msg)?;
    if buf.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds max_bytes",
        ));
    }
    io.write_all(&(buf.len() as u32).to_le_bytes()).await?;
    io.write_all(&buf).await?;
    Ok(())
}
