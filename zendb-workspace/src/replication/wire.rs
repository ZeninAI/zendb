//! Zenin `/zenin/1` wire protocol: message definitions and length-prefixed
//! bincode framing.
//!
//! The format is a 4-byte little-endian length prefix followed by a bincode
//! payload. Any language with a bincode reader can participate.

use std::io;

use bincode::{Decode, Encode};
use zendb_types::{Event, InstallationId, Multiaddr, PublicKey, WorkspaceId};

// ─── Batching ────────────────────────────────────────────────────────────────

/// Decoded batch of events belonging to one table, sent in order.
///
/// Protocol messages carry the custom serialized form of this value as raw
/// bytes so events are not serialized again after batching.
#[derive(Debug, Clone, Encode, Decode)]
pub struct TableBatch {
    pub table: String,
    pub events: Vec<Event>,
}

pub(super) fn encode_table_batch(batch: &TableBatch) -> io::Result<Vec<u8>> {
    let mut event_bytes = Vec::new();
    for event in &batch.events {
        let encoded = zendb_types::utils::serdes::serialize_to_vec(event)?;
        event_bytes.extend_from_slice(
            &u32::try_from(encoded.len())
                .expect("serialized event exceeds the wire batch size")
                .to_le_bytes(),
        );
        event_bytes.extend_from_slice(&encoded);
    }
    Ok(encode_serialized_table_batch(
        &batch.table,
        batch.events.len(),
        &event_bytes,
    ))
}

pub(super) fn encode_serialized_table_batch(
    table: &str,
    event_count: usize,
    event_bytes: &[u8],
) -> Vec<u8> {
    let table_bytes = table.as_bytes();
    let mut encoded = Vec::with_capacity(12 + table_bytes.len() + event_bytes.len());
    encoded.extend_from_slice(
        &u32::try_from(table_bytes.len())
            .expect("table name exceeds the wire batch size")
            .to_le_bytes(),
    );
    encoded.extend_from_slice(table_bytes);
    encoded.extend_from_slice(
        &u32::try_from(event_count)
            .expect("table batch contains too many events")
            .to_le_bytes(),
    );
    encoded.extend_from_slice(event_bytes);
    encoded
}

pub(super) fn decode_table_batch(bytes: &[u8]) -> io::Result<TableBatch> {
    let mut cursor = 0;
    let table_length = read_u32(bytes, &mut cursor)? as usize;
    let table_end = cursor
        .checked_add(table_length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "table name length overflow"))?;
    let table = String::from_utf8(
        bytes
            .get(cursor..table_end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing table name"))?
            .to_vec(),
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "table name is not UTF-8"))?;
    cursor = table_end;

    let event_count = read_u32(bytes, &mut cursor)? as usize;
    let mut events = Vec::with_capacity(event_count);
    for _ in 0..event_count {
        let event_length = read_u32(bytes, &mut cursor)? as usize;
        let event_end = cursor
            .checked_add(event_length)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "event length overflow"))?;
        let event_bytes = bytes
            .get(cursor..event_end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing event"))?;
        events.push(zendb_types::utils::serdes::deserialize_from(event_bytes)?);
        cursor = event_end;
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes in table batch",
        ));
    }

    Ok(TableBatch { table, events })
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "batch cursor overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing batch length"))?;
    *cursor = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

// ─── Receipt types for anti-entropy ──────────────────────────────────────────

/// Summary of what one table has observed from one author.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ReceiptSummary {
    pub table: String,
    pub author: InstallationId,
    pub max_seen: u64,
    pub missing: Vec<(u64, u64)>,
}

/// A contiguous range of sequences we need from one author on one table.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct EventRange {
    pub table: String,
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
    /// Fire-and-forget event push to every ready installation.
    Push { batches: Vec<Vec<u8>> },
    /// Anti-entropy: local receipt summaries. Peer responds with SummaryResponse.
    Summary { receipts: Vec<ReceiptSummary> },
    /// Anti-entropy: peer's receipt summaries in reply to Summary.
    SummaryResponse { receipts: Vec<ReceiptSummary> },
    /// Request specific event ranges by author and sequence.
    Fetch { ranges: Vec<EventRange> },
    /// Response to a Fetch request.
    FetchResponse { batches: Vec<Vec<u8>> },
    /// Protocol-level error. The sender may close the session after this.
    Error(ProtocolError),
}

// ─── Framing ─────────────────────────────────────────────────────────────────

/// Read one length-prefixed bincode message from an async reader.
pub async fn read_message<R>(io: &mut R) -> std::io::Result<Message>
where
    R: futures::AsyncRead + Unpin + Send,
{
    use futures::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    zendb_types::utils::serdes::deserialize_from(&buf)
}

/// Write one message as a length-prefixed bincode frame.
pub async fn write_message<W>(io: &mut W, msg: &Message) -> std::io::Result<()>
where
    W: futures::AsyncWrite + Unpin + Send,
{
    use futures::AsyncWriteExt;
    let buf = zendb_types::utils::serdes::serialize_to_vec(msg)?;
    io.write_all(&(buf.len() as u32).to_le_bytes()).await?;
    io.write_all(&buf).await?;
    Ok(())
}
