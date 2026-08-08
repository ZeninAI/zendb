# Iteration 0004: Zenin Replication

Status: implemented and reflected from the current source.

This iteration adds the private workspace-owned replication runtime. It
replicates table events between admitted installations while preserving table
sequence and catalog ordering.

## Runtime ownership and transport

Replication is optional and is configured by `ReplicationConfig`. When enabled,
the workspace creates a bounded lifecycle around one dedicated operating-system
thread containing a single-thread Tokio runtime, transport swarm, and event
loop. Workspace code communicates with it through an unbounded notification
channel; local commits continue to work when replication is disabled.

The private `/zenin/1` protocol uses authenticated TCP with Noise/Yamux or
QUIC, with optional mDNS discovery. Transport settings cover listen addresses,
port reuse, and discovery. Shutdown is explicit and awaited before the
workspace's final durability barrier.

## Admission

The handshake carries workspace ID, installation ID, display name, public key,
and addresses. The peer ID is derived from the authenticated key. A peer must
match the workspace and the persisted installation identity.

- Active known installations are admitted.
- Unknown installations are recorded as pending in `_installations` and are
  rejected for the current session.
- Known pending or rejected installations are rejected.
- Active installation permissions are enforced at the producing workspace
  boundary. Once a session is admitted, committed event batches are trusted;
  there is no per-event signature or repeated RBAC evaluation in the network
  path.

## Live delivery and batching

The workspace emits a notification only for a locally applied table change or
an installation change. The replication batcher serializes each event once,
groups events by table, length-delimits them, and flushes at the configured
batch size or optional linger interval. `TableBatch` is the wire unit.

An admitted event is applied with `Table::observe`, so duplicate detection,
per-table causal receipts, CRDT merging, topic append, and projected state
updates all use the same local table path. Novel events are forwarded to the
active mesh.

## Mesh and anti-entropy

Active installations form a bounded mesh maintained with graft/prune messages.
Mesh maintenance settings control target and maximum neighbors and its
maintenance interval. Peers exchange table-scoped receipt summaries containing
author maxima and missing ranges.

Sync requests use table, author, and sequence ranges. Fetches read the table's
retained topic through its reader/consumer boundary, using a recent event cache
when available, and return `TableBatch` data. Catalog changes are synchronized
before `_installations`, followed by application tables, so remote table
declarations and membership become available before dependent data.

## Protocol shape and non-goals

The wire protocol contains handshake, push, graft, prune, summary,
summary-response, fetch, fetch-response, and error messages with a four-byte
little-endian frame length. Protocol errors distinguish workspace identity,
admission, authorization, invalid requests, and internal failures.

The current design intentionally does not include a public join API, a global
causal sequence, Gossipsub, signed per-event envelopes, a separate replication
journal, or a second table write path. The table topic remains the durable
event source.
