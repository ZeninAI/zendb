# zendb-sync

Portable records used by ZenDB anti-entropy and snapshot transfer.

This crate contains no sockets, storage implementation, policy evaluator, or
broad synchronization trait. The concrete `zendb-engine::Workspace` owns
journal reads, verified append, range orchestration, snapshot installation, and
retry of control dependencies.

Important records are:

- `ReplicatedEvent` and `SyncEnvelope`: signed shared event plus optional
  ticket-admission evidence;
- `WorkspaceSyncSummary`: workspace identity, durable contiguous frontier, and
  offered snapshot boundary;
- `RangeRequest` and `EventBatch`: exact origin ranges and bounded transfers;
- `SyncSnapshotMeta` and `SyncSnapshotChunk`: manifest-bound, independently
  hashed chunk transfer; and
- `SnapshotManifest` and `SnapshotExport`: validated shared-state snapshot.

Only shared-plane events receive an EventIdentity. Local tables and private
Cell overlays consume no origin sequence and cannot create intentional holes.

`VersionVector` remains a maximum-observed utility and is not a compaction
proof. `ContiguousFrontier` proves a durable gap-free prefix. The minimum
published frontier across all admitted devices provides the conservative
boundary used to derive a tombstone-compaction HLC.

See ADRs 003, 006, and 007 under [`.plan/decisions`](../.plan/decisions/README.md).
