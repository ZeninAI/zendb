# Ad Hoc: Topic Append Performance Improvements

## Context

ZenDB topics are persistent, segmented, append-only logs with one writer and
multiple independent consumers. The topic is used by `Table` as the durable
change log. The causal state is intentionally out of scope for this work.

The current release benchmark measured approximately:

| Surface | Throughput |
|---|---:|
| Topic append | 411k records/s |
| B+ tree put | 2.14M records/s |
| Table insert | 46k records/s |

The topic append path currently:

1. Acquires a thread-local pooled buffer.
2. Encodes the value with bincode.
3. Copies a length prefix into the buffer.
4. Checks segment rotation.
5. Occasionally updates the sparse index under a lock.
6. Calls `File::write_all` for every record.
7. Updates segment, topic, and atomic tail statistics.

The standalone topic benchmark also constructs a `Change`, including payload
allocations, inside the timed loop. This must be separated from the append
measurement before making architectural conclusions.

## Research Basis

- Kafka appends record batches to large log segments and maintains sparse
  offset indexes. It relies heavily on the operating-system page cache rather
  than maintaining a duplicate application page cache.
  - https://kafka.apache.org/41/design/design/
  - https://raw.githubusercontent.com/apache/kafka/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java
- NATS JetStream coalesces writes and supports asynchronous flushing with a
  minimum coalesced size and a maximum flush wait.
  - https://github.com/nats-io/nats-server/blob/main/server/filestore.go
- PostgreSQL WAL uses buffers, background writeback, volume-based thresholds,
  and group commit. WAL segment initialization and recycling reduce rotation
  stalls.
  - https://www.postgresql.org/docs/current/runtime-config-wal.html
  - https://www.postgresql.org/docs/17/wal-configuration.html
- RocksDB distinguishes OS-buffered WAL writes from sync writes and batches
  commits when possible.
  - https://github.com/facebook/rocksdb/wiki/Basic-Operations
- Chronicle Queue uses fixed-size memory-mapped queue blocks and can prepare
  the next cycle before the active one is exhausted.
  - https://github.com/OpenHFT/Chronicle-Queue

## Ranked Recommendations

### 1. Add batched physical appends

Add an `append_batch` API that encodes multiple records into a pooled staging
buffer and writes each segment-sized chunk with one physical file write. Update
the sparse index while building the batch, then publish the new tail only after
the bytes have been written.

Keep the current `append` behavior immediately visible to readers. A batch API
can have an explicit publish boundary, which avoids silently changing the
visibility and failure semantics of existing consumers.

Expected impact: high for bulk and replication workloads.

Complexity: low to medium.

### 2. Add explicit writeback and group-sync policy

Keep `Barrier::Flush` as OS-visible writeback and `Barrier::Sync` as durable
storage synchronization. Add optional byte and time thresholds for writeback,
and make sync happen at batch or transaction boundaries rather than per event.

Do not add a background thread to the core topic initially. ZenDB is synchronous
and single-writer; explicit batching preserves ownership and makes failure
reporting deterministic.

Expected impact: high for workloads that call flush or sync frequently; low for
the current benchmark, which flushes only once after all appends.

Complexity: medium.

### 3. Preallocate and prepare segments

Allow active segments to be preallocated to a configured logical capacity and
prepare the next segment before the current segment reaches its limit. Consider
recycling fully-retired segment files later, subject to consumer retention and
Windows file-delete behavior.

This primarily reduces rotation latency. It should not be expected to explain
the entire average-throughput gap because rotation is relatively infrequent.

Expected impact: medium average impact, high tail-latency impact.

Complexity: medium.

### 4. Prototype an mmap-backed active writer

Implement a separate experimental writer backend using a preallocated mapped
active segment. Serialize directly into the mapped region, then publish the
logical byte tail and record offset after the record is complete.

This must not be implemented as a simple replacement for `File::write_all`.
The mapped format needs a clear committed-record protocol because a
preallocated zero-filled tail cannot be scanned using the current length prefix
alone. Recovery must distinguish committed records from an interrupted write.

The implementation must also define mapping lifetime, segment resize/rotation,
`Flush`/`Sync` behavior, reader visibility, and Windows semantics. `memmap2`
constructors are unsafe because external file mutation or resizing can make a
mapping invalid.

Expected impact: potentially high if profiling shows syscall and buffer-copy
costs dominate.

Complexity: high.

This is an experiment after batching and profiling, not the default first
implementation.

### 5. Keep metadata sparse and readers independent

Retain the current independent reader model, atomic tail publication, and sparse
segment index. Change index insertion from a record-count interval to a byte
interval so large and small records have more predictable seek cost.

The sparse index lock is only touched periodically, so replacing it is not a
priority. If needed later, publish immutable index snapshots with `ArcSwap` or
keep the index writer-owned and publish completed snapshots to readers.

Expected impact: low on append throughput, useful for seek predictability and
reader scalability.

Complexity: low to medium.

## Buffer Pool Decision

Do not add a PostgreSQL-style page buffer pool. The topic is sequential and
append-only, and the operating system already buffers ordinary file writes.
Duplicating the page cache in the process would add memory ownership and
eviction complexity without addressing the primary access pattern.

The existing thread-local `PooledBuf` is the right primitive for temporary
encoding buffers. Add a bounded append-batch buffer on top of it if needed; do
not introduce a general page cache.

## Benchmark Plan Before Implementation

Split the benchmark into independent measurements:

- Prebuilt `Change` values plus `Topic::append`.
- Value construction and allocation only.
- Bincode encoding into a pooled buffer only.
- File write only using already-encoded records.
- Append with sparse indexing disabled or greatly spaced.
- Append with and without segment rotation.
- Append with no barrier, `Flush`, and `Sync`.
- Payload sizes such as 16, 128, and 1024 bytes.
- Ordinary file writer versus mmap writer on the same record format.

Use the benchmark to decide whether the dominant cost is serialization,
system-call frequency, segment management, or durability. Do not infer mmap
benefit from the B+ tree comparison alone because the B+ tree and KeyDir paths
have different encoding and storage mechanics.

## Table Interaction

`Table::insert` appends to the topic before writing materialized state. A
user-space append buffer cannot simply delay the topic write and still preserve
the current crash ordering: state could be updated while the topic record
remains only in process memory.

Therefore:

- `append_batch` is immediately useful for standalone topic, replication, and
  future table-batch workloads.
- Improving one-at-a-time table inserts requires either direct active-segment
  writes, a table-level batch/transaction API, or a deliberate change to table
  failure semantics.
- The causal-state path remains unchanged by this plan.

## Proposed Implementation Order

1. Correct and split the benchmark.
2. Add `append_batch` with pooled staging buffers.
3. Add configurable writeback/group-sync thresholds.
4. Add segment preallocation and proactive rotation.
5. Prototype and benchmark an mmap-backed writer behind a separate internal
   implementation.
6. Adopt mmap only if it wins on representative ZenDB payloads without making
   recovery and cross-platform behavior unacceptable.

Validation for each implementation step is `cargo check --workspace` plus the
existing ignored release benchmark. New tests remain opt-in under the
repository guidelines.
