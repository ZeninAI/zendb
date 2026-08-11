# Ad Hoc

### Existing follow-ups
- Manage physical table-directory cleanup when tables are deleted.
- Review storage iteration methods for maximum performance.
- Define handling for table configuration changes that require migration.
- Trace `Drop` paths to ensure in-memory data is flushed before resources are released.
- Harden logging and error handling.
- Add timestamp-based table retrieval for anti-entropy operations.
- Add network-level compression when replication expands beyond LAN use.
- Define installation-catalog behavior when the current installation is removed.
- Store the before/after table change for the affected cell path, not the whole object.
- Evaluate the EG-Walker algorithm and Loro for text editing.
- Review `Arc` strong-reference counts and ownership across table/state close and delete paths.
- Define post-commit causal and system-projection failure handling; unapplied events must never enter the topic.
- Add replication configuration for enabling and disabling protocol features.
- Add a `ZenDbValue` derive: CRDT types declare native Rust conversions, while the registry bridges them into `Value` for arbitrary application structs.

### Dependency and serialization
- Keep direct dependencies in root workspace declarations and upgrade compatible versions without forcing incompatible major transitive updates.
- Replace unmaintained bincode deliberately; evaluate wincode first for compatibility, postcard for new compact wire formats, and rkyv for measured zero-copy read paths.
- Preserve event, topic, backend, state, and replication format invariants, including the 28-byte stamp prefix and framing rules.
- Add a serializer abstraction, representative byte corpus, benchmarks, recovery checks, and target builds before removing bincode.

### Topic append performance
- Split benchmarks so value construction, encoding, indexing, file writes, rotation, and barrier costs are measured independently.
- Add pooled-buffer `append_batch` with an explicit publish boundary for topic, replication, and future table-batch workloads.
- Add configurable writeback and group-sync thresholds while keeping `Flush` and `Sync` semantics explicit and deterministic.
- Preallocate active segments and prepare the next segment to reduce rotation latency; consider recycling only after retention behavior is defined.
- Prototype an mmap-backed active writer only after profiling; define committed-record recovery, rotation, durability, visibility, and Windows behavior.
- Keep sparse metadata and independent readers; index by byte interval if seek predictability needs improvement, without adding a general page cache.
- Preserve table crash ordering: delayed topic writes require a table batch/transaction design or an explicit change to failure semantics.
