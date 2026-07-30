# 007: Tombstone Pruning Context

Status: Proposed

## Problem

Stable receipt proves event distribution, not materialization below local
boundaries. After deleting a tombstone, current bytes cannot distinguish:

```text
the value never existed
the value existed and was deleted
```

A returning old local value can therefore resurrect data that another replica
deleted and forgot. A Merkle mismatch detects different bytes but cannot
reconstruct the missing deletion meaning.

## Current Rule

ZenDB may create snapshots and compact physical storage files, but
`compact_shared_through` performs no destructive CRDT pruning. Complete
per-origin journal segments may be removed only after a retained snapshot
covers their frontier and every admitted device checkpoint is at least that
far; the compacted sequence/HLC checkpoint remains durable. Presence and
wall-clock age do not weaken this rule. Manager removal is the only operation
that ends an offline device's retention claim.

## Required Design

Future pruning must preserve type-specific evidence for:

- row and Record deletion clocks;
- Set removal clocks;
- OR-Set removed add tags;
- List/Text deletions and structural anchors;
- table lifecycle tombstones.

The evidence must merge convergently and participate in snapshots, state
exchange, and Merkle hashes.

## Acceptance Criteria

- stale local state cannot resurrect a compacted deletion;
- new local state is not mistaken for a historical deletion;
- merge remains associative, commutative, and idempotent;
- post-pruning snapshots remain sufficient for bootstrap;
- retained evidence has a bounded or explicitly managed growth policy.
