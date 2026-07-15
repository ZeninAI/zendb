# 007: Tombstone Compaction Watermarks

Status: Accepted

## Scope

This decision defines when a replica may discard CRDT tombstone metadata from
the **shared plane**. It does not define device presence, routing boundaries,
or storage-file reclamation. Those concerns are covered by ADR 002, ADR 006,
and the storage design respectively.

## Context

Deletion is not immediately removable state in a CRDT. A Record field
tombstone, Set removal, OR-Set remove tag, and RGA list deletion may be needed
to prevent an older event from restoring deleted data. Retaining all of that
metadata forever makes a long-lived Workspace grow without bound.

Compaction is safe only after every current Workspace device has durably
observed the relevant shared history. A device's most recently seen event, its
wall clock, a heartbeat, and the current maximum HLC do not prove this. They
may skip a causal hole.

## Decision

Shared event history has a per-origin contiguous frontier as defined by ADR
003. A device advances its published frontier only after it has durably
installed every shared event for that origin through the reported sequence.

For each known shared origin `o`, a replica derives:

```text
stable_seq[o] = min(device.replication_frontier[o] for every live Device Cell)
```

If any live device has not reported a frontier for `o`, its value is zero. A
device that is merely unreachable remains in this calculation until a Manager
tombstones its Device Cell. This deliberately favors safety over reclaiming
space while an offline member still has a right to return.

Because shared events from an origin have strictly increasing HLCs, the
corresponding compaction point is:

```text
shared_watermark = min(event_hlc(o, stable_seq[o]) for every known origin)
```

There is no usable shared watermark until every participating origin has a
non-zero stable sequence. The implementation may retain extra history; it must
not compact beyond this value.

The scalar HLC watermark deliberately matches the existing recursive
`Type::compact(Hlc)` interface. The frontier service proves the watermark; CRDT
values need only apply it.

## CRDT Compaction Contract

A CRDT may remove metadata only when doing so cannot change the resolved value
for any event at or below the supplied watermark:

```text
Record
  remove a tombstoned field Cell when its structural HLC <= watermark,
  then compact the live child recursively.

Set
  remove a dead element when its removal clock <= watermark.

OR-Set
  remove an add-tag/removal pair only when both clocks <= watermark.

List and Text
  remove a tombstoned RGA element only when its deletion is stable and no
  retained element or retained operation still needs it as an insertion anchor.
```

A row-root or table-level tombstone needs the same rule. A future compaction
coordinator must remove a root Cell tombstone only when its HLC is at or below
the shared watermark; recursive `Value::compact` alone cannot do that job.

## Local State

Local tables and device-private overlays from ADR 006 are absent from the
shared journal and do not participate in `replication_frontier`. They use a
separate local retention policy based on their own durable local journal. A
shared watermark must never compact local data, and local progress must never
raise a shared watermark.

## Snapshots and Journal Pruning

State compaction and journal pruning are distinct operations. Before dropping
shared journal history through a watermark, a replica needs a durable snapshot
that includes every event through that boundary. A bootstrap peer must then
offer either:

1. a snapshot at or after the requested boundary plus the later journal tail;
   or
2. retained events from before the request.

This lets a long-offline admitted device recover without relying on tombstones
that have already been compacted.

## Consequences

`VersionVector` is a maximum-observed summary, not a compaction proof. The
implementation therefore uses durable per-origin gap tracking, published
contiguous frontiers, and snapshot boundary metadata before invoking shared
CRDT compaction.

Compaction is conservative by design. Removing an offline member changes the
watermark obligation, so that action is an intentional Manager membership
decision rather than an automatic liveness timeout.

The first implementation retains the shared journal even after materialized
state compaction. This is intentionally conservative: snapshot creation and
recursive tombstone compaction are implemented, while physical history pruning
is deferred until crash-safe snapshot retention and fallback coverage can be
proven across every supported storage failure.
