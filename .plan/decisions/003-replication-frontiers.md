# 003: Shared Journal and Replication Frontiers

Status: Accepted

## Scope

This decision defines identities and progress for the shared journal only. The
local-versus-shared routing decision is ADR 006. Tombstone compaction is ADR
007.

## Shared Event Identity

Only a mutation routed to the shared journal receives a distributed event
identity:

```text
EventIdentity
  origin_device_id
  origin_seq
```

`origin_seq` is allocated when, and only when, a mutation enters the shared
journal. Local tables and local-only subtrees have local journal records but no
shared EventIdentity. This is required: if local mutations consumed the same
sequence, every peer would see intentional gaps and could never prove a
contiguous shared prefix.

The origin's shared sequence is strictly increasing, and the HLC assigned to
its shared events is strictly increasing in sequence order.

## Contiguous Frontier

`VersionVector::max_seen` means only that a replica has seen some event at the
largest recorded sequence. It is not safe progress: sequence 10 may arrive
before sequence 9.

Each device maintains a `ContiguousFrontier` for the shared journal:

```text
ContiguousFrontier
  applied_through: map<DeviceId, u64>
```

`applied_through[origin] = n` means the device has applied every shared event
from that origin with sequence `1..=n`. Out-of-order shared events remain
pending until the next missing sequence arrives, then the prefix advances.

Each device checkpoints its frontier in its own nested Device record:

```text
devices.<device_id>.replication_frontier
```

The checkpoint is monotonic, durable shared control state. A device may write
only its own checkpoint. Checkpoint cadence is an implementation choice;
delaying it is conservative because it can only hold the derived frontier back.

Publishing a checkpoint is itself a shared event from that device. To avoid a
permanent one-event lag, the checkpoint value may include the sequence reserved
for that checkpoint event. This is valid only when allocation, durable append,
and profile commit are one serialized operation: after the event is appended,
its own origin prefix is durably contiguous through that sequence.

## Anti-Entropy

Peers exchange their current frontiers during synchronization and request exact
shared-journal ranges:

```text
peer requests origin O, sequences local_frontier[O] + 1 .. remote_frontier[O]
```

If a requester already holds later out-of-order events, it still asks for the
gap beginning at its contiguous prefix plus one.

## Stable Frontier

For every currently admitted Device Cell, read its latest frontier checkpoint.
The Workspace-wide stable frontier is derived, never declared by a special
device:

```text
stable[origin] =
  min(device.replication_frontier.applied_through[origin]
      for every live Device cell)
```

This says that every admitted device has applied every shared event from
`origin` through `stable[origin]`. A long-unreachable member remains in this
minimum until a Manager tombstones membership. That deliberately holds shared
compaction back rather than silently forgetting a member that may return.

## Non-Goals

A frontier is not a causal context, a total-order claim, a liveness signal, or
an authorization fact. It does not describe local-only mutations. ADR 007
uses the stable frontier to derive the scalar watermark consumed by CRDT
compaction.
