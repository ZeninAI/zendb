# 008: Operator Reconciliation and Leases

Status: Proposed

## Scope

This decision concerns declarative distributed operators: how a device decides
to run one, how it hands work over, and what a lease can honestly guarantee.
It does not define device liveness, membership, or CRDT compaction.

## Context

An `OperatorSpec` is desired Workspace state, not an instruction sent to one
particular host. Every device that receives the spec runs a local reconciler.
It compares the desired specification with its own capabilities and roles and
may attempt to host the operator. There is no central scheduler.

Two disconnected devices can each believe that a previous holder has stopped.
Therefore a replicated lease cannot guarantee that only one process is running
at every instant. Treating it as a distributed lock would make operator output
unsafe under partition.

## Decision

An OperatorSpec declares the portable requirements for its worker:

```text
OperatorSpec
  operator_id
  desired_state: Running | Stopped
  required_capabilities: set of labels
  input_tables and output declaration
  effect requirements: Read | WriteShared | LocalOnly
  runtime configuration
```

Only a device with `Dispatcher` may create, change, enable, or stop an
OperatorSpec. `Dispatcher` controls desired work; it does not by itself permit
the device to write ordinary shared data.

A local reconciler is eligible to host a Running spec only when its own live
Device record:

1. advertises every required capability;
2. has access to all declared inputs (all members currently receive shared
   data, so this is primarily a future validation point rather than a routing
   rule); and
3. has `Contributor` when the declared effects publish shared data.

The worker's replicated events are authorized as events from the hosting
DeviceId. An operator has no separate principal, role set, or authority. Its
declared effect requirements constrain what the local runner may attempt; they
never elevate the host device's Workspace permissions.

## Lease Record

The Workspace stores one lease Cell per operator:

```text
OperatorLease
  holder_device_id
  fence: Hlc
  expires_at: Hlc
```

The Cell uses normal deterministic CRDT resolution. A candidate may claim a
missing or locally expired lease by writing a lease with a strictly greater
`fence`. The holder renews before its own view of `expires_at`. It stops making
new lease-scoped output when it sees another winning holder or cannot renew in
time.

`fence` is a monotonically ordered run generation, normally the HLC of the
successful claim. It is not a key, a role, or evidence that every other device
has stopped.

The lease expiry is a local failure-detector input. Clock error, delayed
replication, and partitions can produce overlapping workers. Device presence
from ADR 002 may rank candidates but must not turn a lease into a hard lock.

## Fenced Output

An operator that publishes shared derived state must make that state
generation-aware. Each output is attributed to:

```text
operator_id + fence
```

Readers expose only the output for the resolved winning fence. Older output is
retained until it is safe to compact, then discarded as obsolete derived data.
This makes a stale worker's late publication invisible after a handoff rather
than attempting to retract arbitrary CRDT writes.

The runtime must not use a lease as permission to mutate arbitrary user-owned
tables. Such writes need an explicit application-level idempotency and conflict
model. External side effects are outside ZenDB's consistency boundary and must
use the external system's idempotency key or fencing mechanism.

## Reconciliation Sequence

1. A Dispatcher writes or changes an OperatorSpec.
2. Every local reconciler evaluates the spec against its own Device record.
3. An eligible device observes a missing or expired lease and writes a claim.
4. The device whose claim is currently resolved as the winner starts or keeps a
   worker and periodically renews it.
5. The worker publishes only generation-attributed output while its own lease
   view still names its DeviceId and fence.
6. A new winning claim causes the previous holder to stop on observation; a
   partition may delay that observation, which fenced output contains.

## Consequences

Leases provide eventual handoff and a clear recovery path, not linearizable
execution or exactly-once side effects. The operator API needs a local runner,
lease controller, and output gate, but no central scheduler trait.

Operator placement remains device-centric: capabilities are availability
labels, roles authorize the hosting device, and the DeviceId signs the emitted
events.
