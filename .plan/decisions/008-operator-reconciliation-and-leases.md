# 008: Distributed Operators And Leases

Status: Proposed

## Desired State

Future operator specs will be rows in a cataloged system table. A spec declares
source, config version, inputs, outputs, effects, desired state, and required
device capability labels.

Every device will run one concrete local reconciler. A device is eligible only
when it supports the source/config version, advertises required capabilities,
and has Contributor for inherited output. The worker acts as its hosting
DeviceId; operators receive no principal or independent roles.

## Soft Lease

Singleton work uses a replicated lease row:

```text
operator_id
holder_device_id
fence
expires_at
```

Partitions may temporarily produce multiple workers; the lease is not a
linearizable lock. Shared output must carry operator ID and fence so readers
expose only the winning generation. External effects require destination-side
idempotency or fencing.

## Reconciler

1. Observe specs, leases, local capabilities/roles, and runtime state.
2. Use a pure planner for start, stop, claim, renew, and release actions.
3. Apply actions through one concrete local runner.
4. Persist observations after local effects cross their boundary.
5. Stop publication after lease loss and restore compatible checkpoints on
   takeover.

Table topics remain local input feeds; their offsets are not portable leases or
checkpoints.

## Rhai And Effects

Rhai receives bounded data and queued writes with no ambient filesystem,
process, browser, shell, or network access. Capability labels do not expose
host calls. Nondeterministic external work uses idempotent jobs and result
records.

The existing native `Operator` trait remains the local execution ABI and is not
the distributed control plane.
