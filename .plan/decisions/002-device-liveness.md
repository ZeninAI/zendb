# 002: Device Presence and Liveness

Status: Accepted

## Scope

This decision answers one question: what may one device locally conclude about
another device's current reachability? It does not define membership, operator
placement, leases, replication progress, or authorization.

## Decision

Presence is signed transport soft state. It is not a Workspace CRDT row and is
not a durable `Online` or `Offline` device status.

```text
PresenceHeartbeat
  device_id
  presence_seq
  emitted_hlc
  advertised_idle_period_ms
  signature
```

`presence_seq` is persisted in the device profile and increases monotonically.
It lets receivers discard replayed or delayed messages. `emitted_hlc` orders
evidence from the device; it is not a trustworthy wall-clock age calculation.

Any message on a mutually authenticated transport session with the target
device refreshes direct presence. An idle session emits `PresenceHeartbeat`;
an active session piggybacks the same information on ordinary traffic.

Presence messages verify with the current primary key from ADR 005 only. A
historic secondary key must never refresh liveness. A peer re-authenticates or
rekeys an existing session after a key promotion before that session may again
refresh direct presence.

## Graceful Departure

Before intentionally closing transport, sleeping, or logging out, a device
sends a signed, best-effort `DepartureNotice` to its connected peers. It is
soft state just like a heartbeat. A later heartbeat with a higher
`presence_seq` supersedes it.

A departure notice gives peers an immediate local indication that the device
will not answer probes. It does not tombstone membership, change roles, alter
authorization, change replication progress, or transfer operator work. If a
device crashes before the notice arrives, timeout-based local detection remains
the fallback.

## Local Failure Detector

Every observer keeps non-replicated per-peer state:

```text
last_direct_contact: local monotonic instant
recent direct arrival samples
highest presence_seq observed
local suspicion score
```

During warm-up, the observer uses the peer's advertised idle period and a local
grace multiplier. Once it has enough direct arrival samples, it should use an
accrual detector such as phi, exposing a continuous suspicion score rather than
a Workspace-wide timeout. Different consumers may choose different thresholds.

The resulting local classifications are:

```text
Direct
  Recent authenticated transport activity from the exact target device.

Indirect
  Signed evidence learned through another peer or synchronization, but no
  direct path confirmation. It may trigger a direct probe.

Suspect
  The local arrival model crossed a consumer-selected threshold.

Unreachable
  Direct probes failed or the local model crossed a higher threshold.

Departed
  A newer valid DepartureNotice was received.

Unknown
  There is no sufficient direct evidence, for example after local restart.
```

These are local observations. They are not replicated, do not affect
membership, and never authorize or revoke data access.

## Direct and Indirect Evidence

Only a session authenticated as the target DeviceId refreshes
`last_direct_contact`. A relay cannot turn an old heartbeat into new direct
contact. Relayed presence remains indirect because it can be delayed or replayed
by an otherwise honest peer.

For the normal 3-5 device Workspace, direct probes and authenticated sessions
are sufficient. A larger cluster may later add SWIM-style indirect probing and
local suspicion/refutation, without turning suspicion into membership removal.

## Related Decisions

- ADR 001 defines whether the Device is a member.
- ADR 003 defines replication progress, which is not presence.
- ADR 005 defines the primary key used to sign presence messages.
