# 002: Device Presence And Liveness

Status: Implemented

## Decision

Liveness is signed transport soft state. It is not stored in `_devices`, does
not affect membership, and cannot grant or revoke authority.

An authenticated idle peer sends:

```text
PresenceHeartbeat
  device_id
  presence_seq
  emitted_hlc
  advertised_idle_period_ms
  signature
```

The receiver uses monotonic arrival time, bounded interval samples, and a local
grace multiplier to derive `Direct`, `Indirect`, `Suspect`, `Unreachable`,
`Departed`, or `Unknown`, plus a continuous suspicion score. Different devices
may use different heartbeat intervals because the intended idle period travels
with each heartbeat.

Before intentional shutdown a device sends a best-effort signed
`DepartureNotice`. A newer heartbeat supersedes it; crashes still rely on
failure detection. Relayed heartbeats are indirect and never refresh direct
reachability.

Presence must use the current primary signing key. Historic rotation keys do
not prove current liveness.

## Consequences

Presence ranks peers and future operator candidates. It is not a lock,
health proof, frontier, or removal mechanism. An unreachable device remains a
member until a Manager tombstones its row.
