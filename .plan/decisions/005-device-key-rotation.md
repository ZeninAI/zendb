# 005: Device Key Rotation

Status: Accepted

## Scope

This decision defines the signing keys for one stable DeviceId. It does not
define membership, roles, session liveness, encryption at rest, or Workspace
content encryption.

## Device Key Ring

Each Device record contains one atomically replaced nested record:

```text
DeviceKeyRing: Record
  primary_key
  secondary_key: optional
  primary_from_seq: u64
  phase: Stable | Staged
```

There is no protocol-level `KeyId`, expiry, or validity interval. A public key
is its own value. A new device begins with its initial public key as primary,
no secondary key, `primary_from_seq = 1`, and `phase = Stable`.

`primary_key` signs ordinary shared events at and after `primary_from_seq`.
`secondary_key` is either a staged candidate or the historic verifier for
older shared events. `phase` makes those meanings unambiguous: only a
`Staged` secondary key may promote itself.

The complete `key_ring` Cell is replaced atomically. Its fields are not
independent CRDT state because the transition is a small single-device state
machine.

## Rotation

1. The primary key signs a staging update that preserves the primary key and
   `primary_from_seq`, installs the new public key as secondary, and sets
   `phase = Staged`.
2. The device waits until ADR 003's stable frontier covers the staging event.
   Every admitted device has then applied the candidate public key.
3. The secondary key signs its first shared event, the promotion marker. It
   atomically swaps the keys, sets `primary_from_seq` to that event's
   `origin_seq`, and returns the ring to `phase = Stable`.
4. Later ordinary shared events use the new primary key. The previous primary
   remains secondary only to verify events older than `primary_from_seq`.
5. Before a future rotation overwrites that historic secondary key, the stable
   frontier must cover the prior promotion marker.

## Verification

```text
ordinary event, origin_seq >= primary_from_seq
  verify with primary_key only

ordinary event, origin_seq < primary_from_seq, phase = Stable
  verify with secondary_key when present

staging update
  require existing phase Stable; verify with existing primary_key

promotion marker
  require existing phase Staged; verify with existing secondary_key
```

Blindly accepting the historic secondary key for every new event would let a
compromised retired key continue authoring mutations. Likewise, allowing a
secondary key to promote outside `Staged` would let it reclaim primary status.

Presence messages always use the current primary key; ADR 002 does not accept
historic keys for liveness.

## Failure Boundary

One DeviceId represents one non-cloned local credential profile. Concurrently
running a restored clone can produce conflicting key-ring histories or events
signed by a discarded candidate key. The local profile must prevent this; a
compromised or cloned profile requires Manager re-admission, not a best-effort
merge of key histories.
