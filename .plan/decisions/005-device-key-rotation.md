# 005: Device Signing-Key Rotation

Status: Implemented

## Key Ring

Each Device row atomically stores:

```text
DeviceKeyRing
  primary_key
  secondary_key: optional
  primary_from_seq
  phase: Stable | Staged
```

There is no key ID, validity timestamp, or DeviceId derivation from a key.

## Protocol

1. Current primary signs a ring replacement staging a new secondary.
2. Ordinary events continue using the current primary.
3. The device waits until stable receipt covers the stage event.
4. The staged key signs promotion, swaps key order, and records the promotion
   origin sequence as `primary_from_seq`.
5. The old primary verifies only earlier event sequences.
6. A later stable rotation may replace that historic key.

Presence always uses the current primary. A staged secondary may sign only its
promotion. Divergent cloned profiles are unsupported; recovery uses Manager
removal and fresh admission.
