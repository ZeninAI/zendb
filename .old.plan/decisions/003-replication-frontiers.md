# 003: Shared Events And Frontiers

Status: Implemented

## Event Identity

Only inherited mutations receive:

```text
EventIdentity { origin_device_id, origin_seq }
```

The sequence is per device across the Workspace shared journal. Local table
and local path mutations consume no sequence, so they create no remote gaps.
The Event HLC remains the CRDT conflict clock; origin sequence is durable
transport identity, not a causal or total order.

## Contiguous Receipt

`ContiguousFrontier[origin] = n` proves that every event `1..=n` was durably
received and classified. Out-of-order events wait behind gaps. Classification
may apply state, wait on catalog/device dependencies, or deliberately skip
materialization below a local boundary.

Peers request exact missing ranges. Each device checkpoints its frontier in
its own `_devices` row. A maximum-seen VersionVector is only a hint.

## Stable Frontier

Stable receipt is the element-wise minimum checkpoint across every admitted
device. Offline devices hold it back until Manager removal. It gates signing
key rotation and informs retention.

Stable receipt does not prove identical materialized state because local
boundaries may skip events. Merkle roots and explicit state reconciliation
cover that distinction. It also does not by itself authorize tombstone pruning;
ADR 007 explains why.
