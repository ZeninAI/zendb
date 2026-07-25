# Architecture Decisions

| ADR | Status | Subject |
|---|---|---|
| [001](001-devices-and-roles.md) | Implemented | Device membership, fixed roles, field ownership |
| [002](002-device-liveness.md) | Implemented | Signed presence and departure soft state |
| [003](003-replication-frontiers.md) | Implemented | Shared identities and contiguous receipt |
| [004](004-device-onboarding.md) | Implemented | Ticket and direct device onboarding |
| [005](005-device-key-rotation.md) | Implemented | Stable DeviceId and two-key rotation |
| [006](006-local-sync-boundaries.md) | Implemented | One-tree local/inherited routing and repair |
| [007](007-tombstone-compaction-watermarks.md) | Proposed | Safe delete pruning context |
| [008](008-operator-reconciliation-and-leases.md) | Proposed | Distributed operators and leases |
| [009](009-shared-table-lifecycle.md) | Implemented | Catalog-backed tables and system tables |

An implemented ADR describes behavior covered by current APIs and tests. A
proposed ADR is a boundary for future work and must not be inferred from legacy
types with similar names.
