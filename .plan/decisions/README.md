# Architecture Decisions

These decisions are intentionally narrow. Read them in numeric order when
building the distributed control plane; a decision links to another only when
that dependency is necessary.

| Decision | Subject | Answers |
|---|---|---|
| [001](001-devices-and-roles.md) | Devices and roles | Who is a member, what roles exist, and who may change each Device field? |
| [002](002-device-liveness.md) | Presence and liveness | How does a replica make a local reachability estimate? |
| [003](003-replication-frontiers.md) | Replication frontiers | How does a device prove contiguous shared-history receipt? |
| [004](004-device-onboarding.md) | Device onboarding | How does an unknown installation become a Device? |
| [005](005-device-key-rotation.md) | Key rotation | How does a stable DeviceId change signing keys safely? |
| [006](006-local-sync-boundaries.md) | Local and shared boundaries | What does `Cell.sync` mean without losing private nested state? |
| [007](007-tombstone-compaction-watermarks.md) | Compaction watermarks | When is shared deletion metadata safe to remove? |
| [008](008-operator-reconciliation-and-leases.md) | Operators and leases | How are declarative workers placed and handed over without a central scheduler? |
| [009](009-shared-table-lifecycle.md) | Shared table lifecycle | How is replicated table existence authorized and ordered against its data? |

ADRs 001 through 007 and 009 are implemented by the current client-side
Workspace runtime. ADR 008 is intentionally still proposed and its distributed
operator control plane is not implemented. A similarly named legacy operator
type must not be read as completion of ADR 008.
