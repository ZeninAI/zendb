# ZenDB Design Guide

Read in this order:

1. [Architecture](architecture.md) for crate boundaries and runtime flow.
2. [Decisions](decisions/README.md) for one concept at a time.
3. [Roadmap](roadmap.md) for work that is genuinely unfinished.

## Invariants

- ZenDB runs inside client applications; optional hosted services are not data
  or membership authorities.
- DeviceId is the sole database authorization subject.
- Every admitted device receives shared data and is an implicit Reader.
- Roles are fixed, workspace-wide, and non-overlapping.
- `_catalog`, `_devices`, and `_enrollment_tickets` are real Tables.
- Table and nested local boundaries use one Cell tree, never an overlay.
- Sync policy is local, durable, HLC-neutral, and absent from wire state.
- Local mutations consume no shared origin sequence.
- Returning to shared state merges original CRDT clocks and forces state repair.
- Presence, durable receipt, materialization, and authorization are separate.
- Tombstone deletion is disabled until pruning context is designed.
- Distributed operators remain proposed; the existing operator runtime is local.
- The local operator runtime is optional and wraps, rather than parameterizes,
  the concrete Workspace.

Historical broad design files were deleted. Git history remains the place to
inspect superseded principal, OAuth, custom-RBAC, `_control`, and overlay
proposals.
