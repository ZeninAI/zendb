# Open Roadmap

Completed behavior belongs in code, tests, and implemented ADRs. This file
lists only unresolved work.

## 1. Pruning Context

Design and prove type-specific retained delete evidence for rows, Records,
Sets, OR-Sets, Lists, Text, and table lifecycle. Include it in snapshots and
Merkle state before enabling irreversible CRDT tombstone pruning.

Exit criteria are defined by ADR 007.

## 2. Targeted State Repair

Replace full-snapshot fallback for Merkle disagreement/re-inheritance with
bounded table, row, and subtree exchange. Preserve the current correctness
contract: original mutation clocks decide merge and local boundaries remain
untouched.

This is an optimization. The durable marker plus verified snapshot is the
implemented correctness path.

## 3. Catalog Migration

Add explicit migration tooling for changing a table's persisted physical
`TableConfig`. Never reinterpret existing files from a new catalog default.

## 4. Distributed Operators

Finalize and implement ADR 008 after data-plane stabilization:

- cataloged operator specs and observations;
- concrete local reconciler;
- capability and role eligibility;
- soft lease renewal and takeover;
- fenced shared output and external-effect idempotency;
- bounded Rhai APIs.

## 5. Protocol Hardening

- property/fuzz tests for malformed snapshots, event ordering, and key rotation;
- crash injection around profile/journal/table commits;
- multi-device partition/rejoin tests with nested local boundaries;
- resource limits for snapshot decode and state repair.
