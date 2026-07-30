# 006: Local And Inherited Boundaries

Status: Implemented

## Policy

Application data Cells have replica-local `SyncPolicy::{Inherit, Local}`.
`Inherit` follows the parent, and a row root resolves against a shared table
boundary. `Local` protects the whole subtree; no child can escape it. There is
no table-level policy, and catalog rows cannot be localized.

Policy is durable and HLC-neutral. It is excluded from signatures, replicated
Cell projections, snapshots, and comparable Merkle roots.

## One Tree

Table keeps one `State<PrimaryKey, Cell>`, one cache, and one local Change
topic. There is no shared tree plus overlay.

Below a local boundary:

- local writes require no role and enter no shared journal;
- received remote events remain durable in the journal but are not materialized;
- policy-aware state merge updates shared siblings and preserves local children;
- remote ancestor replacement cannot erase a local descendant.

Local row and nested mutations use generation-bound `TableHandle` event methods
and produce the same Table topic format as shared or received mutations.

## Returning To Inherit

The Workspace transition:

1. persists the HLC-neutral policy change;
2. requires Contributor before publishing local state;
3. appends a publication-intent Change containing the current projection and
   original data HLCs;
4. lets the background replication projector sign and journal that Merge;
5. persists a workspace reconciliation marker;
6. requests and policy-aware merges verified current state on the next peer
   synchronization;
7. clears the marker only after successful snapshot installation.

The toggle HLC never chooses a winner. LWW, sets, counters, records, and lists
use their normal CRDT merge rules. Current repair is snapshot-granular;
targeted Merkle exchange is a future bandwidth optimization.
