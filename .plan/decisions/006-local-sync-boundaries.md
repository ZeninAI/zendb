# 006: Local And Inherited Boundaries

Status: Implemented

## Policy

Every Cell has replica-local `SyncPolicy::{Inherit, Local}`. `Inherit` follows
its parent/table. `Local` protects the whole subtree; no child can escape it.
The `_catalog` row policy supplies the table-wide boundary.

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

Local table mutations and local nested mutations use the same fluent row/path
API and produce the same Table topic format as shared/received mutations.

## Returning To Inherit

The Workspace transition:

1. persists the HLC-neutral policy change;
2. requires Contributor before publishing local state;
3. sends the current shared-state projection in a normal Merge event while
   retaining every original data HLC;
4. persists a workspace reconciliation marker;
5. requests and policy-aware merges verified current state on the next peer
   synchronization;
6. clears the marker only after successful snapshot installation.

The toggle HLC never chooses a winner. LWW, sets, counters, records, and lists
use their normal CRDT merge rules. Current repair is snapshot-granular;
targeted Merkle exchange is a future bandwidth optimization.
