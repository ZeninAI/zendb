# 006: Local and Shared Sync Boundaries

Status: Accepted

## Scope

This decision defines what `sync = false` means for a table or nested Cell. It
does not define peer transport, shared-journal frontiers, or tombstone
compaction.

## Decision

ZenDB has two mutation planes:

```text
Shared plane
  Replicated Workspace data. Normal Workspace permissions apply.

Local plane
  Device-private data. It is never exported and does not require a Workspace
  data role to mutate.
```

The current `Event.sync` boolean is transitional routing metadata. It is not a
security claim and must not be trusted from a caller or a remote envelope. The
local write router resolves the effective sync policy at the target path before
creating a journal record. Shared events enter the shared journal; local events
enter only the local journal.

## Table Boundary

A local table has a local catalog entry and local state only. It is not
announced to peers, does not receive a shared EventIdentity, and can use the
same CRDT types as a shared table.

A shared table has a shared default policy. A device may still create a
local-only boundary within one of its rows.

## Nested Boundary and Overlay

`Cell.sync = Some(false)` is local routing metadata for that device. It is not
replicated Workspace data and is not merged from another device. It creates a
local overlay boundary at that path.

An explicit `Some(true)` can select the shared plane only while every ancestor
is already in the shared layer. It cannot escape an enclosing local boundary.
Otherwise a private parent would implicitly publish a child that no other
device can interpret.

The resolved value presented to local code is:

```text
resolved row = shared row plus the local overlay at local-only paths
```

Remote shared events apply only to the shared layer. They cannot overwrite,
delete, or otherwise mutate a local overlay, including when the remote event
targets an ancestor Record or replaces a whole shared row. This overlay is
required; merely filtering outbound events would allow a remote ancestor
replace to erase the local-only descendant.

If a shared ancestor is currently tombstoned or has an incompatible type, that
shared structural state takes precedence for the resolved read: the overlay is
retained privately but hidden until the shared path is live and compatible
again. This prevents a private child from resurrecting a deleted shared row.

Local events beneath a local boundary apply only to that overlay. They may use
the full local CRDT type system, including local tombstones. They are never
exported and do not consume a shared origin sequence.

## Changing a Boundary

Disabling sync at a path forks a local overlay from the currently resolved
shared subtree. Future local writes target the overlay; the shared subtree
continues to evolve independently on other devices.

Re-enabling sync discards the local overlay and reveals the current shared
subtree. It does not silently publish local changes. Publishing is an explicit
operation that produces normal shared mutations, requires ordinary Workspace
permissions, and clears the overlay only after those mutations are durable.

`SetSync` is therefore a local policy operation. It is not a shared mutation
and must not be replicated as one.

## Authorization

Any admitted device may create and mutate its own local tables and local
overlays, regardless of its Workspace data role. This is not a permissions
bypass because local-plane data never enters the Workspace.

Only explicit publication from local to shared state is subject to
`Contributor` and all normal shared-data validation.

## Interaction With Replication

ADR 003's `EventIdentity` and `ContiguousFrontier` describe only shared
events. Local events have local ordering only. This prevents intentional
local-only writes from creating permanent gaps in a peer's shared-event
frontier.

Every exported event is already known to target the shared plane. A receiver
still applies its own local overlay rules, preserving its device-private state
while merging the shared mutation.
