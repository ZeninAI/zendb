# Iteration 0005: Devices Lifecycle And API

Status: implemented.

This iteration aligns the device subsystem with the lean `States` and `Tables`
APIs while making local-peer lifecycle behavior explicit.

## Ownership

`Devices` owns the durable `PeerStore`, the `_devices` `TableHandle`, and the
in-memory `DeviceRecord` cache. `Devices::create/open` construct the peer
store and the cyclic `_devices` handle. Opening a device registry loads its
durable records before returning.

`Tables::create/open` owns table-runtime initialization. After listeners are
registered, table creation registers the initial local device and table open
requires the current peer to already be registered. The table catalog rows are
written during table creation. `Workspace::assemble` only composes the state
and table handlers. Durability coordination is outside the workspace.

Peer and table mutations update their storage backends in-memory. This layer
does not call `sync` or `flush`; a later storage owner is responsible for
durability coordination.

## Local Peer Rules

The create path registers the creator as an `Operator` exactly once through
`register_local_device`. The open path uses `require_local_device`; opening a
workspace never silently promotes an unknown peer. A future join/enrollment
flow must explicitly establish both the peer's `_devices` record and its
`_peers` clock record before that peer can open the workspace.

## API

The public `Devices` surface is the registry facade:

- `local_peer_id`
- `list`
- `get`
- `upsert`

Clock, receipt, authorization, durability, and initialization operations are
crate-internal. `record` becomes `get`, `write_record` becomes
`upsert_internal`, and `bootstrap_local` is replaced by the explicit
`register_local_device` / `require_local_device` lifecycle operations.

`Devices::upsert` returns `Result<bool>` like the state and table catalogs;
unchanged records do not mint or publish an event.

## Layout

`devices/mod.rs` contains declarations and re-exports. `devices/runtime.rs`
contains `Devices` and `DeviceRecord`; `devices/peer.rs` contains `PeerStore`,
`PeerRecord`, and `ClockCheckpoint`; `devices/receipts.rs` contains receipt
window bookkeeping. Internal peer/receipt types are not re-exported from the
crate root until a replication API requires them.

## Multi-peer Constraint

`PeerStore::open` currently requires a local `_peers` clock checkpoint. A
registered peer without that checkpoint remains an explicit enrollment/join
case rather than being silently initialized during ordinary open.
