# zendb-workspace

`zendb-workspace` is ZenDB's synchronous orchestration layer. It owns Catalog
and peer policy without introducing transport, replication workers, snapshots,
or an async service framework.

## Catalog

Catalog is the sole workspace storage boundary. It owns physical paths and all
`DurableStorage::create` and `open` calls.

- `_table_catalog` is a normal self-registering Table. Blob cells contain
  `CatalogEntry { config: TableConfig }`.
- `_state_catalog` is `State<String, StateConfig>` and contains its own
  declaration.
- `_devices` and application Tables are eagerly opened from table catalog
  declarations.
- `_peer_state` and application States are obtained through typed Catalog
  handles.

There is no format version, generic name validator, migration branch, or
runtime liveness flag. Exact system names are reserved structurally. Deleting
an application Table returns `ResourceBusy` while a handle still owns it;
otherwise Catalog removes the declaration and directory before the name can be
recreated.

## Peers And Roles

`_devices` stores `PeerId -> DeviceRecord`, where each record has a display
name and a set of `Roles`.

- `Contributor` writes existing application Tables and may update its own
  metadata without changing roles.
- `Operator` includes Contributor behavior and may manage Tables and peer
  records.
- `Dispatcher` is persisted but has no behavior in this iteration.

`_peer_state` stores `PeerRecord { receipts, clock }`. Only the local peer has
a clock checkpoint. The module loads all records into an `ArcSwap` snapshot
for lock-free reads, serializes mutations, and tracks dirty peers for flush.
Minting persists the local sequence high-water mark before returning, so a
failed mutation burns a sequence rather than risking reuse.

## Table Access

`TableHandle::read` returns a guard that dereferences to the real storage
`Table`. Callers use `ReadBackend` and `OrderedReadBackend` methods directly,
and iterators stay lazy while the guard is held. There are no `get` or
materializing `entries` proxy methods on the handle.

`TableConsumer` owns a named Topic consumer. `next_change` decodes one Change,
`read` acquires a direct Table guard, and `commit` persists the consumer
cursor. It does not build an intermediate row snapshot.

Local mutations are linear: authorize, mint, insert, then observe. There is no
callback-based mutation API, reconciliation poison state, or event journal
outside each Table's Topic.
