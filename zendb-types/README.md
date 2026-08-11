# zendb-types

`zendb-types` owns ZenDB's portable data model and binary utilities. It has no
storage, networking, or workspace policy.

## Values And Operations

Every registered CRDT is a named struct that owns its `EventTime` and
tombstone metadata. `Value` stores those structs directly; there is no separate
metadata wrapper. Records own child `Value`s and recursively repair
missing or mismatched path parents before dispatching an operation.

The `zendb_type!` and `zendb_container_type!` macros accept a named struct and
its implementation block. Each `op_*` method becomes one variant in the
generated `<Type>Op` enum. The generated facade removes `op_`, mints an
`EventTime` from the process-wide clock, and calls the operation directly. A
type's default construction also mints its metadata `EventTime`; the explicit
`EventTime::ZERO` constant is reserved for the uninitialized clock and sentinel
checks. No implicit update or delete variants are added. A container's single
Records expose an `insert(name, value)` operation with per-field last-writer-wins
replacement. `ensure_child` becomes the typed path repair implementation. Its borrowed
segment argument defines the container's segment type, and the registry combines
those inferred types into the global `Segment` enum. `ensure_child` returns an
optional mutable child `Value`; `None` means the incoming path was stale and must
be ignored.

`Type::apply` only dispatches an operation enum to its `op_*` method. The
operation implementation owns conflict acceptance, timestamp advancement, and
tombstone changes. Empty values created for self-healing paths are reset to
`EventTime::ZERO` before the incoming operation is applied.

Primitive scalar values expose both `set` and `delete` operations. A newer set
updates the payload and clears the tombstone; a newer delete preserves the
payload, marks the value tombstoned, and advances its `EventTime`.

Operations may return either `bool` or `Result<bool, Error>`. The type macro
generates `<Type>OpError` as the aggregate `Type::Error` only when at least one
operation is fallible; otherwise the type uses `Infallible`. Each distinct
underlying error type gets one aggregate variant, so operations sharing an
error type are deduplicated. The registry then wraps those per-type errors in
its global `TypeError` enum.

The closed registry generates `Value`, `TypeOp`, `TypeTag`, `Segment`, primary
keys, type dispatch, and `From`/`TryFrom` conversions. `Value` itself implements
both `Type` and `ContainerType`, so heterogeneous values use the same operation
and child traversal contracts as concrete CRDT types. Conversions that create
new values use the type's default metadata clock; conversions from an existing
`Value` preserve its metadata. Applying an operation with a different type tag
self-heals the value when the operation's `EventTime` is newer; stale operations
are ignored. `TypeMismatch` remains the error for explicit typed conversions.
The generated `ValueSlot` trait applies the same path operation to an
`Option<Value>`, creating the root value from the path or operation type when
the slot is empty.

Registered values currently include scalar types, Installation, Text, Float,
and Record. The deferred priority queue, set, multi-value register,
observed-remove set, and operation-id model are not part of this design.

## Events

`EventId` is `(InstallationId, sequence)` and identifies a batch for topic
deduplication, anti-entropy, and recovery. An `Event` contains one primary key
and a vector of `PathOp` values. Each `PathOp` carries an `EventTime`, which is
the only clock used by CRDT application. The lazy process-wide clock stores its
96-bit timestamp state in one lock-free 128-bit compare-and-swap word and is
shared by generated facades and workspace replication.

`Installation` records admission state and route hints. `Pending` and
`Rejected` have no authority; `Active(Permissions)` grants the configured
permissions. Installation rejection is an explicit typed operation.

## Binary Utilities

`utils::serdes` is the shared bincode layer with little-endian fixed-width
integers, serialized sizing, direct buffer writes, and vector serialization.
`Blob::encode` and `Blob::decode` use the same configuration.
