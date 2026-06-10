# Sunday Architecture Summary

## Core Storage Boundary

- Extract the current table state enum into a generic `State<K, V>`.
- `State<K, V>` owns either an ordered `BPlusTree` or unordered `KeyDir` and implements `Backend<K, V>`.
- `State` has no events, event log, replication, or reactive behavior.
- Indexes and computations use arbitrary typed `State<K, V>` instances for persistent internal state.
- `Table` is an event-capable layer over `State<PrimaryKey, Cell>` and an optional `OrderLog`.
- Tables without an event log use direct backend operations; event/event methods are unavailable or invalid.

## Change Model

Indexes and computations receive a table change containing:

```rust
struct Change {
    event: Event,
    previous: Option<Cell>,
    current: Option<Cell>,
}
```

- `Event` already contains the table ID and primary key.
- `previous` and `current` are resolved row-root values before and after applying the event.
- Synchronous consumers receive borrowed change values; asynchronous consumers receive owned values.

## Synchronous Indexes

- A synchronous index belongs to exactly one `Table` and lives inside that table.
- It participates directly in the table's event-write path.
- It owns and directly mutates arbitrary internal `State<K, V>` instances.
- It receives immutable `&Table` access, allowing all table reads.
- It emits derived events through a `EventCollector`, not by mutating the table directly.

```rust
trait SyncIndex {
    fn on_change(
        &mut self,
        table: &Table,
        change: ChangeRef<'_>,
        output: &mut EventCollector,
    ) -> Result<(), IndexError>;
}
```

After every synchronous index processes the current change, the table applies collected events recursively. Index implementers are responsible for preventing infinite recursive output.

## Asynchronous Indexes

- An asynchronous index is also bound to one table and has the same logical capabilities.
- It runs outside the table write path on an executor/thread chosen by the consuming application.
- It owns its internal `State<K, V>` instances.
- It receives owned changes.
- It may rarely read or write its attached table through a shared `TableHandle`.
- Expensive work must happen without holding a table lock.

The storage engine must not own an async runtime.

## Shared Table Concurrency

Use per-table reader/writer locking:

```rust
struct TableHandle {
    inner: Arc<RwLock<Table>>,
}
```

- Multiple readers may access one table concurrently.
- No direct write, event insertion, materialization, or synchronous index update may happen while readers exist.
- One writer owns the entire logical table write operation.
- Different tables can be read or written concurrently by different threads.
- Prefer closure-based `read` and `write` methods so lock guards and borrowed values cannot escape.
- Never hold a lock across `.await`.

The database table registry should contain clonable `TableHandle`s. `HashMap::get_disjoint_mut` may help internal scoped operations but is not the primary concurrency API.

## Computations

- A computation is a database-level resource, unlike an index which is table-bound.
- It may subscribe to multiple tables and emit events to multiple tables.
- It owns arbitrary persistent `State<K, V>` instances.
- Computations run outside the storage engine on application-managed threads/executors.
- The consuming application controls scheduling, backpressure, Rhai configuration, and custom functions.

## Rhai

- Rhai is the preferred initial persisted scripting option.
- Persist Rhai source, then compile it into an interpreted AST when opening the database.
- Rhai computations should receive changes, mutate owned states, and emit events.
- The consuming application can register custom functions such as markdown parsing or LLM-provider calls.
- Native Rust computations and Rhai computations should share the same logical change/state/event-emission model.
- Complex or expensive functionality should generally be implemented as registered Rust functions called from Rhai.

## Ownership Summary

| Resource | Scope | Ownership and execution |
|---|---|---|
| `State<K, V>` | Generic persistent data | Exclusively owned, normally lock-free |
| `Table` | Event-capable `PrimaryKey -> Cell` state | Shared through per-table `RwLock` |
| Synchronous index | One table | Lives in table; runs in write path |
| Asynchronous index | One table | Application executor; accesses table through `TableHandle` |
| Computation | Database | Application executor; may access multiple table handles |

## Remaining Decisions

- Exact public API for attaching, opening, stopping, and dropping indexes and computations.
- How asynchronous change queues and backpressure interact with table writes.
- Whether table event logs retain changes until asynchronous consumers acknowledge them.
- Failure and rebuild semantics for synchronous and asynchronous indexes.
- Exact Rhai host API for states, table reads, and event emission.
