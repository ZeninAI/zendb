# zendb-storage

`zendb-storage` owns persistence mechanics. Its backends are independent of
workspace membership, authorization, and network policy.

## Table

`Table` combines:

```text
State<PrimaryKey, Value>
State<InstallationId, ReceiptWindow>
Topic<Change>
recovery consumer
```

`Table::insert(Event)` assigns the next sequence for the event author on that
table and applies the event's `PathOp` batch against one working row. The
batch gets one `EventId`, while each operation carries its own `EventTime`.
The batch is committed atomically. A CRDT no-op returns
`InsertOutcome::Ignored`; remote observations still advance receipt state even
when they do not change materialized state.

Rows store `Value` directly. A `Change` stores the event, previous `Value`, and
resulting current `Value`. Type metadata and tombstones belong to the concrete
value, not to storage. `Table` implements `ReadBackend` and
`OrderedReadBackend`, but not `WriteBackend`.

## Topics And Retention

Named consumers decode records incrementally and persist cursors only when
`commit` is called. Anonymous readers start at the earliest retained offset.
Since `Event` starts with its fixed-width `EventId`, the first 16 bytes of a
change record identify its batch. `TopicReader::seek` supports event-identity
predicates, offsets, earliest, and latest targets.

Each segment keeps a sparse logical-offset-to-byte-position index. Retention
trimming removes only fully consumed segments, so repair can resume from
retained offsets.
