# zendb-it

`zendb-it` contains coarse end-to-end tests for the public ZenDB workspace API.
It intentionally leaves storage algorithms, binary codecs, and individual
authorization branches to their owning crates.

The suite covers:

- table and local typed-state lifecycle across durable workspace reopen;
- physical cleanup and durable catalog removal after lifecycle deletion;
- two live workspaces using distinct loopback TCP ports in separate processes;
- authenticated application-event replication from the parent-controlled
  workspace to the waiting worker workspace;
- replicated table creation, listener installation, and table deletion;
- durable verification of replicated data after both peers stop.

The fixture enrolls both installations and prepares the second durable
workspace directly before exercising the live replication runtime. The parent
test process opens and drives workspace A. A worker process opens workspace B,
keeps its replication runtime alive, and waits for the parent to finish.

After the worker exits, the test reopens both workspaces offline and verifies
that workspace B durably received the parent process's data and catalog
changes.

## Storage Throughput

Ignored release-mode tests measure storage primitives and the complete
workspace `TableHandle::insert` path without slowing normal integration runs:

```text
cargo test -p zendb-it --release --test storage_benchmarks -- --ignored --nocapture --test-threads=1
```

The workspace workload includes permission checks, causal stamping, table
insertion, projection, listener dispatch, and replication notification.

Run them with:

```text
cargo test -p zendb-it
```

The tests install a `tracing-subscriber` formatter configured globally at
`trace`; `RUST_LOG` does not change the level. Use
`cargo test -p zendb-it -- --nocapture` to display logs from passing tests:

```text
cargo test -p zendb-it -- --nocapture
```
