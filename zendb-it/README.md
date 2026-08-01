# zendb-it

`zendb-it` contains coarse end-to-end tests for the public ZenDB workspace API.
It intentionally leaves storage algorithms, binary codecs, and individual
authorization branches to their owning crates.

The suite covers:

- table and local typed-state lifecycle across durable workspace reopen;
- physical cleanup and durable catalog removal after lifecycle deletion;
- two live workspaces using distinct loopback TCP ports in one test process;
- recovery when the second workspace starts after the first has attempted its
  persisted route;
- bidirectional authenticated application-event replication;
- replicated table creation, listener installation, and table deletion;
- durable verification of replicated data after both peers stop.

Initial join synchronization is not implemented yet. The two-peer fixture
therefore enrolls both devices once, clones the synchronized durable workspace,
and replaces the private local identity record in the second copy. This seeds
the state that a future initial-sync protocol will produce; all replication
behavior under test then runs between normal live `Workspace` instances.

Network readiness is established through repeated writes on an existing table.
Each attempt uses a new primary key, and the test proceeds only after each
workspace has received an event from the other.

Run them with:

```text
cargo test -p zendb-it
```
