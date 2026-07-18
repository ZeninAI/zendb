# zendb-operator

Optional local operator execution for ZenDB.

`OperatorHost<D>` wraps one concrete `Arc<zendb_engine::Workspace>` and owns
only local runtime concerns:

- the native operator catalog and workers;
- typed local State instances;
- processing-time timers;
- the application-supplied async Executor;
- built-in full-text, Merkle, and Rhai operators.

The core Workspace emits table open/close notifications. The host converts
those notifications into topic consumer attachment and worker lifecycle. The
Workspace has no generic operator parameter and no dependency on Rhai or an
executor.

All host-owned files live under the Workspace's `_operator/` directory.

This crate implements the existing imperative local runtime. It does not claim
to implement ADR 008 distributed placement, replicated leases, or fencing.
