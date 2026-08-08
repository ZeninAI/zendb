# ZenDB

ZenDB is a synchronous embedded CRDT database with durable local storage,
workspace authorization, and workspace-owned peer-to-peer replication.

## Crates

| Crate | Responsibility |
|---|---|
| `zendb-types` | IDs, installations, permissions, CRDT values, and binary utilities |
| `zendb-storage` | B+ tree, KeyDir, SkipList, State, segmented Topic, indexed topics, and Table |
| `zendb-workspace` | Workspace lifecycle, catalogs, membership, workspace clock, network admission, and the Zenin libp2p runtime |
| `zendb-it` | Integration coverage for workspace lifecycle and replication |

Applications provide an account-root libp2p keypair, display name, and optional
route hints through `PeerIdentity`. ZenDB derives a distinct Ed25519 installation key for each
`(WorkspaceId, InstallationId)` pair and persists only its public key.

## Storage

A `Table` combines materialized `PrimaryKey -> Cell` state, per-installation
causal state, and a `Topic<Change>`. The table writes applied cells directly to
State. Local CRDT no-ops are not appended to the topic; remote observations
that do not change local state update causal receipt state without entering the
topic until the later no-op replication response is implemented.

Because `Event` starts with `stamp` and bincode uses fixed-int encoding, the
first 28 bytes of every record are the `EventStamp`. Topic readers can filter
by installation or time by scanning only this prefix.
Topic segments maintain a sparse byte-position index for bounded local seeks.

A `State` is caller-typed local storage without a change topic. The system
state catalog remains open for the workspace lifetime; the workspace hybrid
clock is stored in `_identity` alongside the workspace and installation IDs.

## Zenin Protocol

Every open workspace owns a private libp2p swarm. A single custom `/zenin/1`
session protocol carries authenticated handshakes, admission notifications,
live pushes, mesh graft/prune, and table-scoped anti-entropy summaries and
fetches.
When replication is enabled, local commits wake the replication runtime through
an unbounded Tokio channel. Disabled replication has no controller and local
commits use a no-op notification path.

Events are grouped per table in `TableBatch` envelopes. Once an installation is
admitted, every committed event is trusted; there is no per-event signature or
RBAC re-check in the network. RBAC is enforced only by the producing device
before local insertion.

Pending installations remain dialable route candidates but do not enter the
replication mesh. Active installations exchange receipt summaries per table;
each table owns an independent per-installation sequence stream.

## Network Admission And Workspace Merge

`Workspace` exposes only `create` and `open`. `WorkspaceConfig::workspace_id`
may be supplied when creating independent replicas that should later merge.

Applications exchange installation IDs, public keys, and routes through their
own product flow and add the remote installation as `Pending`. Zenin dials that
route; an authenticated same-workspace handshake also records unknown peers as
`Pending`. A manager uses the normal `Installations::upsert` path to write
`Active(permissions)` or `Rejected`.

Activation starts ordinary anti-entropy rather than a bootstrap phase. The
installation, catalog, and application topics merge in catalog-first order.
Permissions govern future local writes, not events already committed before
the two workspace clusters merged.

## Layout

```text
workspace-root/
  _identity
  _lock
  tables/
    _catalog/
    _installations/
    <table-name>/
      state/
      causal/
      topic/
  states/
    _catalog
    <state-name>
```

The project is not migration-stable. Compile all crates with:

```text
cargo check --workspace
```

## Releases

Pushes to `main`, `release-candidate`, and `develop` are processed by
`.github/workflows/release.yml`. Conventional commit titles determine whether
the push produces a release: `feat` creates a minor release, `fix`, `perf`,
`refactor`, and dependency changes create patch releases, and breaking changes
create major releases. Documentation, tests, styling, CI, and ordinary chores
do not release.

`main` publishes stable versions, `release-candidate` publishes `-rc.N`
versions, and `develop` publishes `-dev.N` versions. Semantic-release derives
the version from Git tags and commit history. The workflow temporarily applies
that version to the workspace manifests, publishes `zendb-types`,
`zendb-storage`, and `zendb-workspace` in dependency order, and then creates the
GitHub release. No version bump is committed to the repository.

Configure the repository secret `CARGO_REGISTRY_TOKEN` with a crates.io API
token before enabling the workflow. The first release should have a matching
version baseline tag, such as `v0.1.0`, if the project should start from that
version rather than semantic-release's default initial version.
