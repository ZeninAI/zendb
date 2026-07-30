# Iteration 0003: Identity Boundary And Bootstrap

Status: architecture proposal for the identity and bootstrap layer.

Priority: this document supersedes iter-0002 where the two disagree. It is
scoped to identity, peer identity, and workspace bootstrap. Callbacks,
listeners, and the remote-event seam are deferred to iter-0004.

## 1. Problem

Iteration 0002 left the workspace owning both `WorkspaceId` and the local
`PeerId` in a single `_identity` blob, with `Workspace::create` generating
both. This does not survive the open modes that the system must eventually
support:

- **Create**: generate `WorkspaceId` and a local `PeerId` together. Current
  behavior.
- **Open**: read both from disk. Current behavior.
- **Join**: the caller already knows the target `WorkspaceId` and is minting a
  new device identity (`PeerId` + private key) for this joiner. The workspace
  must store the provided `WorkspaceId` but must not generate the peer
  identity.
- **Account**: one user owns several devices and is a member of several
  workspaces. A single device identity (`PeerId` + private key) is reused
  across every workspace that device participates in. If the workspace owned
  the private key, the same key would be copied into every workspace
  directory that device touches.

The common thread is that **peer identity has a different lifecycle than
workspace identity**. `WorkspaceId` is 1:1 with a workspace directory. A
device identity is 1:1 with a device and spans every workspace that device
participates in. The workspace must not own device identity.

A second concern is future cryptographic event signing. Events will
eventually be signed by the producing device's private key. The workspace
must be able to sign without ever holding private key material directly, so
that key storage (plaintext file, OS keychain, HSM, KMS) remains an
application deployment decision.

## 2. Target Boundary

| Concern | Owner | Location | Lifecycle |
| --- | --- | --- | --- |
| `WorkspaceId` | Workspace | `zendb-workspace::Bootstrap` (`_identity` file) | 1:1 with workspace directory |
| `PeerId` + private key | Application | `PeerIdentity` trait in `zendb-types`, impl chosen by the application | 1:1 with a device; spans workspaces |
| Connection hints | Application (ephemeral) | Passed to `Workspace::join`, forwarded to networking | Used once at join time |
| Account → devices and workspaces | Application / future `zendb-account` | Above the workspace | The workspace is unaware of accounts |

The workspace identity blob is intentionally small and stable: a single
`WorkspaceId`. It does not grow as the system adds accounts, key stores, or
join flows, because none of those concerns belong to the workspace.

### 2.1 Why not a `DurableStorage` impl in `zendb-storage`

The workspace identity blob is a single `WorkspaceId` written once at create
or join and read once at open. It is never updated, never compacted, and has
no stats worth surfacing. A `DurableStorage` impl would carry unused
`compact`/`stats`/incremental-flush surface for a 64-byte write-once file.

The generic file-lock utility (advisory `try_lock` on a file) is reusable and
may live in `zendb-types::utils` if other crates need it. The identity-aware
`Bootstrap` type stays in `zendb-workspace` because it knows about
`WorkspaceId`, which is a workspace concept, not a storage backend.

## 3. PeerIdentity Trait

Introduce a trait in `zendb-types::identity` that abstracts a device's
cryptographic identity:

    pub trait PeerIdentity: Send + Sync {
        fn peer_id(&self) -> PeerId;
        fn sign(&self, message: &[u8]) -> Result<Signature, SigningError>;
    }

    pub struct Signature { ... }
    pub enum SigningError { ... }

The trait is intentionally minimal. The workspace uses `peer_id()` for
minting `EventId`s and `sign()` for future event signatures. It never sees
private key material directly.

A default in-memory implementation lives in `zendb-types` so that tests,
examples, and local-only deployments do not pay boilerplate:

    pub struct LocalPeerIdentity { keypair: Keypair, peer_id: PeerId }

    impl LocalPeerIdentity {
        pub fn generate() -> Self { ... }
    }

    impl PeerIdentity for LocalPeerIdentity { ... }

`LocalPeerIdentity` wraps an ed25519 `Keypair` generated via the existing
`libp2p-identity` dependency. It is the moral equivalent of the current
`PeerId::generate()` call, but retains the private key so `sign` works.

Application-supplied impls may back the same trait by an OS keychain, HSM, or
KMS without workspace changes.

### 3.1 Signature type

`Signature` is an opaque owned byte container. It is `Encode`/`Decode` so
future event envelopes can carry it, but iteration 0003 does not yet attach
signatures to events. The type lands now so the trait is stable.

`SigningError` covers key-unavailable and backend-failure cases. It is a
distinct error type, not a re-export of `zendb_workspace::Error`, because
signing failures can occur outside the workspace (e.g. an account layer
signing a join request).

## 4. Bootstrap Type

`zendb-workspace` owns a single `Bootstrap` type that merges the current
`WorkspaceLock` and `WorkspaceIdentity`:

    pub(crate) struct Bootstrap {
        identity: WorkspaceIdentity,
        _lock: File,
    }

    struct WorkspaceIdentity {
        workspace_id: WorkspaceId,
    }

    impl Bootstrap {
        pub(crate) fn create(root: &Path) -> Result<Self>;
        pub(crate) fn open(root: &Path) -> Result<Self>;
        pub(crate) fn join(root: &Path, workspace_id: WorkspaceId) -> Result<Self>;
        pub(crate) fn workspace_id(&self) -> WorkspaceId;
    }

- `create` acquires the lock, generates a `WorkspaceId`, writes `_identity`,
  returns.
- `open` acquires the lock, reads `_identity`, returns.
- `join` acquires the lock, writes the caller-provided `WorkspaceId`, returns.
  The caller is responsible for the join transport; `Bootstrap` only persists
  the target workspace identity.

The lock file path (`_lock`) and identity file path (`_identity`) remain
workspace-internal constants. `Bootstrap` is `pub(crate)`; the public surface
is the `Workspace` constructors.

A `Drop` impl releases the OS file lock when `Bootstrap` is dropped. There is
no `flush` because the blob is write-once.

## 5. Workspace Constructors

The public constructors change to take a `PeerIdentity` and, for join, the
target `WorkspaceId` plus connection hints:

    pub fn create(
        root: impl AsRef<Path>,
        peer: Arc<dyn PeerIdentity>,
        config: WorkspaceConfig,
    ) -> Result<Self>;

    pub fn open(
        root: impl AsRef<Path>,
        peer: Arc<dyn PeerIdentity>,
    ) -> Result<Self>;

    pub fn join(
        root: impl AsRef<Path>,
        workspace_id: WorkspaceId,
        peer: Arc<dyn PeerIdentity>,
        hints: JoinHints,
    ) -> Result<Self>;

`Arc<dyn PeerIdentity>` is stored on the `Workspace` and cloned into
`Devices` so that minting and (future) signing reach the same identity. The
workspace never stores the private key.

### 5.1 JoinHints placeholder

    pub struct JoinHints {
        // Filled out in the replication iteration.
    }

    impl Default for JoinHints { ... }

The type exists now so `Workspace::join` has a stable signature. The
replication iteration will add fields (multiaddrs, bootstrap peer list,
dial timeout). `Workspace::join` in this iteration persists the provided
`WorkspaceId` via `Bootstrap::join` and otherwise behaves like `create`
minus `WorkspaceId` generation. It does not yet contact any network.

### 5.2 WorkspaceConfig

`WorkspaceConfig` is an empty placeholder struct retained for future
workspace-level configuration. It carries no fields in this iteration; the
local device display name is not a workspace concern and is omitted. Fields
are added when concrete workspace-level config is needed.

## 6. Devices Construction

`Devices::create` and `Devices::open` currently take `local_peer_id: PeerId`.
They change to take `peer: Arc<dyn PeerIdentity>`:

    pub(crate) fn create(
        registry: Arc<TableEntry>,
        state: StateHandle<PeerId, PeerRecord>,
        peer: Arc<dyn PeerIdentity>,
    ) -> Result<Arc<Self>>;

    pub(crate) fn open(...) -> Result<Arc<Self>>;

`Devices` stores the `Arc<dyn PeerIdentity>` and uses `peer.peer_id()` where
it currently uses the bare `PeerId`. `local_peer_id()` continues to return
`PeerId` for callers that only need the public identity.

`bootstrap_local` continues to use `peer.peer_id()` as the row key. The local
device display name is omitted in this iteration; `bootstrap_local` no longer
takes a name argument.

## 7. Removed And Moved

| Removed / moved | Reason |
| --- | --- |
| `WorkspaceIdentity` struct in `workspace.rs` (held both IDs) | Replaced by `Bootstrap` owning only `WorkspaceId` |
| `WorkspaceLock` struct | Merged into `Bootstrap` |
| `PeerId::generate()` call inside `Workspace::create` | Peer identity is now application-supplied |
| `WorkspaceId::generate()` call inside `Workspace::create` | Moves into `Bootstrap::create` |
| Standalone `fs::write`/`fs::read` of `_identity` in `workspace.rs` | Owned by `Bootstrap` |

`PeerId::generate()` and `WorkspaceId::generate()` remain on the types for
use by `LocalPeerIdentity::generate()` and `Bootstrap::create` respectively.
They are not removed from `zendb-types`; they are just no longer called from
`Workspace::create`.

## 8. Wall-Time Utility

The workspace clock reads the wall clock in two places (`Workspace::assemble`
and `PeerStore::mint`/`observe`). Both currently inline
`SystemTime::now().duration_since(UNIX_EPOCH)` with manual `Error::Io` /
`Error::ClockExhausted` mapping.

Add `zendb-types::utils::time::physical_ms()` returning `u64` milliseconds
since the Unix epoch. It centralizes the `SystemTime` → `u64` conversion and
the overflow handling. Workspace call sites consume it instead of inlining
`SystemTime::now()`.

There is no file-lock utility module. `Bootstrap` inlines the
`std::fs::File::try_lock` call directly; the lock is a single-use bootstrap
concern, not a reusable utility.

## 9. Implementation Sequence

Each phase should compile before the next one. No tests are added unless
explicitly requested.

### Phase 1: PeerIdentity trait and Signature

- Add `PeerIdentity`, `Signature`, `SigningError`, and `LocalPeerIdentity`
  to `zendb-types::identity`.
- Add `LocalPeerIdentity::generate()` wrapping an ed25519 `Keypair`.
- Implement `PeerIdentity` for `LocalPeerIdentity`.
- Export the new types from `zendb-types`.

### Phase 2: wall-time utility

- Add `zendb-types::utils::time::physical_ms()` returning `u64`.
- Update workspace call sites to consume it instead of inlining
  `SystemTime::now().duration_since(UNIX_EPOCH)`.

### Phase 3: Bootstrap type

- Add `Bootstrap` to `zendb-workspace` with `create`, `open`, `join`, and
  `workspace_id`.
- Inline the `std::fs::File::try_lock` call in `Bootstrap`; no separate
  file-lock utility module.
- Remove `WorkspaceIdentity` and `WorkspaceLock` from `workspace.rs`.
- `Bootstrap` is `pub(crate)`; only `Workspace` consumes it.

### Phase 4: Workspace constructors

- Change `Workspace::create` and `Workspace::open` to take
  `Arc<dyn PeerIdentity>`.
- Add `Workspace::join` taking `WorkspaceId`, `Arc<dyn PeerIdentity>`, and
  `JoinHints`.
- Store `Arc<dyn PeerIdentity>` on `Workspace`; pass it into `Devices`.
- Remove `PeerId::generate()` and `WorkspaceId::generate()` calls from
  `workspace.rs`.

### Phase 5: Devices constructor update

- Change `Devices::create` and `Devices::open` to take
  `Arc<dyn PeerIdentity>`.
- Replace bare `local_peer_id` usage with `peer.peer_id()`.
- Keep `Devices::local_peer_id()` returning `PeerId`.

### Phase 6: integration test and documentation

- Update `zendb-it` to construct a `LocalPeerIdentity` and pass it to
  `Workspace::create` / `Workspace::open`.
- Update `zendb-types` and `zendb-workspace` READMEs.
- Run `cargo fmt --all`, `cargo check --workspace`, and
  `clippy --workspace --lib --no-deps -- -D warnings`.

## 10. Resolved Implementation Decisions

1. `PeerIdentity` is a trait in `zendb-types`, not a concrete struct, so the
   application can swap key stores without workspace changes.
2. `LocalPeerIdentity` is the default in-memory impl in `zendb-types` so
   tests and local-only deployments do not pay boilerplate.
3. `Bootstrap` is `pub(crate)` in `zendb-workspace`. The public surface is the
   `Workspace` constructors.
4. `Workspace::join` persists the caller-provided `WorkspaceId` and otherwise
   behaves like `create` minus `WorkspaceId` generation. Networking is out of
   scope for this iteration.
5. `JoinHints` is a placeholder type with a `Default` impl. Fields are added
   in the replication iteration.
6. The workspace stores `Arc<dyn PeerIdentity>` and never sees private key
   material directly.
7. There is no file-lock utility module. `Bootstrap` inlines the
   `std::fs::File::try_lock` call directly.
8. `PeerId::generate()` and `WorkspaceId::generate()` remain on the types;
   they are no longer called from `Workspace::create`.
9. `WorkspaceConfig` is an empty placeholder struct retained for future
   workspace-level configuration; it carries no fields in this iteration.
10. `zendb-types::utils::time::physical_ms()` centralizes wall-clock reads;
    workspace call sites consume it instead of inlining `SystemTime::now()`.

## 11. Completion Criteria

Iteration 0003 is complete when:

- `PeerIdentity`, `Signature`, `SigningError`, and `LocalPeerIdentity` are
  exported from `zendb-types`;
- `LocalPeerIdentity::generate()` produces a usable `PeerIdentity` without
  the caller handling private key bytes;
- `Bootstrap` owns the workspace lock and the `WorkspaceId` blob, with
  `create`, `open`, and `join` constructors;
- `WorkspaceLock` and the old `WorkspaceIdentity` struct are gone;
- `Workspace::create`, `Workspace::open`, and `Workspace::join` take
  `Arc<dyn PeerIdentity>` (and `WorkspaceId` + `JoinHints` for join);
- `Devices` is constructed with `Arc<dyn PeerIdentity>` and uses
  `peer.peer_id()` for minting;
- the workspace never stores or generates a private key;
- `Bootstrap` inlines the `std::fs::File::try_lock` call (no file-lock
  utility module);
- `zendb-types::utils::time::physical_ms()` centralizes wall-clock reads;
- `WorkspaceConfig` is an empty placeholder struct;
- `zendb-it` constructs a `LocalPeerIdentity` and passes it to the workspace
  constructors;
- the workspace compiles, clippy is clean, and documentation reflects the
  new identity boundary.
