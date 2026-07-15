# Zenin V2 Architecture Proposals

> **Status: historical exploration.** The accepted records in
> [decisions/](decisions/) are normative. Implemented ADRs 001 through 007 and
> 009 supersede this document. ADR 008 remains proposed. Principal/OAuth models,
> broad transport traits, and similarly named deleted APIs below are not current.

This document distills the architecture decisions we discussed after reviewing the current `zendb` repository. The goal is to make the direction concrete, technically coherent, and implementable in small steps.

The central conclusions are:

- Keep `zendb` generic at the data layer.
- Make operators, sync, identity, and coordination first-class inside the database engine.
- Separate shared replicated truth from local runtime state.
- Add a small number of new primitives rather than rewriting the engine.

---

## Executive summary

### What should stay true

1. `zendb` should remain a general-purpose local-first CRDT database.
2. Zenin should build a concrete note/graph/workspace model on top of it.
3. Operators should become a native database subsystem, not app glue.
4. Sync should be signed event replication between local replicas, with an optional coordination service.
5. User onboarding, device onboarding, membership, and operator placement should be explicit system-level concepts.

### What should change

1. The current local operator runtime should evolve into a two-layer system:
   - **database-native operator control plane**
   - **device-local operator execution runtime**
2. The current local topic consumer model should remain for local incremental processing, but it should not be treated as the future distributed checkpoint model.
3. Synced events need stable identities.
4. The engine needs native system tables or catalogs for:
   - identities
   - memberships
   - devices
   - operator specs
   - operator jobs
   - operator leases
   - policies

---

## What the current engine already gives you

The current codebase already has the right lower-level separation:

- `zendb-types`: CRDT values, paths, events, HLCs
- `zendb-storage`: state backends and per-table durable change topics
- `zendb-engine`: tables, local operators, timers, facets, built-in `Rhai`

This means you already have a good **local execution kernel**.

### Strong parts of the current model

1. **Generic nested CRDT values**
   - good substrate for document bodies, metadata, graph nodes, and rich blocks

2. **Durable per-table change streams**
   - good for local operator processing and resumability

3. **Local durable operator state**
   - good for FTS, caches, materializers, and facets

4. **Compile-time operator kinds with runtime configs**
   - good for a bounded set of native execution engines such as:
     - `Rhai`
     - `FullTextIndex`
     - `MerkleTree`
     - future `Wasm`

### Limits of the current model

1. Table consumer offsets are local, not distributed truth.
2. Operators are currently spawned imperatively, not reconciled from desired state.
3. There is no native identity, membership, or device model.
4. There is no native placement, lease, or job model.
5. `Rhai` is good as a local scripting runtime, but not yet shaped for cross-device AI/tool execution.

---

## Architectural stance

There are two decisions that should be held simultaneously.

### 1. Keep the data layer generic

Do **not** add a concrete `Note` type to `zendb`.

Why:

- Zenin is not only a notes product.
- The current nested heterogeneous CRDT model is the right substrate.
- A hard-coded note type would leak product semantics into the wrong layer.

### 2. Make operator control native to the database

Do make **operators, sync, identity, onboarding, and placement** native database concerns.

Why:

- operators can create operators
- operators can create jobs
- operator permissions affect data safety
- operator placement affects product semantics
- sync and membership affect what operators are allowed to read and write

So the right split is:

- **generic storage/data model**
- **native database control plane**
- **product-level schema above that**

---

## Target system model

Zenin should be understood as four interacting layers.

## Layer 1: generic local-first database substrate

This is `zendb-types` + `zendb-storage`.

Responsibilities:

- CRDT values
- events
- tables
- durable local topics
- local state backends

## Layer 2: database-native control plane

This belongs in `zendb-engine`.

Responsibilities:

- operator specs
- operator jobs
- operator leases
- sync journal
- version vectors
- users/devices/memberships
- policies
- bootstrap/import/export

## Layer 3: device-local runtime

Also in `zendb-engine`, but conceptually separate from the control plane.

Responsibilities:

- local workers
- local FTS and caches
- local topic consumers
- local runner bindings
- local secrets
- local transport sessions

## Layer 4: Zenin product schema

This sits above the engine.

Responsibilities:

- `nodes`
- `edges`
- `comments`
- `tasks`
- graph UX
- note block schema
- templates
- domain-specific operators

---

## Data model decisions

## Shared replicated product tables

These should exist at the Zenin schema layer.

### `nodes`

Primary graph entity.

Fields:

- `id`
- `kind`
- `title`
- `body`
- `props`
- `labels`
- `created_at`
- `updated_at`
- `archived`
- `sensitivity`

Recommended shape:

- `title`: `Text` or `String`
- `body`: nested CRDT block/document tree
- `props`: `Record`
- `labels`: `Set` or `OrSet`

### `edges`

First-class relationships.

Fields:

- `id`
- `from`
- `to`
- `kind`
- `props`
- `created_at`
- `created_by`

### `comments`

Collaborative review layer.

### `tasks`

Explicit task objects.

### `derived_artifacts`

Shared derived outputs such as:

- summaries
- contradiction markers
- extracted tasks
- backlinks metadata

You can split this later if needed.

## Shared replicated system tables

These should be native engine-managed system tables or catalogs.

### `_users`

- `user_id`
- public keys
- status
- profile metadata

### `_devices`

- `device_id`
- `user_id`
- device public key
- labels
- trust tier
- status
- last_seen

### `_memberships`

- `workspace_id`
- `user_id`
- role
- status
- granted_by
- granted_at

### `_policies`

- workspace sync policy
- operator policy
- AI/tool policy
- data sensitivity policy

### `_invites`

- `invite_id`
- target identity or token
- role
- expiry
- inviter signature

### `_operator_specs`

- desired operator definitions

### `_operator_jobs`

- queued external or nondeterministic work

### `_operator_results`

- structured outputs and status

### `_operator_leases`

- singleton/shared operator ownership

### `_device_capabilities`

- sanitized device capability summaries

### `_audit_log`

- operator creation
- approvals
- side effects
- membership changes

## Local-only runtime state

These should remain local and not be treated as workspace truth:

- full-text index
- embeddings cache
- editor cursor/layout state
- prompt cache
- local runner configuration
- device secrets
- local operator worker state
- local topic consumer offsets
- temporary transport/session state

---

## Note and graph modeling

Use a hybrid model.

### What should live inside a node

- title
- document body
- node-local properties
- labels

### What should not be buried inside the note body

- backlinks
- relationship graph
- derived counters
- comments as the only source of collaboration state
- operator outputs that conceptually act like separate objects

### Practical example: backlinks

Do not update a note's frontmatter with backlink counts.

Instead:

1. parse links from note content
2. materialize `edges`
3. optionally materialize `backlink_count`
4. let the UI read from those derived tables

This reduces write amplification and keeps graph semantics first-class.

---

## Operator redesign

This is the most important redesign in the document.

The current engine treats operators as local runtime workers that are spawned imperatively. That is still useful, but it is not the right top-level model for Zenin.

The new model should be:

- operators are durable database objects
- workers are local realizations of those objects
- the engine reconciles desired operator state into live workers

## Two-layer operator architecture

### Operator control plane

Native to the database.

Responsibilities:

- store operator specs
- validate permissions
- reconcile enabled/disabled state
- place operators onto devices
- create and manage jobs
- manage leases for singleton work
- checkpoint shared operator progress
- record audit events

### Operator execution runtime

Device-local.

Responsibilities:

- attach to local table topics
- maintain local state
- process batches
- execute timers
- emit writes
- execute approved host capabilities

This keeps the runtime you already have, but gives it a real distributed control model.

## Operator classes

Not every operator should behave the same way.

### Class A: `LocalIndexer`

Runs on every eligible device.
Writes only local state.

Examples:

- full-text index
- local semantic cache
- viewport/search helpers

### Class B: `SharedMaterializer`

Consumes shared changes and writes shared derived knowledge.

Examples:

- backlinks
- contradiction markers
- extracted tasks
- auto-tags

### Class C: `ExternalRunner`

Consumes shared changes or queued jobs and executes tool/AI/network/browser effects.

Examples:

- Copilot CLI proofreading
- deal search
- browser automation
- local or cloud LLM generation

### Class D: `AssistantAction`

User-invoked and usually short-lived.

Examples:

- explain note
- generate operator from prompt
- rewrite paragraph

## Operator spawning

If operators can create operators, they should **not** directly start local workers.

They should create durable intent:

- create child operator spec
- enqueue child operator job

Then the database control plane decides:

- whether it is allowed
- whether approval is needed
- which device may run it
- whether it runs now or later

This is the safe and correct meaning of "operators can spawn operators".

## Proposed operator primitives

### `OperatorSpec`

Durable desired state.

Fields:

- `id`
- `name`
- `version`
- `enabled`
- `class`
- `source_kind`
- `source_code` or artifact ref
- `subscriptions`
- `trigger_mode`
- `placement_policy`
- `permissions`
- `approval_policy`
- `retry_policy`
- `outputs`
- `parent_operator_id`
- `created_by_user_id`

### `PlacementPolicy`

Start small:

- `EveryDevice`
- `SingletonAnyCapable`
- `SingletonPerUser`
- `PinnedDevice`
- `ManualOnly`

And add selectors:

- required labels
- required capabilities
- preferred labels
- fallback allowed

### `OperatorJob`

For expensive, external, or nondeterministic work.

Fields:

- `job_id`
- `operator_id`
- `input_ref`
- `status`
- `claimed_by`
- `attempt`
- `idempotency_key`
- `created_at`
- `started_at`
- `completed_at`

### `OperatorLease`

For singleton/shared ownership.

Fields:

- `operator_id`
- `shard_id`
- `holder_device_id`
- `epoch`
- `expires_at`

### `OperatorCheckpoint`

For shared resumability.

Fields:

- `operator_id`
- `shard_id`
- `checkpoint_kind`
- `checkpoint_value`

## Permission model for operators

Effective permission should be the intersection of:

- operator request
- workspace policy
- membership role
- device capability policy
- data sensitivity policy
- user approval state

Requested permissions should be explicit across:

- read scopes
- write scopes
- external capabilities
- execution style

Per-note overrides should be indirect through labels and policy classes, not arbitrary ACL sprawl.

## Rhai and the DSL boundary

`Rhai` should remain orchestration logic, not raw ambient authority.

That means:

- it may read input and produce output
- it may create jobs or child specs if allowed
- it may call approved named host capabilities
- it should not get arbitrary shell/network/filesystem escape hatches

Good host functions:

- `db.create_operator_spec(...)`
- `db.enqueue_job(...)`
- `db.emit_local(...)`
- `db.emit_shared(...)`
- `llm.generate(...)`
- `http.fetch(...)`
- `shell.exec_named_runner(...)`

Bad model:

- unrestricted process spawning
- unrestricted browser access
- unrestricted network/filesystem access

---

## Sync architecture

This needs to be explicit and technical.

The correct architecture is:

- local replicas on devices
- signed shared event replication between replicas
- optional central coordination service for discovery, relay, invite delivery, and convenience

The coordination service should not be the sole source of truth for workspace content.

## Shared sync journal vs local table topics

This distinction is critical.

### Local table topics

Keep the current per-table `Topic<Change>` model for:

- local operator processing
- local resumability
- local recovery

Do not treat local topic offsets as the distributed sync substrate.

### Shared sync journal

Add a separate shared replicated journal for synced events.

This journal is the basis for:

- peer replication
- deduplication
- version-vector exchange
- shared operator checkpoints

## Stable synced event identity

Synced events need a globally stable identity.

Add fields like:

- `origin_device_id`
- `origin_seq`
- `author_user_id`
- `signature`

Logical event identity:

- `event_id = (origin_device_id, origin_seq)`

This is necessary for:

- deduplication
- anti-entropy sync
- stable operator checkpoints
- auditability

## Replica sync protocol

Each replica should maintain a version vector:

- `device_id -> max origin_seq seen`

When two replicas connect:

1. authenticate device and workspace membership
2. exchange version vectors
3. request missing ranges
4. transfer missing signed events
5. verify signatures and policies
6. apply events
7. update version vector
8. optionally enter live tail mode

This gives you deterministic, replayable, eventually consistent sync.

## Initial bootstrap

Do not bootstrap by replaying all history by default.

Default bootstrap should be:

- state snapshot
- version-vector anchor
- tail of events after that anchor

This is the right default for mobile and large workspaces.

## Shared operator failover

Current local consumer offsets are not enough.

Two valid strategies exist:

### MVP strategy: snapshot-first takeover

When a new device takes ownership of a shared operator:

1. acquire lease
2. rebuild from current shared state snapshot
3. resume from now

Tradeoff:

- recomputation
- slower failover
- small implementation surface

### Long-term strategy: stable event checkpointing

Checkpoint shared operator progress against stable shared event identity, not local topic offsets.

That enables better resume behavior after device handoff.

Recommendation:

- start with snapshot-first
- add stable event checkpointing later

---

## Identity, permissions, and onboarding

These need to be part of the technical design, not product hand-waving.

## Identity model

Use three identities:

### User identity

- long-term user keypair
- represented in `_users`

### Device identity

- per-device keypair
- represented in `_devices`

### Workspace identity

- workspace id
- workspace metadata
- policy and membership state

## Membership and roles

Start simple.

Roles:

- `Owner`
- `Admin`
- `Editor`
- `Commenter`
- `Viewer`
- `OperatorAdmin`

For MVP, keep read visibility broad:

- all members can read full workspace content
- roles limit actions

This is much simpler than per-note encrypted access compartments.

## Central service role

The coordination service should provide:

- auth bootstrap
- invite delivery
- device discovery
- relay
- push notifications
- optional encrypted backup
- optional job claim arbitration for side-effect-heavy workloads

The workspace should still fundamentally exist without it.

## User onboarding flow

### Invite flow

1. admin creates signed invite
2. invite is delivered through service or direct sharing
3. invited user creates or authenticates identity
4. workspace membership is approved
5. membership is recorded in replicated system state
6. user enrolls first device

## Device onboarding flow

Preferred solution: QR pairing.

1. new device generates device keypair
2. existing trusted device opens bootstrap session
3. QR transfers rendezvous/bootstrap info
4. trusted device approves new device
5. trusted device sends:
   - signed device membership
   - encrypted workspace bootstrap bundle
   - snapshot
   - sync bootstrap info
6. new device installs snapshot and joins normal replication

## Revocation

Revocation is replicated system state.

### Device revocation

- mark device revoked
- peers reject future sync
- device cannot claim new operator work

### User revocation

- mark membership revoked
- peers stop future sync
- future bootstrap denied

For MVP, do not try to magically erase plaintext that was already synced to an authorized device in the past.

---

## What is synchronized and what is not

This needs to stay explicit.

## Synchronized

- user-authored knowledge
- graph structure
- comments
- tasks
- memberships
- policies
- operator specs
- operator jobs/results
- shared derived outputs
- device capability summaries
- audit summaries

## Not synchronized

- FTS indexes
- embeddings cache
- UI state
- device-local secrets
- runner local configuration details
- local topic consumer offsets
- local worker internals

## Conditionally synchronized

- summaries
- tags
- contradiction markers
- AI outputs
- deal results

These are synchronized if they are part of workspace truth; otherwise they stay local draft/cache state.

---

## Concrete implementation direction for current repo

This section translates the architecture into changes against the current crates.

## What should remain unchanged for now

Do not rewrite:

- CRDT value model in `zendb-types`
- table storage model in `zendb-storage`
- local operator worker runtime shape in `zendb-engine`

Those are not the first bottlenecks.

## What needs to be added

### In `zendb-types`

Add stable sync metadata to synced events:

- `origin_device_id`
- `origin_seq`
- `author_user_id`
- `signature`

This is the minimum needed for real replication and shared operator checkpoints.

### In `zendb-storage`

Do not change local table topics much initially.

Add, later, a shared replication journal abstraction rather than overloading local topics.

### In `zendb-engine`

Add native system state for:

- operator specs
- jobs
- leases
- devices
- memberships
- policies
- version vectors

Add a reconciler loop that:

- watches specs and memberships
- decides whether this device should run a worker
- starts/stops workers accordingly

Keep current worker execution as the local backend of that reconciler.

---

## Incremental implementation roadmap

This is the most important section for keeping the change surface small.

The guiding rule is:

- **first add metadata and internal structure**
- **then add one new control loop**
- **then add one new sync primitive**
- **only then broaden capabilities**

## Phase 0: tighten existing semantics

Goal: small cleanup before adding new concepts.

Implement:

1. make `Rhai` write mode explicit:
   - `LocalOnly`
   - `SharedAllowed`, subject to an effect gate
2. remove the unused `state_path` setting and make durable state explicit
3. clearly separate local-only vs shared table conventions in code/docs

Why first:

- small surface
- removes ambiguity
- makes later operator work less brittle

## Phase 1: enrich operator metadata without changing execution

Goal: preserve current runtime, add richer operator definitions.

Implement:

1. introduce internal `OperatorSpec` model
2. add fields for:
   - class
   - placement
   - permissions
   - approval policy
   - outputs
3. split persistent operator definition from local runtime phase

Keep:

- existing worker spawn model
- existing per-table subscriptions

Why:

- small schema/control change
- almost no change to local processing loop

## Phase 2: add a database-native operator reconciler

Goal: stop treating `dispatch_operator` as the only real primitive.

Implement:

1. engine control loop that reads operator specs
2. if spec applies to this device, start local worker
3. if spec no longer applies, stop local worker
4. keep `dispatch_operator` as a convenience wrapper that writes a spec

Outcome:

- operators become desired state
- workers become realizations

Why:

- this is the architectural turning point
- still small enough because it reuses current workers

## Phase 3: add device identity and capability registration

Goal: make placement meaningful.

Implement:

1. `_devices`
2. `_device_capabilities`
3. local device manifest
4. reconciler checks placement policy against device capability summary

Do not add full onboarding yet.
Just add internal capability-aware placement.

Why:

- unlocks `EveryDevice` vs `SingletonAnyCapable`
- still avoids full sync complexity

## Phase 4: add operator jobs for external work

Goal: keep external AI/tool work out of inline change callbacks.

Implement:

1. `_operator_jobs`
2. `_operator_results`
3. job claim and status transitions
4. `Rhai`/DSL APIs for enqueueing jobs
5. local runner interface for processing claimed jobs

Keep shared materializers separate from external jobs.

Why:

- isolates nondeterminism
- improves retries/audit/idempotency
- small additive change

## Phase 5: add stable synced event identity

Goal: prepare for real replica sync and shared operator checkpoints.

Implement:

1. event origin metadata
2. per-device origin sequence
3. signature verification plumbing

Do not build the full sync service yet.
Just make events capable of participating in it.

Why:

- foundational
- bounded surface in `zendb-types`

## Phase 6: add shared replication journal and version vectors

Goal: move from local-only assumptions to real cross-device sync substrate.

Implement:

1. shared sync journal
2. version vector state
3. peer diff/request protocol
4. snapshot + tail bootstrap path

Keep:

- current local table topics for local operators

Why:

- this is the right sync foundation
- avoids polluting local topics with distributed concerns

## Phase 7: add workspace membership and device onboarding

Goal: make the system usable across real users/devices.

Implement:

1. `_users`
2. `_memberships`
3. `_invites`
4. QR device bootstrap
5. snapshot-based device onboarding

Why:

- by now the sync substrate exists
- onboarding builds on top of it instead of forcing premature abstractions

## Phase 8: add singleton leases and shared operator failover

Goal: make shared materializers and singleton operators robust.

Implement:

1. `_operator_leases`
2. snapshot-first takeover
3. idempotent materializer write conventions

Do not start with stable event checkpoints immediately.

Why:

- lease + rebuild is enough for MVP shared ownership
- keeps the failure model understandable

## Phase 9: add stable shared operator checkpoints

Goal: improve failover efficiency and correctness.

Implement:

1. `_operator_checkpoints`
2. checkpoints keyed by shared event identity
3. resumable shared materializers without full rebuild

Why last:

- useful, but not required to validate the product
- depends on stable sync event identity already existing

## Phase 10: add optional hosted client adapters

Goal: improve usability, not redefine correctness.

Implement outbound adapters in `zendb-external` for:

1. auth bootstrap
2. invite delivery
3. relay
4. discovery/presence
5. push notifications
6. optional job claim arbitration

No server implementation belongs in this repository's embedded API. The
adapters are optional from an architectural perspective, even if they become
the default product experience.

---

## Recommended MVP boundary

To keep the surface manageable, the first serious Zenin MVP should support:

- generic CRDT storage
- Zenin product schema for `nodes`, `edges`, `comments`, `tasks`
- local operators
- operator specs as desired state
- capability-aware placement
- job-based external runners
- snapshot-based sync bootstrap
- signed synced events
- one simple role model
- QR device onboarding

Avoid in the MVP:

- arbitrary per-note ACLs
- fine-grained encrypted compartments
- unrestricted process execution
- sophisticated distributed checkpointing
- too many placement modes

Recommended minimal placement policies:

- `EveryDevice`
- `SingletonAnyCapable`
- `ManualOnly`

Recommended minimal capability classes:

- local/shared table access
- timers
- one local AI runner
- one cloud AI runner
- one shell-backed named runner

---

## Final recommendation

If reduced to the core technical decisions:

1. Keep `zendb` generic at the storage/data-type layer.
2. Make operators, sync, identity, and coordination native to `zendb-engine`.
3. Treat local table topics as local runtime feeds, not the global sync substrate.
4. Introduce stable signed synced event identities.
5. Model operators as durable specs plus local worker realizations.
6. Use jobs for external and nondeterministic work.
7. Use leases for singleton/shared operators.
8. Keep shared truth, shared derived state, and local runtime state explicitly separate.
9. Add features incrementally in the order above, so each phase is useful on its own.

That path keeps the architecture coherent and keeps the implementation surface narrow at each step.

---

## Final API Resolution

The exploratory proposals in this document resolve to the following public
contracts:

1. Shared operator intent is `zendb_types::OperatorSpec`.
2. Native operator config is an opaque canonical payload inside the spec;
   engine dispatch config is only a local adapter.
3. `OperatorObservation` is status, never desired state.
4. `plan_reconciliation` plans local actions; the concrete Workspace worker realizes them.
5. `LeaseConsistency` declares whether singleton ownership is advisory or authoritative.
6. Workspace-owned checkpoints make handoff recoverable without treating local
   topic offsets as distributed checkpoints.
7. Workspace-owned jobs are mandatory for nondeterministic/external work.
8. `CapabilityHost` exposes named device-local capabilities and never raw
   ambient authority to Rhai.
9. `zendb-external` contains outbound client adapters only; no server API is
   part of the embedded database.

### Rejected simplifications

- A single `OperatorEntry { config, phase }` cannot represent desired state,
  observed state, lease ownership, or handoff evidence.
- A boolean `running` flag cannot express placement failure, missing
  capability, approval, lease loss, or stale generation.
- A lease without an epoch/fencing token permits an expired worker to write
  after takeover.
- Inline external calls inside `process` cannot provide idempotency, retries,
  auditability, or safe failover.
- Rhai configuration cannot be treated as permission. It is a request and
  resource-limit declaration only.
- A hosted coordinator cannot be a hidden dependency of `Workspace`.

The current engine worker can remain as the native execution ABI, but its
creation must ultimately be driven by the local reconciler rather than by the
imperative dispatch call.
