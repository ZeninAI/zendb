# Current Direction

> **Status: historical exploration.** The accepted records in
> [decisions/](decisions/) are normative. Implemented ADRs 001 through 007 and
> 009 supersede this document. ADR 008 remains proposed. Principal/OAuth models,
> broad transport traits, and similarly named deleted APIs below are not current.

This document merges the current conclusions from [ideas.md](C:\Users\cngru\Documents\Zenin\zendb\ideas.md) and [in-depth-operator.md](C:\Users\cngru\Documents\Zenin\zendb\in-depth-operator.md) with the later crate, sync, identity, and networking decisions.

It is intentionally narrower than `ideas.md`.

Its purpose is to answer one question:

> What are we actually building next, and what should stay fixed while we build it?

---

## 1. Locked decisions

These decisions should now be treated as the working baseline.

### 1.1 Keep the data layer generic

`zendb` stays a general-purpose local-first CRDT database.

We are **not** baking a product-specific `Note` type into the engine.

Reason:

- the current heterogeneous CRDT value model is the right substrate
- Zenin is broader than notes
- product semantics belong in schema and operators, not storage primitives

### 1.2 Operator control is a database concern

Operators are not just app glue.

The database must own:

- operator specs
- placement intent
- jobs
- leases
- checkpoints
- permissions

The runtime that executes an operator remains device-local, but the control plane is part of the database model.

### 1.3 Shared truth and local runtime state stay separate

We are keeping a hard split between:

- replicated shared state
- local durable runtime state
- ephemeral in-memory execution state

This is one of the most important constraints in the whole design.

### 1.4 `zendb-engine` should become the top-level integration crate

The intended final dependency direction is:

```text
zendb-types
zendb-storage
zendb-identity
zendb-transport
zendb-sync
        \
         -> zendb-engine
```

Meaning:

- lower crates define reusable abstractions and data structures
- `zendb-engine` composes them into the concrete database runtime
- lower crates should not depend on `zendb-engine` long-term

Important note:

The current code is not fully there yet. In particular, `zendb-sync` still has integration that should eventually become trait-based instead of engine-coupled.

### 1.5 Networking is not a dumb pipe

The transport layer must support:

- topology-aware peer graphs instead of naive full broadcast
- multi-bearer connectivity
- path handoff between bearers
- optional centralized coordination
- operation without centralized coordination

So networking is not just socket IO. It is part of the system architecture.

### 1.6 OIDC is for bootstrap and online identity, not the whole permission model

We should assume standard OAuth 2.0 + OIDC flows for login.

But workspace authority should not depend only on remote OIDC scopes at runtime.

The correct split is:

- OIDC authenticates a user to an identity provider
- the service or trusted issuer maps that to workspace claims
- the workspace issues durable device/workspace credentials
- peer-to-peer and offline access use those workspace credentials

So OIDC is part of onboarding and service integration, not the whole offline trust model.

### 1.7 Roles are coarse defaults, not the full policy system

Roles like `Owner`, `Admin`, `Editor`, `Viewer`, `OperatorAdmin` are useful.

But they are only:

- a compact membership language
- a default capability envelope

They are not sufficient for:

- per-note restrictions
- per-operator authority
- device restrictions
- external tool restrictions

Those need policy objects layered above roles.

### 1.8 Stable synced event identity is useful but not the same as causal ordering

We are **not** introducing stable event identity to force global causal ordering.

Offline edits, reordering, and later reconciliation are still expected.

Stable synced event identity is only useful for:

- replication deduplication
- range replay
- checkpoints
- leases and job progress
- auditability

This remains important, but it should not be confused with the CRDT merge model.

### 1.9 Local topics remain important

Per-table local topics are still the right substrate for:

- local indexers
- local caches
- device-local runtime processing
- resumable workers

They should remain.

What changes is that they should not be treated as the complete distributed operator/sync model.

---

## 2. Crate-level structure

This is the current intended crate split.

## 2.1 `zendb-types`

Owns:

- CRDT values
- paths
- patches/events
- timestamps/HLC-related primitives
- low-level shared data encodings

Should not know about:

- OIDC
- transport
- membership workflows
- operator placement

## 2.2 `zendb-storage`

Owns:

- durable storage
- local table/topic persistence
- backend mechanics

Should not know about:

- user identities
- peer networking
- workspace onboarding

## 2.3 `zendb-identity`

Owns:

- user, device, workspace, and invite IDs
- memberships and roles
- peer claims
- workspace credentials
- bootstrap bundle formats
- OIDC-facing types and mapping surfaces

Should not own:

- transport sessions
- replication semantics
- engine runtime behavior

## 2.4 `zendb-transport`

Owns:

- network endpoints
- discovery providers
- rendezvous services
- handshake protocol objects
- authenticated session abstractions
- bearer-neutral transport framing

Should not own:

- CRDT merge
- replication journal logic
- workspace materialization

## 2.5 `zendb-sync`

Owns:

- replica identity
- sync summaries
- version vectors or equivalent digest structures
- journal envelopes
- snapshot metadata
- sync message shapes

Long-term it should depend on storage/identity/transport abstractions, not directly on `zendb-engine`.

## 2.6 `zendb-engine`

Owns:

- database lifecycle
- table registry
- local operator runtime
- operator control-plane integration
- sync integration
- identity integration
- permission evaluation
- product/system table orchestration

This is the crate where all other pieces should come together.

---

## 3. Operator direction

This is the current operator model we are converging on.

## 3.1 Two-layer operator model

Every operator has two forms:

1. **control-plane object**
   - shared intent
   - durable
   - possibly replicated
2. **runtime worker**
   - local realization
   - ephemeral or device-durable
   - derived from control-plane state plus device capabilities

That means:

```text
operator spec exists
    -> reconciler evaluates desired placement
        -> local worker may start
            -> worker may produce shared writes or local outputs
```

## 3.2 Operator classes

We should keep the class split from the earlier docs.

### `LocalIndexer`

Examples:

- full-text index
- semantic embedding cache
- local UI/materialized search helpers

Properties:

- local only
- never replicated as outputs unless explicitly promoted
- may depend on device-local files or indexes

### `SharedMaterializer`

Examples:

- backlink counts
- related-node edges
- shared summaries written into shared tables

Properties:

- writes canonical shared derived data
- should be deterministic where possible
- should usually have singleton or bounded placement

### `ExternalRunner`

Examples:

- AI prompt execution
- calling Copilot CLI
- web/API calls
- shell or browser automation

Properties:

- nondeterministic
- expensive
- usually job-based
- strongly permissioned

### `AssistantAction`

Examples:

- user-triggered rewrite
- one-off summarize command
- explicit generate-flashcards action

Properties:

- user initiated
- may internally create jobs
- not necessarily a standing background operator

## 3.3 Do not give Rhai ambient authority

Rhai should not directly own unrestricted OS/network powers.

Better model:

- Rhai expresses operator logic
- host-provided capabilities expose bounded tools
- capability usage is checked against policy
- external side effects flow through jobs, runners, and audit paths

This is mandatory if the system is going to execute AI-generated operators.

## 3.4 Operator spawning remains allowed

Operators should be able to create:

- other operator specs
- jobs
- derived artifacts
- audit records

But they should do that by writing durable control-plane state, not by directly mutating runtime internals.

---

## 4. Sync direction

## 4.1 Sync model

The sync system should be based on:

- anti-entropy between replicas
- signed or attributable replicated envelopes
- snapshot bootstrap
- incremental journal catch-up
- eventual convergence

We are not building a strict globally ordered event log.

## 4.2 Shared journal vs local topics

Keep both:

### Local topics

Use for:

- local operator input
- local resumability
- local indexing/materialization

### Shared sync journal

Use for:

- replica-to-replica exchange
- anti-entropy
- checkpointable shared processing
- replication recovery

## 4.3 Stable event identity

The current direction is:

- do not force a causal total order
- do add stable identity for synced records when we need robust replay/dedup/checkpoint semantics

This can be staged later.

It is not the first thing to build.

## 4.4 Shared failover

For shared singleton work:

- use leases
- start with snapshot-first failover
- add shared checkpoints later

This keeps the MVP tractable while leaving a path to more exact recovery.

---

## 5. Networking direction

This is the main refinement that came after the earlier operator docs.

## 5.1 Target transport behavior

The system should not assume one device talks directly to every other device.

Instead, the transport/rendezvous layer should build and maintain a partial overlay graph.

Goals:

- reduce burst fanout
- use strong peers as relays/routers
- adapt to observed bandwidth and latency
- avoid flooding the cluster

## 5.2 Multi-bearer connectivity

The same peer relationship may have multiple possible bearers:

- LAN/direct
- Bluetooth
- QUIC/P2P
- relay/tunneling service

The system should:

- rank candidate bearers
- select an active bearer
- monitor health/cost
- hand off when quality changes

This means a peer session should be abstracted from the underlying bearer.

## 5.3 Optional infrastructure

Central services are useful but optional.

Possible service roles:

- durable workspace/user/device registry
- rendezvous assist
- relay/tunneling fallback
- invite/bootstrap endpoint
- token exchange / credential issuance

But the system should also support reduced modes where some of those services are absent.

## 5.4 Recommended transport abstractions

At the abstraction level, the transport side should grow around:

- `DiscoveryProvider`
- `RendezvousProvider` (with an outbound hosted adapter in `zendb-external`)
- `TransportSession`
- bearer adapters
- path scoring / path selection
- overlay peer selection
- handoff controller

Conceptually:

```text
discovery -> candidate peers/endpoints
    -> rendezvous/approval
        -> authenticated session
            -> path health observation
                -> handoff if a better bearer/path exists
```

## 5.5 What networking should not do

The transport layer should not know:

- how CRDT merge works
- how operator placement works
- what a note means

It should expose reliable enough peer/session abstractions for the upper layers.

---

## 6. Identity and onboarding direction

## 6.1 Three identities matter

We should consistently model:

- **user identity**: human account
- **device identity**: concrete device installation with keys/capabilities
- **workspace identity**: the shared workspace/security boundary

These are different objects and should remain different.

## 6.2 Onboarding flow

The practical onboarding model should be:

1. user authenticates via OIDC or another configured provider
2. service or trusted issuer resolves workspace membership
3. device gets registered
4. device receives workspace bootstrap material and credential
5. device joins replication and transport graph

If there is no central service, bootstrap can come from invite bundles or direct trusted transfer.

## 6.3 Permissions

Permission evaluation should be intersection-based:

- membership role
- workspace policy
- operator policy
- device capability
- per-object overrides where present

That is the correct model for high-trust automation without ambient authority.

---

## 7. What exists now vs what should change next

## 7.1 What already exists in the repo

Now present:

- `ideas.md`
- `in-depth-operator.md`
- `zendb-identity`
- `zendb-transport`
- `zendb-sync`

Those crates currently define the shape of the future architecture.

## 7.2 What is still structurally incomplete

The main gaps now are:

1. `zendb-sync` still needs to be pushed toward trait-based integration instead of engine-coupled assumptions
2. transport is still type-level scaffolding, not path-management logic
3. identity has types, but not the full issuance/verification/runtime workflow
4. engine does not yet own the full control-plane reconciler
5. shared system tables/catalog wiring is not implemented end to end

---

## 8. Immediate implementation direction

This is the smallest credible sequence from here.

## Step 1: normalize crate boundaries

Goal:

- make the dependency direction match the intended architecture

Changes:

- remove direct `zendb-engine` assumptions from `zendb-sync`
- define storage/engine-facing traits that `zendb-engine` implements
- keep data types in the lower crates

This is the highest-value cleanup because it prevents architectural drift.

## Step 2: formalize engine-facing integration traits

Add explicit traits for:

- reading sync journal state
- publishing replicated writes
- opening local topic streams
- registering device identity/capabilities
- evaluating operator policy

This creates a stable seam between crates without collapsing them back into one crate.

## Step 3: add system catalogs to the engine

Implement engine-owned system tables for:

- devices
- memberships
- policies
- operator specs
- operator jobs
- operator leases

Do this without changing the CRDT substrate.

## Step 4: add the operator reconciler

Implement:

- desired operator state
- local capability matching
- local worker start/stop decisions

At this stage, keep failover simple.

## Step 5: add job-based external execution

Move external side effects behind:

- job objects
- host capability adapters
- result records
- audit logs

This is the step that makes AI/tool integration sane.

## Step 6: deepen transport from scaffolding into policy

Add:

- bearer capability summaries
- path scoring
- session health tracking
- handoff decisions
- overlay neighbor selection

This should happen before building too much service-dependent sync behavior.

## Step 7: add bootstrap and credential lifecycle

Complete:

- invite flow
- device registration
- workspace credential issuance
- peer credential verification
- revocation hooks

## Step 8: add shared journal/checkpoint evolution

Only after the earlier pieces are stable:

- add robust shared journal flow
- add better failover
- add stable shared checkpoints
- add stronger dedup/replay identity

---

## 9. What we are not doing right now

To keep the surface incremental, we should explicitly defer:

- a built-in product-specific note type
- full causal event ordering
- perfect shared failover correctness on day one
- a mandatory central server
- per-note policy complexity in the first slice
- a fully general operator DSL runtime with unrestricted host access

---

## 10. Practical current focus

If I were driving the repo from here, I would treat the current focus as:

1. keep the base database generic
2. clean up the crate boundaries so `zendb-engine` is the top-level integrator
3. make sync and transport abstractions real enough to support future networking choices
4. bring operators into a proper engine-native control plane
5. only then expand AI/external execution and hosted coordination

That is the narrowest path that still preserves the long-term Zenin architecture.

---

## 11. Canonical API Boundary (Final)

This section supersedes earlier sketches wherever names or ownership differ.

### 11.1 Client-only crate ownership

| Crate | Owns | Must not own |
|---|---|---|
| `zendb-types` | CRDT values, IDs, authorization vocabulary, operator desired objects, leases, jobs, checkpoints | I/O, storage, sessions, OIDC clients, hosted service protocols |
| `zendb-identity` | device keys, credentials, invites, bootstrap evidence, local verification/admission interfaces | a mandatory identity server |
| `zendb-transport` | client discovery, rendezvous providers, raw bearers, handshakes, authenticated sessions | policy evaluation, CRDT sync, server handlers |
| `zendb-sync` | replication messages and engine integration traits | engine storage, hosted scheduling, server coordination |
| `zendb-engine` | local database, policy snapshot, catalogs, reconciler, worker runner, capability host | a required cloud control plane |
| `zendb-external` | optional outbound client adapters for hosted credential, discovery, rendezvous, and job services | server implementations or server-side authority contracts |

The embedded database must be fully usable with only `zendb-types`,
`zendb-storage`, `zendb-engine`, and any explicitly selected peer adapters.

### 11.2 Final operator object model

`OperatorSpec` is the durable desired object. It contains the operator class,
source reference, canonical config bytes, inputs, trigger, placement policy,
permission request, approval policy, retry policy, and output policy.

`OperatorRuntimeConfig` is only the local worker adapter: compiled topic
subscriptions and polling tuning. It is not cluster desired state.

`OperatorObservation` is status and may be rebuilt after restart. It must never
be used to overwrite the spec. `Workspace` owns local worker start/stop,
observations, leases, jobs, and checkpoints; `LeaseConsistency` records whether
lease fencing is advisory or authoritative; and the pure
`plan_reconciliation` function computes actions.

The Workspace-owned job catalog is the durable boundary for nondeterministic
work. A job is not an inline capability call and a result is not accepted
merely because a worker produced bytes. The result must retain the job identity,
claim epoch, output hash, sensitivity, and producing device.

### 11.3 Final reconciliation loop

1. Read a coherent snapshot of specs, observations, device capabilities, leases,
   policy admissions, policy epoch, and current time.
2. Reject disabled, deleted, expired, unauthorized, or unsupported specs.
3. Match placement and required capabilities against authenticated local
   device facts.
4. Acquire or renew a fenced lease for singleton/shared classes.
5. Produce a plan without mutating desired state.
6. Apply start/stop/renew/release actions through separate stores and runner
   interfaces.
7. Persist observations only after the local action crosses its own boundary.
8. On handoff, stop the old holder, verify the new lease epoch, restore the
   latest compatible checkpoint or snapshot, then resume input processing.

Lease expiry is not proof that a previous process stopped. Every shared write
must carry the current fencing token, and the receiving database must reject a
stale epoch.

### 11.4 Rhai and capability boundary

Rhai receives change values and a narrow queued-write API. `RhaiOperatorConfig`
contains execution limits, requested capability IDs, and a declared write mode.
The write mode is not authority: the effective permission is the intersection
of operator spec, creator, principal, device trust, workspace policy, class,
resource sensitivity, and current policy epoch.

Rhai has no ambient filesystem, process, browser, network, or arbitrary shell
access. `CapabilityHost` exposes named bounded adapters and executes what is
actually installed on this device.
External side effects should normally be represented by `OperatorJob` records.

### 11.5 External services

No trait in the embedded database acts as a server handler. `zendb-external`
contains only outbound client interfaces. Hosted responses are reachability or
transport hints and must still pass local credential verification, revocation,
authorization, handshake, and durable admission checks.

### 11.6 Trait budget

Traits are retained for real substitution boundaries: CRDT extensions,
authorization, key stores, credential providers, transport bearers, sync
storage, executors, native operators, external adapters, and workspace control
effects. Local algorithms are concrete: `PathSelector`,
`plan_reconciliation`, Workspace-owned operator control, and
`CapabilityHost` are concrete engine boundaries. A dummy trait is not a design
goal.
