# In-Depth Operator Stack Redesign

This document explains how I would implement the new operator stack described in [ideas.md](C:\Users\cngru\Documents\Zenin\zendb\ideas.md).

This is not exact Rust code. It is implementation-oriented pseudo-code plus explanations, written against the current `zendb` codebase so that the changes are understandable and incremental.

The goal is to answer:

- what exists today
- what new pieces need to exist
- where they fit into the current crates
- how to implement them in small steps
- what each step changes semantically

---

## 1. Current operator stack in the repo

Today the operator stack in `zendb-engine` is roughly:

1. `Workspace::dispatch_operator(...)` persists an operator config and maybe starts a worker.
2. `Workspace::build_worker(...)` opens topic consumers for matching tables.
3. `OperatorWorker` holds:
   - input consumers
   - timer inbox
   - lifecycle event queue
   - published facet
4. `run_loop::run(...)`:
   - creates the operator instance
   - calls lifecycle hooks
   - polls changes from inputs
   - commits topic offsets after success
   - tears down on finish/failure/cancel

Relevant current files:

- [zendb-engine/src/workspace/operators.rs](C:\Users\cngru\Documents\Zenin\zendb\zendb-engine\src\database\operators.rs)
- [zendb-engine/src/operator/worker.rs](C:\Users\cngru\Documents\Zenin\zendb\zendb-engine\src\operator\worker.rs)
- [zendb-engine/src/operator/run_loop.rs](C:\Users\cngru\Documents\Zenin\zendb\zendb-engine\src\operator\run_loop.rs)
- [zendb-engine/src/operator/traits.rs](C:\Users\cngru\Documents\Zenin\zendb\zendb-engine\src\operator\traits.rs)

### Current mental model

The current system treats an operator as:

- a durable config entry
- plus a local runtime worker

This is good for:

- local indexing
- local scripting
- table-driven materialization on one replica

This is not enough for:

- operators as native durable database objects
- operator spawning
- device-aware placement
- job-based external AI/tool execution
- shared singleton ownership
- lease-based failover

So the redesign does not throw away the runtime. It puts a control plane above it.

---

## 2. Target mental model

After the redesign, an operator should be understood as two separate things.

### A. Operator control plane object

Durable, database-native, and replicated if the operator is shared.

This is the **desired state**:

- what operator exists
- whether it is enabled
- who created it
- what class it is
- where it is allowed to run
- what it may read/write
- whether it produces jobs

### B. Local operator runtime worker

Ephemeral, device-local, and derived from the desired state.

This is the **realized state**:

- is this device running it
- what inputs are attached locally
- what local state is open
- what timers are pending locally
- whether the worker is healthy

### New principle

Current model:

```text
dispatch_operator() -> create worker -> worker is the operator
```

Target model:

```text
create operator spec -> reconciler decides if this device should run it -> worker is a local realization
```

That is the core change.

---

## 3. New concepts to add

I would add the following concepts first.

## 3.1 Operator classes

These are semantic categories. They affect runtime behavior.

```text
enum OperatorClass {
    LocalIndexer,
    SharedMaterializer,
    ExternalRunner,
    AssistantAction,
}
```

Meaning:

- `LocalIndexer`
  - runs on every eligible device
  - writes only local state
- `SharedMaterializer`
  - consumes shared changes
  - writes shared derived state
- `ExternalRunner`
  - performs nondeterministic or external work
  - usually through jobs
- `AssistantAction`
  - user-invoked, often short-lived

## 3.2 Operator spec

This becomes the canonical desired state object.

```text
struct OperatorSpec {
    id: OperatorId,
    name: String,
    version: u64,
    enabled: bool,
    class: OperatorClass,
    source_kind: SourceKind,
    source_ref: SourceRef,
    subscriptions: Vec<Subscription>,
    trigger_mode: TriggerMode,
    placement: PlacementPolicy,
    permissions: PermissionRequest,
    approval_policy: ApprovalPolicy,
    retry_policy: RetryPolicy,
    outputs: OutputPolicy,
    parent_operator_id: Option<OperatorId>,
    created_by_user_id: UserId,
    created_at_ms: u64,
}
```

This is not the same thing as the current `OperatorEntry { config, phase }`.

## 3.3 Placement policy

```text
enum PlacementMode {
    EveryDevice,
    SingletonAnyCapable,
    SingletonPerUser,
    PinnedDevice,
    ManualOnly,
}

struct PlacementPolicy {
    mode: PlacementMode,
    required_labels: Vec<String>,
    required_capabilities: Vec<CapabilityId>,
    preferred_labels: Vec<String>,
    fallback_allowed: bool,
    pinned_device_id: Option<DeviceId>,
}
```

## 3.4 Operator job

This is for external/nondeterministic work.

```text
enum JobStatus {
    Pending,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

struct OperatorJob {
    job_id: JobId,
    operator_id: OperatorId,
    input_ref: InputRef,
    idempotency_key: String,
    status: JobStatus,
    claimed_by: Option<DeviceId>,
    attempt: u32,
    created_at_ms: u64,
    started_at_ms: Option<u64>,
    completed_at_ms: Option<u64>,
}
```

## 3.5 Operator lease

This is for singleton/shared ownership.

```text
struct OperatorLease {
    operator_id: OperatorId,
    shard_id: String,
    holder_device_id: DeviceId,
    epoch: u64,
    expires_at_ms: u64,
}
```

## 3.6 Device capability summary

This is the bridge between operator placement and device-local runners.

```text
struct DeviceCapabilitySummary {
    device_id: DeviceId,
    labels: Vec<String>,
    capabilities: Vec<CapabilityId>,
    trust_tier: TrustTier,
    last_seen_ms: u64,
}
```

---

## 4. Main architectural change

The main architectural change is adding a **reconciler**.

This reconciler lives in `zendb-engine` and continuously answers:

```text
Given:
  - current operator specs
  - device capability summary
  - memberships / policy
  - local worker state
  - leases / jobs

What workers should this device be running right now?
```

This moves the engine from:

```text
imperative spawn model
```

to:

```text
desired-state reconciliation model
```

---

## 5. What I would keep unchanged initially

To keep the change surface small, I would explicitly preserve these pieces in the first phases:

1. the `Operator` trait shape
2. the `DispatchOperator` enum pattern
3. `OperatorWorker`
4. `run_loop::run(...)`
5. current topic polling and committing
6. current timer model

In other words:

- do not rewrite worker execution first
- change how workers are selected and managed first

This is the most important implementation discipline in the redesign.

---

## 6. Step-by-step implementation plan

The rest of the document is organized as implementation phases.

Each phase describes:

- purpose
- code changes
- pseudo-code
- semantic effect

---

## Phase 0: tighten current semantics

### Goal

Make the current runtime less ambiguous before introducing a control plane.

### Changes

1. make operator writes explicit:
   - local-only request
   - shared-write request that must cross an effect gate
2. remove the unused `state_path` setting and make durable state explicit
3. document local-only vs shared table conventions

### Why this matters

Right now `Rhai` is too vague about whether emitted writes are local or shared.
That ambiguity will make later operator placement wrong.

### Pseudo-code

```text
enum WriteMode {
    LocalOnly,
    SharedAllowed,
}

struct PendingEvent {
    table: String,
    key: PrimaryKey,
    value: Option<Value>,
    write_mode: WriteMode,
}
```

Then in the `Rhai` apply path:

```text
fn apply_pending_event(event, effect_gate, db) {
    authorize_with_operator_policy(event, effect_gate)?;
    db.insert_event(...);
}
```

### Semantic effect

This does not change operator control yet.
It only makes operator side effects explicit.

---

## Phase 1: enrich operator metadata without changing execution

### Goal

Introduce richer operator definitions while keeping the current `dispatch_operator -> worker` behavior.

### Current code boundary

Today the main persistent struct is effectively:

```text
OperatorEntry {
    config,
    phase,
}
```

This is too small.

### Change

Split this into:

```text
OperatorSpecEntry
OperatorRuntimeEntry
```

### Proposed shapes

```text
struct OperatorSpecEntry<Cfg> {
    spec_id: OperatorId,
    name: String,
    config: Cfg,
    class: OperatorClass,
    placement: PlacementPolicy,
    permissions: PermissionRequest,
    approval_policy: ApprovalPolicy,
    outputs: OutputPolicy,
    enabled: bool,
    created_by: UserId,
}

struct OperatorRuntimeEntry {
    phase: OperatorPhase,
    local_worker_open: bool,
    last_error: Option<String>,
}
```

### Files to touch

- [zendb-engine/src/workspace/mod.rs](C:\Users\cngru\Documents\Zenin\zendb\zendb-engine\src\database\mod.rs)
- [zendb-engine/src/workspace/operators.rs](C:\Users\cngru\Documents\Zenin\zendb\zendb-engine\src\database\operators.rs)
- maybe add a new `operator/spec.rs`

### Pseudo-code

```text
pub fn create_operator_spec<V>(
    &self,
    name: &str,
    config: V::Config,
    runtime: OperatorRuntimeConfig,
    class: OperatorClass,
    placement: PlacementPolicy,
    permissions: PermissionRequest,
) -> io::Result<OperatorId>
```

At this phase, `dispatch_operator(...)` can simply become:

```text
fn dispatch_operator(...) {
    let spec_id = create_operator_spec(...)?;
    start_worker_immediately_if_possible(spec_id)?;
}
```

### Semantic effect

Operators become richer durable objects, but runtime behavior stays nearly the same.

---

## Phase 2: add a database-native reconciler

### Goal

Stop treating `dispatch_operator(...)` as the true primitive.

The true primitive becomes:

```text
persist operator spec
```

and the reconciler decides whether to run a worker.

### New component

Add a background reconciler loop in `Workspace`.

### High-level loop

```text
loop forever:
    desired = compute_desired_local_workers()
    actual = current_local_workers()

    to_start = desired - actual
    to_stop = actual - desired

    for each spec in to_start:
        start_worker_for_spec(spec)

    for each spec in to_stop:
        stop_worker_for_spec(spec)

    sleep / wait for invalidation
```

### What `compute_desired_local_workers()` does

Initially, very simple:

```text
for each operator spec:
    if !spec.enabled:
        skip
    if spec.mode == EveryDevice:
        desired = true
    else if matching input tables exist locally:
        desired = true
```

At this phase, you still do not need capabilities or leases.

### Code shape

Add something like:

```text
struct ReconciliationPolicy {
    dirty_flag,
    background_task,
}
```

Workspace methods that mutate operator specs should mark the reconciler dirty:

```text
fn update_operator_spec(...) {
    persist_change(...)
    self.operator_reconciler.mark_dirty()
}
```

### Files to touch

- `zendb-engine/src/workspace/mod.rs`
- new `zendb-engine/src/workspace/reconciler.rs`
- `zendb-engine/src/workspace/operators.rs`

### Semantic effect

This is the real turning point:

- operator specs become desired state
- workers become local realizations

---

## Phase 3: add device identity and capability registration

### Goal

Make placement meaningful without yet implementing full cross-device sync.

### New system state

Add internal state/tables for:

- device info
- capability summary

### Pseudo-code

```text
struct LocalDeviceContext {
    device_id: DeviceId,
    labels: Vec<String>,
    capabilities: Vec<CapabilityId>,
    trust_tier: TrustTier,
}
```

Expose registration:

```text
fn register_local_device_capabilities(&self, ctx: LocalDeviceContext)
```

Reconciler logic becomes:

```text
fn placement_matches(spec, local_device) -> bool {
    match spec.placement.mode {
        EveryDevice => matches_caps(spec, local_device),
        PinnedDevice => spec.placement.pinned_device_id == local_device.device_id,
        SingletonAnyCapable => matches_caps(spec, local_device),
        SingletonPerUser => matches_caps(spec, local_device),
        ManualOnly => false,
    }
}
```

### Files to touch

- new `zendb-engine/src/system/device.rs`
- reconciler
- maybe internal state catalog wrappers

### Semantic effect

This is the first time the engine can say:

- this operator may run here
- this operator may not run here

---

## Phase 4: add operator jobs for external work

### Goal

External AI/tool execution should stop being an inline side effect inside ordinary change processing.

### New rule

Shared materializers may directly write derived tables.

External runners should usually:

1. observe change
2. enqueue job
3. job executor claims and runs job
4. result is persisted

### New state

```text
_operator_jobs
_operator_results
```

### Example flow

Suppose an operator watches requirements documents and wants to proofread them using Copilot CLI.

Pseudo-code:

```text
fn process(changes):
    for change in changes:
        if matches_requirements_note(change):
            job = OperatorJob {
                job_id: new_id(),
                operator_id: self.id,
                input_ref: change.ref(),
                idempotency_key: hash(self.id, change.logical_ref),
                status: Pending,
            }
            db.enqueue_job(job)
```

Then a separate local job runner loop does:

```text
loop:
    jobs = db.list_claimable_jobs_for_device(local_device)
    for job in jobs:
        if claim_job(job):
            result = execute_job(job)
            db.write_job_result(job, result)
```

### New abstraction: host capability execution

Do not call raw shell/network from `Rhai`.

Instead:

```text
trait CapabilityHost {
    fn run_llm(req) -> Result<LlmOutput>;
    fn run_http(req) -> Result<HttpOutput>;
    fn run_named_shell_runner(req) -> Result<ShellOutput>;
}
```

### Semantic effect

The operator stack becomes safe for:

- retries
- audit logs
- idempotency
- eventual remote execution

---

## Phase 5: add stable synced event identity

### Goal

Prepare for real shared replication and shared operator checkpoints.

### Important clarification

This is not causal ordering.

It is just a stable name for a mutation.

### Proposed shape

Add optional or later-required metadata to `Event`:

```text
struct SyncEnvelope {
    origin_device_id: DeviceId,
    origin_seq: u64,
    author_user_id: UserId,
    signature: Vec<u8>,
}
```

Possible integration:

```text
struct Event {
    ...
    sync: bool,
    sync_meta: Option<SyncEnvelope>,
}
```

### Why add it

For:

- replication dedup
- anti-entropy range exchange
- checkpoint references
- auditability

Not for:

- merge correctness
- causality

### Semantic effect

Still no full sync protocol yet.
This phase just makes events capable of participating in one.

---

## Phase 6: add shared replication journal and version vectors

### Goal

Separate local operator feed mechanics from shared replica sync.

### Main rule

Do **not** overload current per-table local topics as the long-term distributed sync substrate.

### New component

Add a shared replication journal abstraction:

```text
struct SharedJournalRecord {
    event: Event,
}
```

And version-vector state:

```text
struct VersionVector {
    seen: Map<DeviceId, u64>,
}
```

### Replica protocol pseudo-code

```text
fn sync_with_peer(peer):
    auth(peer)
    local_vv = db.version_vector()
    remote_vv = peer.version_vector()

    missing_for_peer = diff(local_vv, remote_vv)
    missing_for_local = diff(remote_vv, local_vv)

    send_ranges(peer, missing_for_peer)
    receive_ranges(peer, missing_for_local)

    for event in incoming:
        verify(event)
        apply(event)
        update_version_vector(event)
```

### Relationship to local topics

After a shared event is applied to local tables, local per-table topics may still produce `Change` records for local operators.

That means:

```text
shared sync journal = replica-to-replica substrate
local table topic = local runtime/operator substrate
```

### Semantic effect

This is the first true cross-device sync foundation.

---

## Phase 7: add memberships and onboarding hooks

### Goal

Make operator placement and sync security depend on real identities, not just local configuration.

### New system state

```text
_users
_devices
_memberships
_invites
_policies
```

### Simplified first rule

For MVP:

- all workspace members may read full workspace content
- roles constrain actions, not read visibility

### Device bootstrap pseudo-flow

```text
fn onboard_device(inviter_device, new_device):
    new_device.generate_keypair()
    inviter_device.approve(new_device)
    inviter_device.issue_device_membership(new_device)
    inviter_device.send_snapshot_bundle(new_device)
    new_device.install_snapshot()
    new_device.start_replication()
```

### Semantic effect

Operators can now be evaluated against:

- membership
- role
- device trust
- workspace policy

---

## Phase 8: add singleton leases and shared failover

### Goal

Allow shared materializers and singleton operators to run on one logical owner at a time.

### New lease flow

```text
fn reconciler_consider_singleton(spec):
    if !placement_matches(spec, local_device):
        return false

    lease = db.get_lease(spec.id, shard="default")
    now = clock.now_ms()

    if lease == None or lease.expires_at_ms < now:
        try_claim_lease(spec.id)

    lease = db.get_lease(spec.id, shard="default")
    return lease.holder_device_id == local_device.device_id
```

### Worker start rule

```text
if spec.class == SharedMaterializer and local_device_holds_lease(spec):
    start worker
else:
    stop worker if running
```

### Failover model for MVP

Snapshot-first takeover:

```text
when lease holder changes:
    new holder rebuilds derived state from shared snapshot
    then resumes normal local incremental operation
```

### Why snapshot-first first

Because current topic offsets are local and not portable.

This keeps implementation small.

### Semantic effect

You gain:

- shared ownership
- device failover
- controlled singleton semantics

without yet needing perfect distributed incremental resume.

---

## Phase 9: add stable shared operator checkpoints

### Goal

Improve shared operator recovery efficiency.

### New state

```text
_operator_checkpoints
```

### Shape

```text
struct OperatorCheckpoint {
    operator_id: OperatorId,
    shard_id: String,
    last_processed_event: EventIdentity,
    updated_at_ms: u64,
}
```

### Processing pseudo-code

```text
fn process_shared_batch(changes):
    result = operator.process(changes)
    if result.success:
        checkpoint = max_event_identity(changes)
        db.store_checkpoint(operator_id, checkpoint)
```

### Restart flow

```text
fn restart_shared_operator(operator_id):
    cp = db.load_checkpoint(operator_id)
    replay shared events after cp
```

### Semantic effect

This avoids full rebuild on each shared operator failover.

This is useful, but not required for MVP.

---

## 7. How the current files change conceptually

This section maps the redesign onto the current engine files.

## 7.1 `workspace/operators.rs`

### Today

This file is mostly:

- imperative creation
- cancellation
- retirement
- direct worker bookkeeping

### After redesign

It should become a mix of:

- operator spec APIs
- worker start/stop helpers
- reconciler support
- lease/job helpers

### New responsibilities

```text
create_operator_spec(...)
update_operator_spec(...)
disable_operator(...)
enqueue_operator_job(...)
try_claim_operator_lease(...)
start_worker_for_spec(...)
stop_worker_for_spec(...)
```

`dispatch_operator(...)` can remain as a convenience wrapper, but it should internally create/update operator specs.

## 7.2 `operator/worker.rs`

### Today

Owns inputs, timers, lifecycle queue, facet, and spawn.

### After redesign

Mostly unchanged.

Possible additions:

- reference to `spec_id`
- reference to runtime kind
- maybe local worker metadata

This is good news: the worker abstraction is not the main problem.

## 7.3 `operator/run_loop.rs`

### Today

It creates the operator instance, processes lifecycle events, polls changes, handles timers, commits offsets, and retires.

### After redesign

Mostly unchanged for local execution.

Possible additions:

- hook for job executor worker flavor
- hook for shared checkpoint publish
- hook for lease-loss shutdown

Pseudo extension:

```text
if worker_lost_lease():
    worker.begin_shutdown(Cancelled or Active)
```

## 7.4 `operator/traits.rs`

### Today

The trait is strongly shaped around local input processing.

### After redesign

The trait can remain mostly stable.

I would avoid breaking it early.

Possible later additions:

- optional capability declaration
- optional control-plane hooks

But first I would keep the trait stable and express most new behavior in config/spec metadata.

---

## 8. The minimum vertical slice I would build first

If I had to implement the operator redesign end-to-end in the smallest useful slice, I would build this:

### Scope

1. richer operator spec
2. reconciler
3. device capability summary
4. one `LocalIndexer`
5. one `ExternalRunner` using jobs

### Example vertical slice

#### A. Local full-text index

- spec class: `LocalIndexer`
- placement: `EveryDevice`
- output: local state only

#### B. Requirements proofreader

- spec class: `ExternalRunner`
- placement: `SingletonAnyCapable`
- required capability: `shell.copilot_cli`
- output: `comments`
- execution style: jobs

### Why this slice

It validates:

- local per-device operator placement
- job-based external work
- control-plane to runtime mapping
- capability-aware scheduling

without requiring:

- full distributed checkpointing
- complex graph materializers
- full onboarding flows

---

## 9. Biggest implementation risks

If I were implementing this, the main risks would be:

## 9.1 Mixing control-plane and runtime concerns too early

Bad:

- stuffing leases, jobs, placement, and worker state directly into `OperatorWorker`

Good:

- keep worker local
- keep control plane durable and separate

## 9.2 Trying to solve full distributed correctness too early

Bad:

- exact shared incremental failover from day one
- exactly-once everything

Good:

- snapshot-first takeover
- idempotent jobs
- append and reconcile

## 9.3 Letting `Rhai` become ambient authority

Bad:

- direct unrestricted shell/network/filesystem APIs

Good:

- explicit capability-gated host runners

## 9.4 Making product schema concerns leak into storage primitives

Bad:

- special-case `Note` inside `zendb`

Good:

- generic data primitives
- stronger engine-native control plane
- product schema one layer above

---

## 10. Final recommended sequence

If I were implementing the whole new operator stack, this is the exact order I would follow:

1. tighten current semantics
2. add `OperatorSpecEntry` and split spec from runtime phase
3. add reconciler loop
4. make `dispatch_operator(...)` write desired state
5. add local device capability registration
6. add `OperatorJob` and `OperatorResult`
7. add capability-gated host runner abstraction
8. add stable sync metadata to events
9. add shared replication journal + version vectors
10. add memberships and device onboarding hooks
11. add singleton leases
12. add shared operator checkpoints

That sequence is important because it preserves the working local runtime while progressively moving orchestration into the database.

---

## 11. Bottom line

The redesign is not:

- "rewrite operators from scratch"

It is:

- keep the current worker/run-loop model
- add a durable operator control plane above it
- route external effects through jobs
- route singleton shared work through leases
- make placement device-aware
- separate local operator feeds from shared replica sync

That is how I would implement the new operator stack while keeping the change surface incremental and understandable.

---

## 12. Final Operator API Contract

This is the final API design for the client-side database. Earlier pseudo-code
that combines config, lifecycle, placement, and ownership into one record is
superseded.

### 12.1 Desired state

`zendb_types::OperatorSpec` is the only durable desired-state object. Its
fields are deliberately divided into:

- identity and generation;
- `OperatorClass` and `OperatorSource`;
- canonical operator config bytes;
- portable input selectors and trigger mode;
- `PlacementPolicy`;
- `OperatorPermissionRequest` and approval policy;
- retry and output policies;
- creator and timestamps.

The engine's `OperatorRuntimeConfig` is a derived local binding. It contains
compiled subscriptions and polling tuning only. It must not become a second
source of truth for placement, permissions, or shared output.

The generated dispatch set is the device-local source boundary. It maps a
portable `OperatorSource` and canonical config bytes to a native adapter.
Unsupported source types and API versions are normal unsupported observations,
not deserialization panics.

### 12.2 Status and execution

`Workspace` persists status, jobs, and checkpoints independently from the spec
and starts or stops its concrete `OperatorWorker` instances.
The existing `Operator` trait is the native Rust execution ABI used beneath
that runner; it is not the reconciler and not a permission model.

`ReconcileSnapshot` includes an `OperatorAdmission` decision for each desired
generation. The planner must never interpret a permission request as approval;
the admission decision comes from the current workspace policy snapshot.

The current `Workspace::dispatch_operator` remains a low-level embedded/test
convenience. The cluster-aware primitive is storing an `OperatorSpec` and
letting reconciliation decide whether this device should realize it.

### 12.3 Ownership and handoff

`Workspace` provides acquisition, renewal, and release for its local lease
records and explicitly records whether consistency is advisory or authoritative.
Each lease carries a monotonically increasing epoch and fencing token. The
Workspace stores the input version vector, state hash, operator generation,
shard, and lease epoch as checkpoint evidence.

The reconciler must not start a `SharedMaterializer` without the required lease.
On lease loss it stops before its next shared write. A replacement verifies the
new epoch, restores a compatible checkpoint or snapshot, and only then starts
processing. This is a client-side replicated protocol, not a server scheduler.

### 12.4 Jobs and capabilities

The Workspace-owned job catalog owns pending, claim, running, result, retry,
and cancellation transitions. Claims carry a device and claim epoch.
`OperatorJobResult` carries
an output hash, sensitivity, producing device, and claim epoch.

`CapabilityHost` reports and executes capabilities installed on the current device.
Its `execute` method runs one named capability invocation. The host is still
required to validate the exact invocation and local availability after policy
authorization. `CapabilityInvocation` is not itself a grant.

### 12.5 Rhai contract

Rhai has a persisted `RhaiExecutionPolicy` containing resource limits,
requested capability IDs, and `RhaiWriteMode`. Default mode is `LocalOnly`.
`SharedAllowed` only declares intent; the operator spec's output policy and
the authorization evaluator must still approve each effect.

The old unused `state_path` option is intentionally removed. Durable script
state must be an explicit engine state handle or a declared output, never an
unverified path supplied by a script. No raw OS, network, browser, process, or
shell API is registered in the Rhai engine.

### 12.6 External boundary

Hosted credentials, presence, rendezvous, and job transport are outbound client
adapters in `zendb-external`. They may return candidates or records, but the
embedded database remains responsible for signature verification, policy,
revocation, lease fencing, and durable admission. There are no server-side
interfaces in `zendb-engine`, `zendb-sync`, or `zendb-transport`.

### 12.7 Trait budget

The final operator surface intentionally does not expose one trait per system
table or algorithm. Workspace methods own the engine catalog, leases, jobs,
checkpoints, and worker lifecycle; `CapabilityHost` groups local capability
discovery and execution. `LeaseConsistency` remains an enum because advisory
and authoritative ownership have different semantics. Reconciliation is a
pure function and path selection is a concrete policy object.
