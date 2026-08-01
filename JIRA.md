# Zenin Jira Operating Model And MVP Plan

Status: Proposed

Prepared: 2026-07-27

Planning horizon: 2026-07-27 through 2027-01-22

Team:

- Two developers
- One UI/UX designer
- One business/product owner

This document proposes how the Zenin Jira project should be configured and
used, and provides the ticket structure for the MVP plan. It does not
change Jira. Ticket identifiers such as `M2-REP-03` are planning identifiers,
not Jira issue keys.

## 1. Product And Architecture Context

Zenin consists of two related products:

1. **ZenDB** is the embedded local-first database. It stores heterogeneous
   key-value tables, resolves concurrent writes through CRDT values, exposes a
   durable change Topic for streaming consumption, and provides local typed
   State for non-replicated data.
2. **Zenin Desktop** is the first product built on ZenDB. It is a desktop
   second-brain and note-taking application. Tauri is the leading application
   framework because it preserves a Rust-native core while leaving a path to
   multiple desktop platforms.

The long-term platform adds peer-to-peer replication, distributed operators,
dynamic operator code, and AI-generated workflows. The MVP is a
thin vertical slice through those ideas:

- Milestone 1: reliable local ZenDB plus a useful local desktop notes app.
- Milestone 2: authenticated LAN synchronization over direct TCP connections.
- Milestone 3: one dynamic operator runtime, minimal distributed scheduling,
  and human-approved AI generation of operators.

### 1.1 Architectural Constraints That Must Be Explicit

The following are decision topics, not assumptions that implementation tickets
may silently settle:

- **"Lock-free" must be defined and demonstrated.** The current workspace
  orchestration uses mutexes and read-write locks. The project must distinguish
  a lock-free storage algorithm from a lock-free end-to-end database API and
  must not claim the latter without a progress guarantee and measurements.
- **Local State does not automatically move with an operator.** A stateful
  operator either remains pinned to one peer, rebuilds its state from durable
  table Topics, or uses a future checkpoint-transfer protocol.
- **A strict singleton cannot be guaranteed during a network partition without
  stronger coordination.** The MVP must define whether singleton means
  best-effort single execution while connected, deterministic ownership, or
  lease-based execution with possible duplicate work during partitions.
- **Exactly-once external side effects are not an MVP promise.** Operator input
  handling should be at-least-once with a durable input identity and
  idempotency/deduplication strategy.
- **Generated code is untrusted.** AI-generated operators require validation,
  resource limits, an explicit host API, and human approval before activation.
- **Replicated Tables and local States have different semantics.** LAN sync
  replicates Tables and their system metadata. It must not accidentally
  replicate local State.

## 2. Current Jira Project Audit

The connected Jira project is a team-managed software project named `Zenin`
with key `ZNIN`.

### 2.1 Available Issue Types

| Issue type | Hierarchy | Current use |
| --- | ---: | --- |
| Epic | 1 | Available, not used by the inspected tickets |
| Feature | 0 | Available, not used |
| Story | 0 | Available, not used |
| Task | 0 | Used by all current Zenin tickets |
| Bug | 0 | Available, not used |
| Subtask | -1 | Available, not used |

`Feature`, `Story`, and `Task` currently occupy the same hierarchy level.
Without an explicit distinction, this creates choice without useful structure.

### 2.2 Available Fields

The project exposes:

- Summary and Description
- Parent
- Assignee and Reporter
- Sprint and Rank
- Story point estimate
- Start date and Due date
- Labels
- Linked issues
- Flagged with the `Impediment` value
- Development integration
- Design integration
- Team
- Environment, attachments, comments, and other system fields

Priority, Components, and Fix Version appear in issue data but are not exposed
in the inspected creation/edit metadata. Existing tickets therefore all use
the default Medium priority and have no component or release target.

### 2.3 Current Workflow

The current global transitions allow:

- To Do
- In Progress
- In Review
- On Hold
- Done

`On Hold` overlaps with the existing `Flagged: Impediment` mechanism. The
project has no visible distinction between an unrefined backlog item and work
that is ready for a sprint.

### 2.4 Placeholder Backlog

The current issues were inspected only to understand the project
configuration. They are disposable placeholders and are not inputs to the
proposed backlog.

The new structure should be created from a clean Jira backlog:

- Do not convert placeholder Tasks into Epics or Decisions.
- Do not preserve their issue keys as roadmap anchors.
- Do not link new work to placeholders for historical continuity.
- Delete the placeholders before creating the hierarchy in section 7.

The proposed planning identifiers and ticket descriptions in this document
are the source for the replacement backlog.

## 3. Recommended Jira Configuration

### 3.1 Issue Types

Use the following issue types:

| Type | Use |
| --- | --- |
| Epic | A coherent outcome delivered over roughly two to six weeks |
| Decision | A ZDR or binding architecture/product decision |
| Story | User-visible or system-observable behavior with acceptance criteria |
| Task | Enabling technical, design, research, release, or operational work |
| Bug | Behavior that violates an accepted contract |
| Subtask | An optional implementation step owned by the same parent outcome |

Recommended changes:

1. Add a `Decision` issue type at the standard issue level. Use it for ZDRs.
2. Hide or remove `Feature`. It duplicates Story at the same hierarchy level.
3. Keep Subtasks optional. Do not decompose every Story into frontend/backend
   subtasks unless that separation creates different owners or review paths.

If adding a Decision type is inconvenient, use Task with the controlled `zdr`
label. Do not create both conventions.

### 3.2 Fields To Add Or Expose

Expose these fields on the create and edit screens:

| Field | Required use |
| --- | --- |
| Parent | Required for every Story and Task in MVP scope |
| Priority | Required on Bugs and sprint candidates |
| Fix Version | Required for every MVP ticket |
| Component | Required for every implementation ticket |
| Story point estimate | Required before engineering work enters a sprint |
| Design | Used on UI/UX Stories and Tasks |
| Linked issues | Used for ZDR gates and cross-Epic dependencies |
| Sprint | Set only during sprint commitment |

Use these Fix Versions:

- `0.1.0-m1-local-notes`
- `0.2.0-m2-lan-sync`
- `0.3.0-mvp-operators-ai`
- `post-mvp`

Use these Components:

- `ZenDB Types`
- `ZenDB Storage`
- `ZenDB Workspace`
- `Replication`
- `Operators`
- `Desktop App`
- `AI`
- `UX`
- `Build and Release`

If Components are unavailable in the team-managed project, add one
single-select `Area` field with those values. Do not use both.

### 3.3 Fields To Hide Or Restrict

- Hide `Team`; one Jira project and one four-person team make it redundant.
- Hide time-tracking fields. Use story points for engineering capacity and
  Git/Jira development links for progress.
- Show `Environment` only for Bugs.
- Use Start date and Due date only for Epics, Decisions, and release gates.
- Keep Design only on UI/UX work.
- Do not add custom Milestone or Platform fields. Fix Version and Component
  already answer those questions.

### 3.4 Priority Policy

| Priority | Meaning |
| --- | --- |
| Highest | Data loss, security boundary failure, corrupted replication, or a blocked milestone |
| High | On the milestone critical path or required for its exit criteria |
| Medium | Normal committed work |
| Low | Improvement that may leave the sprint without endangering the milestone |

Priority is not a substitute for Rank. Rank orders work within one priority.

### 3.5 Controlled Labels

Use Labels only for cross-cutting properties:

- `zdr`
- `mvp`
- `security`
- `performance`
- `reliability`
- `tech-debt`
- `spike`
- `stretch`
- `post-mvp`

Do not repeat Components or Fix Versions as labels.

### 3.6 Workflow

Recommended workflow:

1. **To Do**: refined enough to remain in the backlog, but not necessarily
   committed.
2. **In Progress**: actively owned work. Each developer should have at most one
   primary issue in this state.
3. **In Review**: code, design, or ZDR is ready for review.
4. **Done**: acceptance criteria are met and the result is integrated.

Remove `On Hold`. Use `Flagged: Impediment` and a comment describing the
blocker. This keeps blocked work visible without creating a status where work
can disappear indefinitely.

Backlog versus ready work should be represented by Sprint assignment:

- No Sprint: backlog.
- Current/future Sprint: refined and committed/planned.

### 3.7 Board And Filters

Use one delivery board for the four-person team. Add quick filters:

- `M1`, `M2`, `M3` by Fix Version
- `Database`, `App`, `UX`, `AI`, `Operators` by Component
- `Decisions` by issue type or `zdr` label
- `Bugs`
- `Blocked` by Flagged
- `Unassigned`

Do not create separate boards for each discipline. The major risk is
cross-discipline dependency, so the team needs one view.

## 4. Ticket Operating Rules

### 4.1 ZDR Gate

Every milestone has one primary Decision issue. It must:

- describe the problem and milestone outcome;
- list the decisions that implementation depends on;
- record considered alternatives and rejected options;
- define invariants, failure semantics, and security boundaries;
- define the public interfaces at a high level;
- define explicit non-goals;
- include diagrams and UI flows where relevant;
- identify open questions and owners;
- link the accepted document under `.plan/`;
- link to all implementation Epics using `blocks`.

The Decision moves to Done only after the relevant reviewers agree:

- Both developers for database/runtime decisions
- UI/UX for product interaction decisions
- Business/product owner for scope and user-facing acceptance

No implementation ticket blocked by a Decision may enter In Progress before
that Decision is Done. Time-boxed spikes may run earlier if they are explicitly
marked `spike`, produce evidence for the Decision, and do not establish a
production API.

### 4.2 Definition Of Ready

An implementation ticket is ready when:

- its parent Epic and Fix Version are set;
- blocking ZDRs are Done;
- the description states the outcome and relevant context;
- acceptance criteria are objectively checkable;
- dependencies are linked;
- the Component, Priority, and assignee are set;
- engineering work is estimated at eight points or fewer;
- required designs are linked;
- non-goals prevent accidental scope expansion.

### 4.3 Definition Of Done

An implementation ticket is Done when:

- all acceptance criteria are met;
- the implementation compiles with `cargo check --workspace --all-targets`
  where Rust is affected;
- relevant documentation, README files, and ZDR follow-ups are current;
- UI work matches the accepted design and covers empty, loading, error, and
  offline states;
- observable failure paths provide actionable errors;
- the change is reviewed and integrated;
- deferred work discovered during implementation is captured as a linked
  backlog issue rather than hidden in comments.

Verification automation for replication and operator scheduling is strongly
recommended even though the repository currently treats tests as opt-in.
Distributed failure behavior cannot be validated adequately by compilation
alone. Adopting that automation should be an explicit team decision.

### 4.4 Estimation And Capacity

- Use story points only for work consuming developer capacity.
- Do not point Epics or design/business-only tasks.
- Use `1, 2, 3, 5, 8`. Split work larger than 8.
- Recalibrate after Sprint 1 rather than treating initial points as hours.
- Reserve approximately 20 percent of developer capacity for integration,
  defects, and architecture discoveries.
- UI/UX should work roughly one sprint ahead of implementation.
- The business/product owner owns milestone acceptance and scope trade-offs,
  not implementation estimates.

Suggested WIP limits:

- One primary In Progress issue per developer
- Two In Progress issues for UI/UX
- No more than three team-wide issues waiting In Review

### 4.5 Suggested Role Ownership

These are responsibility defaults, not permanent silos:

- **Developer A, database/runtime lead:** ZenDB core, storage, workspace,
  replication admission, anti-entropy, and operator runtime.
- **Developer B, product/integration lead:** Tauri application service,
  libp2p application runtime, desktop integration, AI provider integration,
  and release packaging.
- **UI/UX:** information architecture, interaction design, prototypes, design
  review, and usability findings. Design should stay one sprint ahead.
- **Business/product:** milestone scope, reference workflows, acceptance
  scenarios, beta coordination, prioritization, and release sign-off.

Both developers review every ZDR and cross-review changes at public crate or
protocol boundaries. UI/UX and business/product review ZDR sections that alter
user flows, permissions, AI behavior, or MVP scope.

Suggested cadence:

- One 45-minute backlog refinement each week
- Sprint planning, review/demo, and retrospective every two weeks
- A dedicated ZDR review before each milestone gate
- A short product/design/development dependency check twice per week
- One internal user demo at the end of every sprint

## 5. Timeline

The schedule uses one planning week, eight three-week sprints, and a short release
buffer.

| Period | Dates | Primary outcome |
| --- | --- | --- |
| Planning week | Jul 27 - Aug 2 | Jira setup, ZenDB baseline ZDR, M1 product ZDR, initial UX |
| Sprint 1 | Aug 3 - Aug 23 | ZenDB release blockers, Tauri shell, notes data path |
| Sprint 2 | Aug 24 - Sep 13 | Milestone 1 exit; M2 replication ZDR accepted |
| Sprint 3 | Sep 14 - Oct 4 | Replication protocol, event admission, LAN runtime |
| Sprint 4 | Oct 5 - Oct 25 | Snapshot/anti-entropy, join/enrollment, sync UX |
| Sprint 5 | Oct 26 - Nov 15 | Milestone 2 hardening and exit; M3 ZDR accepted |
| Sprint 6 | Nov 16 - Dec 6 | Operator model, dynamic runtime, Topic triggering |
| Sprint 7 | Dec 7 - Dec 27 | Capabilities, scheduling, AI generation, approval UI |
| Sprint 8 | Dec 28 - Jan 17 | Reference workflow, multi-peer validation, release |
| Release buffer | Jan 18 - Jan 22 | Highest-priority defects and MVP packaging only |

Decision work deliberately overlaps the preceding milestone:

- M2 ZDR is drafted during Sprint 1 and accepted by the end of Sprint 2.
- M3 ZDR and language/scheduler spikes run during Sprints 3-5 and are accepted
  before Sprint 6.

This overlap does not permit implementation before acceptance. It prevents the
next milestone from beginning with an empty design queue.

### 5.1 Feasibility And Scope-Cut Order

This is an aggressive roadmap for two developers. Replication, sandboxed
dynamic execution, and distributed scheduling all contain architecture risk.
The ticket catalog is a structured candidate backlog, not a promise that every
Medium-priority Story enters a sprint.

The committed vertical slice is:

- M1: workspace lifecycle plus create/edit/list/delete notes
- M2: authenticated enrollment and convergence between two direct LAN peers
- M3: one language, one AI provider, one eligible-peer scheduling mode, and one
  approved reference workflow

Cut scope in this order if velocity or architecture risk requires it:

1. M1 search
2. M1 folders/tags beyond a single simple organization model
3. M3 `EveryInstallation` scheduling if eligible-single-peer scheduling is retained
4. Secondary operator dashboard polish
5. Three-peer demonstration breadth, while retaining two-peer failure coverage

Do not cut:

- durability and recovery work;
- signed admission and authorization;
- replication restart/convergence verification;
- dynamic runtime limits;
- human review before generated code activation;
- explicit partition and duplicate-execution semantics.

## 6. Clean-Slate Jira Setup Sequence

The replacement backlog should be created in this order:

1. Delete the placeholder issues.
2. Apply the issue-type, field, workflow, Component, and Fix Version settings
   from section 3.
3. Create the governance issues and `M0-ZDR-01`.
4. Create the 14 Epics with their Fix Versions and Components.
5. Create the milestone Decisions and link each one as blocking its
   implementation Epics.
6. Create the Stories, Tasks, and Bugs under their proposed parents.
7. Add cross-Epic dependencies from each ticket's `Depends on` list.
8. Assign only planning-week and Sprint 1 work. Leave later tickets in the
   backlog until refinement.

This order keeps Jira clean and prevents provisional issue keys from becoming
architecture dependencies.

## 7. MVP Ticket Catalog

## 7.1 Governance And Planning

### GOV-01: Configure Jira For The MVP

- Type: Task
- Priority: High
- Component: Build and Release
- Sprint: Planning week
- Estimate: 2

**Description**

Apply the issue type, field, workflow, release, component, and board
recommendations from this document so the MVP backlog can be created
consistently.

**Acceptance criteria**

- Decision is available or the `zdr` Task convention is documented.
- Feature is hidden or its distinction from Story is documented.
- Priority, Fix Version, Component/Area, Parent, Points, and Design are visible
  where applicable.
- The three milestone Fix Versions and listed Components exist.
- `On Hold` is removed and Flagged is the blocker mechanism.
- Board filters for milestone, component, decisions, bugs, and blockers exist.

### GOV-02: Create The Clean MVP Issue Hierarchy

- Type: Task
- Priority: High
- Component: Build and Release
- Sprint: Planning week
- Estimate: 2

**Description**

Create the clean Epic, Decision, Story, Task, and Bug hierarchy from section 7
after the placeholder issues are deleted and the project settings are ready.

**Acceptance criteria**

- All 14 Epics exist with the proposed Fix Version and Components.
- M0-ZDR-01 and the three milestone Decisions exist.
- Each milestone Decision blocks its implementation Epics.
- Stories and Tasks have their proposed parent and cross-Epic dependencies.
- Only planning-week and Sprint 1 work is assigned to a Sprint.
- No new issue depends on a deleted placeholder key.

### GOV-03: Add The ZDR Template And Decision Index

- Type: Task
- Priority: High
- Component: ZenDB Workspace
- Sprint: Planning week
- Estimate: 2

**Description**

Create a reusable ZDR structure and a small index describing accepted,
proposed, and superseded records. Preserve the repository rule that later
iteration numbers override earlier decisions.

**Acceptance criteria**

- The template includes context, decision, alternatives, invariants, failure
  behavior, security, interfaces, non-goals, risks, and rollout.
- The index links every current `.plan/` record and its status.
- Jira Decision issues link to the corresponding repository file.

### GOV-04: Define MVP Success Measures And Internal Beta Plan

- Type: Task
- Priority: High
- Component: Build and Release
- Sprint: Planning week
- Estimate: not pointed
- Primary owner: business/product

**Description**

Define what evidence will make each milestone acceptable, who will use the
internal builds, and how findings are captured without turning the MVP into an
open-ended feature list.

**Acceptance criteria**

- Each milestone has three to five measurable product acceptance scenarios.
- The internal beta participants, supported platforms, and feedback channel are
  identified.
- The M3 reference workflow has a named target user and success condition.
- Feedback is triaged into Bug, committed MVP work, or post-MVP backlog.
- Business/product owns final milestone acceptance.

### M0-ZDR-01: Record The Current ZenDB Architecture Baseline

- Type: Decision
- Priority: Highest
- Components: ZenDB Types, ZenDB Storage, ZenDB Workspace
- Sprint: Planning week
- Estimate: not pointed
- Blocks: M1-ZDR-01 and M2-ZDR-01

**Description**

Create the accepted as-built ZenDB decision record that becomes the technical
baseline for the MVP. Consolidate the latest `.plan/` decisions and identify
gaps that must be settled before application integration, replication, and
operators.

**Acceptance criteria**

- The record documents crate boundaries, CRDT/Event semantics, Table/Topic/
  State behavior, workspace identity, installations, roles, receipts, durability,
  and current failure semantics.
- Conflicts between historical plan iterations are resolved in favor of the
  latest accepted decision.
- Known gaps are explicit, including listener failure handling, configuration
  transitions, concurrency claims, replication admission, and placeholder join
  behavior.
- Both developers review and accept the record.
- The accepted file is committed under `.plan/` and linked from the Decision.
- The Decision blocks M1-ZDR-01 and M2-ZDR-01.

## 7.2 Milestone 1: Local ZenDB And Desktop Notes

Fix Version: `0.1.0-m1-local-notes`

Exit date: 2026-09-13

Milestone outcome: a user can install the desktop app, create or open a local
workspace, create and organize notes, edit offline with low latency, restart
without losing acknowledged work, and understand recoverable failures.

### Epic M1-E1: M1 Architecture And Domain

- Type: Epic
- Priority: Highest
- Components: ZenDB Workspace, Desktop App, UX
- Target: Planning week through Sprint 1

**Epic outcome**

The team has one accepted architecture for the local desktop product, a
concrete note-domain mapping, and implementation-ready UX. No application
layer invents persistence semantics outside the accepted model.

**Epic acceptance**

- M0-ZDR-01 and M1-ZDR-01 are Done.
- The note/folder/tag model is implementable using current ZenDB primitives.
- All M1 primary flows and error states have accepted designs.
- M1 implementation Epics have no unresolved architecture blocker.

#### M1-ZDR-01: Decide The Local Database And Desktop Application Architecture

- Type: Decision
- Priority: Highest
- Components: ZenDB Workspace, Desktop App, UX
- Sprint: Planning week
- Estimate: not pointed
- Depends on: M0-ZDR-01
- Blocks: every M1 implementation Epic

**Description**

Define how the desktop application owns a ZenDB workspace, maps note-domain
operations to CRDT Events, crosses the Tauri command boundary, persists
application settings, and exposes errors and durability to the UI.

**Acceptance criteria**

- The ZDR decides whether Tauri is used and records the considered alternative.
- It defines the process model, Rust/application boundary, workspace lifecycle,
  command/event API, and shutdown behavior.
- It defines the MVP note, folder, and tag model and their Table keys, CRDT
  Values, and local State usage.
- It defines autosave, flush, sync, and crash-recovery expectations.
- It defines supported desktop platforms for the MVP.
- It defines explicit non-goals: rich block editor, web/mobile, collaboration
  presence, attachments, semantic search, and operators.
- Both developers, UI/UX, and business/product approve it.
- The accepted record is committed under `.plan/`.

#### M1-DOM-01: Specify The Note, Folder, And Tag Data Model

- Type: Task
- Priority: High
- Components: ZenDB Types, Desktop App
- Sprint: Sprint 1
- Estimate: 3
- Depends on: M1-ZDR-01

**Description**

Turn the accepted domain decision into concrete Rust application types,
primary-key conventions, Table declarations, CRDT paths/operations, and
conversion rules.

**Acceptance criteria**

- Note identity, title, body, timestamps, folder membership, tags, and deletion
  semantics are defined.
- Every persisted field identifies its CRDT type and update operation.
- Local-only settings are separated from replicated domain data even though M1
  has only one peer.
- The model does not require a storage API that bypasses `Table::insert`.
- The mapping is documented for later replication and operator use.

#### M1-UX-01: Design The Local Notes Information Architecture

- Type: Task
- Priority: High
- Component: UX
- Sprint: Planning week / Sprint 1
- Estimate: not pointed
- Depends on: M1-ZDR-01

**Description**

Produce the desktop flows and final interaction design for workspace creation,
note navigation, editing, folders, tags, search, error states, and settings.

**Acceptance criteria**

- The design covers first run, empty workspace, populated workspace, loading,
  recoverable error, fatal open error, and offline/local status.
- Keyboard-first editing and navigation are specified.
- Long titles, large bodies, and narrow desktop windows are covered.
- Design assets are linked through the Jira Design field.
- Business/product approves the M1 scope before Sprint 1 implementation closes.

### Epic M1-E2: ZenDB Local Release Readiness

- Type: Epic
- Priority: Highest
- Components: ZenDB Storage, ZenDB Workspace
- Target: Sprints 1-2

**Epic outcome**

ZenDB has a precise local durability and concurrency contract and no known
silent runtime/catalog divergence that can put note data at risk.

**Epic acceptance**

- Internal listener failures are observable and recoverable.
- Existing storage configuration cannot diverge from its catalog declaration.
- Flush, sync, Drop, and restart behavior are documented and aligned.
- The lock-free/concurrency claim is accurate and evidence-based.
- No open Highest-priority local data-loss Bug remains.

#### M1-DB-01: Make Internal Listener Failures Visible And Recoverable

- Type: Bug
- Priority: Highest
- Components: ZenDB Workspace, ZenDB Storage
- Sprint: Sprint 1
- Estimate: 5
- Depends on: M0-ZDR-01

**Description**

Internal receipt, table-catalog, and installation-registry listeners currently
discard errors after an Event is accepted. Define and implement a recovery
path so durable catalog changes cannot silently leave runtime state stale.
Application listeners may remain fire-and-forget.

**Acceptance criteria**

- Receipt observation, config decoding, physical table open/create/delete, and
  installation record decoding failures are no longer silently ignored.
- A failure is observable through a workspace operation or durability barrier.
- Runtime state can reconcile from the durable Table without deleting user
  data.
- Table deletion with outstanding handles does not remove live storage.
- Public application listener behavior remains documented.

#### M1-DB-02: Define Safe Table And State Configuration Semantics

- Type: Task
- Priority: High
- Components: ZenDB Storage, ZenDB Workspace
- Sprint: Sprint 1
- Estimate: 3
- Depends on: M0-ZDR-01

**Description**

Prevent a changed catalog configuration from disagreeing with an already-open
physical Table or State. The MVP does not need online migration; it needs a
safe, explicit contract.

**Acceptance criteria**

- No configuration update changes only the catalog while the runtime continues
  using the previous configuration.
- Unsupported transitions fail before the catalog is modified.
- Same-configuration upsert remains a no-op.
- The error and reopening behavior are documented.
- Full online configuration migration is captured as post-MVP work.

#### M1-DB-03: Audit Flush, Sync, Drop, And Crash-Recovery Guarantees

- Type: Task
- Priority: Highest
- Components: ZenDB Storage, ZenDB Workspace
- Sprint: Sprint 1
- Estimate: 5
- Depends on: M0-ZDR-01

**Description**

Trace acknowledged writes from Table insertion through Topic, cache, State,
peer receipt checkpoints, explicit durability barriers, and process shutdown.
Align code and documentation with one precise guarantee.

**Acceptance criteria**

- `flush`, `sync`, and Drop behavior are defined separately.
- Workspace, Installations, Tables, States, Table, Topic, and each durable backend
  have an explicit ordering and ownership description.
- Drop-time best-effort failures are distinguishable from explicit sync errors.
- Restart recovery cannot reuse a local event sequence or omit acknowledged
  Table events under the accepted durability contract.
- The application knows which barrier to use during normal shutdown.

#### M1-DB-04: Define ZenDB Concurrency And Progress Guarantees

- Type: Decision
- Priority: High
- Components: ZenDB Storage, ZenDB Workspace
- Sprint: Sprint 1
- Estimate: not pointed

**Description**

Define what the project means by lock-free, identify every lock on the public
read/write path, and establish measurable concurrency claims for the MVP.

**Acceptance criteria**

- The record distinguishes algorithm-level lock freedom, wait freedom, and
  ordinary thread-safe synchronization.
- Current Mutex and RwLock critical sections are identified.
- The MVP claim is worded accurately and has a measurement plan.
- Any lock-free redesign not required for M1 is split into post-MVP tickets.

### Epic M1-E3: Desktop Foundation

- Type: Epic
- Priority: Highest
- Components: Desktop App, ZenDB Workspace, Build and Release
- Target: Sprints 1-2

**Epic outcome**

An installable Tauri desktop shell owns one ZenDB workspace through a stable
Rust application service and shuts down according to the accepted durability
contract.

**Epic acceptance**

- Development and production builds run on the primary MVP platform.
- The UI can create/open a workspace through stable application commands.
- ZenDB guards and backend types do not cross the UI boundary.
- Shutdown reports durability failures and releases the workspace lock.

#### M1-APP-01: Scaffold The Tauri Desktop Application

- Type: Task
- Priority: Highest
- Component: Desktop App
- Sprint: Sprint 1
- Estimate: 3
- Depends on: M1-ZDR-01

**Description**

Initialize the desktop application, frontend toolchain, development commands,
window shell, routing, and shared visual foundation selected by the M1 ZDR.

**Acceptance criteria**

- The app starts in development mode on the primary development platforms.
- Production packaging can be invoked for the primary release platform.
- Routing and the application shell match the accepted information
  architecture.
- The Rust side consumes ZenDB as a workspace dependency without duplicating
  database types in the frontend.

#### M1-APP-02: Implement The Rust Application Service And Tauri Boundary

- Type: Task
- Priority: Highest
- Components: Desktop App, ZenDB Workspace
- Sprint: Sprint 1
- Estimate: 5
- Depends on: M1-APP-01, M1-DOM-01

**Description**

Create the application-owned Rust service that translates UI commands into
ZenDB workspace, Table, and State operations and returns stable application
DTOs and errors.

**Acceptance criteria**

- The UI cannot reach raw storage backends or bypass Table event insertion.
- Commands cover workspace lifecycle and the M1 note-domain operations.
- Application DTOs do not leak lock guards or backend implementation types.
- Long-running or blocking operations do not freeze the UI thread.
- Errors have stable categories suitable for user-facing handling.

#### M1-APP-03: Implement Workspace Create, Open, And Recent Workspace Flows

- Type: Story
- Priority: High
- Components: Desktop App, UX
- Sprint: Sprint 1
- Estimate: 3
- Depends on: M1-APP-02, M1-UX-01

**Description**

As a desktop user, I can create a workspace in a chosen local folder, reopen
it later, and see a clear error when it is unavailable or already open.

**Acceptance criteria**

- Create and open use an application-owned persistent peer identity.
- Recent workspace locations are stored locally outside replicated Tables.
- Missing, moved, locked, and corrupt workspace failures have distinct UI
  states.
- The private key is not stored by ZenDB.

#### M1-APP-04: Implement Application Shutdown And Durability Handling

- Type: Task
- Priority: Highest
- Components: Desktop App, ZenDB Workspace
- Sprint: Sprint 2
- Estimate: 3
- Depends on: M1-DB-03, M1-APP-02

**Description**

Coordinate autosave, explicit sync points, window shutdown, and database
release so the application matches the accepted durability contract.

**Acceptance criteria**

- Closing the app invokes the required durability barrier before process exit.
- A sync failure is shown and does not silently report successful shutdown.
- Repeated save requests are coalesced without losing the latest note state.
- The workspace lock is released after database-owned objects are dropped.

### Epic M1-E4: Local Notes Experience

- Type: Epic
- Priority: Highest
- Components: Desktop App, UX
- Target: Sprints 1-2

**Epic outcome**

A user can manage a useful set of local notes with low-latency offline editing
and intentional navigation, organization, and failure states.

**Epic acceptance**

- Required note CRUD and autosave flows meet their acceptance criteria.
- Reopening the application preserves acknowledged content.
- Empty, deleted, long-content, and error states are usable.
- Medium-priority organization/search scope is included only if the required
  vertical slice and reliability work are on track.
- The Milestone 1 internal build is accepted by business/product.

#### M1-NOTE-01: Create, Edit, Autosave, And Reopen Notes

- Type: Story
- Priority: Highest
- Components: Desktop App, UX
- Sprint: Sprint 1 / Sprint 2
- Estimate: 5
- Depends on: M1-APP-02, M1-DOM-01, M1-UX-01

**Description**

As a user, I can create a note, edit its title and body with low latency, and
reopen the application without losing acknowledged edits.

**Acceptance criteria**

- Creating a note immediately adds it to navigation and focuses the editor.
- Title and body edits use the accepted CRDT operations.
- Autosave does not create a new note identity or replace unrelated fields.
- Reopening reconstructs the same visible content from ZenDB.
- Empty notes and large note bodies have intentional behavior.

#### M1-NOTE-02: List, Select, Rename, And Delete Notes

- Type: Story
- Priority: High
- Components: Desktop App, UX
- Sprint: Sprint 2
- Estimate: 3
- Depends on: M1-NOTE-01

**Description**

As a user, I can navigate my notes and perform their basic lifecycle actions
without leaving the main workspace.

**Acceptance criteria**

- The list has deterministic ordering defined by the product decision.
- Selection survives ordinary refreshes and edits.
- Rename updates the note title rather than its stable identity.
- Delete uses the accepted tombstone/trash behavior and asks for confirmation
  where the UX specifies it.
- Empty and deleted selections do not leave a broken editor.

#### M1-NOTE-03: Organize Notes With Folders And Tags

- Type: Story
- Priority: Medium
- Components: Desktop App, UX
- Sprint: Sprint 2
- Estimate: 5
- Depends on: M1-DOM-01, M1-NOTE-01

**Description**

As a user, I can place a note in a folder and attach tags so a growing local
workspace remains navigable.

**Acceptance criteria**

- Folder and tag identity are stable across rename.
- Folder deletion has defined behavior for contained notes.
- Tag add/remove uses convergent set semantics suitable for later replication.
- Navigation can filter by folder and tag.
- This Story may be reduced to one-level folders for M1; nested folders are
  not implied.

#### M1-NOTE-04: Search Local Notes

- Type: Story
- Priority: Medium
- Components: Desktop App, UX
- Sprint: Sprint 2
- Estimate: 3
- Depends on: M1-NOTE-01

**Description**

As a user, I can quickly filter local notes by title and text without requiring
a network service.

**Acceptance criteria**

- Search behavior and matching fields are explicit.
- Results update without blocking editing.
- Search is local-only and works offline.
- The implementation does not pretend to provide a general ZenDB query engine.
- Full-text indexing, vector search, and semantic search remain post-MVP.

#### M1-REL-01: Package And Validate Milestone 1

- Type: Task
- Priority: Highest
- Component: Build and Release
- Sprint: Sprint 2
- Estimate: 3
- Depends on: all required M1 Stories

**Description**

Produce an installable internal build and execute the Milestone 1 acceptance
flow on the MVP desktop platforms.

**Acceptance criteria**

- A clean machine can install and launch the build.
- A workspace can be created, populated, closed, reopened, and edited.
- Known limitations and supported platforms are documented.
- No open Highest-priority M1 Bug remains.
- Business/product accepts the local note-taking flow.

### Milestone 1 Non-Goals

- Rich block editor, collaboration cursors, comments, sharing, attachments
- Mobile or web clients
- Network replication
- Operator execution
- Semantic/vector search
- Online storage configuration migration

## 7.3 Milestone 2: Authenticated LAN Replication

Fix Version: `0.2.0-m2-lan-sync`

Exit date: 2026-11-15

Milestone outcome: two or more desktop peers on the same LAN can enroll into
one workspace, synchronize replicated Tables over direct libp2p TCP sessions,
disconnect, edit independently, reconnect, and converge without replicating
local State.

### Epic M2-E1: Replication Decision And Failure Model

- Type: Epic
- Priority: Highest
- Components: Replication, ZenDB Workspace, ZenDB Storage
- Target: Draft in Sprints 1-2; accepted before Sprint 3

**Epic outcome**

The team agrees on one transport-independent replication contract, LAN threat
model, snapshot/anti-entropy strategy, and enrollment flow before production
protocol code begins.

**Epic acceptance**

- M2-ZDR-01 is accepted by all required reviewers.
- Snapshot, Topic retention, Table identity, role ordering, and partition
  questions have binding answers.
- The protocol and UI Epics have explicit dependencies on the Decision.
- LAN-only and local-State non-goals are unambiguous.

#### M2-ZDR-01: Decide The LAN Replication Architecture

- Type: Decision
- Priority: Highest
- Components: Replication, ZenDB Workspace, ZenDB Storage
- Sprint: Draft in Sprint 1; accept by end of Sprint 2
- Estimate: not pointed
- Depends on: M0-ZDR-01
- Blocks: every M2 implementation Epic

**Description**

Define the replication protocol, trust boundary, enrollment flow, snapshot and
anti-entropy model, Table identity, role evaluation, Topic retention contract,
and application-visible synchronization states.

**Acceptance criteria**

- The ZDR defines discovery and direct TCP connection behavior and explicitly
  excludes NAT traversal, relay, tunneling, and WAN discovery.
- It defines authenticated peer sessions and canonical signed event/envelope
  bytes bound to WorkspaceId and Table identity.
- It defines deterministic remote-event admission, deduplication, and
  system-table rules.
- It defines initial snapshot plus incremental synchronization and the
  snapshot-to-Topic watermark.
- It defines missing-range exchange using receipt windows and event lookup.
- It defines how installation enrollment establishes both `_installations` and `_peers`
  state without self-promotion.
- It states that local States are not replicated.
- It defines behavior for offline edits, reconnect, partial transfer,
  duplicate delivery, stale roles, table creation/deletion, and peer removal.
- It includes sequence diagrams and security/failure analysis.
- Both developers, UI/UX, and business/product approve it.

#### M2-SPIKE-01: Validate Snapshot, Topic Retention, And Table Identity Options

- Type: Task
- Priority: High
- Components: ZenDB Storage, Replication
- Labels: spike
- Sprint: Sprint 2
- Estimate: 3

**Description**

Produce evidence for the M2 ZDR about initial synchronization, compacted Topic
history, table naming/identity, and the offset needed to switch from snapshot
installation to incremental events.

**Acceptance criteria**

- At least two snapshot strategies and their crash behavior are compared.
- The effect of Topic consumer retention and compaction is documented.
- Table create, delete, and recreate identity semantics are decided or raised
  as an explicit ZDR question.
- The spike does not create a public production replication API.

#### M2-SPIKE-02: Define The LAN Threat And Partition Model

- Type: Task
- Priority: High
- Components: Replication, ZenDB Workspace
- Labels: spike, security
- Sprint: Sprint 2
- Estimate: 3

**Description**

Define what is and is not trusted on a LAN, how a joining peer proves identity,
which peer may grant roles, and how authorization behaves when role changes and
application events arrive in different orders.

**Acceptance criteria**

- Passive observers, unauthorized peers, replay, event forgery, stale installations,
  and partition/reconnect are covered.
- The required transport encryption and peer authentication are explicit.
- Arrival-time role lookup is either rejected or justified with deterministic
  semantics.
- Out-of-scope threats are documented rather than silently ignored.

### Epic M2-E2: Replication Protocol And Storage Support

- Type: Epic
- Priority: Highest
- Components: Replication, ZenDB Storage, ZenDB Workspace
- Target: Sprints 3-5

**Epic outcome**

ZenDB can authenticate, snapshot, exchange, locate, admit, and durably
checkpoint replicated Table Events independently of a particular connection
session.

**Epic acceptance**

- Signed protocol messages and deterministic admission are implemented.
- A new peer can install a consistent snapshot and watermark.
- Existing peers can exchange missing ranges and resume after interruption.
- Duplicate or reordered delivery is safe.
- Local State is never included in replication.

#### M2-REP-01: Implement Versioned Replication Messages And Signed Envelopes

- Type: Task
- Priority: Highest
- Components: Replication, ZenDB Types
- Sprint: Sprint 3
- Estimate: 5
- Depends on: M2-ZDR-01

**Description**

Implement the transport-independent message vocabulary and canonical signed
event envelope accepted by the M2 ZDR without adding replication policy to
`zendb-types`.

**Acceptance criteria**

- Messages are explicitly versioned and size-bounded.
- Signed bytes are domain-separated and bind workspace, table, actor, event,
  and protocol version.
- PeerId/public-key and signature verification behavior is defined.
- Unknown versions and malformed messages fail without mutating workspace data.
- Protocol types live in the crate boundary selected by the ZDR.

#### M2-REP-02: Implement Deterministic Remote Event Admission

- Type: Story
- Priority: Highest
- Components: Replication, ZenDB Workspace
- Sprint: Sprint 3
- Estimate: 5
- Depends on: M2-REP-01

**Description**

Admit authenticated remote events into the existing Table insertion path while
enforcing deduplication, role policy, and system-table invariants before
listener dispatch.

**Acceptance criteria**

- Claimed actor identity alone is never treated as authentication.
- Duplicate EventIds are rejected without reapplying side effects.
- Authorization uses the deterministic rule accepted by the ZDR.
- System catalog and installation events have additional invariant checks.
- Admitted events converge on the same internal insertion/listener path as
  local events.
- Rejected events produce a protocol reason without leaking secrets.

#### M2-REP-03: Add Event Range Lookup For Anti-Entropy

- Type: Task
- Priority: High
- Components: ZenDB Storage, Replication
- Sprint: Sprint 3
- Estimate: 5
- Depends on: M2-ZDR-01

**Description**

Allow replication to locate events for a peer sequence range without scanning
the full retained Topic. Implement the sparse index or lookup structure chosen
by the ZDR.

**Acceptance criteria**

- Lookup accepts the protocol's missing-range representation.
- Index maintenance follows Table insertion and recovery semantics.
- Compaction cannot leave lookup entries pointing at deleted records.
- Storage overhead and lookup complexity are documented.
- The index remains storage-oriented and contains no network policy.

#### M2-REP-04: Implement Initial Snapshot Export And Installation

- Type: Story
- Priority: Highest
- Components: ZenDB Storage, Replication
- Sprint: Sprint 4
- Estimate: 8
- Depends on: M2-REP-01, M2-SPIKE-01

**Description**

Transfer a consistent workspace/table snapshot to a newly enrolled peer and
switch to incremental events at a defined watermark.

**Acceptance criteria**

- System catalogs and installation metadata are installed in the required order.
- Application Table state and the corresponding incremental watermark are
  consistent.
- Local States are excluded.
- Interrupted installation is resumable or safely restartable.
- A failed install does not expose a half-open workspace as healthy.
- Snapshot size limits and progress reporting are available to the app.

#### M2-REP-05: Implement Receipt Exchange And Incremental Anti-Entropy

- Type: Story
- Priority: Highest
- Components: Replication, ZenDB Workspace
- Sprint: Sprint 4
- Estimate: 8
- Depends on: M2-REP-02, M2-REP-03

**Description**

Exchange durable peer receipt summaries, request missing ranges, stream Events,
and continue until both peers agree that the current retained histories have
converged.

**Acceptance criteria**

- Missing ranges are bounded and paginated.
- Duplicate, reordered, and interrupted batches are safe.
- Receipt checkpoints advance only after the accepted durability point.
- New local events may continue while a catch-up session is running.
- Reconnect resumes from durable state rather than restarting all history.
- Table creation/deletion events are synchronized in the ZDR-defined order.

### Epic M2-E3: LAN Transport, Discovery, And Enrollment

- Type: Epic
- Priority: Highest
- Components: Replication, Desktop App
- Target: Sprints 3-5

**Epic outcome**

Desktop peers can discover or directly address each other, authenticate a
libp2p TCP session, enroll with Admin approval, and maintain one observable
sync session per peer.

**Epic acceptance**

- Direct LAN discovery and manual connection work.
- The joiner proves its PeerIdentity and cannot self-promote.
- Workspace close cleanly cancels the network runtime.
- Retry and reconnect are bounded and observable.
- No NAT traversal, relay, or cloud service is required.

#### M2-NET-01: Implement The libp2p LAN Runtime

- Type: Task
- Priority: Highest
- Components: Replication, Desktop App
- Sprint: Sprint 3
- Estimate: 5
- Depends on: M2-ZDR-01

**Description**

Create the application-owned libp2p runtime for direct authenticated TCP
sessions. The ZenDB core remains synchronous and transport-independent.

**Acceptance criteria**

- The runtime listens and dials direct TCP addresses.
- The accepted encrypted/authenticated transport is configured.
- Network async execution does not leak into storage backend APIs.
- Startup, shutdown, reconnect, and cancellation are explicit.
- NAT traversal, relay, and public rendezvous are absent.

#### M2-NET-02: Add LAN Discovery And Manual Connection

- Type: Story
- Priority: High
- Components: Replication, Desktop App
- Sprint: Sprint 3
- Estimate: 3
- Depends on: M2-NET-01

**Description**

Allow peers on the same LAN to discover one another and provide a manual
address fallback for environments where discovery is unavailable.

**Acceptance criteria**

- Discovery exposes peer identity and reachable direct addresses without
  automatically granting workspace access.
- Duplicate discoveries are coalesced.
- Manual address entry has validation and clear failure feedback.
- Discovery can be disabled.

#### M2-NET-03: Implement Admin-Approved Installation Enrollment And Join

- Type: Story
- Priority: Highest
- Components: Replication, ZenDB Workspace, Desktop App
- Sprint: Sprint 4
- Estimate: 5
- Depends on: M2-NET-01, M2-REP-01

**Description**

Replace the placeholder `Workspace::join` behavior with an enrollment protocol
that requires an authorized existing peer and never bootstraps the joiner as
Admin.

**Acceptance criteria**

- A join request proves possession of the joining PeerIdentity.
- An Admin explicitly approves and assigns display metadata and role.
- WorkspaceId, `_installations`, and the joining peer's clock/receipt state are
  established consistently.
- Rejected or expired enrollment leaves no openable partial workspace.
- Rejoining an already enrolled installation has defined behavior.

#### M2-NET-04: Implement Replication Session Coordination

- Type: Task
- Priority: High
- Components: Replication, Desktop App
- Sprint: Sprint 4 / Sprint 5
- Estimate: 5
- Depends on: M2-NET-01, M2-REP-04, M2-REP-05

**Description**

Coordinate peer sessions, initial sync, incremental sync, backoff, cancellation,
and application status without allowing multiple competing sessions for the
same peer.

**Acceptance criteria**

- Each peer has one observable connection/sync state.
- Retry uses bounded backoff and stops on explicit user action.
- Workspace close cancels network work before storage is dropped.
- Session state distinguishes discovery, connecting, enrolling, snapshot,
  catching up, current, offline, and error.

### Epic M2-E4: Installation And Synchronization UX

- Type: Epic
- Priority: High
- Components: Desktop App, UX, Replication
- Target: Sprints 3-5

**Epic outcome**

Users can understand and control installation trust, enrollment, roles, offline
state, initial synchronization, recovery, and peer removal without needing
protocol knowledge.

**Epic acceptance**

- Enrollment and role assignment use accepted designs.
- Real connection/synchronization state drives the UI.
- Offline editing remains available.
- Errors expose only valid recovery actions.
- The product never implies WAN or cloud backup behavior.

#### M2-UX-01: Design Installation, Enrollment, And Sync Status Flows

- Type: Task
- Priority: High
- Component: UX
- Sprint: Sprint 3
- Estimate: not pointed
- Depends on: M2-ZDR-01

**Description**

Design the desktop flows for discovering a peer, approving enrollment,
selecting a role, observing synchronization, recovering from errors, and
removing a installation.

**Acceptance criteria**

- Trust and role consequences are understandable without exposing protocol
  jargon.
- Initial sync progress and offline editing are represented.
- Rejected, disconnected, incompatible, and unauthorized states are covered.
- The UI never implies cloud backup or WAN availability.

#### M2-APP-01: Implement Installation Management And Enrollment UI

- Type: Story
- Priority: High
- Components: Desktop App, UX
- Sprint: Sprint 4
- Estimate: 3
- Depends on: M2-NET-03, M2-UX-01

**Description**

As an Admin, I can discover or address a peer, review its identity, approve it
with a role, and see it in the workspace installation list.

**Acceptance criteria**

- Pending and enrolled installations are visually distinct.
- Role assignment follows ZenDB's Reader/Contributor/Operator/Admin hierarchy.
- Non-Admins cannot approve enrollment.
- Removing or changing a installation role requires confirmation.

#### M2-APP-02: Implement Synchronization Status And Recovery UI

- Type: Story
- Priority: High
- Components: Desktop App, UX
- Sprint: Sprint 4 / Sprint 5
- Estimate: 3
- Depends on: M2-NET-04, M2-UX-01

**Description**

As a user, I can see whether my workspace is local-only, connecting,
synchronizing, current, offline, or failed, and can retry recoverable failures.

**Acceptance criteria**

- Status is based on real session state, not a timer or optimistic flag.
- Initial snapshot progress is visible.
- Offline status does not block local note editing.
- Error details are concise and offer only valid recovery actions.
- The UI does not claim global convergence while another peer is unreachable.

### Epic M2-E5: Replication Verification And Milestone Exit

- Type: Epic
- Priority: Highest
- Components: Replication, Build and Release
- Target: Sprints 3-5

**Epic outcome**

The team has deterministic evidence that enrollment, snapshot, offline edits,
reconnect, restart, authorization, and convergence satisfy the accepted M2
contract.

**Epic acceptance**

- The two/three-peer harness can inject the accepted failure scenarios.
- Required two-peer scenarios pass deterministically.
- Three-peer breadth is completed unless explicitly cut under section 5.1.
- No Highest-priority replication Bug remains.
- Business/product accepts an installable LAN-sync demonstration.

#### M2-VER-01: Build A Deterministic Multi-Peer Verification Harness

- Type: Task
- Priority: Highest
- Components: Replication, Build and Release
- Sprint: Sprint 3 / Sprint 4
- Estimate: 5
- Depends on: M2-ZDR-01

**Description**

Create a controllable harness for two and three peers that can inject
disconnects, reordering, duplicate messages, restarts, and concurrent edits.

**Acceptance criteria**

- The harness uses independent workspace directories and PeerIdentities.
- Tests can pause and resume links without relying on timing sleeps as the
  primary synchronization mechanism.
- It can assert Table convergence and verify that local States remain local.
- It records enough protocol state to diagnose a failed scenario.

#### M2-VER-02: Validate LAN Convergence And Recovery Scenarios

- Type: Task
- Priority: Highest
- Components: Replication, Build and Release
- Sprint: Sprint 5
- Estimate: 5
- Depends on: M2-VER-01, all required M2 replication Stories

**Description**

Execute the accepted failure matrix and resolve all milestone-blocking
replication defects.

**Acceptance criteria**

- Initial enrollment and snapshot work for two and three peers.
- Concurrent offline note edits converge after reconnect.
- Duplicate and reordered batches do not duplicate visible changes.
- Restart during snapshot and incremental catch-up recovers safely.
- Role downgrade/removal and unauthorized events follow the ZDR.
- Table create/delete behavior follows the accepted lifecycle semantics.
- No open Highest-priority replication Bug remains.

#### M2-REL-01: Package And Accept Milestone 2

- Type: Task
- Priority: Highest
- Component: Build and Release
- Sprint: Sprint 5
- Estimate: 3
- Depends on: M2-VER-02, M2-APP-02

**Description**

Produce an internal desktop build demonstrating local-first notes and direct
LAN synchronization between independently installed peers.

**Acceptance criteria**

- A new peer can enroll from a clean installation.
- Both peers can edit offline and converge after reconnect.
- Sync status and failures are visible in the application.
- Supported network assumptions and known limitations are documented.
- Business/product accepts the LAN workflow.

### Milestone 2 Non-Goals

- NAT traversal, hole punching, relay, tunneling, public rendezvous, or WAN sync
- Cloud accounts, hosted backup, or server authority
- Replication of local State
- Presence, live cursors, comments, or real-time co-editing UI
- Strict global ordering across Tables
- Migration compatibility with future protocol versions

## 7.4 Milestone 3: Operators And AI-Generated Workflows

Fix Version: `0.3.0-mvp-operators-ai`

Exit date: 2027-01-17

Milestone outcome: a user can describe one supported workflow, review generated
code in the selected dynamic language, approve it, and run it as a replicated
operator triggered by Table Topic changes on an eligible LAN peer. Runs are
observable, resource-bounded, and at-least-once.

### Epic M3-E1: Operator And AI Decision

- Type: Epic
- Priority: Highest
- Components: Operators, AI, Replication, UX
- Target: Draft in Sprints 3-5; accepted before Sprint 6

**Epic outcome**

The team has one accepted execution, scheduling, safety, state, and AI approval
model and has resolved the language and partition questions with evidence.

**Epic acceptance**

- M3-ZDR-01 selects one dynamic language and one MVP scheduling contract.
- Sandbox/host API limits and prohibited capabilities are explicit.
- Stateful placement and duplicate-execution behavior are explicit.
- The reference workflow and approval UX are accepted.
- No M3 production implementation begins before acceptance.

#### M3-ZDR-01: Decide The Operator, Scheduler, Runtime, And AI Architecture

- Type: Decision
- Priority: Highest
- Components: Operators, AI, Replication, UX
- Sprint: Draft in Sprints 3-4; accept by end of Sprint 5
- Estimate: not pointed
- Depends on: M2-ZDR-01
- Blocks: every M3 implementation Epic

**Description**

Define the operator data model, Topic trigger/checkpoint semantics, local State
ownership, capability advertisement, scheduling modes, leases/partitions,
dynamic-language host API and sandbox, AI generation contract, approval flow,
and run observability.

**Acceptance criteria**

- One dynamic language is selected for the MVP using explicit criteria.
- Operator definition, revision, activation, trigger, placement, capability,
  and run-status models are specified.
- The MVP supports only `EveryInstallation` and one documented eligible-peer/
  singleton mode.
- Partition behavior, duplicate execution, idempotency, and lease/fencing
  limits are explicit; exactly-once is not claimed.
- Stateful operators are pinned or rebuildable; transparent State migration is
  excluded.
- The host API contains an allowlist of Table, local State, logging, and AI
  operations.
- CPU/instruction, memory, output, and execution-time limits are specified.
- Filesystem, CLI, arbitrary network, and secrets access are excluded from the
  MVP host API.
- AI-generated code requires validation, preview, and explicit activation.
- One reference user workflow and measurable acceptance flow are selected.
- Both developers, UI/UX, and business/product approve the record.

#### M3-SPIKE-01: Compare Rhai And Lua For The MVP Runtime

- Type: Task
- Priority: High
- Components: Operators, AI
- Labels: spike
- Sprint: Sprint 4
- Estimate: 3

**Description**

Compare Rhai and Lua against Rust embedding, sandboxability, deterministic
limits, serialization, debugging, package size, editor support, and the
quality of AI-generated code.

**Acceptance criteria**

- Both candidates execute the same small Table-event operator prototype.
- Host API exposure and resource limiting are demonstrated.
- Security limitations and unsafe escape surfaces are documented.
- The recommendation is evidence for M3-ZDR-01, not an implicit final decision.

#### M3-SPIKE-02: Define Singleton And Stateful Placement Semantics

- Type: Task
- Priority: Highest
- Components: Operators, Replication
- Labels: spike
- Sprint: Sprint 4 / Sprint 5
- Estimate: 3

**Description**

Demonstrate how eligible peers choose an executor, what happens under
partition, how duplicate work is identified, and what happens to local State
when ownership changes.

**Acceptance criteria**

- Connected, disconnected, crashed, and rejoined peer scenarios are analyzed.
- The limits of lease-only singleton execution are documented.
- A deterministic input identity and deduplication strategy are proposed.
- Stateful pinning versus replay-based rebuild is compared.
- External side effects are explicitly excluded from the MVP guarantee.

### Epic M3-E2: Operator Runtime And Topic Processing

- Type: Epic
- Priority: Highest
- Components: Operators, ZenDB Storage, ZenDB Workspace
- Target: Sprints 6-8

**Epic outcome**

Replicated, versioned operator definitions can consume durable Table changes,
execute bounded dynamic code against an allowlisted host API, use namespaced
local State, and expose lifecycle/run results.

**Epic acceptance**

- Operator revision and activation state replicate correctly.
- Topic input is at-least-once with durable checkpoints.
- Runtime resource limits and host API restrictions are enforced.
- Failures and source locations are observable.
- Pause, restart, and revision activation preserve the accepted input contract.

#### M3-OP-01: Implement Replicated Operator Definitions And Revisions

- Type: Story
- Priority: Highest
- Components: Operators, ZenDB Workspace
- Sprint: Sprint 6
- Estimate: 3
- Depends on: M3-ZDR-01

**Description**

Store operator definitions, immutable revisions, activation state, trigger
configuration, placement mode, and capability requirements in replicated
ZenDB Tables.

**Acceptance criteria**

- Editing code creates a revision rather than mutating active code invisibly.
- Activation selects one explicit revision.
- Definitions replicate through the M2 path.
- Only authorized roles may create or activate operators.
- Local runtime state and replicated definitions remain separate.

#### M3-OP-02: Implement Topic Triggers And Durable Input Checkpoints

- Type: Story
- Priority: Highest
- Components: Operators, ZenDB Storage
- Sprint: Sprint 6
- Estimate: 5
- Depends on: M3-OP-01

**Description**

Allow an operator instance to consume a named Table Topic stream, build a
stable input identity, and checkpoint progress according to the accepted
at-least-once contract.

**Acceptance criteria**

- Trigger filters and source Table are part of the operator revision.
- Each input exposes Table, EventId, offset/watermark, previous value, and
  current value as accepted by the ZDR.
- Checkpoint advancement occurs only after the run reaches the accepted
  completion point.
- Restart replays incomplete work and does not skip input.
- Backlog growth and unavailable retained history produce observable errors.

#### M3-OP-03: Implement The Dynamic Runtime And Bounded Host API

- Type: Story
- Priority: Highest
- Components: Operators, AI
- Sprint: Sprint 6
- Estimate: 8
- Depends on: M3-ZDR-01, M3-SPIKE-01

**Description**

Embed the selected language and expose only the approved operator APIs for
reading/writing Tables, reading/writing operator-local State, logging, and
returning structured output.

**Acceptance criteria**

- The runtime exposes no filesystem, CLI, arbitrary network, process, or
  unrestricted host-language escape.
- Execution has deterministic instruction/time, memory, and output limits where
  the selected runtime permits them.
- Table writes use normal authorized ZenDB event paths.
- State is namespaced by operator identity/revision/instance as decided.
- Runtime errors include source location and are persisted as run results.
- Cancellation and workspace shutdown stop new runs and bound active shutdown.

#### M3-OP-04: Implement Operator Lifecycle And Run History

- Type: Story
- Priority: High
- Components: Operators, Desktop App
- Sprint: Sprint 6 / Sprint 7
- Estimate: 3
- Depends on: M3-OP-01, M3-OP-03

**Description**

Support draft, validated, active, paused, and failed operator states and expose
bounded local run history for diagnosis.

**Acceptance criteria**

- Invalid code cannot become active.
- Pausing stops new triggers without corrupting the durable checkpoint.
- Run history records revision, peer, input identity, timing, status, and
  bounded logs/error.
- Run history is local State unless the ZDR explicitly chooses a replicated
  summary.

### Epic M3-E3: Capability Scheduling

- Type: Epic
- Priority: Highest
- Components: Operators, Replication
- Target: Sprints 7-8

**Epic outcome**

Operators are placed only on enrolled, authorized, capable peers using the
MVP's accepted every-installation and eligible-single-peer semantics, with visible
partition and state limitations.

**Epic acceptance**

- Capability advertisement is versioned and privacy-bounded.
- Eligible-single-peer placement handles normal failover without skipped input.
- Duplicate work follows the documented deduplication contract.
- Stateful placement cannot silently discard local State.
- `EveryInstallation` is delivered only if critical single-peer scheduling and
  safety work remain on track.

#### M3-SCH-01: Advertise Peer Capabilities

- Type: Story
- Priority: High
- Components: Operators, Replication
- Sprint: Sprint 7
- Estimate: 3
- Depends on: M3-ZDR-01

**Description**

Allow peers to advertise the small, versioned capability set needed for MVP
operator placement without turning installation metadata into an unrestricted
machine inventory.

**Acceptance criteria**

- Capability names and versions are defined by the ZDR.
- Operator runtime and configured AI provider capabilities are represented.
- Stale/offline capability information has a defined expiry or status.
- Capability advertisement does not expose local files or secrets.

#### M3-SCH-02: Schedule Every-Installation Operators

- Type: Story
- Priority: High
- Components: Operators, Replication
- Sprint: Sprint 7
- Estimate: 3
- Depends on: M3-SCH-01, M3-OP-02

**Description**

Run an active `EveryInstallation` operator independently on every enrolled,
authorized peer that satisfies its capabilities.

**Acceptance criteria**

- Each eligible peer owns a distinct instance and local checkpoint.
- A peer joining later starts according to the accepted history policy.
- Paused/deactivated revisions stop on all connected peers.
- Offline peers resume safely when they reconnect.

#### M3-SCH-03: Schedule One Eligible Peer With Lease And Deduplication

- Type: Story
- Priority: Highest
- Components: Operators, Replication
- Sprint: Sprint 7
- Estimate: 8
- Depends on: M3-SPIKE-02, M3-SCH-01, M3-OP-02

**Description**

Implement the MVP's documented singleton/eligible-peer mode, including
ownership choice, lease renewal or equivalent liveness, failover, and duplicate
input protection.

**Acceptance criteria**

- Eligible peers deterministically agree on the preferred executor while
  connected.
- Ownership and lease state are observable and bounded in time.
- Failover does not skip Topic input.
- Duplicate execution under a partition follows the ZDR and is visible.
- Database writes from duplicate runs are idempotent or deduplicated by the
  accepted input identity.
- External non-idempotent effects are unsupported.

#### M3-SCH-04: Enforce Stateful Operator Placement

- Type: Task
- Priority: High
- Components: Operators, ZenDB Workspace
- Sprint: Sprint 7
- Estimate: 3
- Depends on: M3-SCH-03

**Description**

Apply the ZDR's pinning or replay policy when an operator uses local State so
the scheduler cannot silently move it and lose its computation context.

**Acceptance criteria**

- Stateful versus stateless placement is explicit in the operator definition.
- A stateful operator cannot move to a peer without the accepted rebuild or
  reset action.
- Operators report unavailable when their pinned peer is unavailable if no
  rebuild path exists.
- Transparent local State transfer is not implied.

### Epic M3-E4: AI Generation And Operator UX

- Type: Epic
- Priority: Highest
- Components: AI, Desktop App, UX, Operators
- Target: Sprints 6-8

**Epic outcome**

A user can translate a natural-language request into a structured draft
operator, understand its code and capabilities, validate it, explicitly
activate it, and inspect its runtime state.

**Epic acceptance**

- The generated artifact targets a versioned host API.
- Provider credentials and workspace content follow the accepted privacy
  boundary.
- Invalid or unapproved code cannot activate.
- Operator placement, capabilities, revisions, runs, and failures are visible.
- Regeneration never overwrites the active revision implicitly.

#### M3-AI-01: Define The Versioned Operator API Contract And System Prompt

- Type: Task
- Priority: Highest
- Components: AI, Operators
- Sprint: Sprint 6
- Estimate: 3
- Depends on: M3-ZDR-01, M3-OP-03

**Description**

Create the machine-readable operator API contract, examples, restrictions, and
system prompt used to translate a human workflow request into code for the
selected language.

**Acceptance criteria**

- The prompt names only host APIs that exist in the selected runtime version.
- Input/output types, limits, capability requests, and forbidden APIs are
  explicit.
- Generated code declares trigger, placement, state usage, and required
  capabilities separately from executable code.
- Prompt and API versions are recorded on generated revisions.

#### M3-AI-02: Integrate One AI Provider For Operator Generation

- Type: Story
- Priority: High
- Components: AI, Desktop App
- Sprint: Sprint 7
- Estimate: 3
- Depends on: M3-AI-01

**Description**

Send a user request and the versioned operator contract to one configured AI
provider and return a structured draft operator. Local SLM execution and
provider routing are outside the MVP.

**Acceptance criteria**

- Credentials are stored through an application-appropriate secret mechanism,
  not ZenDB replicated Tables.
- Requests have explicit timeout, cancellation, and size limits.
- Provider errors do not create or activate partial operators.
- The generated artifact includes code, metadata, capabilities, and a
  human-readable explanation.

#### M3-AI-03: Validate Generated Operators Before Approval

- Type: Story
- Priority: Highest
- Components: AI, Operators
- Sprint: Sprint 7
- Estimate: 5
- Depends on: M3-AI-02, M3-OP-03

**Description**

Parse, compile, statically inspect where possible, and dry-run generated code
against bounded sample input before it may be approved.

**Acceptance criteria**

- Syntax and unavailable host APIs are rejected.
- Requested capabilities are compared with the operator definition and peer
  availability.
- Dry-run cannot mutate production Tables or State.
- Validation reports actionable source-located errors.
- Passing validation creates a draft, never an active operator.

#### M3-UX-01: Design The Request, Review, Approval, And Monitoring Flow

- Type: Task
- Priority: High
- Component: UX
- Sprint: Sprint 5 / Sprint 6
- Estimate: not pointed
- Depends on: M3-ZDR-01

**Description**

Design how a user describes a workflow, reviews generated code and requested
capabilities, approves activation, and monitors or pauses executions.

**Acceptance criteria**

- The UI communicates that generated code will run on selected installations.
- Code, trigger, placement, state, and capabilities are reviewable before
  activation.
- Validation errors, generation errors, runtime failures, and unavailable
  capabilities are covered.
- Pausing and revision activation are explicit actions.
- The design does not present generated code as inherently safe.

#### M3-APP-01: Implement Operator Generation And Approval UI

- Type: Story
- Priority: Highest
- Components: Desktop App, UX, AI
- Sprint: Sprint 7 / Sprint 8
- Estimate: 5
- Depends on: M3-AI-03, M3-UX-01

**Description**

As a user, I can describe a workflow, inspect the generated operator and its
permissions, correct or regenerate it, and explicitly activate an accepted
revision.

**Acceptance criteria**

- Generation progress and cancellation are visible.
- Code, trigger, schedule, State use, and capabilities are shown.
- Activation is disabled until validation succeeds.
- Regeneration creates a new draft and preserves the previous active revision.
- No workflow is activated solely because generation completed.

#### M3-APP-02: Implement Operator Status, Runs, Pause, And Revision UI

- Type: Story
- Priority: High
- Components: Desktop App, UX, Operators
- Sprint: Sprint 8
- Estimate: 3
- Depends on: M3-OP-04, M3-SCH-03

**Description**

As a user, I can see where an operator is scheduled, whether it is healthy,
recent runs and failures, and can pause it or activate a validated revision.

**Acceptance criteria**

- Assigned peer/capability state and schedule mode are visible.
- Run history is bounded and readable.
- Duplicate/partition warnings are shown when relevant.
- Pause and revision activation have confirmation and real resulting state.

### Epic M3-E5: Reference Workflow And MVP Exit

- Type: Epic
- Priority: Highest
- Components: Operators, AI, Desktop App, Build and Release
- Target: Sprint 8 and release buffer

**Epic outcome**

One useful note workflow demonstrates the full human request -> generated code
-> review -> schedule -> Topic execution -> local State -> visible result path
on the accepted LAN topology.

**Epic acceptance**

- The reference workflow satisfies its business acceptance scenario.
- Runtime, restart, peer-loss, validation, and scheduling failures follow the
  accepted contract.
- The final desktop build includes M1, M2, and M3 vertical slices.
- Security, partition, platform, and capability limitations are documented.
- Business/product signs off on the MVP.

#### M3-DEMO-01: Implement The Reference AI-Generated Note Workflow

- Type: Story
- Priority: Highest
- Components: AI, Operators, Desktop App
- Sprint: Sprint 8
- Estimate: 5
- Depends on: required M3 runtime, scheduler, and UI Stories

**Description**

Deliver one end-to-end workflow selected in the M3 ZDR. The preferred example
is a bounded note-ranking or classification operator that reacts to note
changes, invokes an approved small model/provider operation, stores per-note
results in operator-local State, and maintains a derived top-N result.

**Acceptance criteria**

- The workflow begins as a human-language request.
- Generated code uses only the versioned MVP host API.
- The user reviews and activates it.
- New and edited notes trigger bounded executions.
- Per-note and top-N state survive restart on the assigned peer.
- The application shows results and run failures.
- The scenario works across the accepted two-peer placement mode.

#### M3-VER-01: Validate Operator Failure And Scheduling Scenarios

- Type: Task
- Priority: Highest
- Components: Operators, Build and Release
- Sprint: Sprint 8
- Estimate: 5
- Depends on: M3-DEMO-01

**Description**

Validate runtime limits, restart, duplicate input, peer loss, lease expiry,
capability loss, invalid generated code, and pause/revision behavior.

**Acceptance criteria**

- Infinite or excessive code is terminated within accepted bounds.
- Restart does not skip uncheckpointed Topic input.
- Peer loss follows the documented stateful/stateless placement behavior.
- Duplicate execution follows the idempotency contract.
- Unauthorized or invalid operator revisions cannot activate.
- No open Highest-priority operator security or data-loss Bug remains.

#### M3-REL-01: Package And Accept The MVP

- Type: Task
- Priority: Highest
- Component: Build and Release
- Sprint: Sprint 8 / Release buffer
- Estimate: 5
- Depends on: all milestone exit tickets

**Description**

Produce the MVP desktop build and demonstrate the complete local notes, LAN
replication, and AI-generated operator flow with documented limitations.

**Acceptance criteria**

- The M1 local notes flow works on a clean installation.
- The M2 enrollment, offline editing, and LAN convergence flow works.
- The M3 reference workflow can be generated, reviewed, activated, scheduled,
  observed, paused, and recovered after restart.
- Security and capability limitations are visible in product documentation.
- Supported desktop platforms and network assumptions are documented.
- Business/product signs off on the reference workflow and MVP narrative.

### Milestone 3 Non-Goals

- Filesystem, folder, CLI, process, arbitrary network, or secret access
- Multiple dynamic languages
- Local SLM hosting or model scheduling
- Exactly-once external effects
- Strict singleton execution across network partitions
- Operator State transfer between peers
- Cron, event-time windows, watermarks, backpressure graphs, or DAG execution
- Autonomous activation without human review
- Public operator marketplace

## 8. Post-MVP Backlog

All issues in this section use Fix Version `post-mvp` and remain outside an
active Sprint until promoted by an accepted ZDR.

### 8.1 ZenDB Core And Storage

| Proposed ticket | Type | Priority | Description |
| --- | --- | --- | --- |
| Define and implement online Table/State configuration migration | Epic | Medium | Safely change backend type, page/segment settings, and cache configuration without catalog/runtime divergence |
| Prove or revise ZenDB lock-free architecture | Epic | Medium | Replace blocking critical sections only where measurements and progress guarantees justify the complexity |
| Add backup, restore, export, and workspace repair tooling | Epic | High | Provide user-controlled recovery independent of replication |
| Add encryption at rest and key lifecycle | Epic | High | Encrypt local database files with desktop key-store integration and recovery semantics |
| Add secondary and full-text indexing | Epic | Medium | Support maintained indexes without embedding product query policy in storage |
| Add vector and semantic index primitives | Epic | Low | Enable local semantic retrieval after the base index contract is stable |
| Add schema/version migration policy | Decision | Medium | Define application schema evolution separately from raw on-disk compatibility |
| Add large Blob and attachment storage | Epic | Medium | Stream large content without placing it directly in ordinary event values |
| Add observability and storage diagnostics API | Task | Medium | Expose bounded statistics, compaction status, Topic lag, and recovery state |
| Benchmark and optimize iteration/compaction | Epic | Medium | Establish reproducible baselines for B+ tree, KeyDir, SkipList, merged Table reads, Topic, and compaction |

### 8.2 Networking And Replication

| Proposed ticket | Type | Priority | Description |
| --- | --- | --- | --- |
| Add NAT traversal and hole punching | Epic | Medium | Connect peers across home networks after direct LAN sync is stable |
| Add relay and rendezvous infrastructure | Epic | Medium | Provide reachable discovery and fallback without making the relay a data authority |
| Add DHT/mesh peer discovery | Decision | Low | Define scale, privacy, and abuse constraints before enabling broad discovery |
| Add WAN-aware sync policies | Epic | Medium | Handle bandwidth limits, metered links, long disconnects, and resumable snapshots |
| Add protocol compatibility and migrations | Epic | Medium | Support rolling upgrades after the protocol is no longer intentionally unstable |
| Add workspace sharing and invitation lifecycle | Epic | High | Extend enrollment into user-facing invitations, revocation, and role history |
| Add replicated snapshot distribution and pruning | Epic | Medium | Coordinate safe history retention across intermittently connected peers |
| Add selective table replication | Decision | Low | Define partial replicas without breaking system catalog and authorization invariants |

### 8.3 Operators

| Proposed ticket | Type | Priority | Description |
| --- | --- | --- | --- |
| Add filesystem and folder capabilities | Epic | High | Expose scoped user-approved paths with capability revocation and audit |
| Add CLI and process execution capabilities | Decision | Medium | Define sandbox, argument policy, output limits, secrets, and user consent before implementation |
| Add outbound network and connector capabilities | Epic | Medium | Provide domain-scoped access and secret handles rather than raw credentials |
| Add operator State checkpoint transfer | Epic | High | Move or restore stateful operators without silently resetting local State |
| Add strong singleton coordination | Decision | Medium | Evaluate consensus, authority, fencing, and partition trade-offs |
| Add cron and calendar schedules | Epic | Medium | Trigger operators independently of Table Events |
| Add event-time windows and watermarks | Epic | Low | Introduce bounded stream-processing semantics after basic Topics are proven |
| Add backpressure and resource admission | Epic | Medium | Protect installations when operator input exceeds execution capacity |
| Add operator DAGs and composed workflows | Epic | Low | Connect typed outputs and inputs after single operators are stable |
| Add WASM or a second dynamic runtime | Decision | Low | Avoid multiple runtimes until one host API and sandbox have matured |
| Add operator package signing and marketplace | Epic | Low | Distribute trusted, versioned operator packages with provenance |
| Add operator rollout and rollback policies | Epic | Medium | Stage revisions across peers and automatically revert unhealthy versions |

### 8.4 AI Platform

| Proposed ticket | Type | Priority | Description |
| --- | --- | --- | --- |
| Add local SLM hosting and capability scheduling | Epic | High | Advertise model/runtime resources and place inference on capable peers |
| Add multi-provider model routing | Epic | Medium | Select providers by privacy, cost, latency, and capability |
| Add prompt and operator evaluation harness | Epic | High | Measure generation validity, safety, and workflow success across revisions |
| Add embeddings and retrieval-augmented generation | Epic | Medium | Ground generation in workspace content under explicit data-access policy |
| Add human feedback and repair loops | Epic | Medium | Regenerate or patch failed operators without autonomous activation |
| Add privacy and data-egress controls | Epic | High | Make content sent to remote AI providers explicit and enforceable |
| Add autonomous workflow proposals | Decision | Low | Consider proactive generation only after approval and audit foundations mature |

### 8.5 Zenin Product

| Proposed ticket | Type | Priority | Description |
| --- | --- | --- | --- |
| Build mobile applications | Epic | Medium | Define storage lifecycle, background sync, resource limits, and platform key storage |
| Build a web client | Decision | Low | Decide browser persistence, peer connectivity, sandbox restrictions, and reduced capabilities |
| Add rich block editing | Epic | Medium | Extend beyond the MVP plain/rich-text note model |
| Add attachments and media | Epic | Medium | Integrate large Blob storage, sync progress, and local caching |
| Add importers for Markdown, Obsidian, and Notion | Epic | Medium | Preserve identity and links while importing existing knowledge bases |
| Add sharing, comments, and presence | Epic | Medium | Add collaborative product semantics on top of replicated database primitives |
| Add semantic search and related-note discovery | Epic | Medium | Productize vector/index primitives and operator workflows |
| Add workspace backup and recovery UI | Epic | High | Make core backup/restore accessible to non-technical users |
| Add plugin and template ecosystem | Epic | Low | Package reusable note models, UI extensions, and operators |
| Add cloud-assisted relay without cloud data authority | Decision | Medium | Improve reachability while preserving local-first ownership |

## 9. Reusable Jira Description Templates

### 9.1 Decision / ZDR

```text
Outcome

What must be decided and which milestone outcome depends on it?

Context

Current architecture, constraints, evidence, and why the decision is needed.

Decision questions

- Question 1
- Question 2

Alternatives

- Option A: benefits and costs
- Option B: benefits and costs

Required contents

- Invariants
- Interfaces and ownership boundaries
- Failure and recovery behavior
- Security and trust model
- Non-goals
- Diagrams or UI flows

Acceptance criteria

- The record is committed under .plan/
- Required reviewers approve
- Rejected alternatives are recorded
- Implementation Epics are linked with Blocks
```

### 9.2 Story

```text
Outcome

As a <user/system role>, I can <behavior> so that <value>.

Context

Relevant ZDR, current behavior, and constraints.

Acceptance criteria

- Observable criterion
- Failure/empty/offline criterion
- Authorization or durability criterion

Non-goals

- Explicit excluded behavior

Dependencies

- Blocks / is blocked by
```

### 9.3 Technical Task

```text
Outcome

What engineering capability or evidence will exist when this is done?

Context

Relevant module boundaries and accepted decisions.

Acceptance criteria

- Compile/documentation requirement
- Observable runtime or API requirement
- Failure behavior
- Measurements where performance is relevant

Non-goals

- Work intentionally deferred
```

### 9.4 Bug

```text
Observed behavior

What happens, with logs/errors/data state.

Expected behavior

The accepted contract or linked ZDR.

Reproduction

Minimal deterministic steps and environment.

Impact

Data loss, security, blocked flow, or degraded behavior.

Acceptance criteria

- Root cause is addressed
- Recovery behavior is defined
- Regression verification exists
- Documentation is updated if the contract changed
```

## 10. Scope-Control Rules For The MVP

1. A milestone cannot add implementation scope after its ZDR is accepted
   without recording the decision change and removing equivalent work.
2. Highest-priority data-loss and security Bugs displace feature work.
3. Stretch work is labeled `stretch` and cannot be required by another
   committed ticket.
4. M3 supports one dynamic language and one AI provider.
5. M3 does not expose filesystem, CLI, arbitrary network, or secrets.
6. M2 supports direct LAN connectivity only.
7. Web and mobile remain design considerations, not MVP delivery targets.
8. The release buffer is for defects and packaging, not unfinished Epics.
9. If M2 exits late, reduce M3 scheduling modes or reference workflow breadth;
   do not remove validation, sandbox limits, or human approval.
10. The final MVP narrative is a working vertical slice, not a claim that the
    long-term distributed compute platform is complete.
