# ZeninDB Distributed Direction: Refined Expert Design

This document refines the direction in `.plan/direction.md`, `.plan/ideas.md`,
and `.plan/in-depth-operator.md` after reviewing the repository README files and
the current identity, transport, and sync scaffolding.

It is a design document, not an implementation claim. The goal is to define a
coherent security and distributed-systems model before adding networking to the
database API.

The central recommendation is:

> A `Workspace` remains a local durable replica, while an attached cluster
> runtime gives it authenticated membership, policy-aware replication, and
> bearer-neutral peer connectivity.

The cluster must not be modeled as a server that owns the truth. It is a set of
authorized replicas that exchange signed mutations and snapshots, converge by
CRDT rules, and enforce authorization independently at every replica.

## 1. Design Baseline

The current repository already provides a useful local kernel:

- `zendb-types` provides CRDT values, paths, cells, events, HLCs, and identity
  primitives.
- `zendb-storage` provides durable KV backends and per-table segmented topics.
- `zendb-engine` owns the database lifecycle, tables, states, operators, timers,
  and facets.
- `zendb-identity` provides the beginnings of OIDC, workspace credentials,
  invites, peer claims, and bootstrap bundles.
- `zendb-transport` provides type-level discovery, rendezvous, handshake,
  framing, channels, and session abstractions.
- `zendb-sync` provides type-level journal records, version vectors, range
  messages, snapshot metadata, and engine-independent replication interfaces.

The following decisions should remain fixed:

1. The CRDT and storage layers stay product-agnostic.
2. A local table topic is a local runtime feed, not the distributed journal.
3. An operator specification is durable desired state; a worker is a local
   realization.
4. OIDC is an online authentication and bootstrap mechanism, not the complete
   offline authorization system.
5. Discovery can locate a peer but can never authorize a peer.
6. A signature proves provenance and possession of a key. It does not by itself
   prove permission to perform an operation.
7. Every receiving replica re-evaluates authorization before accepting a remote
   event, job result, snapshot, or control-plane mutation.
8. The system supports eventual convergence, not a global total order or
   exactly-once execution of arbitrary external side effects.

### 1.1 The most important refinement

The existing plans put the shared journal before full membership and onboarding.
That is acceptable for defining data structures, but unsafe for live operation.
No production replication path should be enabled until it has:

- authenticated device identity,
- workspace binding,
- credential verification,
- policy evaluation,
- replay protection, and
- an explicit decision for what data the peer may receive.

The implementation can build journal types first, but the runtime order must be
identity and policy gates first, then data exchange.

## 2. Vocabulary and Boundaries

Use precise terms because several currently overlap.

### 2.1 Workspace

A workspace is the security and data boundary represented by `WorkspaceId`.
It contains:

- shared user-authored data,
- replicated system state,
- membership and policy state,
- the set of admitted devices,
- a workspace signing and policy authority model.

The workspace is the object a user joins. It is normally the same logical unit
that an application calls a database cluster.

### 2.2 Cluster

Use `ClusterId` only if a future product needs several independent replication
domains inside one workspace. Otherwise, treat the workspace as the cluster
identity and keep one explicit `WorkspaceId` in every session and journal
record.

A cluster is not a network topology. It is the logical set of replicas that
participate in one workspace replication domain. The network graph is allowed
to change continuously.

### 2.3 Replica

A replica is one installed database instance for one workspace. A replica has:

- a stable `DeviceId`,
- a local key pair,
- local durable storage,
- a local replication cursor/version vector,
- a local policy cache,
- zero or more active peer sessions.

One physical device may host several replicas, so `DeviceId` and `ReplicaId`
should not be silently treated as the same identity forever. The current
`ReplicaId { device_id, workspace_id }` is a reasonable first form.

### 2.4 Principal

A principal is the subject on which authorization is evaluated. Support at
least:

- `UserPrincipal(UserId)` for a human account,
- `DevicePrincipal(DeviceId)` for a concrete installation,
- `GuestPrincipal(GuestId)` for an anonymous but cryptographically stable
  session or enrollment,
- `ServicePrincipal(ServiceId)` for a trusted hosted component,
- `OperatorPrincipal(OperatorId)` for database automation.

Do not collapse the user and device into one principal. A user can revoke one
device without revoking the user, and a device can be restricted even while
its user remains an editor.

### 2.5 Resource

Every protected object must have a stable resource address. A useful logical
form is:

```text
workspace/<workspace-id>/table/<table-id>/row/<primary-key>/path/<path>
```

The implementation may use a compact binary resource ID, but the evaluator
needs the same hierarchy:

- workspace,
- table or system catalog,
- row/object,
- nested path or field,
- operation-specific subresource such as an audit record or job.

## 3. Security Invariants

These are non-negotiable invariants for the implementation and test suite.

1. No unauthenticated network peer may read or mutate workspace data.
2. Anonymous transport discovery is allowed; anonymous data access is not
   implied by discovery.
3. A remote peer cannot grant itself a role by sending a role string.
4. A valid event signature is insufficient without a valid authorization path.
5. Policy checks happen on local writes and on remote application.
6. A peer may only receive a snapshot or event subset allowed by its effective
   read policy.
7. A revoked device cannot establish new sessions or claim new work after the
   revocation becomes visible to the accepting replica.
8. Already-replicated plaintext cannot be reliably erased from a device that
   was previously authorized to receive it.
9. Ambiguous high-risk policy changes fail closed instead of resolving to an
   accidental allow.
10. External side effects are at-least-once unless the external system provides
    an idempotency contract; the database must record that fact explicitly.
11. A lease loss must stop a shared worker before it performs another shared
    write.
12. Transport failure must not corrupt journal application or turn a partial
    snapshot into a valid replica.

## 4. Target Architecture

Keep the four-layer model from the existing direction and make the security
flow explicit:

```text
application / product schema
        |
        v
zendb-engine
  - Workspace lifecycle
  - system catalogs and policy evaluation
  - operator reconciler and local workers
  - cluster runtime adapter
        |
        +-------------------+
        |                   |
        v                   v
zendb-sync              zendb-transport
  journal protocol        discovery, rendezvous,
  anti-entropy             bearer sessions, framing
  snapshot/tail
        |                   |
        +---------+---------+
                  v
            zendb-identity
             credentials, keys,
             membership, invites
                  |
                  v
            zendb-types / storage
```

The dependency direction should become trait-based:

- `zendb-types` contains serializable primitives and does no I/O.
- `zendb-storage` contains durable mechanics and does not know principals or
  network peers.
- `zendb-identity` contains identity and credential formats plus verification
  interfaces.
- `zendb-transport` contains bearer-neutral connectivity and peer session
  interfaces.
- `zendb-sync` contains replication protocol logic against abstract journal,
  snapshot, authorization, and transport interfaces.
- `zendb-engine` implements the adapters and composes all of them.

The sync crate must not become a storage super-trait. It should contain
protocol data and pure exchange logic; the concrete Workspace owns journal
reads, verified appends, policy checks, cursors, and snapshot installation.

## 5. The Workspace and Cluster Runtime

The database should remain usable without networking:

```text
Workspace
  local storage
  local tables and topics
  local operators
  local policy evaluator
  optional ClusterRuntime
```

Do not make every table write implicitly perform network I/O. Instead, attach a
cluster runtime that observes accepted shared events and publishes them to a
shared journal.

### 5.1 Conceptual API

Names are illustrative, but the ownership model should look like this:

```text
Workspace::create_local(path, config)
Workspace::open(path, config)

db.cluster().create_workspace(genesis_config)
db.cluster().join(join_request)
db.cluster().leave()
db.cluster().status()
db.cluster().peers()
db.cluster().sync_now(peer_id)
db.cluster().issue_invite(invite_request)
db.cluster().approve_device(approval_request)
```

The cluster runtime owns:

- local replica identity,
- peer sessions,
- discovery and rendezvous providers,
- journal publishing and ingestion,
- snapshot export/import,
- anti-entropy scheduling,
- membership and revocation cache,
- policy-aware sync filtering.

The `Workspace` owns the authoritative local transaction boundary. A shared
write should be accepted locally only after authorization and should enter the
shared journal atomically with enough metadata to be replayed or audited.

### 5.2 Standalone creation

Creating a new workspace is different from joining one:

1. generate a workspace identity and initial authority key,
2. generate the first device key,
3. create a root user or explicit anonymous-owner mode,
4. write a signed genesis membership and policy record,
5. bind the local replica to the genesis hash,
6. only then allow the first shared write.

The genesis record must be immutable and included in every bootstrap manifest.
Two databases with the same human-readable name but different genesis hashes
must never be considered the same cluster.

## 6. Identity and Authentication

### 6.1 Three durable identities

Keep the existing three-way split:

1. `UserId`: a human or service account identity.
2. `DeviceId`: a key-bearing installation.
3. `WorkspaceId`: the data and trust boundary.

Add a fourth runtime concept:

4. `ReplicaId`: a device/workspace storage instance.

The device key is the primary proof used in peer sessions. User identity is an
authorization attribute and audit subject, not a substitute for device proof.

### 6.2 Key material

Each device needs:

- a non-exportable or locally protected private key where the platform allows,
- a public key and `DeviceId` binding,
- a key version or key ID,
- a creation time and status,
- optional attestation metadata,
- a rotation/revocation history.

Workspace credentials should reference an issuer key ID and credential serial.
Avoid treating `Vec<u8>` fields as sufficient cryptographic semantics. The
wire types may remain byte-oriented, but verification must specify:

- signature algorithm,
- signed canonical bytes,
- issuer key selection,
- nonce binding,
- expiry and not-before handling,
- revocation behavior.

### 6.3 User-authorized device

The normal flow is:

1. authenticate the user through OIDC or another configured provider,
2. map the external subject to a local `UserId`,
3. create a device key pair,
4. ask a workspace authority to enroll the device,
5. issue a workspace credential bound to `(WorkspaceId, UserId, DeviceId)`,
6. store the device membership in replicated system state,
7. bootstrap a permitted snapshot and journal tail,
8. start normal replication.

OIDC access and refresh tokens should stay in the online-service integration.
They should not be copied into peer-to-peer events or treated as offline
workspace credentials.

### 6.4 Peer handshake

The current transport handshake has the correct broad shape but needs stronger
binding. The session should proceed as:

```text
HELLO
  protocol versions, workspace hint, channel capabilities
CHALLENGE
  workspace, nonce, server/session ephemeral key, policy epoch hint
AUTHENTICATE
  device proof of possession, workspace credential, challenge signature
  optional online token only for an online validation path
AUTHORIZE
  local verifier checks issuer, binding, expiry, revocation, policy
ESTABLISHED
  accepted channels, peer limits, credential expiry, policy digest
```

The challenge signature must cover at least:

```text
protocol version
workspace id
local device id
remote device id if known
session nonce
ephemeral transport key or channel binding
requested channels
```

This prevents replaying a valid credential into another session or workspace.

### 6.5 Authentication versus authorization

Authentication answers:

```text
Which key-bearing device is speaking, and who does it claim to represent?
```

Authorization answers:

```text
May this principal perform this action on this resource, in this context,
through this device and session, at this policy epoch?
```

Do not place the final authorization decision in `WorkspaceCredential.role` or
`scopes`. Those fields can be useful cached inputs, but the accepting replica
must combine them with current membership, policy, device status, resource
sensitivity, and operator restrictions.

## 7. Anonymous and Guest Devices

"Anonymous" must not mean "no cryptographic identity". A network endpoint with
no proof of possession cannot safely join a database cluster.

Use this model instead:

```text
anonymous human
    -> ephemeral or pseudonymous GuestPrincipal
    -> device-generated key pair
    -> signed, narrow guest grant
    -> scoped cluster session
```

### 7.1 Guest enrollment

A guest may be admitted by:

- a one-time invite code,
- a QR pairing approval,
- a local administrator approval,
- a hosted service issuing a short-lived guest grant.

The grant must bind:

- workspace,
- guest/device public key,
- allowed resource selectors,
- allowed actions,
- expiry,
- maximum data volume or rate where appropriate,
- whether the guest may write or only read,
- issuer and policy epoch,
- one-time use or allowed session count.

### 7.2 Guest defaults

Default guest behavior should be:

- no membership enumeration,
- no policy reads,
- no device administration,
- no operator creation,
- no snapshot export,
- no access to system catalogs,
- no access to unrelated tables,
- no durable identity link to a user unless explicitly upgraded,
- read-only access unless a grant explicitly allows writes.

If a product wants an anonymous public board, that is an explicit workspace
policy and resource grant, not a side effect of enabling LAN discovery.

### 7.3 Guest writes

Guest writes should carry a `GuestPrincipal` and grant ID in their authorization
context. They may be merged as normal CRDT events if accepted, but the system
must retain enough audit information to distinguish them from user-authored
events. A guest grant can expire without invalidating already accepted history;
future writes and sessions are rejected.

## 8. Fine-Grained Authorization Model

Roles remain useful defaults, but the requested security model needs a hybrid
of RBAC, ABAC, and resource-scoped grants.

### 8.1 Policy decision shape

Define a pure, deterministic evaluator, ideally in a small new
`zendb-policy` crate or an equally isolated module:

```text
Decision {
    effect: Allow | Deny | Indeterminate,
    obligations: Vec<Obligation>,
    matched_rules: Vec<RuleId>,
    policy_epoch: u64,
    explanation_code: DecisionCode,
}
```

The evaluator must be side-effect free and independently testable. It should
never open a table, contact a peer, or call an OIDC provider.

### 8.2 Request context

Every authorization request should include:

```text
AuthorizationContext {
    principal,
    acting_user: optional UserId,
    device_id,
    workspace_id,
    resource,
    action,
    mutation_kind,
    data_labels,
    sensitivity,
    operator_id: optional,
    job_id: optional,
    transport_kind,
    authenticated_peer: optional,
    policy_epoch,
    online: bool,
    time,
}
```

Do not let callers omit fields silently. An absent context field should either
be represented as `Unknown` and fail closed for sensitive actions, or be
rejected as an invalid request.

### 8.3 Resource actions

Start with explicit actions rather than a single `read/write` bit:

```text
Read
Enumerate
Create
Update
Delete
Merge
Comment
Export
Import
SyncReceive
SyncSend
ManageMembership
ApproveDevice
RevokeDevice
ManagePolicy
CreateOperator
EnableOperator
DisableOperator
ExecuteOperator
CreateJob
ClaimJob
PublishJobResult
ReadAudit
ExportSnapshot
UseCapability
RelayTraffic
```

`SyncSend` and `SyncReceive` must be separate. A peer allowed to send its own
events is not automatically allowed to receive the full workspace.

### 8.4 Rules and bindings

A rule should contain:

```text
PolicyRule {
    rule_id,
    effect: Allow | Deny,
    principals,
    roles,
    actions,
    resource_selectors,
    required_labels,
    excluded_labels,
    max_sensitivity,
    required_device_trust,
    required_capabilities,
    operator_classes,
    transport_constraints,
    time_window,
    expiry,
    requires_online,
    requires_approval,
    rate_limit,
    data_redaction,
    issuer,
}
```

A membership binding should contain:

```text
MembershipBinding {
    workspace_id,
    principal,
    role,
    scope,
    conditions,
    granted_by,
    grant_id,
    policy_epoch,
    valid_from,
    valid_until,
    status,
}
```

Use resource selectors and labels to avoid generating an ACL row for every
field. A specific object can still have a scoped binding when necessary.

### 8.5 Evaluation algorithm

Use deterministic deny-overrides semantics:

1. verify the principal and device are valid,
2. verify workspace binding and membership status,
3. collect applicable role defaults,
4. collect workspace rules,
5. collect resource and sensitivity rules,
6. collect operator/job rules if automation is involved,
7. collect device and transport restrictions,
8. apply explicit denies,
9. require every required authorization layer to allow,
10. return obligations such as redaction, rate, approval, or online-only.

The effective result is an intersection, not a union:

```text
effective = membership
          AND workspace_policy
          AND device_policy
          AND resource_policy
          AND operator_policy
          AND session_policy
```

An `Owner` role must not bypass an explicit device quarantine or a resource
classification rule without a separate break-glass action. Break-glass use
should be time-limited, require fresh authentication, produce an audit event,
and never silently change the underlying membership role.

### 8.6 Policy precedence

Recommended precedence:

1. explicit security deny,
2. device/workspace quarantine,
3. resource-specific deny,
4. operator/capability restriction,
5. resource-specific allow,
6. workspace and role defaults.

When two replicated policy changes conflict at the same authority level, do not
choose an allow based only on HLC. Mark the policy as conflicted and use the
more restrictive effective result until an authorized policy resolution is
recorded.

### 8.7 Policy changes are control-plane mutations

Policy changes deserve stronger rules than ordinary CRDT content:

- signed by an authorized policy administrator,
- versioned by a monotonic policy epoch,
- auditable with old and new policy hashes,
- optionally approved by two administrators for high-risk changes,
- not effective locally until the mutation is durably committed,
- rejected when the target policy epoch is stale for destructive operations.

The workspace need not become a consensus database for ordinary content. For
high-risk authorization changes, however, use a configured authority or an
administrator quorum. Eventual CRDT merge alone cannot safely resolve a
simultaneous "deny device" and "allow device" decision.

### 8.8 Authorization of CRDT events

A CRDT merge rule answers whether a mutation can be combined with local state.
It does not answer whether the mutation was permitted.

For each event:

1. verify event encoding and workspace ID,
2. verify origin device signature,
3. verify origin sequence and event ID,
4. resolve the author/device principal,
5. resolve the authorization context at the declared policy epoch,
6. evaluate the action on the resource/path,
7. apply only if allowed,
8. otherwise quarantine with a reason and retain the audit hash.

Do not silently merge unauthorized events and hope a later policy update fixes
them. If policy state is missing, place the event in a bounded pending queue and
request the required policy records, subject to the peer's allowed sync scope.

## 9. Data and System Catalogs

Keep product tables above the engine. Engine-managed catalogs should include:

```text
_workspace_genesis
_users
_devices
_memberships
_policy_rules
_policy_epochs
_invites
_replicas
_sync_journal
_sync_cursors
_operator_specs
_operator_jobs
_operator_results
_operator_leases
_operator_checkpoints
_device_capabilities
_audit_log
_quarantine
```

The existing plan's `_policies` can be split into rules and epoch metadata once
the evaluator is implemented. System records need stable IDs, signatures or
issuer references where applicable, and explicit local/shared classification.

### 9.1 Shared state

Normally replicate:

- user-authored content,
- graph structure,
- comments and tasks,
- memberships and policy records,
- device status and revocation records,
- operator specs,
- job intent and results when workspace truth requires them,
- shared materialized outputs,
- audit summaries and security decisions.

### 9.2 Local-only state

Do not replicate by default:

- FTS indexes,
- embedding caches,
- UI cursors and layout,
- local secrets,
- shell runner configuration,
- local topic consumer offsets,
- worker internals,
- temporary session state,
- unapproved prompt or tool payload caches.

### 9.3 Conditional state

AI results, summaries, tags, and search artifacts need an explicit output class:

```text
LocalCache
SharedDraft
SharedCanonical
SensitivePrivate
```

The output class determines replication, authorization, audit retention, and
whether a remote operator may consume it.

## 10. Joining a Cluster

Joining must be a durable state machine, not a single `connect()` call.

```text
Standalone
  -> Discovering
  -> CandidateSelected
  -> RendezvousPending
  -> TransportConnected
  -> PeerAuthenticated
  -> AdmissionPending
  -> BootstrapReceiving
  -> BootstrapVerifying
  -> Installing
  -> CatchingUp
  -> Joined
```

Failure states should be explicit:

```text
Rejected
CredentialExpired
Revoked
PolicyUnavailable
SnapshotInvalid
ProtocolIncompatible
Quarantined
```

### 10.1 Join request

The new device sends a signed request containing:

```text
JoinRequest {
    workspace_id,
    candidate_device_id,
    candidate_public_key,
    requested_principal: UserId | GuestId | None,
    requested_role_or_grant,
    requested_capabilities,
    nonce,
    client_version,
    supported_protocols,
}
```

`requested_role_or_grant` is a request, never an entitlement. The approving
authority chooses the actual membership and resource scope.

### 10.2 User device bootstrap

Preferred flow:

1. new device creates a key pair and local staging database,
2. user authenticates online or uses an existing trusted device,
3. existing authority verifies the invite and intended user,
4. authority signs device membership and credential,
5. peers establish an authenticated bootstrap session,
6. source exports a policy-filtered snapshot,
7. source sends a manifest, policy anchor, workspace genesis, and snapshot,
8. destination verifies all hashes and signatures in staging,
9. destination installs atomically,
10. destination requests journal ranges after the snapshot vector,
11. destination enters live tail mode,
12. destination advertises its capabilities and sync summary.

### 10.3 Bootstrap bundle

Improve the current `BootstrapBundle` with:

- workspace genesis hash,
- snapshot manifest hash,
- snapshot content hash or Merkle root,
- complete version-vector anchor rather than only one watermark,
- policy epoch and policy digest,
- credential and revocation anchor,
- destination device key binding,
- allowed bootstrap scope,
- expiry and one-time use metadata,
- source replica and issuer signatures.

The encrypted workspace bundle should be encrypted to the destination device
key or a session key derived from both devices. A relay may forward it but must
not need plaintext access.

### 10.4 Anonymous or guest bootstrap

Guest bootstrap is not a full workspace snapshot. It should be one of:

- no snapshot, followed by scoped live reads,
- a filtered snapshot containing only explicitly public resources,
- a small invite-specific data package.

The guest's grant must be checked while producing the snapshot, and the
destination must enforce the same scope while installing and serving data.

### 10.5 Atomic installation

Never import directly into the live database directory. Use:

1. staging directory,
2. manifest and signature verification,
3. snapshot load and invariant checks,
4. policy/genesis validation,
5. journal tail verification,
6. atomic directory or generation swap,
7. recovery marker removal.

An interrupted install must leave either the old valid replica or the new valid
replica, never a partially installed hybrid.

## 11. Discovery and Rendezvous

Discovery and rendezvous solve reachability, not trust.

### 11.1 Discovery providers

Keep the current `DiscoveryProvider` and extend the result with:

```text
DiscoveredPeer {
    workspace_hint,
    device_id: optional,
    endpoints,
    advertised_protocols,
    bearer_capabilities,
    observed_at,
    expiry,
    score,
    signature_or_proof: optional,
}
```

Useful providers:

- LAN multicast or mDNS,
- explicit IP/port configuration,
- Bluetooth or local pairing,
- hosted presence registry,
- relay directory,
- manually transferred endpoint bundle.

LAN advertisements must be treated as attacker-controlled hints. A malicious
device can advertise a real workspace ID, so the handshake still needs a
credential and challenge signature.

### 11.2 Rendezvous tickets

The current `RendezvousTicket { token: String }` should become an opaque,
short-lived, purpose-bound capability. It should bind:

- workspace ID,
- inviter device ID,
- candidate device key or pairing code hash when known,
- ticket purpose (`PairDevice`, `JoinGuest`, `ConnectPeer`),
- allowed transports,
- expiry,
- one-time use nonce,
- optional allowed resource scope,
- issuer signature.

The rendezvous service should return candidate endpoints and relay instructions
without becoming a workspace data authority. Ticket resolution should not grant
data access by itself.

### 11.3 QR pairing

A QR payload should contain a short-lived ticket or a hash-based pairing code,
not a long-lived workspace secret. Pairing should use an authenticated key
exchange after the QR step. Display a confirmation phrase or channel binding on
both devices so a user can detect a relay or endpoint substitution.

### 11.4 Hosted coordination

An optional service may provide:

- OIDC integration,
- invite delivery,
- presence and endpoint registration,
- relay/tunneling,
- push notifications,
- encrypted backup,
- job arbitration.

It must not be the only place workspace content exists. The workspace should
remain operable in direct LAN mode and in an offline peer-to-peer mode after
devices have valid credentials.

## 12. Networking and Overlay Design

The transport layer should expose a logical peer session above several possible
bearers.

### 12.1 Bearer-neutral interfaces

Add interfaces conceptually equivalent to:

```text
BearerAdapter
  enumerate_endpoints()
  connect(endpoint)
  accept(listener)
  cost()
  capabilities()

TransportSession
  session_id()
  peer_identity()
  health()
  open_channel()
  send()
  receive()
  migrate()
  close()

PathSelector
  rank(candidates)
  select()
  monitor()
  handoff()
```

Candidate bearers include LAN/direct TCP or QUIC, Bluetooth, peer-to-peer
connections, and relay/tunnel paths. The sync layer should not know which one
is active.

### 12.2 Session framing

The current `TransportFrame { channel, request_id, payload }` is a good minimal
placeholder but needs operational metadata:

```text
TransportFrame {
    session_id,
    channel,
    stream_id,
    message_type,
    request_id,
    sequence,
    acknowledgement,
    flags,
    payload_length,
    payload_hash,
    payload,
}
```

The concrete wire protocol may use an established secure multiplexed transport,
but these semantics are still needed for bounds, flow control, resume, and
diagnostics. Enforce maximum frame and message sizes before allocation.

### 12.3 Path selection

Score paths on more than latency:

- authentication and workspace compatibility,
- observed reachability,
- bandwidth,
- latency and loss,
- cost,
- privacy/trust of the relay,
- battery impact,
- MTU and stream support,
- stability over a recent window.

Prefer direct LAN for bulk bootstrap when authenticated and healthy. Prefer a
relay when direct paths are unavailable. Keep an established logical session
while moving the underlying bearer.

### 12.4 Handoff

Bearer handoff should use a session resume token bound to:

- logical session ID,
- peer device keys,
- current policy epoch,
- stream positions,
- expiry.

The new bearer must authenticate before the old bearer is dropped. Sync streams
resume from acknowledged journal ranges, not from an assumed byte offset.

### 12.5 Overlay topology

Avoid naive full-mesh broadcast as the default. Maintain a partial overlay:

- each replica keeps a small number of healthy neighbors,
- high-capacity peers may serve as relays,
- peer selection uses workspace trust and reachability,
- anti-entropy runs over selected neighbors,
- gossip has hop limits, duplicate suppression, and backpressure,
- bootstrap and emergency paths may use more direct connections.

The overlay is a performance and availability optimization, not an authority
layer. A relay forwards authorized protocol messages but does not get to rewrite
their signed content.

## 13. Replication and Synchronization

### 13.1 Event identity

Add a stable identity to shared mutations. The minimum is:

```text
EventIdentity {
    origin_device_id,
    origin_seq,
}
```

The shared envelope should also carry:

```text
SyncEnvelope {
    workspace_id,
    event_id,
    author_principal,
    author_user_id: optional,
    policy_epoch,
    authorization_grant_id: optional,
    event_hash,
    previous_origin_hash: optional,
    signature,
}
```

`origin_seq` is for deduplication and range exchange. It is not a global causal
order. HLC remains useful for CRDT conflict semantics, but must not be used as
the only replication cursor.

The optional origin hash chain helps detect an origin device that forks or
rewrites its sequence. It is not a substitute for a consensus protocol.

### 13.2 Separate journals

Keep two distinct paths:

```text
local table Topic<Change>
  local operators, local resumability, local timers/materializers

shared replication journal
  signed events, anti-entropy, range requests, shared checkpoints
```

When a remote shared event is accepted into a table, it may produce a local
`Change` for local operators. That translation is one-way and local; a local
topic offset must never be presented as a portable distributed checkpoint.

### 13.3 Journal admission pipeline

Every received record follows:

```text
decode with bounds
  -> workspace/genesis check
  -> event hash check
  -> signature check
  -> origin sequence/fork check
  -> credential and revocation check
  -> policy evaluation
  -> duplicate check
  -> CRDT apply
  -> durable journal append
  -> version-vector update
  -> local topic publication
```

Journal append and materialized application need a recoverable transaction or
idempotent replay protocol. A crash between them must not lose the event or
publish it twice in a way that changes the resolved state.

### 13.4 Anti-entropy

The normal peer exchange is:

1. authenticate the session,
2. authorize `SyncSend` and `SyncReceive` scopes,
3. exchange protocol capabilities and workspace summaries,
4. exchange version vectors and snapshot generations,
5. compute missing origin ranges,
6. request ranges within allowed resource scope,
7. verify and apply bounded event batches,
8. acknowledge durable application,
9. optionally subscribe to a live tail,
10. periodically repeat anti-entropy.

Do not assume a peer can serve every range it claims. Handle missing ranges,
compacted ranges, partial scope, and retryable gaps explicitly.

### 13.5 Version vectors and partial replicas

Version vectors work for full replicas. Fine-grained read policies introduce
partial replicas, where a device legitimately has no events for resources it
cannot read.

Therefore a sync summary must eventually include scope information:

```text
ScopedSyncSummary {
    workspace_id,
    policy_epoch,
    resource_scope_hash,
    version_vector,
    snapshot_generation,
}
```

Never interpret absence of an event in a partial replica as proof that the event
does not exist. The peer may simply be outside its read scope.

### 13.6 Snapshot bootstrap

Default bootstrap should be:

```text
filtered snapshot at version vector V
  + policy/genesis anchor
  + journal tail for events after V
```

For full trusted replicas, the filter can be the full workspace. For guests or
restricted devices, the source must filter by resource authorization and the
destination must not receive hidden metadata through indexes, counts, or
backlinks.

Snapshot manifests need content hashes, generation, version vector, policy
epoch, scope hash, source replica, and signatures. A single `journal_highwatermark`
integer is insufficient for multiple origin devices.

### 13.7 Ordering, duplicates, and gaps

The protocol must tolerate:

- duplicate event batches,
- out-of-order origins,
- retries after acknowledgement loss,
- missing ranges due to compaction,
- a peer crash after apply but before acknowledgement,
- an origin device that sends conflicting records for one sequence.

Use idempotent event IDs and durable origin indexes. Quarantine sequence forks
and require an authority or explicit repair procedure; never silently pick one
fork based on arrival order.

### 13.8 Revocation and stale offline devices

Revocation is itself replicated state, so a disconnected device cannot learn it
until it reconnects. Define a product policy for the offline window:

- low-risk content may allow cached credentials until expiry,
- high-risk operations require a recent policy epoch or online check,
- device administration and policy changes require fresh authorization,
- peers reject credentials past expiry or known revocation immediately.

When a revoked device reconnects, it may upload previously created events only
if the receiving policy explicitly permits stale offline writes. Otherwise place
them in quarantine for an authorized review. This decision must be explicit;
silently accepting all old offline events undermines revocation.

### 13.9 Compaction

CRDT tombstones, event history, and authorization evidence cannot be compacted
independently. A compaction watermark should account for:

- known replica version vectors,
- required tombstone retention,
- policy/audit retention,
- pending quarantined events,
- snapshot generations available for recovery.

Never delete the only evidence needed to explain why a remote event was
accepted or rejected if audit retention requires it.

## 14. Operators, Jobs, and Cluster Control

The existing two-layer operator model remains correct and should be tied into
the same policy evaluator.

### 14.1 Operator classes

Keep:

- `LocalIndexer`: local state only, every eligible device,
- `SharedMaterializer`: deterministic shared derived outputs, usually leased,
- `ExternalRunner`: nondeterministic work through jobs,
- `AssistantAction`: explicit user-triggered work.

### 14.2 Operator authority

An operator must not inherit all authority of its creator. Its effective access
is the intersection of:

- creator's ability to create the operator,
- operator spec requested permissions,
- workspace policy,
- operator class restrictions,
- device capability policy,
- resource sensitivity,
- current approval state.

The operator receives a scoped execution context, not the whole `Workspace` as
an ambient authority. Preserve the current trait initially for compatibility,
but add an authorized facade for new operators.

### 14.3 External jobs

External work should use:

```text
pending -> claimed -> running -> succeeded
                         |-> failed -> retryable/terminal
                         |-> cancelled
```

Each job needs:

- operator and creator identity,
- input references and sensitivity,
- idempotency key,
- requested capability,
- allowed device/capability selector,
- lease/claim epoch,
- attempt and timeout,
- result hash,
- redaction policy,
- audit references.

Host runners should expose named, policy-checked capabilities such as
`llm.generate`, `http.fetch`, and `shell.named_runner`. Rhai must not gain
unrestricted process, browser, network, or filesystem access.

### 14.4 Leases

Leases are appropriate for singleton workers, but lease storage is not a magic
consensus protocol. Use:

- holder device ID,
- monotonically increasing epoch,
- expiry,
- last renewal,
- authority/policy epoch,
- shard ID,
- fencing token.

Every shared write from a leased worker must include the fencing token. A worker
that loses the lease must stop before its next write. Snapshot-first failover is
the right MVP; stable event checkpoints can follow.

## 15. Crate-Level Recommendations

### 15.1 `zendb-types`

Keep it pure. Add or stabilize:

- `EventIdentity`,
- sync envelope data types if they are truly shared wire primitives,
- principal/resource identifiers that require no policy evaluation,
- canonical serialization helpers.

Do not add OIDC clients, network sessions, filesystem access, or policy lookup.

### 15.2 `zendb-identity`

Own:

- key and credential formats,
- user/device/workspace relationships,
- invite and bootstrap envelopes,
- peer claims,
- cryptographic verification traits,
- revocation references,
- online OIDC mapping.

Improve the existing types by replacing self-asserted role/scopes with signed
issuer metadata, grant IDs, key IDs, policy epoch, and audience/purpose binding.

### 15.3 New `zendb-policy` option

A small pure crate is preferable if the policy surface grows as described. It
would contain:

- action/resource/principal types,
- policy rule encoding,
- deterministic evaluator,
- decision explanations,
- policy test vectors.

It should not load policy from storage. The engine supplies a policy snapshot.
If a new crate is too disruptive now, keep the same boundary as a dedicated
`zendb-engine::policy` module and preserve the pure evaluator contract.

### 15.4 `zendb-transport`

Own:

- endpoint candidates,
- discovery providers,
- rendezvous tickets,
- secure session establishment,
- bearer adapters,
- multiplexed framing,
- path health and handoff.

Do not put CRDT events or role evaluation in this crate. It can ask an
authorization callback whether a session/channel is permitted.

### 15.5 `zendb-sync`

Own protocol messages and pure exchange mechanics. Storage operations remain
concrete Workspace methods:

```text
ReplicatedEvent / SyncEnvelope
RangeRequest / EventBatch
SnapshotManifest / SnapshotExport
```

Keep the long-term dependency direction one-way. The engine consumes these
records and implements synchronization directly on Workspace-owned storage.

### 15.6 `zendb-engine`

Own:

- `Workspace` lifecycle,
- system catalogs,
- local policy snapshot and evaluation,
- authenticated cluster runtime,
- journal adapter,
- bootstrap coordinator,
- operator reconciler,
- local workers and facets.

The engine is the top-level integrator, not the lower-level protocol owner.

## 16. Recommended Implementation Sequence

The order below is safer than building unauthenticated sync and adding security
later.

### Phase 0: document current semantics

- classify tables and states as local or shared,
- make Rhai write mode explicit (`LocalOnly`, `SharedAllowed` plus an effect gate),
- remove the unused Rhai `state_path` setting and make durable state explicit,
- document that local topic offsets are not portable.

### Phase 1: pure identity and policy primitives

- stabilize principal, resource, action, grant, and credential types,
- implement signature verification interfaces,
- implement a deterministic policy evaluator,
- add deny-overrides, expiry, sensitivity, device, and operator tests,
- add golden serialization and policy decision test vectors.

### Phase 2: local authorization gate

- add authorization to local table/event writes,
- add authorization context to operator writes,
- add quarantine records,
- write audit records for denied control-plane operations,
- keep the database fully local and testable.

### Phase 3: cluster genesis and join state machine

- create workspace genesis,
- generate device credentials,
- implement invite and QR pairing,
- implement user-bound and guest-bound grants,
- add atomic bootstrap staging and installation,
- add device revocation state.

### Phase 4: authenticated transport

- implement one reliable bearer first, preferably a direct local transport,
- complete nonce-bound handshake and channel authorization,
- add frame bounds, flow control, request correlation, and health state,
- add a relay adapter without changing the session API,
- add discovery and rendezvous providers.

### Phase 5: journal and snapshot sync

- add stable event IDs and signed envelopes,
- implement journal append and origin indexes,
- implement version-vector exchange,
- implement full trusted snapshot plus tail,
- apply every received event through the authorization gate,
- add duplicate, retry, gap, fork, and crash-recovery tests.

### Phase 6: scoped/partial replication

- add resource scope hashes to summaries,
- implement filtered snapshots,
- implement filtered range requests,
- test that hidden data cannot leak through metadata or derived tables,
- define stale offline write behavior after revocation.

### Phase 7: control-plane reconciliation

- split operator spec from runtime state,
- add device capability summaries,
- add local reconciler,
- add capability-scoped operator execution contexts,
- add jobs and named host runners.

### Phase 8: shared operators and hosted coordination

- add fencing leases,
- use snapshot-first failover,
- add stable shared checkpoints,
- add partial overlay routing and handoff,
- add optional hosted presence, relay, invite, and job arbitration.

## 17. Verification Strategy

The distributed design is only credible if it has adversarial tests, not just
happy-path integration tests.

### Identity and authorization tests

- wrong workspace credential,
- wrong device binding,
- expired credential,
- revoked device,
- self-asserted role ignored,
- resource-specific deny overrides role allow,
- guest cannot enumerate hidden tables,
- operator cannot exceed its requested scope,
- policy epoch conflict fails closed,
- break-glass action is audited and expires.

### Join and bootstrap tests

- QR ticket replay,
- ticket bound to another device key,
- expired invite,
- invalid genesis hash,
- corrupted snapshot chunk,
- partial installation and restart,
- guest filtered snapshot,
- bootstrap through relay with no relay plaintext access.

### Transport tests

- spoofed LAN advertisement,
- challenge replay,
- session downgrade,
- frame size abuse,
- path handoff during a sync batch,
- relay failure and direct path recovery,
- duplicate frame and acknowledgement loss,
- overlay loop and gossip duplicate suppression.

### Sync tests

- duplicate events,
- out-of-order events,
- missing range after compaction,
- origin sequence fork,
- unauthorized event quarantine,
- policy record arriving after a data event,
- snapshot plus tail convergence,
- partial replica does not imply deletion,
- revoked offline device behavior,
- crash between journal append and table apply.

### Operator tests

- worker starts only when placement and policy allow,
- lease fencing prevents stale writes,
- job retry preserves idempotency,
- capability changes stop a worker,
- operator creation requires control-plane permission,
- external result is classified and replicated according to output policy.

## 18. Final Position

The strongest version of the ZeninDB direction is not a central server and not
an unauthenticated peer mesh. It is:

1. a local-first database with durable CRDT state,
2. a workspace-scoped identity and policy system,
3. a cluster runtime attached to each local `Workspace`,
4. authenticated device-to-device sessions over interchangeable bearers,
5. rendezvous and relays that assist connectivity but do not own trust,
6. signed, authorization-checked replication with snapshot plus tail bootstrap,
7. partial replicas when fine-grained read policy requires them,
8. durable operator desired state reconciled into local workers,
9. jobs and capability-gated runners for nondeterministic effects,
10. leases and fencing for shared singleton work.

The most important security conclusion is that RBAC alone is not enough, and
cryptographic signatures alone are not enough. Roles provide defaults;
resource, device, sensitivity, operator, transport, time, and approval policy
provide the actual boundary. Signatures establish provenance; the receiving
replica still decides whether the operation is admissible.

The most important distributed-systems conclusion is that a cluster is a set of
replicas, not a single network path. Discovery, rendezvous, direct LAN,
Bluetooth, peer-to-peer, and relay connectivity should all feed one logical
authenticated session abstraction. Sync should operate over that abstraction,
retain its own durable journal semantics, and remain independent from the local
operator topics that already work well.

That design preserves the repository's strongest decisions while making
authentication, synchronization, networking, rendezvous, bootstrapping, and
fine-grained authorization explicit enough to implement and test incrementally.

---

## 19. Final API and Boundary Clarification

The design is intentionally a client-side database design. The embedded
database is a replica, policy evaluator, credential verifier, local reconciler,
and worker host. It is not a server by implication.

### 19.1 Canonical operator API

The final operator object graph is:

```text
OperatorSpec                         shared desired state
  -> OperatorAdmission               current policy decision
  -> plan_reconciliation             pure local decision plan
  -> Workspace lease state           advisory or authoritative ownership
  -> OperatorWorker                  local worker realization
  -> Workspace-owned status/jobs     local observations, checkpoints, jobs
```

`OperatorRuntimeConfig` is only a derived worker adapter. It must not contain
the authoritative placement, permission, output, or lease policy. The old
single `OperatorEntry { config, phase }` shape is therefore not the final
control model.

The generated dispatch set is the device-local source boundary. It decides
whether a native or Rhai source is supported and decodes its canonical config;
the replicated spec never assumes every device has the same operator registry.

`OperatorClass` is operationally meaningful:

- `LocalIndexer` produces local state by default;
- `SharedMaterializer` requires deterministic shared-output policy and usually
  a fenced lease;
- `ExternalRunner` uses jobs and named capabilities;
- `AssistantAction` is explicit, short-lived, and user initiated.

### 19.2 Authorization of automation

An operator's effective authority is the intersection of creator authority,
operator permission request, class restrictions, device trust and capabilities,
resource sensitivity, approval state, and current workspace policy. A script,
lease, credential, or role cannot widen that intersection.

Rhai execution has bounded resource limits, explicit capability requests, and a
default local-only write mode. The host never registers unrestricted filesystem,
process, browser, shell, or network functions. External work becomes an
`OperatorJob` with an idempotency key, claim epoch, result hash, and audit
metadata.

### 19.3 Handoff correctness

The control plane detects drift by comparing `OperatorSpec` generation with
`OperatorObservation` and local device facts. A singleton worker is runnable
only while its lease epoch and fencing token are current. A new holder must
restore a compatible checkpoint or snapshot before processing. Local topic
offsets are never treated as portable ownership evidence.

### 19.4 Optional hosted services

The embedded crates contain client-side interfaces only. Optional outbound
adapters live in `zendb-external`:

- `HostedCredentialClient` for credential requests;
- `HostedDiscoveryClient` for presence/discovery;
- `HostedRendezvousClient` for ticket exchange;
- `HostedJobClient` for optional job transport.

These adapters do not define server handlers or authority semantics. Their
responses are untrusted inputs to the normal local verifier, policy evaluator,
transport handshake, and durable stores. A workspace can operate without this
crate using direct peer bootstrap and local network transport.

### 19.5 Final review rule

When a future API proposal is added, it must answer four questions:

1. Is this shared database truth, local runtime state, or an external client
   adapter?
2. Does it carry desired state, observed state, or an effect request?
3. Which local policy and identity boundary verifies it?
4. Can it survive duplicate delivery, restart, lease loss, and offline use?

If those answers are not explicit, the API is not ready to enter the public
surface.

## 20. Trait Budget and Workspace Structure

The canonical client root is the concrete `Workspace`. The engine module and
folder are both `workspace`; the root owns local tables, state, identity
binding, synchronization coordination, policy evaluation, and operator
lifecycle. There is no `Workspace` trait and no compatibility type.

Traits remain only where substitution is real: storage, identity providers,
cryptographic key access, transport bearers, authorization,
native operators, and optional external clients. Path selection and
reconciliation are concrete algorithms. Workspace-owned operator catalogs,
leases, jobs, checkpoints, worker lifecycle, and sync storage operations remain
methods and state of the concrete Workspace. The sync crate contains protocol
records rather than a broad storage interface.
