# Ground-Up Workspace Design

> **Status: historical exploration.** The accepted records in
> [decisions/](decisions/) are normative. Implemented ADRs 001 through 007 and
> 009 supersede this document. ADR 008 remains proposed. Principal/OAuth models,
> broad transport traits, and similarly named deleted APIs below are not current.

This is the current implementation-oriented view of ZeninDB. The word
Workspace means the one concrete local durable root. It is not a trait and it
is not a second object layered over another database object.

## Core Ownership

| Layer | Concrete owner | Responsibility |
|---|---|---|
| Data types | `zendb-types` | CRDT values, HLC, IDs, authorization records, operator desired objects, leases, jobs, checkpoints |
| Durable storage | `zendb-storage` | B+ trees, key directories, topics, tables, typed states |
| Local root | `zendb-engine::Workspace<D>` | Path, device identity, workspace identity, catalogs, tables, states, timers, native workers, future sync and reconciliation methods |
| Identity adapters | `zendb-identity` | Device signing, OIDC validation, credential verification, trust storage, bootstrap records |
| Connectivity adapters | `zendb-transport` | Discovery, rendezvous, bearer connections, handshake, authenticated sessions, path handoff |
| Replication protocol | `zendb-sync` | Sync messages, anti-entropy data, snapshot metadata, and pure exchange records |
| Optional hosted clients | `zendb-external` | Outbound calls to hosted credential, rendezvous, discovery, and job services |

The dependency direction is intentional. `Workspace` can run with only local
storage and an executor. Networking and authentication are attached when the
application needs a cluster.

## Use Case 1: Purely Local Workspace

This is the simplest and fully working path today.

Application-facing classes:

- `Workspace<D>` and `WorkspaceConfig`
- `Executor`
- `TableConfig`, `TableHandle`, `Table`
- `StateConfig`, `StateHandle`, `State<K, V>`
- `Operator`, `DispatchConfig`, `DispatchOperator`
- `OperatorRuntimeConfig` and `Subscription`
- CRDT `Event`, `Hlc`, `Op`, `Value`, and `Path`

Flow:

1. The application defines an operator set with `define_operator_set!`.
2. It calls `Workspace::create` or `Workspace::open` with a path, executor,
   and `WorkspaceConfig`.
3. The Workspace persists `_workspace_id` and `_device_id`, opens its local
   catalogs, and starts its timer scheduler.
4. `Workspace::table` creates or opens a table and returns a weak
   `TableHandle`. The application upgrades the handle for one operation.
5. The application inserts CRDT events. The table applies them to materialized
   state and emits local topic changes.
6. `Workspace::state` opens typed private or derived state.
7. `Workspace::dispatch_operator` registers a local native operator and starts
   it when matching inputs exist. This is an imperative local convenience,
   not the future cluster desired-state path.
8. The worker receives changes and timers, calls the `Operator` lifecycle, and
   exposes its typed facet through `Workspace::facet`.
9. Dropping the last Workspace tears down its workers and owned resources.

There is no identity handshake, discovery, rendezvous, sync, lease, or hosted
service in this flow.

## Use Case 2: Several Devices on One Local Network

Example: an iPhone, a laptop, and two desktop devices on the same LAN.

Application-facing classes:

- Local device identity: `DeviceSigner`, `DeviceId`, `KeyId`
- Enrollment records: `BootstrapRequest`, `BootstrapApproval`,
  `WorkspaceCredential`, `WorkspaceTrustStore`
- Discovery: `DiscoveryProvider`, `DiscoveredPeer`
- Pairing/reachability: `RendezvousProvider`, `RendezvousRequest`,
  `RendezvousTicket`
- Connection: `BearerAdapter`, `RawTransport`, handshake messages,
  `TransportSession`
- Replication: sync message and snapshot metadata types; Workspace-owned sync methods

Flow:

1. Each installation creates and persists one `DeviceId` and private signing
   key. The same `DeviceId` is used by HLC ordering and replication metadata.
2. The new device creates a `BootstrapRequest` containing its workspace ID,
   device ID, key ID, and public key. It does not become trusted merely because
   it is visible on the LAN.
3. An already trusted device receives the request through a local pairing
   channel or QR transfer, checks membership policy, and returns a signed
   `BootstrapApproval` containing the initial snapshot anchor and approved
   device membership.
4. The new device stores the credential and trust records locally. A guest or
   anonymous user is represented by a stable cryptographic `GuestId`; anonymous
   means no mapped human user, not an unauthenticated network endpoint.
5. `DiscoveryProvider` finds candidate endpoints. Discovery data is untrusted
   reachability information and never grants workspace access.
6. `RendezvousProvider` converts a pairing or peer request into a reachable
   endpoint. On a LAN this may be a direct address; it does not need a relay.
7. `BearerAdapter` opens a `RawTransport`. The handshake proves the peer key,
   workspace ID, credential, expiry, and revocation state before producing a
   `TransportSession`.
8. Workspace sync methods use the authenticated peer and `zendb-sync` message
   types to exchange summaries, request missing ranges, or install a staged
   snapshot.
   The receiving Workspace rechecks signatures, sequence, membership, and
   policy before append.
9. A disconnected device keeps local writes. Reconnection exchanges durable
   shared journal identities and version vectors; local operator topic offsets
   are not used as replication cursors.

The current repository has the records and adapter boundaries for this flow.
`Workspace::join_workspace` and `Workspace::synchronize_workspace` are still
explicit stubs; they do not claim that this protocol is already wired.

## Use Case 3: Devices Behind Different Networks

The local flow remains the same after the connection path changes.

The application may add `HostedDiscoveryClient` and
`HostedRendezvousClient` from `zendb-external`, or provide its own local
`DiscoveryProvider` and `RendezvousProvider`. A hosted service returns
presence, tickets, or relay endpoints only. It is not trusted workspace state.

Flow:

1. The Workspace asks the external client for a candidate or rendezvous ticket.
2. The application selects a bearer and opens a `RawTransport`.
3. The normal workspace handshake authenticates the peer. A relay does not
   bypass credential verification or authorization.
4. `TransportSession` carries sync frames. `PathSelector` can choose a better
   bearer while preserving the same logical session identity.
5. Replication still crosses concrete Workspace sync methods; hosted
   reachability never writes directly into CRDT state.

This is why rendezvous is a transport concern, not a database concern. It
answers "where can I try to connect?" The Workspace and identity layers answer
"who is this and what may it do?"

## Use Case 4: User Login, Guest Access, and Account Switching

There are two identities that must not be conflated:

- `DeviceId`: the durable installation/replica identity used by HLC and sync.
- `PrincipalId`: the current actor, such as `User(UserId)`, `Guest(GuestId)`,
  `Device(DeviceId)`, `Service(ServiceId)`, or `Operator(OperatorId)`.

User login flow:

1. An `OidcProvider` validates an online token and returns `OidcClaims`.
2. The application derives `WorkspaceClaims` for the selected workspace.
3. A credential request is checked against those claims and workspace policy.
   Credential issuance is an external or trusted-peer operation, not a hidden
   server interface in the embedded engine.
4. The signed `WorkspaceCredential` is stored in the local
   `WorkspaceTrustStore` and is checked by `WorkspaceCredentialVerifier`.
5. Every operation is evaluated with an `AuthorizationContext`; roles are
   only inputs. Resource selectors, action, sensitivity, device trust,
   capabilities, policy epoch, approval, and expiry all matter.

Guest flow:

1. The device still proves a `DeviceId` and signing key.
2. The workspace grants a scoped credential to a `GuestId`, usually with an
   expiry and restricted resource selectors.
3. A guest can be anonymous as a human identity while remaining accountable as
   a cryptographic principal.

Account switching on one device:

1. `DeviceId` and its key do not change.
2. The active principal and selected workspace credential change.
3. Cached credentials for the old principal are not silently used for the new
   session; authorization is evaluated under the new principal.
4. Device-level membership and revocation still apply because the device is
   the replication actor. If policy requires per-user device separation, the
   application uses separate Workspace profiles/directories.

## Use Case 5: Declarative Operator Placement and Handoff

This is the Kubernetes-like control model, but the control plane is local
Workspace state replicated between clients rather than a required server.

Shared types:

- `OperatorSpec`: desired state and generation
- `OperatorObservation`: status only
- `PlacementPolicy`, `DeviceCapabilitySummary`, `OperatorPermissionRequest`
- `OperatorLease`, `OperatorCheckpoint`, `OperatorJob`, `OperatorJobResult`
- `ReconcileSnapshot`, `ReconcileAction`, `plan_reconciliation`
- `CapabilityHost` for named device-local capabilities

Flow:

1. A user or operator writes an `OperatorSpec` containing source, config,
   trigger, placement, permissions, approval, retry, and output policy.
2. The Workspace reads a coherent snapshot of desired specs, local
   observations, policy admissions, device capabilities, lease records, and
   current time.
3. `plan_reconciliation` returns start, stop, renew, or release actions. It is
   a pure concrete function and does not mutate state.
4. Workspace-owned lease logic checks placement and fencing. Advisory leases
   require idempotent/CRDT-safe output; authoritative leases require a
   serialized epoch source.
5. The concrete Workspace worker starts the native `Operator` only after source
   support, capability, policy, and lease checks pass.
6. Observations are written after the action crosses its boundary. They never
   overwrite desired state.
7. On handoff, the old holder stops, the new holder verifies generation and
   fencing epoch, restores a compatible checkpoint, and then processes input.
8. External or nondeterministic work becomes an `OperatorJob` with an
   idempotency key. It is not an arbitrary network call from `process`.

The current local worker lifecycle is implemented. The desired-state
reconciler, durable shared operator catalogs, lease operations, checkpoints,
and job transitions are represented by types and explicit Workspace/planner
boundaries, but their coordination methods remain stubs.

## Trait Audit

Kept as traits because substitution is real:

- `Executor`: applications choose Tokio, smol, or a test executor.
- `Operator`: applications provide different native operator types.
- `DispatchConfig` and `DispatchOperator`: generated type-erasure boundary for
  an application operator set.
- `CapabilityHost`: applications install different named local capabilities.
- `DeviceSigner` and `WorkspaceTrustStore`: OS keystore, file store, and tests.
- `OidcProvider` and `WorkspaceCredentialVerifier`: provider and crypto
  adapters.
- `DiscoveryProvider`, `RendezvousProvider`, `BearerAdapter`,
  `RawTransport`, `TransportSession`: genuinely different network paths.
- Sync message types: protocol data belongs in `zendb-sync`; storage and staged
  installation remain concrete Workspace operations.
- Hosted client traits: independent optional external services.

Kept concrete because there is no useful alternate implementation boundary:

- `Workspace` and all of its local catalogs and lifecycle methods.
- `PathSelector` and `plan_reconciliation`.
- Operator observations, leases, jobs, checkpoints, and worker lifecycle.
- `LeaseConsistency`, authorization decisions, and other policy records.
