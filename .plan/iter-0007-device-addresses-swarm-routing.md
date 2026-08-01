# Iteration 0007: Device Addresses And Swarm Routing

Status: implemented.

Iteration 0006 made `_devices` the source of truth for workspace membership and
made `ReplicationStateListener` project membership changes into the private
libp2p runtime. The runtime still has no durable route from an enrolled device
to a network endpoint. It dials staged bootstrap strings and mDNS discoveries,
but it does not dial the devices already present in the registry.

This iteration adds Admin-managed device addresses, projects the complete
device registry into the Swarm, and adds retry behavior for temporarily
unreachable devices. Addresses remain routing hints. The device public key is
the identity, the derived `PeerId` is the Swarm key, and Noise authenticates the
peer reached at an address.

This iteration does not implement initial joiner synchronization, relays, NAT
traversal, automatic external-address discovery, or device-authored presence.

---

## 1. Problems

### 1.1 Enrolled Devices Have No Durable Route

`DeviceRecord` currently stores the display name, role, and workspace public
key. On workspace open, `ReplicationController` knows which installations are
remote, but the worker has no address at which to dial them. Replication works
only when mDNS happens to find the peer or an unrelated bootstrap hint is
available.

### 1.2 The Swarm Does Not Mirror The Registry

The runtime's membership state is currently split between a set of remote
installation IDs and a live-only blacklist. After a worker restart, a removed
device is simply unknown rather than explicitly revoked. mDNS can also cause
the worker to dial peers that are not enrolled in the workspace.

The worker needs a complete projection of the current registry:

```text
InstallationId -> DeviceRecord.public_key -> PeerId -> candidate addresses
```

Only the first two values are persisted. `PeerId` and the merged runtime
address book are derived.

### 1.3 Discovery Is Not Membership

mDNS and Identify can discover addresses, but discovery does not grant access.
A peer discovered on the same LAN may belong to another workspace or may have
been removed from this one. Unknown peers must not become Gossipsub peers merely
because libp2p found them.

### 1.4 A Single Dial Attempt Is Insufficient

Devices are routinely offline when a workspace opens. An initial failed dial
must not fail `Workspace::open`, and it cannot be the final attempt. The worker
needs bounded reconnect scheduling that is reset by a successful connection or
new address information.

---

## 2. Design Principles

1. `_devices` remains the authoritative membership catalog.
2. A public key identifies a device; an address never does.
3. Registry addresses are durable Admin-authorized routing hints.
4. mDNS and Identify addresses remain transient.
5. An empty address list is valid. Such a device may connect inbound or be
   discovered later.
6. Registry changes are projected into the running worker through commands;
   the worker never reads workspace tables directly.
7. Dial failures are runtime state, not workspace errors or replicated data.
8. Initial joining remains a separate pre-trust flow. Its bootstrap hints are
   not enrolled device routes.

---

## 3. Persisted Address Representation

### 3.1 ZenDB Multiaddr Wraps The External Multiaddr

ZenDB's `Multiaddr` is part of the persisted device-record data model and
therefore lives in `zendb-types`. That crate depends on the standalone
`multiaddr` value crate, not the libp2p transport or Swarm runtime.

It is a public newtype around `multiaddr::Multiaddr`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Multiaddr(Libp2pMultiaddr);

impl Multiaddr {
    pub fn from_libp2p(address: Libp2pMultiaddr) -> Result<Self, MultiaddrError>;
    pub fn as_libp2p(&self) -> &Libp2pMultiaddr;
    pub fn into_libp2p(self) -> Libp2pMultiaddr;
}

impl FromStr for Multiaddr { /* Multiaddr parsing */ }
impl Display for Multiaddr { /* canonical Multiaddr text */ }
```

The wrapper exists for the same reason as the persisted `PublicKey` wrapper:
ZenDB cannot implement bincode traits for an external type. Encoding writes the
canonical binary multiaddr returned by `Multiaddr::to_vec`; decoding uses
`Multiaddr::try_from(Vec<u8>)`. Text is only an application-facing form and is
not the on-disk representation.

ZenDB's `Multiaddr` represents a route to the device, not the device identity. Its
terminal protocol must not be `/p2p/<peer-id>`. The destination `PeerId` is
derived from `DeviceRecord.public_key` and supplied separately to `DialOpts`.
Intermediate peer components remain possible for future relay routes ending in
`/p2p-circuit`, without duplicating the destination identity in the record.

Examples:

```text
/ip4/192.0.2.10/tcp/7400
/ip6/2001:db8::10/tcp/7400
/dns4/laptop.example.net/tcp/7400
```

### 3.2 DeviceRecord Gains Addresses

```rust
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceRecord {
    pub display_name: String,
    pub role: Option<Role>,
    pub public_key: PublicKey,
    pub addresses: Vec<Multiaddr>,
}
```

The vector preserves Admin preference order. The worker removes duplicate
addresses when building a dial attempt. The list is not capped in this
iteration; workspace Admins are already trusted to control registry data.

This is an intentional breaking bincode change. ZenDB has no migration
requirement, so old device records do not need compatibility handling.

### 3.3 Addresses Are Admin-Owned

`DeviceRecord` is still an opaque blob containing role, public key, display
name, and addresses. Only an Admin may replace it. A Contributor or Reader may
not update just its own address list because the current event shape cannot
prove that the privileged fields were left unchanged under concurrent writes.

For this iteration, applications provision stable addresses out of band and an
Admin writes them through `Devices::upsert`. If later work needs devices to
publish frequently changing endpoints themselves, it should add a separate
leased `_device_presence` table keyed by `InstallationId`. That table can have
self-author authorization and expiry semantics without weakening `_devices`.

---

## 4. Listening And Advertising Are Different

A local listen address is runtime configuration. A remote dial address is
replicated workspace data. They must not be treated as the same value.

```rust
pub struct ReplicationConfig {
    pub batch: BatchConfig,
    pub topology: TopologyConfig,
    pub dial: DialConfig,
    pub listen_addresses: Vec<Multiaddr>,
    pub outbound_capacity: usize,
    pub gossipsub_max_transmit_size: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            // existing fields omitted
            dial: DialConfig::default(),
            listen_addresses: vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()],
        }
    }
}
```

`build_swarm` calls `listen_on` for every configured address instead of owning
one hard-coded address. An empty vector creates an outbound-only worker.

ZenDB does not automatically copy `SwarmEvent::NewListenAddr` into the local
`DeviceRecord`. Wildcard addresses, ephemeral ports, and private interface
addresses are often not remotely dialable, and a non-Admin installation cannot
rewrite its registry row. The application or Admin chooses the advertised
addresses deliberately.

A typical stable setup is:

```text
listen address:     /ip4/0.0.0.0/tcp/7400       (runtime config)
advertised address: /dns4/laptop.example/tcp/7400 (DeviceRecord)
```

---

## 5. Registry Projection

### 5.1 Controller State

`ReplicationController.remote_devices` becomes a map containing the current
route projection rather than a set containing only installation IDs:

```rust
struct PeerRoute {
    peer_id: PeerId,
    addresses: Vec<Multiaddr>,
}

remote_devices: Mutex<BTreeMap<InstallationId, PeerRoute>>,
```

`PeerRoute` is private and derived from `DeviceRecord`. It is never persisted.
The map still determines whether at least one remote device exists and thus
whether replication should run.

When the controller starts a worker, it passes a complete snapshot of all
remote routes. This removes the startup race in which the worker exists before
it has learned about already-enrolled devices.

### 5.2 Worker Commands

The current `Allow(PeerId)` and `Revoke(PeerId)` commands are replaced by
commands that carry the complete desired route:

```rust
enum Command {
    Event { table: String, event: Event },
    UpsertPeer {
        installation_id: InstallationId,
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },
    RemovePeer {
        installation_id: InstallationId,
        peer_id: PeerId,
    },
    Stop,
}
```

The installation ID is included for deterministic replacement and diagnostics;
Swarm operations use the derived `PeerId`.

### 5.3 Device Change Semantics

`ReplicationStateListener` reacts after `DeviceRegistryListener` has updated
the cache. The controller rebuilds the desired route projection from the whole
cache and diffs it against its previous projection. Full reconciliation makes
duplicate-key quarantine and concurrent row changes deterministic:

| Registry transition | Runtime action |
|---|---|
| New remote record | add route, unblacklist PeerId, dial immediately |
| Same key, addresses changed | replace catalog routes, reset retry, dial if disconnected |
| Public key changed | remove, blacklist, and disconnect old PeerId; add and dial new PeerId |
| Remote row deleted | remove route, blacklist, and disconnect PeerId |
| Local row invalidated | preserve iteration 0006 drain-then-stop behavior |

Changing addresses does not disconnect an already authenticated connection.
The new list is used on the next reconnect. Removing a route stops future dial
attempts but does not by itself revoke the device; deleting the row or changing
its key performs revocation.

The first-remote and final-remote listener ordering from iteration 0006 remains
unchanged. On first enrollment the worker starts with the new route already in
its startup snapshot, then the enrollment event is queued. On final removal the
removal command and registry event are queued before the worker drains and
stops.

### 5.4 One Public Key Per Installation

Workspace identity derivation includes `InstallationId` and therefore creates
a one-to-one mapping between an installation and its workspace public key.
`Devices::upsert` trusts the Admin-provided derived key and does not perform a
redundant registry scan during enrollment.

The admission path still treats a Gossipsub source as valid only when its
`PeerId` resolves to exactly one current device record and that record is
`Envelope.author`. This handles conflicting replicated raw state that bypassed
the typed enrollment API: the worker neither dials nor accepts Gossipsub
messages from the ambiguous PeerId until an Admin corrects the registry.

This is a correctness invariant, not an address validation feature.

---

## 6. Worker Address Book

### 6.1 Address Sources Stay Separate

The worker owns an in-memory entry for each enrolled PeerId:

```rust
struct PeerEntry {
    installation_id: InstallationId,
    catalog: Vec<Multiaddr>,
    mdns: HashSet<Multiaddr>,
    identified: HashSet<Multiaddr>,
    retry: RetryState,
}
```

Catalog addresses are replaced by `UpsertPeer`. mDNS addresses are inserted and
expired by mDNS events. Identify addresses are retained only while the worker
runs and only for already-enrolled peers. Keeping sources separate prevents an
mDNS expiry from deleting an Admin-provided route.

Before a dial, the worker builds one ordered, deduplicated list: catalog
addresses first, then current mDNS addresses, then Identify addresses. The
catalog order remains the application's preference order.

The worker deliberately does not use `Swarm::add_peer_address` as its primary
address book. libp2p 0.56 broadcasts additions to behaviours but does not offer
a symmetric remote-peer address removal API. Explicit `DialOpts` built from the
current `PeerEntry` avoid stale addresses after a registry update.

### 6.2 Registry Membership Gates Discovery

mDNS and Identify enrich routes only after the discovered `PeerId` is found in
the current enrolled-peer map. Discovery of an unknown or ambiguous PeerId is
ignored. An unknown established connection is disconnected and its peer is not
admitted into Gossipsub.

An enrolled device with no catalog addresses is still allowed to connect
inbound and can still acquire an mDNS route. Address absence therefore does not
change authorization or runtime lifecycle.

Gossipsub's explicit-peer list is not used as the device registry. Enrolled
devices are dial targets and ordinary scored Gossipsub peers; the mesh retains
control over grafting and pruning. This preserves the topology and latency
scoring established in iteration 0006.

### 6.3 Admission Remains Independent

The worker checks that `message.source` is currently enrolled before sending an
envelope across the admission bridge. This avoids spending bounded admission
capacity on unknown peers.

`admission::admit_event` still performs the authoritative source-to-author and
role checks. A connection having passed the worker gate does not bypass
admission, and a stored address never participates in authorization.

---

## 7. Dialing And Reconnection

### 7.1 Authenticated Known-Peer Dials

Every registry-based dial uses the public-key-derived PeerId:

```rust
let options = DialOpts::peer_id(entry.peer_id)
    .condition(PeerCondition::DisconnectedAndNotDialing)
    .addresses(entry.candidate_addresses())
    .build();

swarm.dial(options)?;
```

The transport address selects an endpoint. The expected PeerId plus Noise
authentication proves which peer answered. A stale or malicious address may
cause a failed dial but cannot impersonate the enrolled device.

### 7.2 Retry State

```rust
pub struct DialConfig {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for DialConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
        }
    }
}
```

The worker attempts each enrolled peer with at least one candidate address
immediately on startup. An outgoing connection error schedules the next attempt
with exponential backoff capped by `max_backoff_ms`. A successful connection
resets the backoff. When the last connection to a peer closes, reconnect is
scheduled. A catalog or discovery address addition resets the backoff and
permits an immediate attempt.

No retry state is persisted. Restarting the workspace starts with an immediate
dial, which is the desired recovery behavior. No random jitter is introduced in
this iteration; workspace device sets are expected to be small.

Initial dial failures do not fail workspace open or device enrollment. Errors
constructing the Swarm or applying an invalid listen configuration remain
worker startup errors.

---

## 8. Enrollment And Bootstrap Boundaries

Direct enrollment now includes stable address hints:

```rust
workspace.devices().upsert(
    installation_id,
    DeviceRecord {
        display_name: "new-laptop".to_owned(),
        role: Some(Role::Contributor),
        public_key,
        addresses: vec!["/dns4/laptop.example/tcp/7400".parse()?],
    },
)?;
```

The list may be empty when the peer is LAN-only, inbound-only, or not yet
provisioned. An Admin can replace the record later to add or remove routes.

`JoinHints.bootstrap_peers` remains separate. A staged joiner has no trusted
copy of `_devices`, so a bootstrap hint is pre-trust information used only by
the future initial synchronization protocol. Once the device registry is
synchronized, normal replication uses `DeviceRecord.addresses` and the
public-key-derived PeerIds. The active replication worker must not treat raw
join hints as enrolled peers.

---

## 9. Implementation Sequence

### Phase 0 - Persisted Address Type

1. Add `Multiaddr` under `zendb-types` with Multiaddr byte encoding,
   parsing, display, and conversion accessors.
2. Add `addresses: Vec<Multiaddr>` to `DeviceRecord` and update every record
   construction site.
3. Re-export `Multiaddr` from `zendb-types` and, for API convenience,
   `zendb-workspace`; document that destination PeerIds are not embedded in
   stored addresses.
4. Add configurable `listen_addresses` and `DialConfig` to
   `ReplicationConfig`.

### Phase 1 - Registry Projection

5. Replace the controller's remote installation set with a route map.
6. Pass the complete route snapshot into `RunningReplication::start`.
7. Replace `Allow` and `Revoke` with `UpsertPeer` and `RemovePeer`, preserving
   first-remote start and final-remote drain ordering.
8. Rely on installation-scoped key derivation for normal enrollment and
   quarantine ambiguous PeerIds encountered in replicated raw state.

### Phase 2 - Swarm Routing

9. Add the worker-local multi-source address book.
10. Dial every routable enrolled peer immediately with known-peer `DialOpts`.
11. Gate mDNS, Identify, connection establishment, and inbound Gossipsub
    messages by current registry membership.
12. Handle address replacement, key replacement, and row deletion without
    leaving stale dial routes.
13. Add capped exponential reconnect scheduling driven by Swarm connection
    events and address changes.
14. Add real multi-process tests for offline-peer retry, bidirectional event
    exchange, replicated table creation, and durable reopen verification.
15. Run `cargo check --workspace --all-targets` and update the root and
    `zendb-workspace` READMEs when implementation lands.

---

## 10. Completion Criteria

- `DeviceRecord` stores zero or more bincode-capable `Multiaddr` values.
- Device addresses are transport routes and do not duplicate the destination
  PeerId.
- `ReplicationConfig` controls listen addresses and reconnect backoff.
- Starting replication receives a complete snapshot of enrolled remote routes.
- Adding a remote record with addresses causes an immediate authenticated dial.
- Updating addresses replaces future dial candidates without disconnecting a
  valid existing connection.
- Replacing a public key revokes the old PeerId before dialing the new PeerId.
- Removing a device cancels retries, blacklists the PeerId, and disconnects it.
- Empty-address devices remain enrolled and may connect inbound or through
  trusted discovery.
- mDNS and Identify addresses are accepted only for enrolled, unambiguous
  PeerIds and are never persisted automatically.
- Unknown peers are not admitted to the workspace Gossipsub mesh.
- Gossipsub admission still verifies source, author, and role independently of
  the routing layer.
- Failed dials retry with capped backoff and do not fail workspace open.
- Duplicate public keys cannot silently map multiple installations to one
  Swarm peer.
- `JoinHints` remain pre-trust initial-sync data rather than active membership.
- `cargo check --workspace --all-targets` succeeds.

---

## 11. Deferred Work And Non-Goals

This iteration does not implement:

- initial joiner synchronization or a bootstrap authentication protocol;
- device-authored or leased presence records;
- automatic persistence of observed, mDNS, Identify, or listen addresses;
- NAT address discovery, AutoNAT, UPnP, hole punching, relay, or rendezvous;
- address health scores or long-term address success statistics;
- connection limits or a global dial concurrency scheduler;
- a public connection-status API;
- delivery acknowledgements or anti-entropy.

The likely future dynamic-address design is a separate `_device_presence`
system table. Its rows can be self-authored, short-lived, and merged with the
Admin-owned stable addresses from `_devices`. It must not turn transient network
observation into workspace membership.

---

## 12. Dependency Direction

```text
zendb-types              (replication IDs, peer values, envelope, persisted Multiaddr)
  ^
zendb-storage            (durable Table and Change)
  ^
zendb-workspace
  devices                (DeviceRecord and device policy)
  replication controller (registry-to-route projection)
  replication worker     (Multiaddr address book, dial/retry, Swarm)
```

The standalone `multiaddr::Multiaddr` value is a `zendb-types` concern because
it is part of the persisted record schema. Transport construction, discovery,
dialing, and Swarm policy remain private to `zendb-workspace`.
