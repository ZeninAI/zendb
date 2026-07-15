# 004: Device Onboarding

Status: Accepted

## Context

Membership is represented by a live `devices.<device_id>` Cell. A joining
installation is not a Workspace device merely because it discovered a peer,
has network access, or knows a Workspace fingerprint. It must first obtain a
live Device record containing its initial public key.

ZenDB needs two, and only two, admission protocols:

1. a bearer presentation, normally carried in a QR code or link; and
2. direct pre-admission by a Manager that already knows the joining device.

Discovery, rendezvous, transport, and snapshot transfer are supporting
mechanisms. They find a reachable peer and move bytes; they never grant
membership on their own.

## Shared Joining Material

Before either protocol, the joining installation creates and persists its
random `DeviceId` and its initial signing key pair. It sends this transport-only
request to a reachable Workspace peer:

```text
BootstrapRequest
  workspace fingerprint
  candidate_device_id
  candidate_public_key
  requested_name
  capabilities
  proof of candidate private-key possession
  optional ticket admission proof
```

The candidate-private-key proof binds the request to the exact DeviceId and
public key. It prevents a relay, QR scanner, or discovery service from
substituting its own key. `BootstrapRequest` is not a replicated table row and
does not create a `Pending` device state.

After admission, the peer transfers a bootstrap snapshot and journal tail. The
new Device record has an empty role Set unless a Manager separately grants
roles, so every newly admitted device starts as an implicit Reader. Operator
eligibility is a local reconciliation decision, defined separately in ADR 008;
bootstrap does not persist a global "ready for work" flag.

## Protocol One: Ticket Admission

A Manager creates a replicated ticket under the control record:

```text
WorkspaceControl: Record
  enrollment_tickets: Record
    <ticket_id>: Cell<Record(EnrollmentTicket)>

EnrollmentTicket: Record
  verifier_public_key
  expires_hlc
```

The `ticket_id` names the Cell only; it is not a device-key identifier. The
Manager creates a short-lived ticket signing key pair. The public verifier and
expiry are replicated. The QR code or link carries the Workspace fingerprint,
ticket ID, rendezvous hints, and the private ticket credential.

The private ticket credential is the presentation secret. It is not stored in
the Workspace and is never placed in a replicated event. The candidate signs
an admission proof with it over the exact candidate DeviceId, candidate public
key, requested name, capabilities, ticket ID, and Workspace fingerprint. It
also proves possession of its own device private key.

Any live device may act as a bootstrap peer for this narrow operation. It
verifies both proofs and relays a ticket-admission control event that contains
the verifiable ticket signature, but not the secret. Every receiving replica
can independently verify that the ticket was live, unexpired, and authorized
the exact newly created Device record. This is deliberately a limited exception
to ordinary role checks: a valid ticket can create only its bound, previously
absent Device Cell with an empty role Set. It
cannot change an existing device, grant a role, create a ticket, or write
ordinary data.

The ticket signature travels beside the signed admission event as admission
evidence in the synchronization envelope. It is not embedded in the CRDT
Device value. This keeps the materialized schema small while preserving enough
evidence for every later receiver to validate the exceptional write.

Because the ticket verifier is public, a reader peer can validate and relay a
ticket admission without secretly becoming a Manager. This is essential for an
offline-tolerant Workspace: the Manager that created the QR code may be gone
when another live peer performs the bootstrap.

A bearer ticket is an admission capability, not a login session or an
encryption key. A ticket may be deleted by a Manager or expire. Strict
single-use semantics cannot be guaranteed among disconnected replicas: two
holders can present the same ticket to different peers before they synchronize.
Use direct Manager admission when exactly one pre-known device must be allowed.

## Protocol Two: Direct Manager Admission

A Manager may already know the candidate DeviceId and initial public key, for
example through a local pairing exchange, an application account flow, or an
out-of-band device inventory. No enrollment ticket is needed.

The protocol is:

1. The candidate sends its DeviceId, public key, requested name, capabilities,
   and proof of private-key possession to the Manager or another trusted
   pairing channel.
2. The Manager creates `devices.<candidate_device_id>` with the supplied
   initial key, requested metadata, and an empty roles Set. The Manager may do
   this before the candidate is online.
3. The candidate contacts any reachable Workspace peer, proves possession of
   the key now present in its Device record, and receives the bootstrap snapshot
   and journal tail.

The peer performing step 3 need not be the admitting Manager. It only needs
the replicated Device record and a valid authenticated session with the
candidate. The Manager's authorization is consumed by the Device-cell creation,
not retained as a permanent bootstrap dependency.

## Workspace Creation

The first local device initializes the Workspace control record with its own
Device record:

```text
devices[first_device_id] = Device {
  name,
  key_ring: {
    primary_key: initial_public_key,
    secondary_key: none,
    primary_from_seq: 1,
    phase: Stable,
  },
  roles: { Contributor, Dispatcher, Manager },
  capabilities,
}
```

There is no permanent creator authority. The first device remains privileged
only while its replicated role Set contains the corresponding role values.

## Bootstrap and Transport Boundary

Both protocols use the same post-admission bootstrap path:

1. find a peer through LAN discovery, a QR rendezvous hint, Bluetooth, relay,
   or application-provided connectivity;
2. establish a mutually authenticated encrypted session using the candidate
   and peer device keys;
3. verify the candidate is now a live Device Cell;
4. transfer and validate an independently hashed, chunked snapshot plus the
   required journal tail; and
5. begin ordinary anti-entropy synchronization.

Rendezvous hints and relays are untrusted byte transport. OAuth or other
application authentication may help decide whether to issue a ticket or make a
direct Manager admission, but it is not part of ZenDB's database protocol.

## Consequences

The control value contains `enrollment_tickets` only because ticket admission
requires a replicated verifier available to every peer. Direct Manager
admission needs no separate table.

`zendb-types` should model the portable ticket record and ticket-admission
proof. Transport-specific QR encoding, links, LAN discovery, relay requests,
and application OAuth integration remain client-side adapters rather than
Workspace state.
