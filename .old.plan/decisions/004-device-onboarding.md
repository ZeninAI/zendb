# 004: Device Onboarding

Status: Implemented

## Candidate

A joining installation first persists a generated DeviceId and initial signing
key. Discovery or encrypted connectivity is not admission. The end state must
be a live `_devices[candidate]` row with an empty role set.

## Ticket Flow

A Manager creates `_enrollment_tickets[ticket_id]` containing a public verifier
and expiry. The QR code/link presentation carries the private ticket credential,
Workspace fingerprint, ticket ID, and optional reachability hints.

The candidate binds its DeviceId, public key, requested name, capabilities,
ticket, and Workspace into proof signed by both candidate and ticket keys. Any
admitted peer may validate and relay the resulting exceptional admission.

Ticket evidence can only create the bound absent Device row with no roles. It
cannot update a device, grant roles, write data, or create tickets. Strict
single-use is not guaranteed across disconnected replicas.

## Direct Flow

A Manager that already knows the candidate DeviceId and public key creates the
Device row directly. The candidate later contacts any peer, pins the expected
peer public key, and proves possession of its admitted key. The admitting
Manager need not stay online.

## Shared Bootstrap

Both flows establish an encrypted session, validate admission, transfer a
manifest and independently hashed snapshot chunks, install catalog/system/data
state, and continue normal anti-entropy. Hosted OAuth may influence an
application decision to admit but is outside the database protocol.
