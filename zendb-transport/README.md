# zendb-transport

Concrete client-side identity, authenticated transport, enrollment, and
presence mechanics for ZenDB.

This crate deliberately does not expose a graph of one-implementation bearer,
session, discovery, or path-selection traits. The current product has concrete
mechanisms and leaves application-specific Bluetooth, WebRTC, or relay adapters
outside this crate until a second implementation requires a stable interface.

## DeviceProfile

`DeviceProfile` durably owns one random stable DeviceId, current/staged Ed25519
private keys, the local HLC, shared-origin sequence, and presence sequence. It
uses alternating checksummed files so a partial profile update does not destroy
the previous valid slot.

The profile allocates a shared sequence before journal append and commits it
only after append succeeds. Opening a Workspace reconciles the counter from the
durable journal. Key material is zeroized in memory on drop where supported.

## SecureTcpSession

`SecureTcpSession::connect()` and `accept()` perform a signed ephemeral X25519
handshake bound to WorkspaceId, DeviceId, declared session purpose, and both
ephemeral keys. The derived directional keys protect length-delimited frames
with ChaCha20-Poly1305 and monotonic nonces.

The caller supplies the authorization closure because transport can prove a
key but cannot decide whether that key is admitted in current Workspace state.
Replication sessions require an admitted key. Bootstrap sessions first prove
candidate-key possession and defer the narrow admission decision to the
following bootstrap request.

## Enrollment And Presence

`EnrollmentPresentation` is the QR/link bearer object. Its private ticket
credential is local presentation material and never enters replicated state.
Helpers build and verify candidate-bound ticket proofs and direct requests.

`PresenceTracker` verifies no signatures itself; the Workspace first verifies
the signed `PresenceHeartbeat` or `DepartureNotice` against current membership,
then feeds it to the tracker. It derives `Direct`, `Indirect`, `Suspect`,
`Unreachable`, `Departed`, or `Unknown` from the sender-advertised idle period,
bounded arrival samples, and local grace policy. It also exposes a continuous
suspicion score and never treats relayed evidence as direct contact.

`DiscoveredPeer`, `NetworkEndpoint`, and rendezvous records are untrusted
reachability values. They never grant membership. Hosted outbound clients live
in `zendb-external`; server handlers are outside this repository.
