# zendb-transport

Client-side connectivity contracts for ZeninDB.

The layers are deliberately separate:

1. `DiscoveryProvider` and `RendezvousProvider` produce untrusted reachability
   candidates and tickets.
2. `BearerAdapter` creates a pre-authentication `RawTransport`.
3. The handshake verifies the device key and workspace credential.
4. `TransportSession` exposes an authenticated logical session.
5. `PathSelector` may select a better bearer without changing logical identity.

This crate does not evaluate workspace policy, implement CRDT replication, or
define server-side relay handlers. Hosted rendezvous/presence clients belong in
`zendb-external`.
