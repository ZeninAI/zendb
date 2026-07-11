# zendb-external

Optional outbound client adapters for hosted services.

This crate is not required by the embedded database. It contains no server
handlers, server storage, scheduler, credential authority, or relay
implementation. Its traits describe calls an application may make from a
local database to an external service:

- `HostedCredentialClient` requests a workspace credential;
- `HostedRendezvousClient` obtains reachability tickets and endpoints;
- `HostedDiscoveryClient` publishes presence and retrieves untrusted peers;
- `HostedJobClient` transports job records and results.

All returned identity, reachability, job, and authorization data remains
subject to the normal local verification, policy, handshake, and durable
replication boundaries.
