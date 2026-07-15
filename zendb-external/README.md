# zendb-external

Optional outbound client adapters for hosted services.

This crate is not required by the embedded database. It contains no server
handlers, server storage, scheduler, credential authority, or relay
implementation. Its traits describe calls an application may make from a
local database to an external service:

- `HostedRendezvousClient` obtains reachability tickets and endpoints;
- `HostedDiscoveryClient` publishes presence and retrieves untrusted peers;

An application may also use its own hosted OAuth or account service before it
creates a database ticket or directly admits a device. That service does not
issue a ZenDB workspace credential and is not an authorization authority inside
the Workspace.

All returned reachability data remains subject to normal local handshake and
durable replication validation.
