//! Workspace-owned replication: configuration, lifecycle, and runtime.
//!
//! Module layout:
//! - `wire`       -- `/zenin/1` protocol messages and framing
//! - `transport`  -- TCP + QUIC + Noise + Yamux swarm construction
//! - `protocol`   -- ConnectionHandler and NetworkBehaviour (libp2p glue)
//! - `batcher`    -- serialized per-table event accumulation
//! - `sync`       -- anti-entropy: receipts, ranges, recent cache, fetch
//! - `engine`     -- replication logic: admission, routing, commit, broadcast
//! - `controller` -- thread lifecycle and event loop

mod batcher;
mod controller;
mod engine;
mod protocol;
mod sync;
mod transport;
pub(crate) mod wire;

pub use controller::{DiscoveredPeer, PeerDiscoveryListener};
pub(crate) use controller::{ReplicationController, ReplicationNotification};
