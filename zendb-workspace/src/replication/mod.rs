//! Workspace-owned replication: configuration, lifecycle, and runtime.
//!
//! Module layout:
//! - `config`     -- tuning knobs (intervals, sizes, linger, mesh parameters)
//! - `wire`       -- `/zenin/1` protocol messages and framing
//! - `transport`  -- TCP + QUIC + Noise + Yamux swarm construction
//! - `protocol`   -- ConnectionHandler and NetworkBehaviour (libp2p glue)
//! - `mesh`       -- event-driven peer selection and forwarding topology
//! - `batcher`    -- linger-based per-table event accumulation
//! - `sync`       -- anti-entropy: receipts, ranges, recent cache, fetch
//! - `engine`     -- replication logic: admission, routing, commit, broadcast
//! - `controller` -- thread lifecycle and event loop

mod batcher;
mod config;
mod controller;
mod engine;
mod mesh;
mod protocol;
mod sync;
mod transport;
pub(crate) mod wire;

pub use config::{MeshConfig, ReplicationConfig, ZeninConfig};
pub(crate) use controller::{ReplicationController, ReplicationNotification};
