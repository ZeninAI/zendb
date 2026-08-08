//! Configurable swarm construction with TCP, QUIC, Noise, and Yamux.
//!
//! The transport stack can provide two connection paths:
//! - TCP + Noise + Yamux (reliable, works everywhere)
//! - QUIC (lower latency, built-in encryption and multiplexing)

use libp2p::swarm::NetworkBehaviour;
use libp2p::{Swarm, SwarmBuilder, noise, tcp, yamux};
use libp2p_identity::Keypair;

use crate::TransportConfig;

/// Build a fully configured libp2p Swarm over TCP+Noise+Yamux and QUIC.
pub(super) fn build_swarm<B>(
    keypair: Keypair,
    behaviour: B,
    config: &TransportConfig,
) -> Result<Swarm<B>, String>
where
    B: NetworkBehaviour,
{
    if !config.enable_tcp && !config.enable_quic {
        return Err("at least one replication transport must be enabled".to_owned());
    }

    let builder = SwarmBuilder::with_existing_identity(keypair).with_tokio();
    if config.enable_tcp && config.enable_quic {
        builder
            .with_tcp(
                tcp::Config::default().nodelay(true),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(|e| e.to_string())?
            .with_quic()
            .with_dns()
            .map_err(|e| e.to_string())?
            .with_behaviour(|_| behaviour)
            .map_err(|e| e.to_string())?
            .build()
            .pipe(Ok)
    } else if config.enable_tcp {
        builder
            .with_tcp(
                tcp::Config::default().nodelay(true),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(|e| e.to_string())?
            .with_behaviour(|_| behaviour)
            .map_err(|e| e.to_string())?
            .build()
            .pipe(Ok)
    } else {
        builder
            .with_quic()
            .with_dns()
            .map_err(|e| e.to_string())?
            .with_behaviour(|_| behaviour)
            .map_err(|e| e.to_string())?
            .build()
            .pipe(Ok)
    }
}

/// Extension trait for inline pipe (avoids a let binding).
trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}

impl<T> Pipe for T {}
