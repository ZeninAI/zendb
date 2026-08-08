//! Application identity input used to derive workspace-scoped peer keys.

use libp2p_identity::Keypair;

use crate::Multiaddr;

pub trait PeerIdentity: Send + Sync {
    fn keypair(&self) -> &Keypair;

    fn display_name(&self) -> &str;

    /// Public route hints that should be stored in the local Installation.
    fn addresses(&self) -> Vec<Multiaddr> {
        Vec::new()
    }
}
