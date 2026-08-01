//! Application identity input used to derive workspace-scoped peer keys.

use libp2p_identity::Keypair;

pub trait PeerIdentity: Send + Sync {
    fn keypair(&self) -> &Keypair;

    fn display_name(&self) -> &str;
}
