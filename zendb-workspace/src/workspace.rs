//! Workspace identity, lock lifecycle, and synchronous orchestration API.

use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use libp2p_identity::Keypair;
use zendb_types::{
    Envelope, InstallationId, PeerIdentity, PublicKey, WorkspaceId,
    utils::{deserialize_from, serialize_to_vec},
};

use crate::{
    AdmitError, Error, Result,
    consts::{IDENTITY_FILE, LOCK_FILE, PEERS_STATE_NAME},
    installations::{Installations, PeerState},
    replication::{ReplicationConfig, ReplicationController},
    states::States,
    tables::Tables,
};

const WORKSPACE_KEY_DOMAIN: &[u8] = b"zendb/workspace-transport-key/v1\0";

#[derive(Debug, Clone, Encode, Decode)]
struct LocalIdentity {
    workspace_id: WorkspaceId,
    installation_id: InstallationId,
}

struct WorkspaceIdentity {
    installation_id: InstallationId,
    keypair: Keypair,
}

impl WorkspaceIdentity {
    fn derive(
        identity: &dyn PeerIdentity,
        workspace_id: WorkspaceId,
        installation_id: InstallationId,
    ) -> Result<Self> {
        Ok(Self {
            installation_id,
            keypair: derive_workspace_keypair(identity, workspace_id, installation_id)?,
        })
    }
}

/// Derive the public transport key an Admin stores for an installation.
pub fn derive_workspace_public_key(
    identity: &dyn PeerIdentity,
    workspace_id: WorkspaceId,
    installation_id: InstallationId,
) -> Result<PublicKey> {
    derive_workspace_keypair(identity, workspace_id, installation_id)
        .map(|keypair| PublicKey::from_libp2p(keypair.public()))
}

/// Workspace-level runtime configuration.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceConfig {
    pub replication: ReplicationConfig,
}

/// Connection hints assigned by an Admin before joining.
#[derive(Debug, Clone)]
pub struct JoinHints {
    pub bootstrap_peers: Vec<String>,
    pub installation_id: InstallationId,
}

enum Mode {
    Create,
    Join,
    Open,
}

struct WorkspaceAssembly {
    root: PathBuf,
    workspace_id: WorkspaceId,
    workspace_identity: WorkspaceIdentity,
    display_name: String,
    lock: File,
    mode: Mode,
    replication_config: ReplicationConfig,
}

pub struct Workspace {
    root: PathBuf,
    workspace_id: WorkspaceId,
    replication: Arc<ReplicationController>,
    tables: Arc<Tables>,
    states: Arc<States>,
    installations: Arc<Installations>,
    // Declared last so the lock outlives every storage-owning field.
    _lock: File,
}

impl Workspace {
    pub fn create(
        path: impl AsRef<Path>,
        identity: Arc<dyn PeerIdentity>,
        config: WorkspaceConfig,
    ) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let lock = acquire_lock(&root)?;
        let identity_path = root.join(IDENTITY_FILE);
        if identity_path.exists() {
            return Err(Error::AlreadyExists("workspace identity".to_owned()));
        }
        let local_identity = LocalIdentity {
            workspace_id: WorkspaceId::generate(),
            installation_id: InstallationId::generate(),
        };
        let workspace_identity = WorkspaceIdentity::derive(
            identity.as_ref(),
            local_identity.workspace_id,
            local_identity.installation_id,
        )?;
        fs::write(identity_path, serialize_to_vec(&local_identity)?)?;
        Self::assemble(WorkspaceAssembly {
            root,
            workspace_id: local_identity.workspace_id,
            workspace_identity,
            display_name: identity.display_name().to_owned(),
            lock,
            mode: Mode::Create,
            replication_config: config.replication,
        })
    }

    pub fn open(
        path: impl AsRef<Path>,
        identity: Arc<dyn PeerIdentity>,
        config: WorkspaceConfig,
    ) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let lock = acquire_lock(&root)?;
        let bytes = fs::read(root.join(IDENTITY_FILE))?;
        let local_identity: LocalIdentity = deserialize_from(&bytes)?;
        let workspace_identity = WorkspaceIdentity::derive(
            identity.as_ref(),
            local_identity.workspace_id,
            local_identity.installation_id,
        )?;
        Self::assemble(WorkspaceAssembly {
            root,
            workspace_id: local_identity.workspace_id,
            workspace_identity,
            display_name: identity.display_name().to_owned(),
            lock,
            mode: Mode::Open,
            replication_config: config.replication,
        })
    }

    /// Stage an assigned installation for the future initial-sync protocol.
    ///
    /// Iteration 0006 does not transfer the existing installation registry, so the
    /// newly created local storage cannot authenticate or join the mesh yet.
    pub fn join(
        path: impl AsRef<Path>,
        workspace_id: WorkspaceId,
        identity: Arc<dyn PeerIdentity>,
        hints: JoinHints,
        config: WorkspaceConfig,
    ) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let lock = acquire_lock(&root)?;
        if root.join(IDENTITY_FILE).exists() {
            return Err(Error::AlreadyExists("workspace identity".to_owned()));
        }
        let local_identity = LocalIdentity {
            workspace_id,
            installation_id: hints.installation_id,
        };
        let workspace_identity =
            WorkspaceIdentity::derive(identity.as_ref(), workspace_id, hints.installation_id)?;
        fs::write(root.join(IDENTITY_FILE), serialize_to_vec(&local_identity)?)?;
        Self::assemble(WorkspaceAssembly {
            root,
            workspace_id,
            workspace_identity,
            display_name: identity.display_name().to_owned(),
            lock,
            mode: Mode::Join,
            replication_config: config.replication,
        })
    }

    fn assemble(assembly: WorkspaceAssembly) -> Result<Self> {
        let WorkspaceAssembly {
            root,
            workspace_id,
            workspace_identity,
            display_name,
            lock,
            mode,
            replication_config,
        } = assembly;
        let states = match mode {
            Mode::Create | Mode::Join => States::create(&root)?,
            Mode::Open => States::open(&root)?,
        };
        let peer_state = states.get::<InstallationId, PeerState>(PEERS_STATE_NAME)?;
        let public_key = PublicKey::from_libp2p(workspace_identity.keypair.public());
        let tables = match mode {
            Mode::Create => Tables::create(
                &root,
                peer_state,
                workspace_identity.installation_id,
                display_name,
                public_key,
            )?,
            Mode::Open => Tables::open(
                &root,
                peer_state,
                workspace_identity.installation_id,
                &public_key,
            )?,
            Mode::Join => Tables::join(&root, peer_state, workspace_identity.installation_id)?,
        };
        let installations = tables.installations.clone();
        let replication = ReplicationController::build(
            workspace_id,
            workspace_identity.installation_id,
            workspace_identity.keypair,
            tables.clone(),
            installations.clone(),
            replication_config,
        )?;

        Ok(Self {
            root,
            workspace_id,
            replication,
            tables,
            states,
            installations,
            _lock: lock,
        })
    }

    pub fn flush(&self) -> Result<()> {
        self.installations.flush()?;
        self.tables.flush()?;
        self.states.flush()
    }

    pub fn sync(&self) -> Result<()> {
        self.installations.sync()?;
        self.tables.sync()?;
        self.states.sync()
    }

    pub const fn id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn installations(&self) -> &Installations {
        &self.installations
    }

    pub fn tables(&self) -> &Tables {
        &self.tables
    }

    pub fn states(&self) -> &States {
        &self.states
    }

    pub fn admit_event(
        &self,
        envelope: Envelope,
        gossipsub_source: libp2p_identity::PeerId,
    ) -> std::result::Result<(), AdmitError> {
        crate::admission::admit_event(
            &self.tables,
            &self.installations,
            envelope,
            gossipsub_source,
        )
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        self.replication.shutdown();
        let _ = self.flush();
    }
}

fn acquire_lock(root: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(LOCK_FILE))?;
    file.try_lock()
        .map_err(|_| Error::AlreadyExists("workspace is already open".to_owned()))?;
    Ok(file)
}

fn derive_workspace_keypair(
    identity: &dyn PeerIdentity,
    workspace_id: WorkspaceId,
    installation_id: InstallationId,
) -> Result<Keypair> {
    let mut domain = Vec::with_capacity(
        WORKSPACE_KEY_DOMAIN.len()
            + workspace_id.as_bytes().len()
            + installation_id.as_bytes().len(),
    );
    domain.extend_from_slice(WORKSPACE_KEY_DOMAIN);
    domain.extend_from_slice(workspace_id.as_bytes());
    domain.extend_from_slice(installation_id.as_bytes());
    let seed = identity
        .keypair()
        .derive_secret(&domain)
        .ok_or(Error::KeyDerivationUnsupported)?;
    Keypair::ed25519_from_bytes(seed).map_err(|error| {
        Error::CorruptLocalState(format!("derived workspace key is invalid: {error}"))
    })
}
