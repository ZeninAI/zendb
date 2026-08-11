//! Workspace identity, construction, and synchronous public API.
//!
//! Creation is a two-phase commit: storage is assembled and synchronized first,
//! then `_identity` is written as the durable marker that the workspace exists.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use libp2p_identity::Keypair;
use tokio::sync::mpsc::unbounded_channel;
use zendb_types::{
    Event, EventId, EventTime, Installation, InstallationId, InstallationOp, InstallationState,
    Multiaddr, PathOp, PeerIdentity, Permissions, PublicKey, TypeOp, WorkspaceId, global_clock,
    utils::{deserialize_from, serialize_to_vec},
};

use crate::{
    Error, Result,
    config::{ReplicationConfig, WorkspaceConfig},
    core::WorkspaceCore,
    installations::{Installations, Membership},
    replication::ReplicationController,
    states::States,
    system::{
        IDENTITY_FILE, INSTALLATIONS_TABLE_NAME, LOCK_FILE, TABLE_CATALOG_NAME,
        WORKSPACE_KEY_DOMAIN,
    },
    tables::{TableStore, Tables},
};

#[derive(Debug, Clone, Encode, Decode)]
struct WorkspaceState {
    workspace_id: WorkspaceId,
    installation_id: InstallationId,
    clock: EventTime,
}

#[derive(Clone, Copy)]
enum AssemblyMode {
    Create,
    Open,
}

pub struct Workspace {
    root: PathBuf,
    workspace_id: WorkspaceId,
    core: Arc<WorkspaceCore>,
    installations: Installations,
    tables: Tables,
    replication: Option<ReplicationController>,
    // Declared last so the lock outlives every storage-owning field.
    _lock: File,
}

impl Workspace {
    pub fn create(
        path: impl AsRef<Path>,
        peer_identity: Arc<dyn PeerIdentity>,
        config: WorkspaceConfig,
    ) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let lock = acquire_lock(&root)?;
        let identity_path = root.join(IDENTITY_FILE);
        if identity_path.exists() {
            return Err(Error::WorkspaceExists);
        }

        let state = WorkspaceState {
            workspace_id: config.workspace_id.unwrap_or_else(WorkspaceId::generate),
            installation_id: InstallationId::generate(),
            clock: EventTime::ZERO,
        };
        let keypair = derive_workspace_keypair(
            peer_identity.as_ref(),
            state.workspace_id,
            state.installation_id,
        )?;
        let display_name = peer_identity.display_name().to_owned();
        let addresses = peer_identity.addresses();
        let workspace = Self::assemble(
            root.clone(),
            state,
            keypair,
            lock,
            AssemblyMode::Create,
            config.replication,
            display_name,
            addresses,
        )?;

        // System storage must be durable before the identity becomes visible;
        // otherwise a later open could accept state for incomplete storage.
        workspace.persist(zendb_storage::Barrier::Sync)?;
        Ok(workspace)
    }

    pub fn open(
        path: impl AsRef<Path>,
        peer_identity: Arc<dyn PeerIdentity>,
        config: WorkspaceConfig,
    ) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let lock = acquire_lock(&root)?;
        let bytes = fs::read(root.join(IDENTITY_FILE))?;
        let state: WorkspaceState = deserialize_from(&bytes)?;
        if let Some(configured) = config.workspace_id
            && configured != state.workspace_id
        {
            return Err(Error::WorkspaceIdMismatch {
                stored: state.workspace_id,
                configured,
            });
        }
        let keypair = derive_workspace_keypair(
            peer_identity.as_ref(),
            state.workspace_id,
            state.installation_id,
        )?;
        let display_name = peer_identity.display_name().to_owned();
        let addresses = peer_identity.addresses();
        Self::assemble(
            root,
            state,
            keypair,
            lock,
            AssemblyMode::Open,
            config.replication,
            display_name,
            addresses,
        )
    }

    fn assemble(
        root: PathBuf,
        mut state: WorkspaceState,
        keypair: Keypair,
        lock: File,
        mode: AssemblyMode,
        replication_config: ReplicationConfig,
        display_name: String,
        addresses: Vec<Multiaddr>,
    ) -> Result<Self> {
        let states = match &mode {
            AssemblyMode::Create => States::create(&root)?,
            AssemblyMode::Open => States::open(&root)?,
        };
        let table_store = match &mode {
            AssemblyMode::Create => TableStore::create(&root, state.installation_id)?,
            AssemblyMode::Open => TableStore::open(&root)?,
        };
        let catalog_table = table_store.get(TABLE_CATALOG_NAME)?;
        let installations_table = table_store.get(INSTALLATIONS_TABLE_NAME)?;
        let public_key = PublicKey::from_libp2p(keypair.public());
        let membership = match mode {
            AssemblyMode::Create => {
                // The installations table owns its sequence stream, so the
                // initial local installation event starts at sequence 1 there.
                let mut installation = Installation::default();
                installation.display_name = display_name.clone();
                installation.public_key = public_key;
                installation.addresses = addresses.clone();
                installation.state = InstallationState::Active(Permissions::FULL);
                let time = global_clock().mint();
                let installation_event = Event {
                    id: EventId {
                        author: state.installation_id,
                        sequence: 0,
                    },
                    primary_key: state.installation_id.into(),
                    operations: vec![PathOp {
                        path: Vec::new(),
                        time,
                        op: TypeOp::Installation(InstallationOp::Set {
                            incoming: installation.clone(),
                        }),
                    }],
                };
                installations_table.insert_event(installation_event)?;
                state.clock = time;
                Membership::create(state.installation_id, installation)
            }
            AssemblyMode::Open => {
                // Opening validates membership and restores the last durable
                // hybrid-clock checkpoint from the workspace state.
                let membership = {
                    let installations = installations_table.read();
                    Membership::open(&installations, state.installation_id, &public_key)?
                };
                global_clock().observe(state.clock);
                membership
            }
        };
        let (replication_tx, replication_rx) = if replication_config.enabled {
            let (sender, receiver) = unbounded_channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let core = Arc::new(WorkspaceCore {
            table_store,
            states,
            membership,
            replication_notifications: replication_tx,
        });
        if matches!(mode, AssemblyMode::Open) {
            let local_installation_id = state.installation_id;
            let mut installation = core
                .membership
                .get(&local_installation_id)
                .ok_or(Error::LocalInstallationNotEnrolled(local_installation_id))?;
            if installation.display_name != display_name || installation.addresses != addresses {
                installation.display_name = display_name;
                installation.addresses = addresses;
                core.commit_change(
                    &installations_table,
                    local_installation_id.into(),
                    vec![PathOp {
                        path: Vec::new(),
                        time: global_clock().mint(),
                        op: TypeOp::Installation(InstallationOp::Set {
                            incoming: installation.clone(),
                        }),
                    }],
                )?;
            }
        }
        let replication = if replication_config.enabled {
            Some(ReplicationController::start(
                &core,
                state.workspace_id,
                keypair,
                replication_config,
                replication_rx.expect("enabled replication has a notification receiver"),
            )?)
        } else {
            None
        };
        let installations = Installations::new(core.clone(), installations_table);
        let tables = Tables::new(core.clone(), catalog_table);

        Ok(Self {
            root,
            workspace_id: state.workspace_id,
            core,
            installations,
            tables,
            replication,
            _lock: lock,
        })
    }

    pub fn persist(&self, barrier: zendb_storage::Barrier) -> Result<()> {
        self.core.table_store.persist(barrier)?;
        self.core.states.persist(barrier)?;
        let state = WorkspaceState {
            workspace_id: self.workspace_id,
            installation_id: self.core.membership.local_installation_id(),
            clock: zendb_types::global_clock().snapshot(),
        };
        let bytes = serialize_to_vec(&state)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.root.join(IDENTITY_FILE))?;
        file.write_all(&bytes)?;
        match barrier {
            zendb_storage::Barrier::Flush => file.flush()?,
            zendb_storage::Barrier::Sync => file.sync_all()?,
        }
        Ok(())
    }

    pub const fn workspace_id(&self) -> WorkspaceId {
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
        &self.core.states
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        // Stop producers before flushing their storage; the lock remains held
        // until every storage-owning field has been dropped.
        if let Some(replication) = &mut self.replication {
            replication.shutdown();
        }
        let _ = self.persist(zendb_storage::Barrier::Flush);
    }
}

fn acquire_lock(root: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(LOCK_FILE))?;
    file.try_lock().map_err(|_| Error::WorkspaceAlreadyOpen)?;
    Ok(file)
}

/// Derive the workspace transport keypair for an installation.
pub fn derive_workspace_keypair(
    peer_identity: &dyn PeerIdentity,
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
    let seed = peer_identity
        .keypair()
        .derive_secret(&domain)
        .ok_or(Error::KeyDerivationUnsupported)?;
    Keypair::ed25519_from_bytes(seed).map_err(|error| Error::KeyDerivationFailed(error.to_string()))
}
