//! Workspace identity, failure-aware construction, and synchronous public API.
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
use zendb_storage::WriteBackend;
use zendb_types::{
    Blob, Envelope, Event, EventId, EventStamp, EventTime, Installation, InstallationId, Op,
    Path as CrdtPath, PeerIdentity, PublicKey, Role, Value, WorkspaceId,
    utils::time::physical_ms,
    utils::{deserialize_from, serialize_to_vec},
};

use crate::{
    AdmitError, Error, Result,
    causal::{CausalState, CausalTracker},
    core::WorkspaceCore,
    installations::{Installations, Membership},
    replication::ReplicationConfig,
    states::States,
    system::{
        CAUSAL_STATE_NAME, IDENTITY_FILE, IDENTITY_TEMP_FILE, INSTALLATIONS_TABLE_NAME, LOCK_FILE,
        STATES_DIR, TABLE_CATALOG_NAME, TABLES_DIR, WORKSPACE_KEY_DOMAIN,
    },
    tables::{TableStore, Tables},
};

#[derive(Debug, Clone, Encode, Decode)]
struct WorkspaceBinding {
    workspace_id: WorkspaceId,
    installation_id: InstallationId,
}

struct LocalInstallationIdentity {
    workspace_id: WorkspaceId,
    installation_id: InstallationId,
    keypair: Keypair,
}

impl LocalInstallationIdentity {
    fn derive(
        peer_identity: &dyn PeerIdentity,
        workspace_id: WorkspaceId,
        installation_id: InstallationId,
    ) -> Result<Self> {
        Ok(Self {
            workspace_id,
            installation_id,
            keypair: derive_workspace_keypair(peer_identity, workspace_id, installation_id)?,
        })
    }
}

/// Workspace-level runtime configuration.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceConfig {
    /// Workspace ID to create, or an optional assertion when opening.
    pub workspace_id: Option<WorkspaceId>,
    /// Replication runtime settings for this workspace.
    pub replication: ReplicationConfig,
}

enum AssemblyMode {
    Create { display_name: String },
    Open,
}

struct WorkspaceAssembly {
    root: PathBuf,
    local_identity: LocalInstallationIdentity,
    lock: File,
    mode: AssemblyMode,
    replication_config: ReplicationConfig,
}

pub struct Workspace {
    root: PathBuf,
    workspace_id: WorkspaceId,
    core: Arc<WorkspaceCore>,
    installations: Installations,
    tables: Tables,
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

        // The identity file is the create commit marker. Without it, any existing
        // storage is partial output from an interrupted create and can be removed.
        cleanup_failed_create(&root);
        let binding = WorkspaceBinding {
            workspace_id: config.workspace_id.unwrap_or_else(WorkspaceId::generate),
            installation_id: InstallationId::generate(),
        };
        let binding_bytes = serialize_to_vec(&binding)?;
        let local_identity = LocalInstallationIdentity::derive(
            peer_identity.as_ref(),
            binding.workspace_id,
            binding.installation_id,
        )?;
        let assembled = Self::assemble(WorkspaceAssembly {
            root: root.clone(),
            local_identity,
            lock,
            mode: AssemblyMode::Create {
                display_name: peer_identity.display_name().to_owned(),
            },
            replication_config: config.replication,
        });
        let workspace = match assembled {
            Ok(workspace) => workspace,
            Err(error) => {
                cleanup_failed_create(&root);
                return Err(error);
            }
        };

        // System storage must be durable before the identity becomes visible;
        // otherwise a later open could accept a binding for incomplete storage.
        if let Err(error) = workspace.sync() {
            drop(workspace);
            cleanup_failed_create(&root);
            return Err(error);
        }

        let identity_temp_path = root.join(IDENTITY_TEMP_FILE);
        let commit_binding = || -> std::io::Result<()> {
            // Sync the temporary binding before the rename so the final identity
            // file is never the only durable evidence of a completed create.
            let mut file = File::create(&identity_temp_path)?;
            file.write_all(&binding_bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&identity_temp_path, identity_path)
        };
        if let Err(error) = commit_binding() {
            drop(workspace);
            cleanup_failed_create(&root);
            return Err(error.into());
        }
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
        let binding: WorkspaceBinding = deserialize_from(&bytes)?;
        if let Some(configured) = config.workspace_id
            && configured != binding.workspace_id
        {
            return Err(Error::WorkspaceIdMismatch {
                stored: binding.workspace_id,
                configured,
            });
        }
        let local_identity = LocalInstallationIdentity::derive(
            peer_identity.as_ref(),
            binding.workspace_id,
            binding.installation_id,
        )?;
        Self::assemble(WorkspaceAssembly {
            root,
            local_identity,
            lock,
            mode: AssemblyMode::Open,
            replication_config: config.replication,
        })
    }

    fn assemble(assembly: WorkspaceAssembly) -> Result<Self> {
        let WorkspaceAssembly {
            root,
            local_identity,
            lock,
            mode,
            replication_config,
        } = assembly;
        let states = match &mode {
            AssemblyMode::Create { .. } => States::create(&root)?,
            AssemblyMode::Open => States::open(&root)?,
        };
        let causal_state = states.get::<InstallationId, CausalState>(CAUSAL_STATE_NAME)?;
        let table_store = match &mode {
            AssemblyMode::Create { .. } => {
                TableStore::create(&root, local_identity.installation_id)?
            }
            AssemblyMode::Open => TableStore::open(&root)?,
        };
        let catalog_table = table_store.get(TABLE_CATALOG_NAME)?;
        let installations_table = table_store.get(INSTALLATIONS_TABLE_NAME)?;
        let public_key = PublicKey::from_libp2p(local_identity.keypair.public());
        let (membership, causal) = match mode {
            AssemblyMode::Create { display_name } => {
                // TableStore::create already authored the two system catalog
                // events at sequences 1 and 2. The initial Admin installation is
                // therefore sequence 3, and its causal row must match that stamp
                // before the tracker starts allocating new events.
                let installation = Installation {
                    display_name,
                    role: Some(Role::Admin),
                    public_key,
                    addresses: Vec::new(),
                };
                let stamp = EventStamp {
                    id: EventId {
                        author: local_identity.installation_id,
                        sequence: 3, // Two events already exist from system-table creation.
                    },
                    time: EventTime {
                        physical_ms: physical_ms().ok_or(Error::ClockExhausted)?,
                        logical: 0,
                    },
                };
                installations_table.insert_event(Event {
                    primary_key: local_identity.installation_id.into(),
                    path: CrdtPath::new(),
                    op: Op::Upsert {
                        value: Value::Blob(Blob::encode(&installation)?),
                    },
                    stamp,
                })?;
                let initial_causal_state = CausalState::from_initial_stamp(stamp);
                causal_state
                    .write_internal()
                    .put(local_identity.installation_id, initial_causal_state.clone())?;
                (
                    Membership::create(local_identity.installation_id, installation),
                    CausalTracker::create(
                        causal_state,
                        local_identity.installation_id,
                        initial_causal_state,
                    ),
                )
            }
            AssemblyMode::Open => {
                // Opening validates the persisted membership and causal state;
                // neither is silently reconstructed when local data is missing.
                let membership = {
                    let installations = installations_table.read();
                    Membership::open(&installations, local_identity.installation_id, &public_key)?
                };
                (
                    membership,
                    CausalTracker::open(causal_state, local_identity.installation_id)?,
                )
            }
        };
        let workspace_id = local_identity.workspace_id;
        let core = WorkspaceCore::new(
            workspace_id,
            local_identity.keypair,
            table_store,
            states,
            membership,
            causal,
            replication_config,
        );
        // Membership is complete before replication derives its peer set. The
        // controller only weakly references the core, so its worker cannot keep
        // the workspace alive after the public owner is dropped.
        core.replication.start_if_needed()?;
        let installations = Installations::new(core.clone(), installations_table);
        let tables = Tables::new(core.clone(), catalog_table);

        Ok(Self {
            root,
            workspace_id,
            core,
            installations,
            tables,
            _lock: lock,
        })
    }

    pub fn flush(&self) -> Result<()> {
        self.core.flush()
    }

    pub fn sync(&self) -> Result<()> {
        self.core.sync()
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

    pub fn admit_event(
        &self,
        envelope: Envelope,
        gossipsub_source: libp2p_identity::PeerId,
    ) -> std::result::Result<(), AdmitError> {
        crate::admission::admit_event(&self.core, envelope, gossipsub_source)
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        // Stop producers before flushing their storage; the lock remains held
        // until every storage-owning field has been dropped.
        self.core.replication.shutdown();
        let _ = self.flush();
    }
}

fn cleanup_failed_create(root: &Path) {
    // Keep the root and lock file so another create can retry, but remove all
    // artifacts that could make a later open look partially valid.
    let _ = fs::remove_file(root.join(IDENTITY_TEMP_FILE));
    let _ = fs::remove_dir_all(root.join(TABLES_DIR));
    let _ = fs::remove_dir_all(root.join(STATES_DIR));
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
