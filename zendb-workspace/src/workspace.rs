//! Workspace identity, lock lifecycle, and synchronous orchestration API.

use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use zendb_types::{
    utils::{deserialize_from, serialize_to_vec},
    PeerId, PeerIdentity, WorkspaceId,
};

use crate::{
    consts::{IDENTITY_FILE, LOCK_FILE, PEERS_STATE_NAME},
    devices::{Devices, PeerState},
    states::States,
    tables::Tables,
    Error, Result,
};

/// Placeholder for future workspace-level configuration.
///
/// Carries no fields in this iteration. The local device display name is not
/// a workspace concern; fields are added when concrete workspace-level config
/// is needed.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceConfig {}

/// Connection hints for `Workspace::join`.
///
/// This is a placeholder type. Fields (multiaddrs, bootstrap peer list, dial
/// timeout) are added in the replication iteration. `Workspace::join` in this
/// iteration persists the caller-provided `WorkspaceId` and otherwise behaves
/// like `create` minus `WorkspaceId` generation; it does not yet contact any
/// network.
#[derive(Debug, Clone, Default)]
pub struct JoinHints {}

enum Mode {
    Create,
    Open,
}

pub struct Workspace {
    root: PathBuf,
    workspace_id: WorkspaceId,
    identity: Arc<dyn PeerIdentity>,
    tables: Arc<Tables>,
    states: Arc<States>,
    devices: Arc<Devices>,
    // Declared last so the lock outlives every storage-owning field.
    _lock: File,
}

impl Workspace {
    /// Create a new workspace at `path`.
    ///
    /// `identity` supplies the local device identity (`PeerId` + private key).
    /// The workspace stores the `WorkspaceId` it generates but never stores
    /// the private key.
    pub fn create(
        path: impl AsRef<Path>,
        identity: Arc<dyn PeerIdentity>,
        _config: WorkspaceConfig,
    ) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let lock = acquire_lock(&root)?;
        let identity_path = root.join(IDENTITY_FILE);
        if identity_path.exists() {
            return Err(Error::AlreadyExists("workspace identity".to_owned()));
        }
        let workspace_id = WorkspaceId::generate();
        fs::write(identity_path, serialize_to_vec(&workspace_id)?)?;
        Self::assemble(root, workspace_id, lock, identity, Mode::Create)
    }

    /// Open an existing workspace at `path`.
    ///
    /// `identity` supplies the local device identity. It must be the same device
    /// that previously created or joined this workspace (its `PeerId` must
    /// match a record in `_devices`).
    pub fn open(path: impl AsRef<Path>, identity: Arc<dyn PeerIdentity>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let lock = acquire_lock(&root)?;
        let bytes = fs::read(root.join(IDENTITY_FILE))?;
        let workspace_id = deserialize_from(&bytes)?;
        Self::assemble(root, workspace_id, lock, identity, Mode::Open)
    }

    /// Join an existing workspace by its known `WorkspaceId`.
    ///
    /// `identity` is the joining device's identity (a fresh `PeerIdentity` for
    /// this joiner). `hints` carries connection bootstrap data; it is
    /// forwarded to the networking layer in a future iteration and not
    /// persisted by the workspace.
    ///
    /// In this iteration `join` persists the provided `WorkspaceId` and
    /// otherwise behaves like `create` minus `WorkspaceId` generation. It
    /// does not yet contact any network.
    pub fn join(
        path: impl AsRef<Path>,
        workspace_id: WorkspaceId,
        identity: Arc<dyn PeerIdentity>,
        hints: JoinHints,
        _config: WorkspaceConfig,
    ) -> Result<Self> {
        let _ = hints; // forwarded to networking in a future iteration
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let lock = acquire_lock(&root)?;
        fs::write(root.join(IDENTITY_FILE), serialize_to_vec(&workspace_id)?)?;
        Self::assemble(root, workspace_id, lock, identity, Mode::Create)
    }

    fn assemble(
        root: PathBuf,
        workspace_id: WorkspaceId,
        lock: File,
        identity: Arc<dyn PeerIdentity>,
        mode: Mode,
    ) -> Result<Self> {
        // States owns the system catalog and materializes _peers during create;
        // Devices needs that typed handle to build its clock and receipt store.
        let states = match mode {
            Mode::Create => States::create(&root)?,
            Mode::Open => States::open(&root)?,
        };
        let peer_state = states.get::<PeerId, PeerState>(PEERS_STATE_NAME)?;

        // Tables creates or opens the system tables, constructs Devices, opens
        // application tables, and installs listeners before returning.
        let tables = match mode {
            Mode::Create => Tables::create(&root, peer_state, identity.clone())?,
            Mode::Open => Tables::open(&root, peer_state, identity.clone())?,
        };
        let devices = tables.devices.clone();

        Ok(Self {
            root,
            workspace_id,
            identity,
            tables,
            states,
            devices,
            _lock: lock,
        })
    }

    pub fn flush(&self) -> Result<()> {
        self.devices.flush()?;
        self.tables.flush()?;
        self.states.flush()
    }

    pub fn sync(&self) -> Result<()> {
        self.devices.sync()?;
        self.tables.sync()?;
        self.states.sync()
    }

    pub fn id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The local device identity backing this workspace session.
    ///
    /// Exposed so callers can sign messages on behalf of the local device
    /// without the workspace ever holding private key material directly.
    pub fn peer_identity(&self) -> &Arc<dyn PeerIdentity> {
        &self.identity
    }

    pub fn devices(&self) -> &Devices {
        &self.devices
    }

    pub fn tables(&self) -> &Tables {
        &self.tables
    }

    pub fn states(&self) -> &States {
        &self.states
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
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
