//! Workspace bootstrap lifecycle and synchronous orchestration API.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use zendb_types::{
    utils::time::physical_ms, EventId, EventStamp, EventTime, PeerId, PeerIdentity, WorkspaceId,
};

use crate::{
    bootstrap::Bootstrap,
    devices::{Devices, PeerRecord},
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
    bootstrap: Bootstrap,
    peer: Arc<dyn PeerIdentity>,
    tables: Tables,
    states: States,
    devices: Devices,
}

impl Workspace {
    /// Create a new workspace at `path`.
    ///
    /// `peer` supplies the local device identity (`PeerId` + private key).
    /// The workspace stores the `WorkspaceId` it generates but never stores
    /// the private key.
    pub fn create(
        path: impl AsRef<Path>,
        peer: Arc<dyn PeerIdentity>,
        _config: WorkspaceConfig,
    ) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let bootstrap = Bootstrap::create(&root)?;
        Self::assemble(root, bootstrap, peer, Mode::Create)
    }

    /// Open an existing workspace at `path`.
    ///
    /// `peer` supplies the local device identity. It must be the same device
    /// that previously created or joined this workspace (its `PeerId` must
    /// match a record in `_devices`).
    pub fn open(path: impl AsRef<Path>, peer: Arc<dyn PeerIdentity>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let bootstrap = Bootstrap::open(&root)?;
        Self::assemble(root, bootstrap, peer, Mode::Open)
    }

    /// Join an existing workspace by its known `WorkspaceId`.
    ///
    /// `peer` is the joining device's identity (a fresh `PeerIdentity` for
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
        peer: Arc<dyn PeerIdentity>,
        hints: JoinHints,
        _config: WorkspaceConfig,
    ) -> Result<Self> {
        let _ = hints; // forwarded to networking in a future iteration
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let bootstrap = Bootstrap::join(&root, workspace_id)?;
        Self::assemble(root, bootstrap, peer, Mode::Create)
    }

    fn assemble(
        root: PathBuf,
        bootstrap: Bootstrap,
        peer: Arc<dyn PeerIdentity>,
        mode: Mode,
    ) -> Result<Self> {
        let local_peer_id = peer.peer_id();
        let physical_ms = physical_ms().ok_or(Error::ClockExhausted)?;
        let catalog_stamp = EventStamp::new(
            EventId::new(local_peer_id, 1),
            EventTime::new(physical_ms, 0),
        );
        let devices_stamp = EventStamp::new(
            EventId::new(local_peer_id, 2),
            EventTime::new(physical_ms, 1),
        );

        let tables_core = match mode {
            Mode::Create => crate::tables::TablesCore::create(&root, catalog_stamp, devices_stamp)?,
            Mode::Open => crate::tables::TablesCore::open(&root)?,
        };
        let states_core = match mode {
            Mode::Create => crate::states::StatesCore::create(&root)?,
            Mode::Open => crate::states::StatesCore::open(&root)?,
        };
        let devices = match mode {
            Mode::Create => Devices::create(
                tables_core.table_entry(crate::tables::DEVICES_NAME)?,
                states_core.peer_state::<PeerId, PeerRecord>()?,
                peer.clone(),
            )?,
            Mode::Open => Devices::open(
                tables_core.table_entry(crate::tables::DEVICES_NAME)?,
                states_core.peer_state::<PeerId, PeerRecord>()?,
                peer.clone(),
            )?,
        };
        tables_core.replay_receipts(&devices)?;
        if matches!(mode, Mode::Create) {
            devices.bootstrap_local()?;
        }

        Ok(Self {
            root,
            bootstrap,
            peer,
            tables: Tables::new(tables_core, devices.clone()),
            states: States::new(states_core),
            devices: (*devices).clone(),
        })
    }

    pub fn id(&self) -> WorkspaceId {
        self.bootstrap.workspace_id()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The local device identity backing this workspace session.
    ///
    /// Exposed so callers can sign messages on behalf of the local device
    /// without the workspace ever holding private key material directly.
    pub fn peer_identity(&self) -> &Arc<dyn PeerIdentity> {
        &self.peer
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

    pub fn flush(&self) -> Result<()> {
        self.devices.flush()
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
