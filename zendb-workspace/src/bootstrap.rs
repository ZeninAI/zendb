//! Workspace bootstrap: lock file and `WorkspaceId` blob.

use std::{
    fs::{self, File, OpenOptions},
    path::Path,
};

use zendb_types::{
    utils::{deserialize_from, serialize_to_vec},
    WorkspaceId,
};

use crate::{Error, Result};

const IDENTITY_FILE: &str = "_identity";
const LOCK_FILE: &str = "_lock";

/// Owns the workspace lock and the persisted `WorkspaceId`.
///
/// The identity file is write-once: it is written at `create` or `join` and
/// read at `open`. There is no `flush` because the blob never updates. The
/// OS file lock is released when this value is dropped.
pub(crate) struct Bootstrap {
    workspace_id: WorkspaceId,
    _lock: File,
}

impl Bootstrap {
    /// Acquire the lock, generate a fresh `WorkspaceId`, persist it.
    pub(crate) fn create(root: &Path) -> Result<Self> {
        let lock = Self::acquire_lock(root)?;
        let identity_path = root.join(IDENTITY_FILE);
        if identity_path.exists() {
            return Err(Error::AlreadyExists("workspace identity".to_owned()));
        }
        let workspace_id = WorkspaceId::generate();
        fs::write(&identity_path, serialize_to_vec(&workspace_id)?)?;
        Ok(Self {
            workspace_id,
            _lock: lock,
        })
    }

    /// Acquire the lock and read the existing `WorkspaceId` from disk.
    pub(crate) fn open(root: &Path) -> Result<Self> {
        let lock = Self::acquire_lock(root)?;
        let bytes = fs::read(root.join(IDENTITY_FILE))?;
        let workspace_id: WorkspaceId = deserialize_from(&bytes)?;
        Ok(Self {
            workspace_id,
            _lock: lock,
        })
    }

    /// Acquire the lock and persist a caller-provided `WorkspaceId`.
    ///
    /// Used by `Workspace::join`: the caller knows the target workspace and
    /// supplies its identity. `Bootstrap` only persists it; the join
    /// transport itself is the caller's responsibility.
    pub(crate) fn join(root: &Path, workspace_id: WorkspaceId) -> Result<Self> {
        let lock = Self::acquire_lock(root)?;
        fs::write(root.join(IDENTITY_FILE), serialize_to_vec(&workspace_id)?)?;
        Ok(Self {
            workspace_id,
            _lock: lock,
        })
    }

    pub(crate) fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
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
}
