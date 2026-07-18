//! Shared-layer snapshots and stable-watermark compaction.

use std::{fs, io, path::Path};

use bincode::{Decode, Encode};
use zendb_replication::{SnapshotExport, SnapshotManifest, WorkspaceSyncSummary};
use zendb_types::{
    compaction_watermark, stable_frontier, Cell, ContiguousFrontier, Hlc, PrimaryKey, WorkspaceId,
};

use super::{Workspace, SNAPSHOT_FILE};

const SNAPSHOT_FORMAT_VERSION: u16 = 2;

#[derive(Debug, Clone, Encode, Decode)]
pub(super) struct WorkspaceSnapshot {
    format_version: u16,
    workspace_id: WorkspaceId,
    frontier: ContiguousFrontier,
    pub(super) system: super::system::SystemSnapshot,
    tables: Vec<SharedTableSnapshot>,
}

#[derive(Debug, Clone, Encode, Decode)]
struct SharedTableSnapshot {
    name: String,
    rows: Vec<(PrimaryKey, Cell)>,
}

impl Workspace {
    /// Capture and durably retain a snapshot of the shared plane. Local tables,
    /// local policy metadata, and values below local boundaries are omitted.
    pub fn export_snapshot(self: &std::sync::Arc<Self>) -> io::Result<SnapshotExport> {
        let _shared_guard = self.shared_mutation.lock();
        let frontier = self.shared_journal.lock().frontier().clone();
        let mut tables = Vec::new();
        for name in self.list_shared_tables()? {
            if self.table_config(&name).is_none() {
                continue;
            }
            let handle = self.table_impl(&name, None)?;
            let rows = handle.get()?.read().shared_rows();
            tables.push(SharedTableSnapshot { name, rows });
        }
        tables.sort_by(|left, right| left.name.cmp(&right.name));

        let payload = WorkspaceSnapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            workspace_id: self.workspace_id().clone(),
            frontier: frontier.clone(),
            system: self.control.lock().snapshot(),
            tables,
        };
        let bytes = bincode::encode_to_vec(&payload, bincode::config::standard())
            .map_err(|error| io::Error::other(error.to_string()))?;
        let generation = next_generation(&self.path.join(SNAPSHOT_FILE));
        let compacted_through = self.shared_watermark()?;
        let export = SnapshotExport {
            manifest: SnapshotManifest {
                workspace_id: self.workspace_id().clone(),
                summary: WorkspaceSyncSummary {
                    workspace_id: self.workspace_id().clone(),
                    frontier,
                    snapshot_generation: Some(generation),
                    compacted_through,
                    requests_state_reconciliation: self.state_reconciliation_required(),
                    table_merkle_roots: self.sync_summary().table_merkle_roots,
                },
                total_bytes: bytes.len() as u64,
                snapshot_hash: *blake3::hash(&bytes).as_bytes(),
                compacted_through,
            },
            bytes,
        };
        persist_snapshot(&self.path.join(SNAPSHOT_FILE), &export)?;
        Ok(export)
    }

    /// Install a verified shared snapshot. The snapshot must already contain
    /// this local Device record; admission is never implied by byte transfer.
    pub(crate) fn install_snapshot(
        self: &std::sync::Arc<Self>,
        export: &SnapshotExport,
    ) -> io::Result<()> {
        let _shared_guard = self.shared_mutation.lock();
        validate_export(self.workspace_id(), export)?;
        let (snapshot, consumed): (WorkspaceSnapshot, usize) =
            bincode::decode_from_slice(&export.bytes, bincode::config::standard())
                .map_err(|error| io::Error::other(error.to_string()))?;
        if consumed != export.bytes.len()
            || snapshot.format_version != SNAPSHOT_FORMAT_VERSION
            || snapshot.workspace_id != *self.workspace_id()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot payload scope or format is invalid",
            ));
        }

        if snapshot.system.device(self.device_id())?.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "snapshot does not admit the local Device",
            ));
        }

        // Installation is idempotent and can be retried after an I/O failure.
        // Local catalog rows are merged policy-aware and intentionally hide a
        // same-named shared row rather than rejecting the whole snapshot.
        self.control.lock().install_snapshot(snapshot.system)?;

        for table in snapshot.tables {
            if !self.is_shared_table(&table.name)? {
                continue;
            }
            let config = self.table_config(&table.name).unwrap_or_default();
            let handle = self.table_impl(&table.name, Some(config))?;
            handle.get()?.write().install_shared_rows(table.rows)?;
        }
        self.shared_journal
            .lock()
            .install_snapshot_frontier(snapshot.frontier)?;
        persist_snapshot(&self.path.join(SNAPSHOT_FILE), export)?;
        self.clear_state_reconciliation_required()?;
        drop(_shared_guard);
        self.recover_shared_journal()
    }

    /// Element-wise minimum of every currently admitted Device checkpoint.
    pub fn stable_frontier(&self) -> io::Result<ContiguousFrontier> {
        let devices = self.devices()?;
        Ok(stable_frontier(
            devices
                .iter()
                .map(|(_, device)| &device.replication_frontier),
        ))
    }

    /// Scalar HLC proven safe by all admitted Device checkpoints.
    pub fn shared_watermark(&self) -> io::Result<Option<Hlc>> {
        let stable = self.stable_frontier()?;
        let journal = self.shared_journal.lock();
        Ok(compaction_watermark(&stable, |origin, sequence| {
            journal.event_hlc(origin, sequence)
        }))
    }

    /// Compact shared CRDT state only after retaining a snapshot at the same
    /// frontier. Journal pruning remains conservative and is not performed.
    pub fn compact_shared(self: &std::sync::Arc<Self>) -> io::Result<Option<Hlc>> {
        let Some(watermark) = self.shared_watermark()? else {
            return Ok(None);
        };
        self.export_snapshot()?;
        let _shared_guard = self.shared_mutation.lock();
        self.control.lock().compact_through(watermark)?;
        for name in self.list_shared_tables()? {
            self.table_impl(&name, None)?
                .get()?
                .write()
                .compact_shared_through(watermark)?;
        }
        Ok(Some(watermark))
    }
}

fn validate_export(workspace_id: &WorkspaceId, export: &SnapshotExport) -> io::Result<()> {
    if export.manifest.workspace_id != *workspace_id
        || export.manifest.summary.workspace_id != *workspace_id
        || export.manifest.total_bytes != export.bytes.len() as u64
        || export.manifest.snapshot_hash != *blake3::hash(&export.bytes).as_bytes()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot manifest validation failed",
        ));
    }
    Ok(())
}

pub(super) fn decode_snapshot(
    workspace_id: &WorkspaceId,
    export: &SnapshotExport,
) -> io::Result<WorkspaceSnapshot> {
    validate_export(workspace_id, export)?;
    let (snapshot, consumed): (WorkspaceSnapshot, usize) =
        bincode::decode_from_slice(&export.bytes, bincode::config::standard())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    if consumed != export.bytes.len()
        || snapshot.format_version != SNAPSHOT_FORMAT_VERSION
        || snapshot.workspace_id != *workspace_id
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot payload scope or format is invalid",
        ));
    }
    Ok(snapshot)
}

fn next_generation(path: &Path) -> u64 {
    load_snapshot(path)
        .ok()
        .flatten()
        .and_then(|snapshot| snapshot.manifest.summary.snapshot_generation)
        .unwrap_or(0)
        .saturating_add(1)
}

fn persist_snapshot(path: &Path, export: &SnapshotExport) -> io::Result<()> {
    let bytes = bincode::encode_to_vec(export, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))?;
    fs::write(path, bytes)?;
    std::fs::File::options().write(true).open(path)?.sync_all()
}

fn load_snapshot(path: &Path) -> io::Result<Option<SnapshotExport>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let (snapshot, consumed) = bincode::decode_from_slice(&bytes, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))?;
    if consumed != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot file contains trailing bytes",
        ));
    }
    Ok(Some(snapshot))
}
