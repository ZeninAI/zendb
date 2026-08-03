//! Shared workspace ownership and the explicit event-application pipeline.

use std::sync::Arc;

use libp2p_identity::Keypair;
use zendb_storage::InsertOutcome;
use zendb_types::{Event, Op, Path, Permission, PrimaryKey, WorkspaceId};

use crate::{
    Result,
    causal::CausalTracker,
    installations::Membership,
    replication::{ReplicationConfig, ReplicationController},
    states::States,
    tables::{OpenTable, TableKind, TableStore},
};

pub(crate) struct WorkspaceCore {
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) states: Arc<States>,
    pub(crate) membership: Membership,
    pub(crate) causal: CausalTracker,
    pub(crate) replication: Arc<ReplicationController>,
}

impl WorkspaceCore {
    pub(crate) fn new(
        workspace_id: WorkspaceId,
        keypair: Keypair,
        table_store: Arc<TableStore>,
        states: Arc<States>,
        membership: Membership,
        causal: CausalTracker,
        replication_config: ReplicationConfig,
    ) -> Arc<Self> {
        // The replication controller may own a worker thread, but that worker
        // must not keep the workspace's operational core alive on its own.
        Arc::new_cyclic(|core| Self {
            table_store,
            states,
            membership,
            causal,
            replication: ReplicationController::new(
                workspace_id,
                keypair,
                core.clone(),
                replication_config,
            ),
        })
    }

    pub(crate) fn commit_local_change(
        &self,
        table: &Arc<OpenTable>,
        primary_key: PrimaryKey,
        path: Path,
        op: Op,
    ) -> Result<InsertOutcome> {
        // TableHandle blocks direct system-table writes, so this path only
        // needs application-data authorization.
        self.membership.require_permission(
            &self.membership.local_installation_id(),
            Permission::WriteData,
        )?;
        self.commit_authorized_local_change(table, primary_key, path, op)
    }

    pub(crate) fn commit_authorized_local_change(
        &self,
        table: &Arc<OpenTable>,
        primary_key: PrimaryKey,
        path: Path,
        op: Op,
    ) -> Result<InsertOutcome> {
        // Commit local change that is already authorized
        // Can come from the commit_local_change function or
        // Can come from the system table modifications where the wrappers already pre-authorize
        let stamp = self.causal.mint()?;
        self.commit_event(
            table,
            Event {
                primary_key,
                path,
                op,
                stamp,
            },
        )
    }

    pub(crate) fn commit_admitted_event(
        &self,
        table: &Arc<OpenTable>,
        event: Event,
    ) -> Result<InsertOutcome> {
        // Facade to commit remote event
        self.commit_event(table, event)
    }

    fn commit_event(&self, table: &Arc<OpenTable>, event: Event) -> Result<InsertOutcome> {
        let outcome = table.insert_event(event)?;
        // Only an applied CRDT change enters the post-commit pipeline. Ignored
        // duplicates must not advance receipts, publish, or notify listeners.
        let InsertOutcome::Applied(change) = &outcome else {
            return Ok(outcome);
        };

        // Minting and table I/O intentionally happen outside this observation
        // lock. A failed local write consumes its sequence and leaves a gap.
        // Keep the remaining order: observe, update projections, publish local
        // events, complete pending shutdown, then notify application listeners.
        let _ = self.causal.observe(change.event.stamp);
        match table.kind() {
            TableKind::Installations => {
                self.membership.apply_installation_change(change);
                self.replication
                    .reconcile_installations(change.event.stamp.id);
            }
            TableKind::Catalog => self.table_store.apply_catalog_change(change),
            TableKind::Application => {}
        }
        if change.event.stamp.id.author == self.membership.local_installation_id() {
            self.replication
                .submit_event(table.name().to_owned(), change.event.clone());
        }
        self.replication
            .complete_pending_stop(change.event.stamp.id);
        table.notify_listeners(change);
        Ok(outcome)
    }

    pub(crate) fn flush(&self) -> Result<()> {
        self.causal.flush()?;
        self.table_store.flush()?;
        self.states.flush()
    }

    pub(crate) fn sync(&self) -> Result<()> {
        self.causal.sync()?;
        self.table_store.sync()?;
        self.states.sync()
    }
}
