//! Shared workspace ownership and the explicit event-application pipeline.

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;
use zendb_storage::InsertOutcome;
use zendb_types::{Event, InstallationId, InstallationState, Op, Path, PrimaryKey, Value};

use crate::{
    Error, Result,
    causal::CausalTracker,
    installations::Membership,
    replication::ReplicationNotification,
    states::States,
    tables::{OpenTable, TableKind, TableStore},
};

pub(crate) struct WorkspaceCore {
    pub(crate) table_store: TableStore,
    pub(crate) states: States,
    pub(crate) membership: Membership,
    pub(crate) causal: CausalTracker,
    pub(crate) replication_notifications: UnboundedSender<ReplicationNotification>,
}

impl WorkspaceCore {
    pub(crate) fn commit_change(
        &self,
        table: &Arc<OpenTable>,
        primary_key: PrimaryKey,
        path: Path,
        op: Op,
    ) -> Result<InsertOutcome> {
        let stamp = self.causal.mint()?;
        let event = Event {
            primary_key,
            path,
            op,
            stamp,
        };
        let replicated = event.clone();
        let outcome = table.insert_event(event)?;
        let _ = self.causal.observe(stamp)?;
        if let InsertOutcome::Applied(change) = &outcome {
            self.project_change(table, change)?;
        }
        self.replication_notifications
            .send(ReplicationNotification::Event {
                table: table.name().to_owned(),
                event: replicated,
            })
            .map_err(|_| Error::Replication("replication runtime stopped".into()))?;
        Ok(outcome)
    }

    /// Apply a remotely produced event to its target table. Returns `true` if
    /// the event was novel, `false` if it was already present.
    pub(crate) fn commit_replication_event(&self, table_name: &str, event: Event) -> Result<bool> {
        let event_id = event.stamp.id;
        let table = self.table_store.get(table_name)?;
        if self.causal.contains(event_id) {
            return Ok(false);
        }
        let stamp = event.stamp;
        let outcome = table.insert_event(event)?;
        let _ = self.causal.observe(stamp)?;
        if let InsertOutcome::Applied(change) = &outcome {
            self.project_change(&table, change)?;
        }
        Ok(true)
    }

    fn project_change(&self, table: &OpenTable, change: &zendb_storage::Change) -> Result<()> {
        match table.kind() {
            TableKind::Installations => {
                self.membership.apply_installation_change(change);
                self.emit_membership_notification(change);
            }
            TableKind::Catalog => {
                self.table_store.apply_catalog_change(change);
            }
            TableKind::Application => {}
        }
        table.notify_listeners(change);
        Ok(())
    }

    /// Emit a granular Admitted or Rejected notification based on the
    /// installation's new materialized state.
    fn emit_membership_notification(&self, change: &zendb_storage::Change) {
        let Ok(installation_id) = InstallationId::try_from(&change.event.primary_key) else {
            return;
        };
        // Determine the new state from the materialized cell.
        let new_state = change.current.as_ref().and_then(|cell| match &cell.value {
            Some(Value::Installation(i)) => Some(i.state),
            _ => None,
        });
        let notification = match new_state {
            Some(state) if state.is_active() => {
                ReplicationNotification::Admitted { installation_id }
            }
            Some(InstallationState::Rejected) => {
                ReplicationNotification::Rejected { installation_id }
            }
            _ => return,
        };
        let _ = self.replication_notifications.send(notification);
    }
}
