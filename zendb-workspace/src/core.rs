//! Shared workspace ownership and the explicit event-application pipeline.

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;
use zendb_storage::InsertOutcome;
use zendb_types::{Event, InstallationId, PathOp, PrimaryKey};

use crate::{
    Error, Result,
    installations::Membership,
    replication::ReplicationNotification,
    states::States,
    tables::{OpenTable, TableKind, TableStore},
};

pub(crate) struct WorkspaceCore {
    pub(crate) table_store: TableStore,
    pub(crate) states: States,
    pub(crate) membership: Membership,
    pub(crate) replication_notifications: Option<UnboundedSender<ReplicationNotification>>,
}

impl WorkspaceCore {
    pub(crate) fn commit_change(
        &self,
        table: &Arc<OpenTable>,
        primary_key: PrimaryKey,
        operations: Vec<PathOp>,
    ) -> Result<InsertOutcome> {
        let event = Event {
            id: zendb_types::EventId {
                author: self.membership.local_installation_id(),
                sequence: 0,
            },
            primary_key,
            operations,
        };
        let outcome = table.insert_event(event)?;
        if let InsertOutcome::Applied(change) = &outcome {
            self.project_change(table, change)?;
            if let Some(sender) = &self.replication_notifications {
                sender
                    .send(ReplicationNotification::Event {
                        table: table.name().to_owned(),
                        event: change.event.clone(),
                    })
                    .map_err(|_| Error::Replication("replication runtime stopped".into()))?;
            }
        }
        Ok(outcome)
    }

    /// Apply a remotely produced event to its target table. Returns `true` if
    /// the event was novel, `false` if it was already present.
    pub(crate) fn commit_replication_event(&self, table_name: &str, event: Event) -> Result<bool> {
        let table = self.table_store.get(table_name)?;
        let outcome = table.observe_event(event)?;
        if let InsertOutcome::Applied(change) = &outcome {
            self.project_change(&table, change)?;
            return Ok(true);
        }
        Ok(false)
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

    /// Notify replication after every materialized installation update so its
    /// route and local handshake views stay current.
    fn emit_membership_notification(&self, change: &zendb_storage::Change) {
        let Ok(installation_id) = InstallationId::try_from(&change.event.primary_key) else {
            return;
        };
        if let Some(sender) = &self.replication_notifications {
            let _ = sender.send(ReplicationNotification::InstallationChanged { installation_id });
        }
    }
}
