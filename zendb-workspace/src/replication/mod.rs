//! Workspace-owned replication configuration, lifecycle, and network worker.

mod config;
mod listeners;
mod runtime;

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use libp2p_identity::Keypair;
use parking_lot::Mutex;
use zendb_storage::Change;
use zendb_types::{Event, EventId, InstallationId, WorkspaceId};

pub use config::{BatchConfig, ReplicationConfig, TopologyConfig};
use listeners::{CatalogListener, ReplicationListener, ReplicationStateListener};
use runtime::{Command, RunningReplication};

use crate::{
    Result,
    consts::TABLE_CATALOG_NAME,
    devices::{DeviceRecord, Devices},
    tables::Tables,
};

#[derive(Clone, Copy)]
enum PendingStop {
    NoRemoteDevices(EventId),
    LocalRevoked(EventId),
}

impl PendingStop {
    const fn event_id(self) -> EventId {
        match self {
            Self::NoRemoteDevices(event_id) | Self::LocalRevoked(event_id) => event_id,
        }
    }
}

pub(crate) struct ReplicationController {
    workspace_id: WorkspaceId,
    local_installation_id: InstallationId,
    keypair: Keypair,
    tables: Arc<Tables>,
    devices: Arc<Devices>,
    config: ReplicationConfig,
    bootstrap_peers: Vec<String>,
    remote_devices: Mutex<BTreeSet<InstallationId>>,
    closed: AtomicBool,
    transition: Mutex<()>,
    runtime: Mutex<Option<RunningReplication>>,
    retired_stops: Mutex<Vec<thread::JoinHandle<()>>>,
    stop_after: Mutex<Option<PendingStop>>,
}

impl ReplicationController {
    pub(crate) fn build(
        workspace_id: WorkspaceId,
        local_installation_id: InstallationId,
        keypair: Keypair,
        tables: Arc<Tables>,
        devices: Arc<Devices>,
        config: ReplicationConfig,
        bootstrap_peers: Vec<String>,
    ) -> Result<Arc<Self>> {
        let remote_devices = devices
            .list()
            .into_iter()
            .filter_map(|(installation_id, _)| {
                (installation_id != local_installation_id).then_some(installation_id)
            })
            .collect();
        let controller = Arc::new(Self {
            workspace_id,
            local_installation_id,
            keypair,
            tables,
            devices,
            config,
            bootstrap_peers,
            remote_devices: Mutex::new(remote_devices),
            closed: AtomicBool::new(false),
            transition: Mutex::new(()),
            runtime: Mutex::new(None),
            retired_stops: Mutex::new(Vec::new()),
            stop_after: Mutex::new(None),
        });
        controller
            .devices
            .registry
            .listeners
            .write()
            .0
            .push(ReplicationStateListener::build(Arc::downgrade(&controller)));
        for (name, table) in controller.tables.tables.read().iter() {
            table.listeners.write().0.push(ReplicationListener::build(
                Arc::downgrade(&controller),
                name.clone(),
            ));
        }
        if let Ok(catalog) = controller.tables.get(TABLE_CATALOG_NAME) {
            catalog
                .listeners
                .write()
                .0
                .push(CatalogListener::build(Arc::downgrade(&controller)));
        }

        let local_is_enrolled = controller
            .devices
            .get(&local_installation_id)
            .is_some_and(|record| controller.local_record_allows_replication(&record));
        if local_is_enrolled && !controller.remote_devices.lock().is_empty() {
            controller.start()?;
        }
        Ok(controller)
    }

    fn local_record_allows_replication(&self, record: &DeviceRecord) -> bool {
        record.role.is_some() && record.public_key.as_libp2p() == &self.keypair.public()
    }

    fn start(self: &Arc<Self>) -> Result<()> {
        let _transition = self.transition.lock();
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut runtime = self.runtime.lock();
        if runtime.is_some() {
            return Ok(());
        }
        *runtime = Some(RunningReplication::start(
            self.workspace_id,
            self.keypair.clone(),
            self.tables.clone(),
            self.devices.clone(),
            self.config.clone(),
            self.bootstrap_peers.clone(),
        )?);
        Ok(())
    }

    fn attach_catalog_table(self: &Arc<Self>, name: &str) {
        let Ok(table) = self.tables.get(name) else {
            return;
        };
        table.listeners.write().0.push(ReplicationListener::build(
            Arc::downgrade(self),
            name.to_owned(),
        ));
    }

    fn device_changed(
        self: &Arc<Self>,
        installation_id: InstallationId,
        previous: Option<DeviceRecord>,
        current: Option<DeviceRecord>,
        change: &Change,
    ) {
        if installation_id == self.local_installation_id {
            self.local_device_changed(current.as_ref(), change);
        } else if let Some(current) = current {
            self.remote_device_upserted(installation_id, previous.as_ref(), &current);
        } else if let Some(removed) = previous {
            self.remote_device_removed(installation_id, &removed, change.event.stamp.id);
        }
    }

    fn local_device_changed(self: &Arc<Self>, current: Option<&DeviceRecord>, change: &Change) {
        if current.is_some_and(|record| self.local_record_allows_replication(record)) {
            *self.stop_after.lock() = None;
            let has_remotes = !self.remote_devices.lock().is_empty();
            if has_remotes {
                let _ = self.start();
            }
        } else if self.runtime.lock().is_some() {
            *self.stop_after.lock() = Some(PendingStop::LocalRevoked(change.event.stamp.id));
        }
    }

    fn remote_device_upserted(
        self: &Arc<Self>,
        installation_id: InstallationId,
        previous: Option<&DeviceRecord>,
        current: &DeviceRecord,
    ) {
        self.remote_devices.lock().insert(installation_id);
        *self.stop_after.lock() = None;
        let local_is_enrolled = self
            .devices
            .get(&self.local_installation_id)
            .is_some_and(|record| self.local_record_allows_replication(&record));
        if local_is_enrolled {
            let _ = self.start();
        }

        if let Some(sender) = self
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| runtime.tx.clone())
        {
            if let Some(previous) =
                previous.filter(|previous| previous.public_key != current.public_key)
            {
                let _ = sender.blocking_send(Command::Revoke(
                    previous.public_key.as_libp2p().to_peer_id(),
                ));
            }
            let _ =
                sender.blocking_send(Command::Allow(current.public_key.as_libp2p().to_peer_id()));
        }
    }

    fn remote_device_removed(
        &self,
        installation_id: InstallationId,
        removed: &DeviceRecord,
        event_id: EventId,
    ) {
        if let Some(sender) = self
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| runtime.tx.clone())
        {
            let _ =
                sender.blocking_send(Command::Revoke(removed.public_key.as_libp2p().to_peer_id()));
        }
        let no_remotes = {
            let mut remotes = self.remote_devices.lock();
            remotes.remove(&installation_id);
            remotes.is_empty()
        };
        if no_remotes && self.runtime.lock().is_some() {
            *self.stop_after.lock() = Some(PendingStop::NoRemoteDevices(event_id));
        }
    }

    fn submit(&self, table: String, event: Event) {
        let sender = self
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| runtime.tx.clone());
        if let Some(sender) = sender {
            let _ = sender.blocking_send(Command::Event { table, event });
        }
    }

    fn finish_pending_stop(&self, event_id: EventId) {
        let _transition = self.transition.lock();
        let mut stop_after = self.stop_after.lock();
        let Some(pending) = *stop_after else {
            return;
        };
        if pending.event_id() != event_id {
            return;
        }
        if matches!(pending, PendingStop::NoRemoteDevices(_))
            && !self.remote_devices.lock().is_empty()
        {
            *stop_after = None;
            return;
        }
        *stop_after = None;
        drop(stop_after);

        if let Some(runtime) = self.runtime.lock().take() {
            self.retired_stops
                .lock()
                .push(thread::spawn(move || runtime.stop()));
        }
    }

    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let runtime = {
            let _transition = self.transition.lock();
            *self.stop_after.lock() = None;
            self.runtime.lock().take()
        };
        if let Some(runtime) = runtime {
            runtime.stop();
        }
        for stop in self.retired_stops.lock().drain(..) {
            let _ = stop.join();
        }
    }
}

impl Drop for ReplicationController {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.get_mut().take() {
            runtime.stop();
        }
        for stop in self.retired_stops.get_mut().drain(..) {
            let _ = stop.join();
        }
    }
}
