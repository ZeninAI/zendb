//! Workspace-owned replication configuration, lifecycle, and network worker.

mod config;
mod listener;
mod runtime;

use std::{
    collections::BTreeSet,
    sync::{Arc, Weak},
    thread,
};

use libp2p_identity::Keypair;
use parking_lot::Mutex;
use zendb_types::{Event, EventId, InstallationId, WorkspaceId};

pub use config::{BatchConfig, ReplicationConfig, TopologyConfig};
use listener::ReplicationListenerFactory;
use runtime::{Command, RunningReplication};

use crate::{
    Result,
    devices::{DeviceRecord, Devices},
    tables::{Tables, listeners::DeviceChangeObserver},
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
            runtime: Mutex::new(None),
            retired_stops: Mutex::new(Vec::new()),
            stop_after: Mutex::new(None),
        });
        if !controller.remote_devices.lock().is_empty() {
            controller.start()?;
        }
        Ok(controller)
    }

    pub(crate) const fn local_installation_id(&self) -> InstallationId {
        self.local_installation_id
    }

    pub(crate) fn listener_factory(
        self: &Arc<Self>,
    ) -> Arc<dyn crate::tables::ChangeListenerFactory> {
        ReplicationListenerFactory::new(Arc::downgrade(self))
    }

    pub(crate) fn observer(self: &Arc<Self>) -> Weak<dyn DeviceChangeObserver> {
        let observer: Arc<dyn DeviceChangeObserver> = self.clone();
        Arc::downgrade(&observer)
    }

    pub(crate) fn submit(&self, table: String, event: Event) {
        let sender = self
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| runtime.tx.clone());
        if let Some(sender) = sender {
            let _ = sender.blocking_send(Command::Event { table, event });
        }
    }

    pub(crate) fn finish_pending_stop(&self, event_id: EventId) {
        let mut stop_after = self.stop_after.lock();
        let Some(pending) = *stop_after else {
            return;
        };
        if pending.event_id() != event_id {
            return;
        }
        let remotes = self.remote_devices.lock();
        if matches!(pending, PendingStop::NoRemoteDevices(_)) && !remotes.is_empty() {
            *stop_after = None;
            return;
        }
        *stop_after = None;
        let runtime = self.runtime.lock().take();
        drop(remotes);
        drop(stop_after);
        if let Some(runtime) = runtime {
            self.retired_stops
                .lock()
                .push(thread::spawn(move || runtime.stop()));
        }
    }

    pub(crate) fn shutdown(&self) {
        *self.stop_after.lock() = None;
        let runtime = self.runtime.lock().take();
        if let Some(runtime) = runtime {
            runtime.stop();
        }
        for stop in self.retired_stops.lock().drain(..) {
            let _ = stop.join();
        }
    }

    fn start(&self) -> Result<()> {
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
}

impl DeviceChangeObserver for ReplicationController {
    fn device_upserted(
        &self,
        installation_id: InstallationId,
        previous: Option<DeviceRecord>,
        current: DeviceRecord,
        event_id: EventId,
    ) {
        if installation_id == self.local_installation_id {
            let expected_public_key = self.keypair.public();
            if current.role.is_none() || current.public_key.as_libp2p() != &expected_public_key {
                *self.stop_after.lock() = Some(PendingStop::LocalRevoked(event_id));
                return;
            }
            *self.stop_after.lock() = None;
            if !self.remote_devices.lock().is_empty() {
                let _ = self.start();
            }
            return;
        }
        let should_start = {
            let mut stop_after = self.stop_after.lock();
            let mut remotes = self.remote_devices.lock();
            let was_empty = remotes.is_empty();
            remotes.insert(installation_id);
            if was_empty {
                *stop_after = None;
            }
            was_empty
        };
        if should_start {
            let _ = self.start();
        }
        let sender = self
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| runtime.tx.clone());
        if let Some(sender) = sender {
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

    fn device_removed(
        &self,
        installation_id: InstallationId,
        removed: DeviceRecord,
        event_id: EventId,
    ) {
        if installation_id == self.local_installation_id {
            *self.stop_after.lock() = Some(PendingStop::LocalRevoked(event_id));
            return;
        }
        let sender = self
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| runtime.tx.clone());
        if let Some(sender) = sender {
            let _ =
                sender.blocking_send(Command::Revoke(removed.public_key.as_libp2p().to_peer_id()));
        }
        {
            let mut stop_after = self.stop_after.lock();
            let mut remotes = self.remote_devices.lock();
            remotes.remove(&installation_id);
            if remotes.is_empty() {
                *stop_after = Some(PendingStop::NoRemoteDevices(event_id));
            }
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
