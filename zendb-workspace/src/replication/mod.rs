//! Workspace-owned replication configuration, lifecycle, and network worker.

mod config;
mod listeners;
mod runtime;

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use libp2p::{Multiaddr, PeerId};
use libp2p_identity::Keypair;
use parking_lot::Mutex;
use zendb_storage::Change;
use zendb_types::{Event, EventId, InstallationId, WorkspaceId};

pub use config::{BatchConfig, DialConfig, ReplicationConfig, TopologyConfig};
use listeners::{CatalogListener, ReplicationListener, ReplicationStateListener};
use runtime::{Command, RunningReplication};

use crate::{Result, consts::TABLE_CATALOG_NAME, devices::Devices, tables::Tables};

#[derive(Clone, Debug, PartialEq, Eq)]
struct PeerRoute {
    peer_id: PeerId,
    addresses: Vec<Multiaddr>,
}

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
    remote_devices: Mutex<BTreeMap<InstallationId, PeerRoute>>,
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
    ) -> Result<Arc<Self>> {
        let (local_is_enrolled, remote_devices) = project_devices(
            &devices,
            local_installation_id,
            keypair.public().to_peer_id(),
        );
        let controller = Arc::new(Self {
            workspace_id,
            local_installation_id,
            keypair,
            tables,
            devices,
            config,
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

        if local_is_enrolled && !controller.remote_devices.lock().is_empty() {
            controller.start()?;
        }
        Ok(controller)
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
            self.remote_devices.lock().clone(),
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

    fn device_changed(self: &Arc<Self>, change: &Change) {
        let (local_is_enrolled, desired) = project_devices(
            &self.devices,
            self.local_installation_id,
            self.keypair.public().to_peer_id(),
        );
        let previous = {
            let mut current = self.remote_devices.lock();
            std::mem::replace(&mut *current, desired.clone())
        };
        let sender = self
            .runtime
            .lock()
            .as_ref()
            .map(|runtime| runtime.tx.clone());

        if let Some(sender) = sender.as_ref() {
            for (installation_id, old) in &previous {
                let replacement = desired.get(installation_id);
                if replacement.is_none_or(|new| new.peer_id != old.peer_id) {
                    let _ = sender.blocking_send(Command::RemovePeer {
                        installation_id: *installation_id,
                        peer_id: old.peer_id,
                    });
                }
            }
            for (installation_id, route) in &desired {
                if previous.get(installation_id) != Some(route) {
                    let _ = sender.blocking_send(Command::UpsertPeer {
                        installation_id: *installation_id,
                        peer_id: route.peer_id,
                        addresses: route.addresses.clone(),
                    });
                }
            }
        }

        if local_is_enrolled && !desired.is_empty() {
            *self.stop_after.lock() = None;
            if sender.is_none() {
                let _ = self.start();
            }
        } else if sender.is_some() {
            *self.stop_after.lock() = Some(if local_is_enrolled {
                PendingStop::NoRemoteDevices(change.event.stamp.id)
            } else {
                PendingStop::LocalRevoked(change.event.stamp.id)
            });
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

fn project_devices(
    devices: &Devices,
    local_installation_id: InstallationId,
    local_peer_id: PeerId,
) -> (bool, BTreeMap<InstallationId, PeerRoute>) {
    let records = devices.list();
    let mut owners = HashMap::<PeerId, usize>::new();
    for (_, record) in &records {
        *owners
            .entry(record.public_key.as_libp2p().to_peer_id())
            .or_default() += 1;
    }

    let local_is_enrolled = records.iter().any(|(installation_id, record)| {
        *installation_id == local_installation_id
            && record.role.is_some()
            && record.public_key.as_libp2p().to_peer_id() == local_peer_id
            && owners.get(&local_peer_id) == Some(&1)
    });
    let remote_devices = records
        .into_iter()
        .filter_map(|(installation_id, record)| {
            let peer_id = record.public_key.as_libp2p().to_peer_id();
            (installation_id != local_installation_id && owners.get(&peer_id) == Some(&1)).then(
                || {
                    (
                        installation_id,
                        PeerRoute {
                            peer_id,
                            addresses: record
                                .addresses
                                .into_iter()
                                .map(|address| address.into_libp2p())
                                .collect(),
                        },
                    )
                },
            )
        })
        .collect();
    (local_is_enrolled, remote_devices)
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
