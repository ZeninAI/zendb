//! Workspace replication lifecycle and projection of the installation registry.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    thread,
};

use libp2p::PeerId;
use libp2p_identity::Keypair;
use parking_lot::{Condvar, Mutex};
use zendb_storage::Change;
use zendb_types::{Event, EventId, InstallationId, WorkspaceId};

use super::{
    ReplicationConfig,
    command::{Command, PeerRoute},
    listeners::{CatalogListener, ReplicationListener, ReplicationStateListener},
    runtime::RunningReplication,
};
use crate::{Result, consts::TABLE_CATALOG_NAME, installations::Installations, tables::Tables};

#[derive(Clone)]
struct RegistryProjection {
    local_is_enrolled: bool,
    remotes: BTreeMap<InstallationId, PeerRoute>,
}

impl RegistryProjection {
    fn should_replicate(&self) -> bool {
        self.local_is_enrolled && !self.remotes.is_empty()
    }
}

#[derive(Clone, Copy)]
enum PendingStop {
    NoRemoteInstallations(EventId),
    LocalRevoked(EventId),
}

impl PendingStop {
    const fn event_id(self) -> EventId {
        match self {
            Self::NoRemoteInstallations(event_id) | Self::LocalRevoked(event_id) => event_id,
        }
    }
}

enum Lifecycle {
    Stopped,
    Starting,
    Running(RunningReplication),
    Stopping(thread::JoinHandle<()>),
    Failed(String),
    Closed,
}

struct ControllerState {
    lifecycle: Lifecycle,
    registry: RegistryProjection,
    pending_stop: Option<PendingStop>,
}

pub(crate) struct ReplicationController {
    workspace_id: WorkspaceId,
    pub(crate) local_installation_id: InstallationId,
    keypair: Keypair,
    tables: Arc<Tables>,
    installations: Arc<Installations>,
    config: ReplicationConfig,
    state: Mutex<ControllerState>,
    state_changed: Condvar,
}

impl ReplicationController {
    pub(crate) fn build(
        workspace_id: WorkspaceId,
        local_installation_id: InstallationId,
        keypair: Keypair,
        tables: Arc<Tables>,
        installations: Arc<Installations>,
        config: ReplicationConfig,
    ) -> Result<Arc<Self>> {
        let registry = project_installations(
            &installations,
            local_installation_id,
            keypair.public().to_peer_id(),
        );
        let should_start = registry.should_replicate();
        let controller = Arc::new(Self {
            workspace_id,
            local_installation_id,
            keypair,
            tables,
            installations,
            config,
            state: Mutex::new(ControllerState {
                lifecycle: Lifecycle::Stopped,
                registry,
                pending_stop: None,
            }),
            state_changed: Condvar::new(),
        });
        controller
            .installations
            .registry
            .add_internal_listener(ReplicationStateListener::build(Arc::downgrade(&controller)));
        for (name, table) in controller.tables.tables.read().iter() {
            table.add_internal_listener(ReplicationListener::build(
                Arc::downgrade(&controller),
                name.clone(),
            ));
        }
        if let Ok(catalog) = controller.tables.get(TABLE_CATALOG_NAME) {
            catalog.add_internal_listener(CatalogListener::build(Arc::downgrade(&controller)));
        }

        if should_start {
            controller.start()?;
        }
        Ok(controller)
    }

    fn start(self: &Arc<Self>) -> Result<()> {
        let (started_with, stopping) = {
            let mut state = self.state.lock();
            loop {
                if !state.registry.should_replicate() {
                    return Ok(());
                }
                match &state.lifecycle {
                    Lifecycle::Closed | Lifecycle::Running(_) => return Ok(()),
                    Lifecycle::Starting => self.state_changed.wait(&mut state),
                    Lifecycle::Stopping(_) => {
                        let Lifecycle::Stopping(join) =
                            std::mem::replace(&mut state.lifecycle, Lifecycle::Starting)
                        else {
                            unreachable!();
                        };
                        break (state.registry.remotes.clone(), Some(join));
                    }
                    Lifecycle::Stopped | Lifecycle::Failed(_) => {
                        state.lifecycle = Lifecycle::Starting;
                        break (state.registry.remotes.clone(), None);
                    }
                }
            }
        };

        if let Some(stopping) = stopping {
            let _ = stopping.join();
            let mut state = self.state.lock();
            if !state.registry.should_replicate() {
                state.lifecycle = Lifecycle::Stopped;
                self.state_changed.notify_all();
                return Ok(());
            }
        }

        let result = RunningReplication::start(
            self.workspace_id,
            self.keypair.clone(),
            self.tables.clone(),
            self.installations.clone(),
            self.config.clone(),
            started_with.clone(),
        );
        let mut state = self.state.lock();
        match result {
            Ok(runtime) if state.registry.should_replicate() => {
                let desired = state.registry.remotes.clone();
                let sender = runtime.tx.clone();
                state.lifecycle = Lifecycle::Running(runtime);
                self.state_changed.notify_all();
                drop(state);
                sync_peers(&sender, &started_with, &desired);
                Ok(())
            }
            Ok(runtime) => {
                state.lifecycle = Lifecycle::Stopping(thread::spawn(move || runtime.stop()));
                self.state_changed.notify_all();
                Ok(())
            }
            Err(error) => {
                if !matches!(state.lifecycle, Lifecycle::Closed) {
                    state.lifecycle = Lifecycle::Failed(error.to_string());
                }
                self.state_changed.notify_all();
                Err(error)
            }
        }
    }

    pub(super) fn attach_catalog_table(self: &Arc<Self>, name: &str) {
        let Ok(table) = self.tables.get(name) else {
            return;
        };
        table.add_internal_listener(ReplicationListener::build(
            Arc::downgrade(self),
            name.to_owned(),
        ));
    }

    pub(super) fn installation_changed(self: &Arc<Self>, change: &Change) {
        let desired = project_installations(
            &self.installations,
            self.local_installation_id,
            self.keypair.public().to_peer_id(),
        );
        let (previous, sender, should_start) = {
            let mut state = self.state.lock();
            let previous = std::mem::replace(&mut state.registry, desired.clone());
            let sender = match &state.lifecycle {
                Lifecycle::Running(runtime) => Some(runtime.tx.clone()),
                _ => None,
            };
            let should_start = if desired.should_replicate() {
                state.pending_stop = None;
                !matches!(state.lifecycle, Lifecycle::Running(_) | Lifecycle::Starting)
            } else {
                if sender.is_some() {
                    state.pending_stop = Some(if desired.local_is_enrolled {
                        PendingStop::NoRemoteInstallations(change.event.stamp.id)
                    } else {
                        PendingStop::LocalRevoked(change.event.stamp.id)
                    });
                }
                false
            };
            (previous, sender, should_start)
        };

        if let Some(sender) = sender {
            sync_peers(&sender, &previous.remotes, &desired.remotes);
        }
        if should_start {
            let _ = self.start();
        }
    }

    pub(super) fn submit(&self, table: String, event: Event) {
        let sender = match &self.state.lock().lifecycle {
            Lifecycle::Running(runtime) => Some(runtime.tx.clone()),
            _ => None,
        };
        if let Some(sender) = sender {
            let _ = sender.blocking_send(Command::Event { table, event });
        }
    }

    pub(super) fn finish_pending_stop(&self, event_id: EventId) {
        let mut state = self.state.lock();
        let Some(pending) = state.pending_stop else {
            return;
        };
        if pending.event_id() != event_id {
            return;
        }
        if matches!(pending, PendingStop::NoRemoteInstallations(_))
            && !state.registry.remotes.is_empty()
        {
            state.pending_stop = None;
            return;
        }
        state.pending_stop = None;

        let runtime = match std::mem::replace(&mut state.lifecycle, Lifecycle::Stopped) {
            Lifecycle::Running(runtime) => runtime,
            lifecycle => {
                state.lifecycle = lifecycle;
                return;
            }
        };
        state.lifecycle = Lifecycle::Stopping(thread::spawn(move || runtime.stop()));
        self.state_changed.notify_all();
    }

    pub(crate) fn shutdown(&self) {
        let lifecycle = {
            let mut state = self.state.lock();
            while matches!(state.lifecycle, Lifecycle::Starting) {
                self.state_changed.wait(&mut state);
            }
            state.pending_stop = None;
            std::mem::replace(&mut state.lifecycle, Lifecycle::Closed)
        };
        match lifecycle {
            Lifecycle::Running(runtime) => runtime.stop(),
            Lifecycle::Stopping(join) => {
                let _ = join.join();
            }
            Lifecycle::Failed(message) => drop(message),
            _ => {}
        }
        self.state_changed.notify_all();
    }
}

fn project_installations(
    installations: &Installations,
    local_installation_id: InstallationId,
    local_peer_id: PeerId,
) -> RegistryProjection {
    let enrolled = installations.list();
    let mut owners = HashMap::<PeerId, usize>::new();
    for (_, installation) in &enrolled {
        *owners
            .entry(installation.public_key.as_libp2p().to_peer_id())
            .or_default() += 1;
    }

    let local_is_enrolled = enrolled.iter().any(|(installation_id, installation)| {
        *installation_id == local_installation_id
            && installation.role.is_some()
            && installation.public_key.as_libp2p().to_peer_id() == local_peer_id
            && owners.get(&local_peer_id) == Some(&1)
    });
    let remotes = enrolled
        .into_iter()
        .filter_map(|(installation_id, installation)| {
            let peer_id = installation.public_key.as_libp2p().to_peer_id();
            (installation_id != local_installation_id && owners.get(&peer_id) == Some(&1)).then(
                || {
                    (
                        installation_id,
                        PeerRoute {
                            peer_id,
                            addresses: installation
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
    RegistryProjection {
        local_is_enrolled,
        remotes,
    }
}

fn sync_peers(
    sender: &tokio::sync::mpsc::Sender<Command>,
    previous: &BTreeMap<InstallationId, PeerRoute>,
    desired: &BTreeMap<InstallationId, PeerRoute>,
) {
    for (installation_id, old) in previous {
        let replacement = desired.get(installation_id);
        if replacement.is_none_or(|new| new.peer_id != old.peer_id) {
            let _ = sender.blocking_send(Command::RemovePeer {
                installation_id: *installation_id,
                peer_id: old.peer_id,
            });
        }
    }
    for (installation_id, route) in desired {
        if previous.get(installation_id) != Some(route) {
            let _ = sender.blocking_send(Command::UpsertPeer {
                installation_id: *installation_id,
                peer_id: route.peer_id,
                addresses: route.addresses.clone(),
            });
        }
    }
}

impl Drop for ReplicationController {
    fn drop(&mut self) {
        let lifecycle = std::mem::replace(&mut self.state.get_mut().lifecycle, Lifecycle::Closed);
        match lifecycle {
            Lifecycle::Running(runtime) => runtime.stop(),
            Lifecycle::Stopping(join) => {
                let _ = join.join();
            }
            Lifecycle::Failed(message) => drop(message),
            _ => {}
        }
    }
}
