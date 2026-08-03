//! Replication lifecycle and projection of workspace installations.

use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
    thread,
};

use libp2p::PeerId;
use libp2p_identity::Keypair;
use parking_lot::{Condvar, Mutex};
use zendb_types::{Event, EventId, Installation, InstallationId, Permissions, WorkspaceId};

use super::{
    ReplicationConfig,
    command::{Command, PeerRoute},
    runtime::ReplicationRuntime,
};
use crate::{Result, core::WorkspaceCore};

#[derive(Clone)]
struct ReplicationPeers {
    local_permissions: Option<Permissions>,
    remote_routes: BTreeMap<InstallationId, PeerRoute>,
}

impl ReplicationPeers {
    fn should_run(&self) -> bool {
        self.local_permissions.is_some() && !self.remote_routes.is_empty()
    }
}

#[derive(Clone, Copy)]
enum PendingStop {
    NoRemoteInstallations(EventId),
    LocalInactive(EventId),
}

impl PendingStop {
    const fn event_id(self) -> EventId {
        match self {
            Self::NoRemoteInstallations(event_id) | Self::LocalInactive(event_id) => event_id,
        }
    }
}

enum Lifecycle {
    Stopped,
    Starting,
    Running(ReplicationRuntime),
    Stopping(thread::JoinHandle<()>),
    Failed,
    Closed,
}

struct ControllerState {
    lifecycle: Lifecycle,
    peers: ReplicationPeers,
    pending_stop: Option<PendingStop>,
}

pub(crate) struct ReplicationController {
    workspace_id: WorkspaceId,
    keypair: Keypair,
    core: Weak<WorkspaceCore>,
    config: ReplicationConfig,
    state: Mutex<ControllerState>,
    state_changed: Condvar,
}

impl ReplicationController {
    pub(crate) fn new(
        workspace_id: WorkspaceId,
        keypair: Keypair,
        core: Weak<WorkspaceCore>,
        config: ReplicationConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            workspace_id,
            keypair,
            core,
            config,
            state: Mutex::new(ControllerState {
                lifecycle: Lifecycle::Stopped,
                peers: ReplicationPeers {
                    local_permissions: None,
                    remote_routes: BTreeMap::new(),
                },
                pending_stop: None,
            }),
            state_changed: Condvar::new(),
        })
    }

    pub(crate) fn start_if_needed(self: &Arc<Self>) -> Result<()> {
        let Some(core) = self.core.upgrade() else {
            return Ok(());
        };
        // Membership is read before taking the lifecycle lock. The resulting
        // peer set is then installed atomically with the start decision.
        let local_installation_id = core.membership.local_installation_id();
        let peers = derive_replication_peers(
            &core.membership.list(),
            local_installation_id,
            self.keypair.public().to_peer_id(),
        );
        drop(core);
        let local_permissions = peers.local_permissions;
        let (initial_routes, stopping) = {
            let mut state = self.state.lock();
            state.peers = peers;
            // Wait for a concurrent transition, but join a stopping worker
            // outside this mutex so shutdown can finish without blocking state
            // updates from another thread.
            loop {
                if !state.peers.should_run() {
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
                        break (state.peers.remote_routes.clone(), Some(join));
                    }
                    Lifecycle::Stopped | Lifecycle::Failed => {
                        state.lifecycle = Lifecycle::Starting;
                        break (state.peers.remote_routes.clone(), None);
                    }
                }
            }
        };
        let local_permissions = local_permissions
            .expect("a running replication projection has an active local installation");

        if let Some(stopping) = stopping {
            let _ = stopping.join();
            let mut state = self.state.lock();
            // Membership may have changed while the old worker stopped. Avoid
            // starting a runtime for a peer set that no longer needs one.
            if !state.peers.should_run() {
                state.lifecycle = Lifecycle::Stopped;
                self.state_changed.notify_all();
                return Ok(());
            }
        }

        let result = ReplicationRuntime::start(
            self.workspace_id,
            local_installation_id,
            self.keypair.clone(),
            self.core.clone(),
            self.config.clone(),
            local_permissions,
            initial_routes.clone(),
        );
        let mut state = self.state.lock();
        match result {
            Ok(runtime) if state.peers.should_run() => {
                let desired_routes = state.peers.remote_routes.clone();
                let desired_permissions = state
                    .peers
                    .local_permissions
                    .expect("a running replication projection has an active local installation");
                let commands = runtime.commands.clone();
                state.lifecycle = Lifecycle::Running(runtime);
                self.state_changed.notify_all();
                drop(state);
                if desired_permissions != local_permissions {
                    let _ =
                        commands.blocking_send(Command::SetLocalPermissions(desired_permissions));
                }
                reconcile_peer_routes(&commands, &initial_routes, &desired_routes);
                Ok(())
            }
            Ok(runtime) => {
                // The desired peer set changed while startup was in progress;
                // stop this runtime immediately instead of exposing it as live.
                state.lifecycle = Lifecycle::Stopping(thread::spawn(move || runtime.stop()));
                self.state_changed.notify_all();
                Ok(())
            }
            Err(error) => {
                if !matches!(state.lifecycle, Lifecycle::Closed) {
                    state.lifecycle = Lifecycle::Failed;
                }
                self.state_changed.notify_all();
                Err(error)
            }
        }
    }

    pub(crate) fn reconcile_installations(self: &Arc<Self>, event_id: EventId) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        let local_installation_id = core.membership.local_installation_id();
        let desired_peers = derive_replication_peers(
            &core.membership.list(),
            local_installation_id,
            self.keypair.public().to_peer_id(),
        );
        drop(core);
        let (previous_peers, commands, needs_start) = {
            let mut state = self.state.lock();
            // The event ID ties a pending stop to the installation change that
            // caused it, allowing core to publish that event before stopping.
            let previous_peers = std::mem::replace(&mut state.peers, desired_peers.clone());
            let commands = match &state.lifecycle {
                Lifecycle::Running(runtime) => Some(runtime.commands.clone()),
                _ => None,
            };
            let needs_start = if desired_peers.should_run() {
                state.pending_stop = None;
                !matches!(state.lifecycle, Lifecycle::Running(_) | Lifecycle::Starting)
            } else {
                if commands.is_some() {
                    state.pending_stop = Some(if desired_peers.local_permissions.is_some() {
                        PendingStop::NoRemoteInstallations(event_id)
                    } else {
                        PendingStop::LocalInactive(event_id)
                    });
                }
                false
            };
            (previous_peers, commands, needs_start)
        };

        if let Some(commands) = commands {
            if previous_peers.local_permissions != desired_peers.local_permissions
                && let Some(permissions) = desired_peers.local_permissions
            {
                let _ = commands.blocking_send(Command::SetLocalPermissions(permissions));
            }
            reconcile_peer_routes(
                &commands,
                &previous_peers.remote_routes,
                &desired_peers.remote_routes,
            );
        }
        if needs_start {
            let _ = self.start_if_needed();
        }
    }

    pub(crate) fn submit_event(&self, table: String, event: Event) {
        let commands = match &self.state.lock().lifecycle {
            Lifecycle::Running(runtime) => Some(runtime.commands.clone()),
            _ => None,
        };
        // Replication is downstream of the durable local commit; a stopped or
        // closed controller may therefore discard this best-effort submission.
        if let Some(commands) = commands {
            let _ = commands.blocking_send(Command::PublishEvent { table, event });
        }
    }

    pub(crate) fn complete_pending_stop(&self, event_id: EventId) {
        let mut state = self.state.lock();
        let Some(pending) = state.pending_stop else {
            return;
        };
        if pending.event_id() != event_id {
            return;
        }
        if matches!(pending, PendingStop::NoRemoteInstallations(_))
            && !state.peers.remote_routes.is_empty()
        {
            state.pending_stop = None;
            return;
        }
        state.pending_stop = None;

        // This is called after submit_event in the core commit pipeline. Stop
        // only once the triggering installation event has entered replication.
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
            Lifecycle::Failed => {}
            _ => {}
        }
        self.state_changed.notify_all();
    }
}

fn derive_replication_peers(
    installations: &[(InstallationId, Installation)],
    local_installation_id: InstallationId,
    local_peer_id: PeerId,
) -> ReplicationPeers {
    // The local installation must still be enrolled to publish. Remote routes
    // are derived from every other registry entry, including entries without
    // currently usable addresses for later discovery.
    let local_permissions = installations
        .iter()
        .find(|(installation_id, installation)| {
            *installation_id == local_installation_id
                && installation.public_key.as_libp2p().to_peer_id() == local_peer_id
        })
        .and_then(|(_, installation)| installation.state.permissions());
    let remote_routes = installations
        .iter()
        .filter_map(|(installation_id, installation)| {
            let permissions = installation.state.permissions()?;
            let peer_id = installation.public_key.as_libp2p().to_peer_id();
            (*installation_id != local_installation_id).then(|| {
                (
                    *installation_id,
                    PeerRoute {
                        peer_id,
                        addresses: installation
                            .addresses
                            .iter()
                            .cloned()
                            .map(|address| address.into_libp2p())
                            .collect(),
                        permissions,
                    },
                )
            })
        })
        .collect();
    ReplicationPeers {
        local_permissions,
        remote_routes,
    }
}

fn reconcile_peer_routes(
    sender: &tokio::sync::mpsc::Sender<Command>,
    previous: &BTreeMap<InstallationId, PeerRoute>,
    desired: &BTreeMap<InstallationId, PeerRoute>,
) {
    // Remove changed peer IDs before adding replacements so an installation
    // whose key changed cannot retain the old transport identity.
    for (installation_id, old) in previous {
        let replacement = desired.get(installation_id);
        if replacement.is_none_or(|new| new.peer_id != old.peer_id) {
            let _ = sender.blocking_send(Command::RemovePeer {
                peer_id: old.peer_id,
            });
        }
    }
    for (installation_id, route) in desired {
        if previous.get(installation_id) != Some(route) {
            let _ = sender.blocking_send(Command::UpsertPeer {
                peer_id: route.peer_id,
                addresses: route.addresses.clone(),
                permissions: route.permissions,
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
            Lifecycle::Failed => {}
            _ => {}
        }
    }
}
