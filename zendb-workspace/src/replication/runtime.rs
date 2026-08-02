//! Thread ownership and Tokio bootstrap for one running replication worker.

use std::{
    collections::BTreeMap,
    sync::{Weak, mpsc as std_mpsc},
    thread,
};

use libp2p::PeerId;
use libp2p_identity::Keypair;
use tokio::sync::mpsc;
use zendb_types::{Envelope, InstallationId, WorkspaceId};

use super::{
    ReplicationConfig,
    command::{Command, PeerRoute},
    swarm::build_swarm,
    worker::{WorkerContext, run_worker},
};
use crate::{Error, Result, admission, core::WorkspaceCore};

pub(super) struct ReplicationRuntime {
    pub(super) commands: mpsc::Sender<Command>,
    join: Option<thread::JoinHandle<()>>,
    admission_join: Option<thread::JoinHandle<()>>,
}

impl ReplicationRuntime {
    pub(super) fn start(
        workspace_id: WorkspaceId,
        local_installation_id: InstallationId,
        keypair: Keypair,
        core: Weak<WorkspaceCore>,
        config: ReplicationConfig,
        initial_routes: BTreeMap<InstallationId, PeerRoute>,
    ) -> Result<Self> {
        if config.channel_capacity == 0 {
            return Err(Error::Replication(
                "channel_capacity must be greater than zero".to_owned(),
            ));
        }
        let (inbound_tx, inbound_rx) =
            std_mpsc::sync_channel::<(Envelope, PeerId)>(config.channel_capacity);
        // Storage-backed admission runs on its own blocking thread. The bounded
        // channel keeps workspace I/O off the async loop while applying backpressure.
        let admission_join = thread::Builder::new()
            .name(format!("zendb-admission-{workspace_id}"))
            .spawn(move || {
                while let Ok((envelope, source)) = inbound_rx.recv() {
                    let Some(core) = core.upgrade() else {
                        break;
                    };
                    let _ = admission::admit_event(&core, envelope, source);
                }
            })?;
        let (commands, command_rx) = mpsc::channel(config.channel_capacity);
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(0);
        // The zero-capacity handshake makes start() return only after the
        // runtime and swarm have either initialized or reported an error.
        let join = thread::Builder::new()
            .name(format!("zendb-replication-{workspace_id}"))
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    match build_swarm(workspace_id, keypair, &config) {
                        Ok((swarm, topic)) => {
                            let _ = ready_tx.send(Ok(()));
                            run_worker(
                                swarm,
                                command_rx,
                                WorkerContext {
                                    topic,
                                    local_installation_id,
                                    batch: config.batch,
                                    topology: config.topology,
                                    dial: config.dial,
                                    initial_routes,
                                    inbound: inbound_tx,
                                },
                            )
                            .await;
                        }
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                        }
                    }
                });
            })?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                commands,
                join: Some(join),
                admission_join: Some(admission_join),
            }),
            Ok(Err(error)) => {
                let _ = join.join();
                let _ = admission_join.join();
                Err(Error::Replication(error))
            }
            Err(error) => {
                let _ = join.join();
                let _ = admission_join.join();
                Err(Error::Replication(error.to_string()))
            }
        }
    }

    pub(super) fn stop(mut self) {
        // Worker shutdown drains pending batches before its join completes;
        // only then is it safe to stop the admission thread.
        let _ = self.commands.blocking_send(Command::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        if let Some(admission_join) = self.admission_join.take() {
            let _ = admission_join.join();
        }
    }
}
