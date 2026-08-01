//! Thread ownership and Tokio bootstrap for one running replication worker.

use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc as std_mpsc},
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
use crate::{Error, Result, admission, installations::Installations, tables::Tables};

pub(super) struct RunningReplication {
    pub(super) tx: mpsc::Sender<Command>,
    join: Option<thread::JoinHandle<()>>,
    admission_join: Option<thread::JoinHandle<()>>,
}

impl RunningReplication {
    pub(super) fn start(
        workspace_id: WorkspaceId,
        keypair: Keypair,
        tables: Arc<Tables>,
        installations: Arc<Installations>,
        config: ReplicationConfig,
        remote_installations: BTreeMap<InstallationId, PeerRoute>,
    ) -> Result<Self> {
        if config.outbound_capacity == 0 {
            return Err(Error::Replication(
                "outbound_capacity must be greater than zero".to_owned(),
            ));
        }
        let local_installation_id = installations.local_installation_id();
        let (inbound_tx, inbound_rx) =
            std_mpsc::sync_channel::<(Envelope, PeerId)>(config.outbound_capacity);
        let admission_join = thread::Builder::new()
            .name(format!("zendb-admission-{workspace_id}"))
            .spawn(move || {
                while let Ok((envelope, source)) = inbound_rx.recv() {
                    let _ = admission::admit_event(&tables, &installations, envelope, source);
                }
            })?;
        let (tx, rx) = mpsc::channel(config.outbound_capacity);
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(0);
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
                                rx,
                                WorkerContext {
                                    topic,
                                    local_installation_id,
                                    batch_config: config.batch,
                                    topology_config: config.topology,
                                    dial_config: config.dial,
                                    remote_installations,
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
                tx,
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
        let _ = self.tx.blocking_send(Command::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        if let Some(admission_join) = self.admission_join.take() {
            let _ = admission_join.join();
        }
    }
}
