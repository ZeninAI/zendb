//! Shared/local mutation routing and signed journal application.

use std::{io, sync::Arc};

use zendb_types::{
    DeviceId, DeviceKeyPhase, DeviceKeyRing, Event, EventIdentity, Op, Path, PrimaryKey,
    ReplicatedEvent, SignatureBytes, SyncEnvelope, Value, WorkspaceAction,
};

use crate::{DispatchOperator, TableConfig};

use super::{now_ms, Workspace};

const MAX_SHARED_EVENT_BYTES: usize = 8 * 1024 * 1024;

impl<D> Workspace<D>
where
    D: DispatchOperator,
{
    /// Route one application mutation to the local or shared plane according
    /// to the table's durable configuration. Only shared mutations allocate an
    /// EventIdentity and require Contributor.
    pub fn mutate(
        self: &Arc<Self>,
        table_id: &str,
        primary_key: PrimaryKey,
        path: Path,
        op: Op,
    ) -> io::Result<Option<EventIdentity>> {
        let declared_shared = self.is_shared_table(table_id)?;
        let mut config = self.table_config(table_id).unwrap_or_default();
        if declared_shared {
            config.sync = true;
        } else if config.sync {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "table is configured as shared locally but is absent from Workspace control",
            ));
        }
        let hlc = self.device_profile.next_hlc(now_ms())?;
        let table = self.table(table_id, Some(config.clone()))?;
        let shared = config.sync
            && !matches!(op, Op::SetSync { .. })
            && table.get()?.read().is_path_shared(&primary_key, &path);
        let event = Event {
            table_id: table_id.into(),
            primary_key,
            path,
            op,
            hlc,
            sync: shared,
            signature: Vec::new(),
        };
        if !shared {
            if config.sync {
                table.get()?.write().insert_local_overlay_event(event)?;
            } else {
                table.get()?.write().insert_event(event)?;
            }
            return Ok(None);
        }
        self.require_local_action(WorkspaceAction::Contribute)?;
        self.commit_shared_event(event).map(Some)
    }

    pub(crate) fn commit_shared_event(self: &Arc<Self>, event: Event) -> io::Result<EventIdentity> {
        self.commit_shared_event_with_admission(event, None)
    }

    pub(crate) fn commit_shared_event_with_admission(
        self: &Arc<Self>,
        mut event: Event,
        ticket_admission: Option<zendb_types::TicketAdmissionEvidence>,
    ) -> io::Result<EventIdentity> {
        if let Some(evidence) = &ticket_admission {
            self.validate_ticket_admission(&event, evidence)?;
        } else {
            self.validate_local_shared_event(&event)?;
        }
        let _shared_guard = self.shared_mutation.lock();
        event.sync = true;
        let origin_seq = self.device_profile.next_origin_seq();
        let identity = EventIdentity {
            origin_device_id: self.device_id(),
            origin_seq,
        };
        let payload_hash = hash_event(&event)?;
        let signing_bytes = signing_bytes(
            self.workspace_id(),
            identity,
            payload_hash,
            &ticket_admission,
        )?;
        let signature = self.device_profile.sign_primary(&signing_bytes);
        let replicated = ReplicatedEvent {
            envelope: SyncEnvelope {
                workspace_id: self.workspace_id().clone(),
                event_id: identity,
                payload_hash,
                signature: signature.0,
                ticket_admission,
            },
            event,
        };
        self.shared_journal.lock().store(replicated)?;
        self.device_profile.commit_origin_seq(origin_seq)?;
        self.drain_origin(identity.origin_device_id)?;
        Ok(identity)
    }

    pub(crate) fn commit_shared_event_with_staged_key(
        self: &Arc<Self>,
        mut event: Event,
    ) -> io::Result<EventIdentity> {
        let _shared_guard = self.shared_mutation.lock();
        event.sync = true;
        let origin_seq = self.device_profile.next_origin_seq();
        let identity = EventIdentity {
            origin_device_id: self.device_id(),
            origin_seq,
        };
        let payload_hash = hash_event(&event)?;
        let signature = self.device_profile.sign_staged(&signing_bytes(
            self.workspace_id(),
            identity,
            payload_hash,
            &None,
        )?)?;
        self.shared_journal.lock().store(ReplicatedEvent {
            envelope: SyncEnvelope {
                workspace_id: self.workspace_id().clone(),
                event_id: identity,
                payload_hash,
                signature: signature.0,
                ticket_admission: None,
            },
            event,
        })?;
        self.device_profile.commit_origin_seq(origin_seq)?;
        self.drain_origin(identity.origin_device_id)?;
        Ok(identity)
    }

    pub(crate) fn commit_frontier_checkpoint(
        self: &Arc<Self>,
        mut frontier: zendb_types::ContiguousFrontier,
    ) -> io::Result<EventIdentity> {
        let _shared_guard = self.shared_mutation.lock();
        let origin_seq = self.device_profile.next_origin_seq();
        // The checkpoint event is already durable before its payload is
        // applied, so it may truthfully include its own sequence. This avoids
        // an endless one-event checkpoint lag.
        frontier.advance_to(self.device_id(), origin_seq);
        let at = self.device_profile.next_hlc(now_ms())?;
        let mut event = super::control::replace_field_event(
            at,
            self.device_id(),
            "replication_frontier",
            super::control::frontier_value(&frontier),
        );
        event.sync = true;
        let identity = EventIdentity {
            origin_device_id: self.device_id(),
            origin_seq,
        };
        let payload_hash = hash_event(&event)?;
        let signature = self.device_profile.sign_primary(&signing_bytes(
            self.workspace_id(),
            identity,
            payload_hash,
            &None,
        )?);
        self.shared_journal.lock().store(ReplicatedEvent {
            envelope: SyncEnvelope {
                workspace_id: self.workspace_id().clone(),
                event_id: identity,
                payload_hash,
                signature: signature.0,
                ticket_admission: None,
            },
            event,
        })?;
        self.device_profile.commit_origin_seq(origin_seq)?;
        self.drain_origin(identity.origin_device_id)?;
        Ok(identity)
    }

    /// Verify and durably ingest one event received from an authenticated peer.
    /// The event may remain pending until an earlier sequence gap arrives.
    pub fn ingest_shared_event(self: &Arc<Self>, event: ReplicatedEvent) -> io::Result<bool> {
        let _shared_guard = self.shared_mutation.lock();
        self.verify_replicated_event(&event)?;
        self.device_profile.observe_hlc(event.event.hlc, now_ms())?;
        let stored = self.shared_journal.lock().store(event)?;
        // A ticket admission may be waiting on a ticket event from another
        // origin. Retry every admitted origin after each ingest so arrival
        // order cannot permanently strand that dependency.
        let origins: Vec<_> = self
            .devices()?
            .into_iter()
            .map(|(device_id, _)| device_id)
            .collect();
        for origin in origins {
            self.drain_origin(origin)?;
        }
        Ok(stored)
    }

    pub fn shared_frontier(&self) -> zendb_types::ContiguousFrontier {
        self.shared_journal.lock().frontier().clone()
    }

    pub fn shared_range(
        &self,
        origin: DeviceId,
        from_inclusive: u64,
        to_inclusive: u64,
    ) -> Vec<ReplicatedEvent> {
        self.shared_journal
            .lock()
            .range(origin, from_inclusive, to_inclusive)
    }

    pub(crate) fn recover_shared_journal(self: &Arc<Self>) -> io::Result<()> {
        let origins: Vec<DeviceId> = self
            .devices()?
            .into_iter()
            .map(|(device_id, _)| device_id)
            .collect();
        for origin in origins {
            self.drain_origin(origin)?;
        }
        if let Some(local_device) = self.device(self.device_id())? {
            self.device_profile
                .reconcile_key_ring(&local_device.key_ring)?;
        }
        Ok(())
    }

    fn drain_origin(self: &Arc<Self>, origin: DeviceId) -> io::Result<()> {
        loop {
            let Some(next) = self.shared_journal.lock().next_unapplied(origin) else {
                break;
            };
            self.verify_replicated_event(&next)?;
            if let Err(error) = self.apply_shared_event(&next) {
                if error.kind() == io::ErrorKind::WouldBlock {
                    break;
                }
                return Err(error);
            }
            self.shared_journal
                .lock()
                .mark_applied(next.envelope.event_id)?;
        }
        Ok(())
    }

    fn apply_shared_event(self: &Arc<Self>, replicated: &ReplicatedEvent) -> io::Result<()> {
        let event = &replicated.event;
        if event.table_id == "_control" {
            if let Some(evidence) = &replicated.envelope.ticket_admission {
                self.validate_ticket_admission(event, evidence)?;
                self.control.lock().apply_ticket_admission(event)?;
            } else {
                self.control.lock().apply_authorized(event)?;
            }
            if let Some(name) = super::control::shared_table_target(event) {
                let state = self.control.lock().shared_table_state(name)?;
                if state.is_some_and(|(live, _)| live) {
                    if self.table_config(name).is_some_and(|config| !config.sync) {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "replicated shared table conflicts with a local-only table",
                        ));
                    }
                    let mut config = self.table_config(name).unwrap_or_default();
                    config.sync = true;
                    self.table(name, Some(config))?;
                } else {
                    self.close_table(name);
                }
            }
            return Ok(());
        }
        let author = self.device(event.hlc.device_id())?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "author Device is not admitted",
            )
        })?;
        if !author.allows(WorkspaceAction::Contribute) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "author Device is not a Contributor",
            ));
        }
        match self.control.lock().shared_table_state(&event.table_id)? {
            Some((true, created_at)) if event.hlc >= created_at => {}
            Some((false, deleted_at)) if event.hlc <= deleted_at => return Ok(()),
            Some((true, _)) => return Ok(()),
            Some((false, _)) => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "shared table is tombstoned and must be recreated before writing",
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "shared data is waiting for its table creation event",
                ));
            }
        }
        if self
            .table_config(&event.table_id)
            .is_some_and(|config| !config.sync)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared event conflicts with a local-only table of the same name",
            ));
        }
        let table = if self.contains_table(&event.table_id) {
            self.table(&event.table_id, None)?
        } else {
            self.table(
                &event.table_id,
                Some(TableConfig {
                    sync: true,
                    ..TableConfig::default()
                }),
            )?
        };
        table.get()?.write().insert_shared_event(event.clone())
    }

    fn verify_replicated_event(&self, replicated: &ReplicatedEvent) -> io::Result<()> {
        if replicated.envelope.workspace_id != *self.workspace_id()
            || !replicated.envelope.matches_event(&replicated.event)
            || !replicated.event.sync
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared event scope is invalid",
            ));
        }
        let payload_hash = hash_event(&replicated.event)?;
        if payload_hash != replicated.envelope.payload_hash {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared event payload hash mismatch",
            ));
        }
        let origin = replicated.envelope.event_id.origin_device_id;
        let device = self.device(origin)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "event origin Device is not admitted",
            )
        })?;
        self.validate_key_ring_transition(
            &device.key_ring,
            &replicated.event,
            replicated.envelope.event_id,
        )?;
        let key = verification_key_for_event(
            &device.key_ring,
            &replicated.event,
            replicated.envelope.event_id,
        )
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "no valid key for shared event",
            )
        })?;
        let bytes = signing_bytes(
            self.workspace_id(),
            replicated.envelope.event_id,
            payload_hash,
            &replicated.envelope.ticket_admission,
        )?;
        if !zendb_transport::DeviceProfile::verify(
            key,
            &bytes,
            &SignatureBytes(replicated.envelope.signature.clone()),
        ) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid shared event signature",
            ));
        }
        Ok(())
    }

    fn require_local_action(&self, action: WorkspaceAction) -> io::Result<()> {
        let device = self.device(self.device_id())?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local Device is not admitted",
            )
        })?;
        if device.allows(action) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local Device role denied the action",
            ))
        }
    }

    fn validate_local_shared_event(&self, event: &Event) -> io::Result<()> {
        if event.table_id == "_control" {
            return self.control.lock().validate_authorized(event);
        }
        self.require_local_action(WorkspaceAction::Contribute)?;
        if self
            .control
            .lock()
            .shared_table_state(&event.table_id)?
            .is_some_and(|(live, _)| live)
        {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "shared table is not live in Workspace control state",
            ))
        }
    }

    fn validate_key_ring_transition(
        &self,
        current: &DeviceKeyRing,
        event: &Event,
        identity: EventIdentity,
    ) -> io::Result<()> {
        if !is_key_ring_event(event, identity.origin_device_id) {
            return Ok(());
        }
        let Op::Replace {
            value: Value::Record(record),
        } = &event.op
        else {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "key ring must be atomically replaced as a Record",
            ));
        };
        let proposed = DeviceKeyRing::from_cell(&zendb_types::Cell {
            value: Some(Value::Record(record.clone())),
            hlc: event.hlc,
            sync: None,
        })
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let staging = current.phase == DeviceKeyPhase::Stable
            && proposed.phase == DeviceKeyPhase::Staged
            && proposed.primary_key == current.primary_key
            && proposed.primary_from_seq == current.primary_from_seq
            && proposed.secondary_key.is_some()
            && proposed.secondary_key != Some(current.primary_key);
        if staging {
            if current.secondary_key.is_some()
                && self
                    .stable_frontier()?
                    .applied_through(&identity.origin_device_id)
                    < current.primary_from_seq
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "previous promotion is not stable enough to replace historic key",
                ));
            }
            return Ok(());
        }
        let promotion = current.phase == DeviceKeyPhase::Staged
            && proposed.phase == DeviceKeyPhase::Stable
            && proposed.primary_key
                == current.secondary_key.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "staged key ring has no candidate",
                    )
                })?
            && proposed.secondary_key == Some(current.primary_key)
            && proposed.primary_from_seq == identity.origin_seq;
        if promotion {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Device key ring replacement is not a valid stage or promotion transition",
        ))
    }

    fn validate_ticket_admission(
        &self,
        event: &Event,
        evidence: &zendb_types::TicketAdmissionEvidence,
    ) -> io::Result<()> {
        let ticket = self
            .control
            .lock()
            .ticket(&evidence.ticket_id)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "ticket admission is waiting for its replicated ticket",
                )
            })?;
        if event.hlc.physical_ms() > ticket.expires_at.physical_ms() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "enrollment ticket expired before admission",
            ));
        }
        let bytes = zendb_transport::ticket_admission_signing_bytes(
            self.workspace_id(),
            &evidence.ticket_id,
            evidence.candidate_device_id,
            evidence.candidate_public_key,
            &evidence.requested_name,
            &evidence.capabilities,
        )?;
        if !zendb_transport::DeviceProfile::verify(
            ticket.verifier_key,
            &bytes,
            &evidence.ticket_signature,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "enrollment ticket signature is invalid",
            ));
        }
        if super::control::device_target(&event.path) != Some(evidence.candidate_device_id)
            || event.path.len() != 2
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "ticket evidence does not match the admission target",
            ));
        }
        let Op::Merge { cell } = &event.op else {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "ticket admission must merge a Device record",
            ));
        };
        let admitted = zendb_types::DeviceRecord::from_cell(cell)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if admitted.name != evidence.requested_name
            || admitted.key_ring.primary_key != evidence.candidate_public_key
            || admitted.key_ring.secondary_key.is_some()
            || admitted.roles.len() != 0
            || admitted.capabilities != evidence.capabilities
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "ticket evidence does not match the admitted Device record",
            ));
        }
        Ok(())
    }
}

fn hash_event(event: &Event) -> io::Result<[u8; 32]> {
    let bytes = bincode::encode_to_vec(event, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))?;
    if bytes.len() > MAX_SHARED_EVENT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shared event exceeds the 8 MiB protocol limit",
        ));
    }
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn signing_bytes(
    workspace_id: &zendb_types::WorkspaceId,
    identity: EventIdentity,
    payload_hash: [u8; 32],
    ticket_admission: &Option<zendb_types::TicketAdmissionEvidence>,
) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(
        (workspace_id, identity, payload_hash, ticket_admission),
        bincode::config::standard(),
    )
    .map_err(|error| io::Error::other(error.to_string()))
}

fn verification_key_for_event(
    current: &DeviceKeyRing,
    event: &Event,
    identity: EventIdentity,
) -> Option<zendb_types::DevicePublicKey> {
    if is_key_ring_event(event, identity.origin_device_id) {
        if let Op::Replace {
            value: Value::Record(record),
        } = &event.op
        {
            let proposed = DeviceKeyRing::from_cell(&zendb_types::Cell {
                value: Some(Value::Record(record.clone())),
                hlc: event.hlc,
                sync: None,
            })
            .ok()?;
            if current.phase == DeviceKeyPhase::Staged
                && proposed.phase == DeviceKeyPhase::Stable
                && proposed.primary_key == current.secondary_key?
                && proposed.secondary_key == Some(current.primary_key)
                && proposed.primary_from_seq == identity.origin_seq
            {
                return current.secondary_key;
            }
        }
    }
    current.verification_key(identity.origin_seq).copied()
}

fn is_key_ring_event(event: &Event, origin: DeviceId) -> bool {
    if event.table_id != "_control" || event.path.len() != 3 {
        return false;
    }
    let names: Vec<&str> = event
        .path
        .iter()
        .filter_map(|step| match &step.segment {
            zendb_types::Segment::Record(name) => Some(name.as_str()),
            _ => None,
        })
        .collect();
    let origin = origin.to_string();
    names.len() == 3 && names[0] == "devices" && names[1] == origin && names[2] == "key_ring"
}
