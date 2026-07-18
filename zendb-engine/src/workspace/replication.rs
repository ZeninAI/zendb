//! Shared/local mutation routing and signed journal application.

use std::{fs, io, io::Write, sync::Arc};

use zendb_types::{
    DeviceId, DeviceKeyPhase, DeviceKeyRing, Event, EventIdentity, Op, Path, PrimaryKey,
    ReplicatedEvent, SignatureBytes, SyncEnvelope, SyncPolicy, Value, WorkspaceAction,
};

use super::{now_ms, Workspace};

const MAX_SHARED_EVENT_BYTES: usize = 8 * 1024 * 1024;

impl Workspace {
    /// Change one row/path synchronization boundary on this replica.
    ///
    /// Localizing a path only persists policy metadata. Re-inheriting a path
    /// publishes its current shared projection as a CRDT merge with the
    /// projection's existing clocks; the policy toggle itself is never sent.
    pub fn set_path_sync_policy(
        self: &Arc<Self>,
        table_id: &str,
        primary_key: PrimaryKey,
        path: Path,
        policy: SyncPolicy,
    ) -> io::Result<bool> {
        let table = self.table(table_id).open()?;
        if policy == SyncPolicy::Inherit {
            self.require_local_action(WorkspaceAction::Contribute)?;
        }
        let projected = {
            let table = table.get()?;
            let mut table = table.write();
            if !table.set_sync_policy(&primary_key, path.clone(), policy)? {
                return Ok(false);
            }
            if policy == SyncPolicy::Local {
                return Ok(true);
            }
            table.shared_cell(&primary_key, &path)
        };

        // An Inherit cell below a Local ancestor remains effectively local.
        let Some(cell) = projected else {
            return Ok(true);
        };
        self.mark_state_reconciliation_required()?;
        let event = Event {
            table_id: table_id.into(),
            primary_key,
            path,
            op: Op::Merge { cell },
            hlc: self.device_profile.next_hlc(now_ms())?,
        };
        self.commit_shared_event(event)?;
        Ok(true)
    }

    pub(crate) fn state_reconciliation_required(&self) -> bool {
        self.state_reconciliation_required
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn mark_state_reconciliation_required(&self) -> io::Result<()> {
        let path = self.path.join(super::RECONCILIATION_FILE);
        let mut file = fs::File::create(path)?;
        file.write_all(b"required")?;
        file.sync_all()?;
        self.state_reconciliation_required
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub(crate) fn clear_state_reconciliation_required(&self) -> io::Result<()> {
        let path = self.path.join(super::RECONCILIATION_FILE);
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        self.state_reconciliation_required
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

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
        let config = self.table_config(table_id).unwrap_or_default();
        let hlc = self.device_profile.next_hlc(now_ms())?;
        let table = self.table_impl(table_id, Some(config.clone()))?;
        let shared = declared_shared && table.get()?.read().is_path_shared(&primary_key, &path);
        let event = Event {
            table_id: table_id.into(),
            primary_key,
            path,
            op,
            hlc,
        };
        if !shared {
            table.get()?.write().insert_event(event)?;
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
        event: Event,
        ticket_admission: Option<zendb_types::TicketAdmissionEvidence>,
    ) -> io::Result<EventIdentity> {
        if let Some(evidence) = &ticket_admission {
            self.validate_ticket_admission(&event, evidence)?;
        } else {
            self.validate_local_shared_event(&event)?;
        }
        let _shared_guard = self.shared_mutation.lock();
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
        event: Event,
    ) -> io::Result<EventIdentity> {
        let _shared_guard = self.shared_mutation.lock();
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
        let event = super::system::replace_field_event(
            at,
            self.device_id(),
            "replication_frontier",
            super::system::frontier_value(&frontier),
        );
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
        if super::system::is_system_event(event) {
            if let Some(evidence) = &replicated.envelope.ticket_admission {
                self.validate_ticket_admission(event, evidence)?;
                self.control.lock().apply_ticket_admission(event)?;
            } else {
                self.control.lock().apply_authorized(event)?;
            }
            if let Some(name) = super::system::shared_table_target(event) {
                let state = self.control.lock().shared_table_state(name)?;
                match state {
                    Some((true, _)) => {
                        self.table_impl(name, None)?;
                    }
                    Some((false, _)) => {
                        self.close_table(name);
                    }
                    // A local catalog boundary deliberately hides the remote
                    // lifecycle event on this replica.
                    None => {}
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
        let table = self.table_impl(&event.table_id, None)?;
        table.get()?.write().insert_shared_event(event.clone())
    }

    fn verify_replicated_event(&self, replicated: &ReplicatedEvent) -> io::Result<()> {
        if replicated.envelope.workspace_id != *self.workspace_id()
            || !replicated.envelope.matches_event(&replicated.event)
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
        if super::system::is_system_event(event) {
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
            sync: SyncPolicy::Inherit,
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
        if super::system::device_target(event) != Some(evidence.candidate_device_id)
            || !event.path.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "ticket evidence does not match the admission target",
            ));
        }
        if self.device(evidence.candidate_device_id)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "ticket admission cannot replace a live Device",
            ));
        }
        let Op::Replace { value } = &event.op else {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "ticket admission must replace a Device row",
            ));
        };
        let admitted = zendb_types::DeviceRecord::from_cell(&zendb_types::Cell {
            value: Some(value.clone()),
            hlc: event.hlc,
            sync: SyncPolicy::Inherit,
        })
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
                sync: SyncPolicy::Inherit,
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
    if event.table_id != super::system::DEVICES_TABLE
        || event.primary_key != PrimaryKey::Blob(origin.0.to_vec().into())
        || event.path.len() != 1
    {
        return false;
    }
    matches!(
        &event.path[0].segment,
        zendb_types::Segment::Record(name) if name == "key_ring"
    )
}
