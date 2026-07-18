//! System tables and device authorization.

use std::{io, path::Path as FsPath};

use bincode::{Decode, Encode};
use zendb_replication::{Table, TableConfig};
use zendb_storage::backend::_traits::{DurableStorage, ReadBackend};
use zendb_types::crdt::values::{Blob, OrSetOp, Record, SetOp};
use zendb_types::{
    CapabilityId, Cell, ContiguousFrontier, DeviceId, DeviceKeyRing, DeviceRecord,
    EnrollmentTicket, EnrollmentTicketId, Event, Hlc, Op, Path, PathStep, PrimaryKey, Segment,
    SyncPolicy, SyncScope, TypeOp, TypeTag, Value, WorkspaceAction, WorkspaceRole,
};

pub(crate) const CATALOG_TABLE: &str = "_catalog";
pub(crate) const DEVICES_TABLE: &str = "_devices";
pub(crate) const TICKETS_TABLE: &str = "_enrollment_tickets";
const CONFIG_FIELD: &str = "config";

#[derive(Debug, Clone, Encode, Decode)]
pub(super) struct SystemSnapshot {
    pub(super) catalog: Vec<(PrimaryKey, Cell)>,
    pub(super) devices: Vec<(PrimaryKey, Cell)>,
    pub(super) tickets: Vec<(PrimaryKey, Cell)>,
}

impl SystemSnapshot {
    pub(super) fn device(&self, device_id: DeviceId) -> io::Result<Option<DeviceRecord>> {
        parse_device(row(&self.devices, &device_key(device_id)))
    }

    pub(super) fn ticket(
        &self,
        ticket_id: &EnrollmentTicketId,
    ) -> io::Result<Option<EnrollmentTicket>> {
        parse_ticket(row(&self.tickets, &ticket_key(ticket_id)))
    }
}

/// Concrete owner of Workspace system tables. It is intentionally not a
/// trait: every Workspace has exactly one catalog, device table, and ticket
/// table.
pub(crate) struct WorkspaceControl {
    catalog: Table,
    devices: Table,
    tickets: Table,
}

impl WorkspaceControl {
    pub(crate) fn create(
        tables_path: &FsPath,
        local_device_id: DeviceId,
        initial_device: DeviceRecord,
        initial_hlc: Hlc,
    ) -> io::Result<Self> {
        let system_config = system_table_config();
        let mut control = Self {
            catalog: Table::create_with_policy(
                &tables_path.join(CATALOG_TABLE),
                system_config.clone(),
                SyncPolicy::Inherit,
            )?,
            devices: Table::create_with_policy(
                &tables_path.join(DEVICES_TABLE),
                system_config.clone(),
                SyncPolicy::Inherit,
            )?,
            tickets: Table::create_with_policy(
                &tables_path.join(TICKETS_TABLE),
                system_config.clone(),
                SyncPolicy::Inherit,
            )?,
        };

        for name in [CATALOG_TABLE, DEVICES_TABLE, TICKETS_TABLE] {
            control.catalog.insert_shared_event(catalog_event(
                initial_hlc,
                name,
                Some(system_config.clone()),
            )?)?;
        }
        control.devices.insert_shared_event(admit_event(
            initial_hlc,
            local_device_id,
            initial_device,
        ))?;
        control.sync()?;
        Ok(control)
    }

    pub(crate) fn open(tables_path: &FsPath) -> io::Result<Self> {
        let config = system_table_config();
        Ok(Self {
            catalog: Table::open_with_policy(
                &tables_path.join(CATALOG_TABLE),
                config.clone(),
                SyncPolicy::Inherit,
            )?,
            devices: Table::open_with_policy(
                &tables_path.join(DEVICES_TABLE),
                config.clone(),
                SyncPolicy::Inherit,
            )?,
            tickets: Table::open_with_policy(
                &tables_path.join(TICKETS_TABLE),
                config,
                SyncPolicy::Inherit,
            )?,
        })
    }

    pub(crate) fn snapshot(&self) -> SystemSnapshot {
        SystemSnapshot {
            catalog: self.catalog.shared_rows(),
            devices: self.devices.shared_rows(),
            tickets: self.tickets.shared_rows(),
        }
    }

    pub(crate) fn install_snapshot(&mut self, snapshot: SystemSnapshot) -> io::Result<()> {
        self.catalog.install_shared_rows(snapshot.catalog)?;
        self.devices.install_shared_rows(snapshot.devices)?;
        self.tickets.install_shared_rows(snapshot.tickets)
    }

    pub(crate) fn compact_through(&mut self, watermark: Hlc) -> io::Result<bool> {
        let mut changed = false;
        changed |= self.catalog.compact_shared_through(watermark)? > 0;
        changed |= self.devices.compact_shared_through(watermark)? > 0;
        changed |= self.tickets.compact_shared_through(watermark)? > 0;
        Ok(changed)
    }

    pub(crate) fn table_config(&self, name: &str) -> io::Result<Option<TableConfig>> {
        let Some(cell) = ReadBackend::get(&self.catalog, &catalog_key(name)) else {
            return Ok(None);
        };
        if cell.is_tombstone() {
            return Ok(None);
        }
        decode_table_config(&cell).map(Some)
    }

    pub(crate) fn catalog_names(&self) -> Vec<String> {
        ReadBackend::entries(&self.catalog)
            .filter_map(|(key, cell)| {
                (!cell.is_tombstone())
                    .then(|| primary_string(&key).map(str::to_owned))
                    .flatten()
            })
            .collect()
    }

    pub(crate) fn put_local_table(
        &mut self,
        name: &str,
        config: TableConfig,
        at: Hlc,
    ) -> io::Result<()> {
        self.catalog
            .insert_local_root_event(catalog_event(at, name, Some(config))?)
    }

    pub(crate) fn set_catalog_policy(
        &mut self,
        name: &str,
        policy: SyncPolicy,
    ) -> io::Result<bool> {
        self.catalog
            .set_sync_policy(&catalog_key(name), Path::new(), policy)
    }

    pub(crate) fn catalog_cell(&self, name: &str) -> Option<Cell> {
        ReadBackend::get(&self.catalog, &catalog_key(name)).map(|cell| cell.into_owned())
    }

    pub(crate) fn delete_local_table(&mut self, name: &str, at: Hlc) -> io::Result<bool> {
        if self.table_config(name)?.is_none() {
            return Ok(false);
        }
        self.catalog
            .insert_local_root_event(catalog_event(at, name, None)?)?;
        Ok(true)
    }

    pub(crate) fn device(&self, device_id: DeviceId) -> io::Result<Option<DeviceRecord>> {
        parse_device(ReadBackend::get(&self.devices, &device_key(device_id)).as_deref())
    }

    pub(crate) fn devices(&self) -> io::Result<Vec<(DeviceId, DeviceRecord)>> {
        ReadBackend::entries(&self.devices)
            .filter_map(|(key, cell)| {
                if cell.is_tombstone() {
                    return None;
                }
                let id = primary_entity_id(&key)
                    .map(DeviceId::from_bytes)
                    .ok_or_else(|| invalid_error("Device table key is not a 128-bit Blob"));
                Some(id.and_then(|id| {
                    DeviceRecord::from_cell(&cell)
                        .map(|device| (id, device))
                        .map_err(|error| invalid_error(error.to_string()))
                }))
            })
            .collect()
    }

    pub(crate) fn ticket(
        &self,
        ticket_id: &EnrollmentTicketId,
    ) -> io::Result<Option<EnrollmentTicket>> {
        parse_ticket(ReadBackend::get(&self.tickets, &ticket_key(ticket_id)).as_deref())
    }

    pub(crate) fn shared_table_state(&self, name: &str) -> io::Result<Option<(bool, Hlc)>> {
        let Some(cell) = ReadBackend::get(&self.catalog, &catalog_key(name)) else {
            return Ok(None);
        };
        if !cell.sync.resolve(SyncScope::Shared).is_shared() {
            return Ok(None);
        }
        Ok(Some((!cell.is_tombstone(), cell.hlc)))
    }

    pub(crate) fn shared_tables(&self) -> io::Result<Vec<String>> {
        Ok(ReadBackend::entries(&self.catalog)
            .filter_map(|(key, cell)| {
                let name = primary_string(&key)?;
                (!cell.is_tombstone()
                    && cell.sync.resolve(SyncScope::Shared).is_shared()
                    && !is_system_table(name))
                .then(|| name.to_owned())
            })
            .collect())
    }

    pub(crate) fn validate_authorized(&self, event: &Event) -> io::Result<()> {
        let author = self.device(event.hlc.device_id())?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "author Device is not admitted",
            )
        })?;
        self.validate_shape(event)?;

        match event.table_id.as_str() {
            CATALOG_TABLE => require(&author, WorkspaceAction::Contribute),
            TICKETS_TABLE => require(&author, WorkspaceAction::Manage),
            DEVICES_TABLE => {
                let target = device_target(event).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid Device target")
                })?;
                let is_self = target == event.hlc.device_id();
                let field = device_field(event);
                match (event.path.is_empty(), field, &event.op) {
                    (true, _, Op::Replace { .. }) => {
                        require(&author, WorkspaceAction::Manage)?;
                        if self.device(target)?.is_some() {
                            return denied("Device admission cannot replace a live Device");
                        }
                        Ok(())
                    }
                    (true, _, Op::Delete) => require(&author, WorkspaceAction::Manage),
                    (false, Some("roles"), _) => require(&author, WorkspaceAction::Manage),
                    (false, Some("name"), _) if is_self => Ok(()),
                    (false, Some("name"), _) => require(&author, WorkspaceAction::Manage),
                    (false, Some("capabilities" | "key_ring" | "replication_frontier"), _)
                        if is_self =>
                    {
                        Ok(())
                    }
                    (false, Some("capabilities" | "key_ring" | "replication_frontier"), _) => {
                        denied("Device-owned fields may be changed only by that Device")
                    }
                    _ => denied("invalid Device mutation ownership"),
                }
            }
            _ => require(&author, WorkspaceAction::Contribute),
        }
    }

    pub(crate) fn apply_authorized(&mut self, event: &Event) -> io::Result<bool> {
        self.validate_authorized(event)?;
        self.apply_validated(event)
    }

    pub(crate) fn apply_ticket_admission(&mut self, event: &Event) -> io::Result<bool> {
        if event.table_id != DEVICES_TABLE || !event.path.is_empty() {
            return denied("ticket admission must replace one Device row");
        }
        let Op::Replace { value } = &event.op else {
            return denied("ticket admission must replace one Device row");
        };
        let candidate = DeviceRecord::from_cell(&Cell {
            value: Some(value.clone()),
            hlc: event.hlc,
            sync: SyncPolicy::Inherit,
        })
        .map_err(|error| invalid_error(error.to_string()))?;
        validate_initial_device(&candidate)?;
        self.devices.insert_shared_event(event.clone())?;
        Ok(true)
    }

    fn apply_validated(&mut self, event: &Event) -> io::Result<bool> {
        match event.table_id.as_str() {
            CATALOG_TABLE => self.catalog.insert_shared_event(event.clone())?,
            DEVICES_TABLE => self.devices.insert_shared_event(event.clone())?,
            TICKETS_TABLE => self.tickets.insert_shared_event(event.clone())?,
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn validate_shape(&self, event: &Event) -> io::Result<()> {
        match event.table_id.as_str() {
            CATALOG_TABLE => {
                let name = primary_string(&event.primary_key)
                    .ok_or_else(|| invalid_error("catalog key must be a String"))?;
                if is_system_table(name) && ReadBackend::contains(&self.catalog, &catalog_key(name))
                {
                    return denied("system catalog entries cannot be changed");
                }
                if !event.path.is_empty() {
                    return denied("catalog mutations target the row root");
                }
                match &event.op {
                    Op::Replace { value } => {
                        decode_table_config_value(value)?;
                        Ok(())
                    }
                    Op::Delete => Ok(()),
                    Op::Merge { cell } => {
                        decode_table_config(cell)?;
                        Ok(())
                    }
                    _ => denied("catalog mutation must replace or delete a row"),
                }
            }
            TICKETS_TABLE => {
                primary_entity_id(&event.primary_key)
                    .ok_or_else(|| invalid_error("ticket key must be a 128-bit entity ID"))?;
                if !event.path.is_empty() {
                    return denied("ticket mutations target the row root");
                }
                match &event.op {
                    Op::Replace { value } => {
                        EnrollmentTicket::from_cell(&Cell {
                            value: Some(value.clone()),
                            hlc: event.hlc,
                            sync: SyncPolicy::Inherit,
                        })
                        .map_err(|error| invalid_error(error.to_string()))?;
                        Ok(())
                    }
                    Op::Delete => Ok(()),
                    _ => denied("ticket mutation must replace or delete a row"),
                }
            }
            DEVICES_TABLE => self.validate_device_shape(event),
            _ => Ok(()),
        }
    }

    fn validate_device_shape(&self, event: &Event) -> io::Result<()> {
        device_target(event).ok_or_else(|| invalid_error("invalid Device key"))?;
        if event.path.is_empty() {
            return match &event.op {
                Op::Replace { value } => {
                    let record = DeviceRecord::from_cell(&Cell {
                        value: Some(value.clone()),
                        hlc: event.hlc,
                        sync: SyncPolicy::Inherit,
                    })
                    .map_err(|error| invalid_error(error.to_string()))?;
                    validate_initial_device(&record)
                }
                Op::Delete => Ok(()),
                _ => denied("Device root mutation must replace or delete"),
            };
        }
        let field = device_field(event);
        match (field, &event.op) {
            (
                Some("name"),
                Op::Replace {
                    value: Value::String(_),
                },
            ) => Ok(()),
            (Some("roles"), Op::Type(TypeOp::Set(_))) => Ok(()),
            (Some("capabilities"), Op::Type(TypeOp::OrSet(_))) => Ok(()),
            (
                Some("key_ring"),
                Op::Replace {
                    value: Value::Record(_),
                },
            ) => Ok(()),
            (
                Some("replication_frontier"),
                Op::Replace {
                    value: Value::Record(_),
                },
            ) => Ok(()),
            _ => denied("Device field operation does not match its schema"),
        }
    }

    fn sync(&mut self) -> io::Result<()> {
        DurableStorage::sync(&mut self.catalog)?;
        DurableStorage::sync(&mut self.devices)?;
        DurableStorage::sync(&mut self.tickets)
    }

    pub(crate) fn capability_event(
        &self,
        author_hlc: Hlc,
        target: DeviceId,
        capability: CapabilityId,
        enabled: bool,
    ) -> io::Result<Event> {
        Ok(Event {
            table_id: DEVICES_TABLE.into(),
            primary_key: device_key(target),
            path: vec![PathStep::new(
                TypeTag::Record,
                Segment::Record("capabilities".into()),
            )],
            op: Op::Type(TypeOp::OrSet(if enabled {
                OrSetOp::Add {
                    key: PrimaryKey::String(capability.0),
                }
            } else {
                OrSetOp::Remove {
                    key: PrimaryKey::String(capability.0),
                    observed: Vec::new(),
                }
            })),
            hlc: author_hlc,
        })
    }
}

pub(crate) fn catalog_event(
    author_hlc: Hlc,
    name: &str,
    config: Option<TableConfig>,
) -> io::Result<Event> {
    Ok(Event {
        table_id: CATALOG_TABLE.into(),
        primary_key: catalog_key(name),
        path: Path::new(),
        op: match config {
            Some(config) => Op::Replace {
                value: table_config_value(&config)?,
            },
            None => Op::Delete,
        },
        hlc: author_hlc,
    })
}

pub(crate) fn ticket_event(
    author_hlc: Hlc,
    ticket_id: &EnrollmentTicketId,
    ticket: &EnrollmentTicket,
) -> Event {
    Event {
        table_id: TICKETS_TABLE.into(),
        primary_key: ticket_key(ticket_id),
        path: Path::new(),
        op: Op::Replace {
            value: ticket
                .to_cell(author_hlc)
                .value
                .expect("ticket Cell is live"),
        },
        hlc: author_hlc,
    }
}

pub(crate) fn delete_ticket_event(author_hlc: Hlc, ticket_id: &EnrollmentTicketId) -> Event {
    Event {
        table_id: TICKETS_TABLE.into(),
        primary_key: ticket_key(ticket_id),
        path: Path::new(),
        op: Op::Delete,
        hlc: author_hlc,
    }
}

pub(crate) fn shared_table_target(event: &Event) -> Option<&str> {
    (event.table_id == CATALOG_TABLE)
        .then(|| primary_string(&event.primary_key))
        .flatten()
        .filter(|name| !is_system_table(name))
}

pub(crate) fn is_system_event(event: &Event) -> bool {
    matches!(
        event.table_id.as_str(),
        CATALOG_TABLE | DEVICES_TABLE | TICKETS_TABLE
    )
}

pub(crate) fn device_field_path(_device_id: DeviceId, field: &str) -> Vec<PathStep> {
    vec![PathStep::new(
        TypeTag::Record,
        Segment::Record(field.into()),
    )]
}

pub(crate) fn role_event(
    author_hlc: Hlc,
    target: DeviceId,
    role: WorkspaceRole,
    enabled: bool,
) -> Event {
    Event {
        table_id: DEVICES_TABLE.into(),
        primary_key: device_key(target),
        path: device_field_path(target, "roles"),
        op: Op::Type(TypeOp::Set(if enabled {
            SetOp::Add {
                key: PrimaryKey::String(role.as_str().into()),
            }
        } else {
            SetOp::Remove {
                key: PrimaryKey::String(role.as_str().into()),
            }
        })),
        hlc: author_hlc,
    }
}

pub(crate) fn replace_field_event(
    author_hlc: Hlc,
    target: DeviceId,
    field: &str,
    value: Value,
) -> Event {
    Event {
        table_id: DEVICES_TABLE.into(),
        primary_key: device_key(target),
        path: device_field_path(target, field),
        op: Op::Replace { value },
        hlc: author_hlc,
    }
}

pub(crate) fn admit_event(author_hlc: Hlc, target: DeviceId, record: DeviceRecord) -> Event {
    Event {
        table_id: DEVICES_TABLE.into(),
        primary_key: device_key(target),
        path: Path::new(),
        op: Op::Replace {
            value: record
                .to_cell(author_hlc)
                .value
                .expect("Device Cell is live"),
        },
        hlc: author_hlc,
    }
}

pub(crate) fn remove_device_event(author_hlc: Hlc, target: DeviceId) -> Event {
    Event {
        table_id: DEVICES_TABLE.into(),
        primary_key: device_key(target),
        path: Path::new(),
        op: Op::Delete,
        hlc: author_hlc,
    }
}

pub(crate) fn frontier_value(frontier: &ContiguousFrontier) -> Value {
    Value::Record(Record::from_fields(frontier.entries().map(
        |(origin, sequence)| {
            (
                origin.to_string(),
                Cell {
                    value: Some(Value::Int((*sequence).try_into().unwrap_or(i64::MAX))),
                    hlc: Hlc::ZERO,
                    sync: SyncPolicy::Inherit,
                },
            )
        },
    )))
}

pub(crate) fn key_ring_value(key_ring: &DeviceKeyRing, at: Hlc) -> io::Result<Value> {
    key_ring
        .to_cell(at)
        .value
        .ok_or_else(|| invalid_error("key ring encoded as tombstone"))
}

pub(crate) fn device_target(event: &Event) -> Option<DeviceId> {
    (event.table_id == DEVICES_TABLE)
        .then(|| primary_entity_id(&event.primary_key))
        .flatten()
        .map(DeviceId::from_bytes)
}

pub(crate) fn validate_initial_device(device: &DeviceRecord) -> io::Result<()> {
    if !device.roles.is_empty()
        || device.key_ring.secondary_key.is_some()
        || device.key_ring.primary_from_seq != 1
        || !device.replication_frontier.entries().next().is_none()
    {
        return denied("new Device must start as Reader with one key and an empty frontier");
    }
    Ok(())
}

fn system_table_config() -> TableConfig {
    TableConfig::default()
}

fn table_config_value(config: &TableConfig) -> io::Result<Value> {
    let blob = Blob::encode(config).map_err(|error| invalid_error(error.to_string()))?;
    Ok(Value::Record(Record::from_fields([(
        CONFIG_FIELD.into(),
        Cell {
            value: Some(Value::Blob(blob)),
            hlc: Hlc::ZERO,
            sync: SyncPolicy::Inherit,
        },
    )])))
}

fn decode_table_config(cell: &Cell) -> io::Result<TableConfig> {
    let value = cell
        .value
        .as_ref()
        .ok_or_else(|| invalid_error("catalog row is tombstoned"))?;
    decode_table_config_value(value)
}

fn decode_table_config_value(value: &Value) -> io::Result<TableConfig> {
    let Value::Record(record) = value else {
        return Err(invalid_error("catalog row is not a Record"));
    };
    let blob = record
        .get(CONFIG_FIELD)
        .and_then(|cell| cell.value.as_ref())
        .and_then(|value| match value {
            Value::Blob(blob) => Some(blob),
            _ => None,
        })
        .ok_or_else(|| invalid_error("catalog config is not a Blob"))?;
    blob.decode()
        .map_err(|error| invalid_error(error.to_string()))
}

fn parse_device(cell: Option<&Cell>) -> io::Result<Option<DeviceRecord>> {
    let Some(cell) = cell.filter(|cell| !cell.is_tombstone()) else {
        return Ok(None);
    };
    DeviceRecord::from_cell(cell)
        .map(Some)
        .map_err(|error| invalid_error(error.to_string()))
}

fn parse_ticket(cell: Option<&Cell>) -> io::Result<Option<EnrollmentTicket>> {
    let Some(cell) = cell.filter(|cell| !cell.is_tombstone()) else {
        return Ok(None);
    };
    EnrollmentTicket::from_cell(cell)
        .map(Some)
        .map_err(|error| invalid_error(error.to_string()))
}

fn row<'a>(rows: &'a [(PrimaryKey, Cell)], key: &PrimaryKey) -> Option<&'a Cell> {
    rows.iter()
        .find_map(|(candidate, cell)| (candidate == key).then_some(cell))
}

fn catalog_key(name: &str) -> PrimaryKey {
    PrimaryKey::String(name.into())
}

fn device_key(device_id: DeviceId) -> PrimaryKey {
    PrimaryKey::Blob(device_id.0.to_vec().into())
}

fn ticket_key(ticket_id: &EnrollmentTicketId) -> PrimaryKey {
    PrimaryKey::Blob(ticket_id.0.to_vec().into())
}

fn primary_entity_id(key: &PrimaryKey) -> Option<[u8; 16]> {
    match key {
        PrimaryKey::Blob(value) => value.as_slice().try_into().ok(),
        _ => None,
    }
}

fn primary_string(key: &PrimaryKey) -> Option<&str> {
    match key {
        PrimaryKey::String(value) => Some(value),
        _ => None,
    }
}

pub(crate) fn is_system_table(name: &str) -> bool {
    matches!(name, CATALOG_TABLE | DEVICES_TABLE | TICKETS_TABLE)
}

fn device_field(event: &Event) -> Option<&str> {
    event.path.first().and_then(|step| match &step.segment {
        Segment::Record(field) => Some(field.as_str()),
        _ => None,
    })
}

fn require(device: &DeviceRecord, action: WorkspaceAction) -> io::Result<()> {
    if device.allows(action) {
        Ok(())
    } else {
        denied("Device role denied the control operation")
    }
}

fn denied<T>(message: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::PermissionDenied, message))
}

fn invalid_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
