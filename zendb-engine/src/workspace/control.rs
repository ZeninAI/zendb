//! Durable replicated Workspace control state and device authorization.

use std::{collections::BTreeSet, io, path::Path};

use zendb_storage::{
    core::{
        keydir::KeyDirConfig,
        traits::{Backend, DurableStorage},
    },
    frontend::state::{State, StateConfig},
};
use zendb_types::crdt::values::{OrSetOp, Record, SetOp};
use zendb_types::{
    CapabilityId, Cell, ContiguousFrontier, DeviceId, DeviceKeyRing, DeviceRecord,
    EnrollmentTicket, EnrollmentTicketId, Event, Hlc, Op, PathStep, PrimaryKey, Segment, Type,
    TypeOp, TypeTag, Value, WorkspaceAction, WorkspaceRole,
};

const CONTROL_KEY: &str = "workspace";
const DEVICES: &str = "devices";
const TICKETS: &str = "enrollment_tickets";
const SHARED_TABLES: &str = "shared_tables";

fn parse_control_root(root: &Cell) -> io::Result<()> {
    let record = match root.value.as_ref() {
        Some(Value::Record(record)) => record,
        _ => return invalid("Workspace control root is not a Record"),
    };
    let devices = match record.get(DEVICES).and_then(|cell| cell.value.as_ref()) {
        Some(Value::Record(devices)) => devices,
        _ => return invalid("Workspace devices field is not a Record"),
    };
    match record.get(TICKETS).and_then(|cell| cell.value.as_ref()) {
        Some(Value::Record(_)) => {}
        _ => return invalid("Workspace enrollment_tickets field is not a Record"),
    }
    match record
        .get(SHARED_TABLES)
        .and_then(|cell| cell.value.as_ref())
    {
        Some(Value::Record(_)) => {}
        _ => return invalid("Workspace shared_tables field is not a Record"),
    }
    for (id, cell) in devices.fields() {
        if cell.is_tombstone() {
            continue;
        }
        parse_device_id(id)?;
        DeviceRecord::from_cell(cell)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    Ok(())
}

pub(crate) struct WorkspaceControl {
    state: State<String, Cell>,
    root: Cell,
}

impl WorkspaceControl {
    pub(crate) fn validate_root(root: &Cell) -> io::Result<()> {
        parse_control_root(root)
    }

    pub(crate) fn device_from_root(
        root: &Cell,
        device_id: DeviceId,
    ) -> io::Result<Option<DeviceRecord>> {
        parse_control_root(root)?;
        let Some(Value::Record(root_record)) = root.value.as_ref() else {
            unreachable!("validated above")
        };
        let Some(Value::Record(devices)) = root_record
            .get(DEVICES)
            .and_then(|cell| cell.value.as_ref())
        else {
            unreachable!("validated above")
        };
        let Some(cell) = devices
            .get(&device_id.to_string())
            .filter(|cell| !cell.is_tombstone())
        else {
            return Ok(None);
        };
        DeviceRecord::from_cell(cell)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    pub(crate) fn ticket_from_root(
        root: &Cell,
        ticket_id: &EnrollmentTicketId,
    ) -> io::Result<Option<EnrollmentTicket>> {
        parse_control_root(root)?;
        let Some(Value::Record(root_record)) = root.value.as_ref() else {
            unreachable!("validated above")
        };
        let Some(Value::Record(tickets)) = root_record
            .get(TICKETS)
            .and_then(|cell| cell.value.as_ref())
        else {
            unreachable!("validated above")
        };
        let Some(cell) = tickets
            .get(&ticket_id.0)
            .filter(|cell| !cell.is_tombstone())
        else {
            return Ok(None);
        };
        EnrollmentTicket::from_cell(cell)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    pub(crate) fn create(
        path: &Path,
        first_device_id: DeviceId,
        first_device: DeviceRecord,
        at: Hlc,
    ) -> io::Result<Self> {
        let root = Cell {
            value: Some(Value::Record(Record::from_fields([
                (
                    DEVICES.into(),
                    Cell {
                        value: Some(Value::Record(Record::from_fields([(
                            first_device_id.to_string(),
                            first_device.to_cell(at),
                        )]))),
                        hlc: at,
                        sync: None,
                    },
                ),
                (
                    TICKETS.into(),
                    Cell {
                        value: Some(Value::Record(Record::default())),
                        hlc: at,
                        sync: None,
                    },
                ),
                (
                    SHARED_TABLES.into(),
                    Cell {
                        value: Some(Value::Record(Record::default())),
                        hlc: at,
                        sync: None,
                    },
                ),
            ]))),
            hlc: at,
            sync: Some(true),
        };
        let mut state = State::create(path, StateConfig::Unordered(KeyDirConfig::default()))?;
        state.put(CONTROL_KEY.into(), root.clone())?;
        state.sync()?;
        Ok(Self { state, root })
    }

    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let state: State<String, Cell> =
            State::open(path, StateConfig::Unordered(KeyDirConfig::default()))?;
        let root = state
            .get(&CONTROL_KEY.to_owned())
            .map(|value| value.into_owned())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing Workspace control root")
            })?;
        Ok(Self { state, root })
    }

    pub(crate) fn root(&self) -> &Cell {
        &self.root
    }

    pub(crate) fn install_root(&mut self, root: Cell) -> io::Result<()> {
        Self::validate_root(&root)?;
        self.root = root;
        self.state.put(CONTROL_KEY.into(), self.root.clone())?;
        self.state.sync()
    }

    pub(crate) fn compact_through(&mut self, watermark: Hlc) -> io::Result<bool> {
        let changed = self.root.compact(watermark).map_err(io::Error::other)?;
        if changed {
            self.state.put(CONTROL_KEY.into(), self.root.clone())?;
            self.state.sync()?;
        }
        Ok(changed)
    }

    pub(crate) fn capability_event(
        &self,
        author_hlc: Hlc,
        target: DeviceId,
        capability: CapabilityId,
        add: bool,
    ) -> io::Result<Event> {
        let op = if add {
            OrSetOp::Add {
                key: PrimaryKey::String(capability.0),
            }
        } else {
            let device = self
                .device_cell(target)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Device is not admitted"))?;
            let record = match device.value.as_ref() {
                Some(Value::Record(record)) => record,
                _ => return invalid("Device Cell is not a Record"),
            };
            let set = match record
                .get("capabilities")
                .and_then(|cell| cell.value.as_ref())
            {
                Some(Value::OrSet(set)) => set,
                _ => return invalid("Device capabilities field is not an OR-Set"),
            };
            set.remove(PrimaryKey::String(capability.0))
        };
        Ok(Event {
            table_id: "_control".into(),
            primary_key: PrimaryKey::String(CONTROL_KEY.into()),
            path: device_field_path(target, "capabilities"),
            op: Op::Type(TypeOp::OrSet(op)),
            hlc: author_hlc,
            sync: true,
            signature: Vec::new(),
        })
    }

    pub(crate) fn device(&self, device_id: DeviceId) -> io::Result<Option<DeviceRecord>> {
        let Some(cell) = self.device_cell(device_id)? else {
            return Ok(None);
        };
        DeviceRecord::from_cell(cell)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    pub(crate) fn devices(&self) -> io::Result<Vec<(DeviceId, DeviceRecord)>> {
        let devices = self.devices_record()?;
        let mut result = Vec::new();
        for (id, cell) in devices.fields() {
            if cell.is_tombstone() {
                continue;
            }
            let device_id = parse_device_id(id)?;
            let record = DeviceRecord::from_cell(cell)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            result.push((device_id, record));
        }
        Ok(result)
    }

    pub(crate) fn ticket(
        &self,
        ticket_id: &EnrollmentTicketId,
    ) -> io::Result<Option<EnrollmentTicket>> {
        let root = match self.root.value.as_ref() {
            Some(Value::Record(record)) => record,
            _ => return invalid("Workspace control root is not a Record"),
        };
        let tickets = match root.get(TICKETS).and_then(|cell| cell.value.as_ref()) {
            Some(Value::Record(record)) => record,
            _ => return invalid("Workspace enrollment_tickets field is not a Record"),
        };
        let Some(cell) = tickets
            .get(&ticket_id.0)
            .filter(|cell| !cell.is_tombstone())
        else {
            return Ok(None);
        };
        EnrollmentTicket::from_cell(cell)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    pub(crate) fn apply_authorized(&mut self, event: &Event) -> io::Result<bool> {
        self.validate_authorized(event)?;
        self.apply_validated(event)
    }

    pub(crate) fn validate_authorized(&self, event: &Event) -> io::Result<()> {
        let author = event.hlc.device_id();
        let author_record = self.device(author)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "author Device is not admitted",
            )
        })?;
        self.authorize_control_event(author, &author_record, event)?;
        self.validate_authorized_shape(event)
    }

    pub(crate) fn shared_table_state(&self, name: &str) -> io::Result<Option<(bool, Hlc)>> {
        let root = match self.root.value.as_ref() {
            Some(Value::Record(record)) => record,
            _ => return invalid("Workspace control root is not a Record"),
        };
        let tables = match root.get(SHARED_TABLES).and_then(|cell| cell.value.as_ref()) {
            Some(Value::Record(record)) => record,
            _ => return invalid("Workspace shared_tables field is not a Record"),
        };
        Ok(tables
            .get(name)
            .map(|cell| (!cell.is_tombstone(), cell.hlc)))
    }

    pub(crate) fn shared_tables(&self) -> io::Result<Vec<String>> {
        let root = match self.root.value.as_ref() {
            Some(Value::Record(record)) => record,
            _ => return invalid("Workspace control root is not a Record"),
        };
        let tables = match root.get(SHARED_TABLES).and_then(|cell| cell.value.as_ref()) {
            Some(Value::Record(record)) => record,
            _ => return invalid("Workspace shared_tables field is not a Record"),
        };
        Ok(tables
            .fields()
            .filter_map(|(name, cell)| (!cell.is_tombstone()).then(|| name.to_owned()))
            .collect())
    }

    /// Apply an event whose exceptional ticket-admission proof was validated by
    /// the onboarding coordinator.
    pub(crate) fn apply_ticket_admission(&mut self, event: &Event) -> io::Result<bool> {
        let target = device_target(&event.path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "ticket may only create a Device Cell",
            )
        })?;
        if event.path.len() != 2 || self.device(target)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "ticket may only create a previously absent Device Cell",
            ));
        }
        let Op::Merge { cell } = &event.op else {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid ticket admission operation",
            ));
        };
        let device = DeviceRecord::from_cell(cell)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        validate_initial_device(&device)?;
        self.apply_validated(event)
    }

    fn apply_validated(&mut self, event: &Event) -> io::Result<bool> {
        let changed = self
            .root
            .apply_event_from(event, true, event.hlc.device_id())
            .map_err(io::Error::other)?;
        if changed {
            self.state.put(CONTROL_KEY.into(), self.root.clone())?;
            self.state.sync()?;
        }
        Ok(changed)
    }

    fn authorize_control_event(
        &self,
        author: DeviceId,
        author_record: &DeviceRecord,
        event: &Event,
    ) -> io::Result<()> {
        let Some(first) = event.path.first() else {
            return denied("the Workspace control root cannot be replaced");
        };
        let Segment::Record(section) = &first.segment else {
            return denied("invalid control path");
        };
        if section == TICKETS {
            return require(author_record, WorkspaceAction::Manage);
        }
        if section == SHARED_TABLES {
            return require(author_record, WorkspaceAction::Contribute);
        }
        if section != DEVICES {
            return denied("unknown Workspace control section");
        }
        let target = device_target(&event.path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid Device control path",
            )
        })?;
        if event.path.len() == 2 {
            return require(author_record, WorkspaceAction::Manage);
        }
        let field = event.path.get(2).and_then(|step| match &step.segment {
            Segment::Record(field) => Some(field.as_str()),
            _ => None,
        });
        match field {
            Some("roles") => require(author_record, WorkspaceAction::Manage),
            Some("name") if target == author => Ok(()),
            Some("name") => require(author_record, WorkspaceAction::Manage),
            Some("capabilities" | "key_ring" | "replication_frontier") if target == author => {
                Ok(())
            }
            _ => denied("Device field is not writable by this author"),
        }
    }

    fn validate_authorized_shape(&self, event: &Event) -> io::Result<()> {
        let section = event.path.first().and_then(|step| match &step.segment {
            Segment::Record(section) => Some(section.as_str()),
            _ => None,
        });
        if section == Some(TICKETS) {
            if event.path.len() != 2 {
                return denied("enrollment ticket mutations must target one whole ticket");
            }
            return match &event.op {
                Op::Merge { cell } => EnrollmentTicket::from_cell(cell)
                    .map(|_| ())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
                Op::Delete => Ok(()),
                _ => denied("enrollment tickets may only be created or deleted atomically"),
            };
        }
        if section == Some(SHARED_TABLES) {
            if event.path.len() != 2 {
                return denied("shared table mutations must target one table name");
            }
            return match &event.op {
                Op::Merge { cell } if matches!(cell.value, Some(Value::Bool(true))) => Ok(()),
                Op::Delete => Ok(()),
                _ => denied("shared table state may only be created or tombstoned"),
            };
        }
        let target = device_target(&event.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid Device path"))?;
        if event.path.len() == 2 {
            return match &event.op {
                Op::Merge { cell } if self.device(target)?.is_none() => {
                    let device = DeviceRecord::from_cell(cell)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    validate_initial_device(&device)
                }
                Op::Delete => Ok(()),
                Op::Merge { .. } => denied("an existing Device cannot be replaced atomically"),
                _ => denied("Device membership may only be created or tombstoned atomically"),
            };
        }
        if event.path.len() != 3 {
            return denied("Device fields cannot be mutated through an internal CRDT path");
        }
        let field = match &event.path[2].segment {
            Segment::Record(field) => field.as_str(),
            _ => return denied("invalid Device field path"),
        };
        match (field, &event.op) {
            (
                "name",
                Op::Replace {
                    value: Value::String(_),
                },
            ) => Ok(()),
            ("roles", Op::Type(TypeOp::Set(SetOp::Add { key } | SetOp::Remove { key }))) => {
                let PrimaryKey::String(role) = key else {
                    return denied("Workspace role keys must be strings");
                };
                WorkspaceRole::parse(role).map(|_| ()).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "unknown Workspace role")
                })
            }
            (
                "capabilities",
                Op::Type(TypeOp::OrSet(OrSetOp::Add { key } | OrSetOp::Remove { key, .. })),
            ) => {
                if matches!(key, PrimaryKey::String(_)) {
                    Ok(())
                } else {
                    denied("Device capability keys must be strings")
                }
            }
            (
                "key_ring",
                Op::Replace {
                    value: Value::Record(_),
                },
            ) => Ok(()),
            (
                "replication_frontier",
                Op::Replace {
                    value: Value::Record(record),
                },
            ) => {
                for (origin, cell) in record.fields() {
                    parse_device_id(origin)?;
                    if !matches!(cell.value, Some(Value::Int(value)) if value >= 0) {
                        return denied("replication frontier values must be non-negative integers");
                    }
                }
                Ok(())
            }
            _ => denied("Device field operation does not match its schema"),
        }
    }

    fn devices_record(&self) -> io::Result<&Record> {
        let root = match self.root.value.as_ref() {
            Some(Value::Record(record)) => record,
            _ => return invalid("Workspace control root is not a Record"),
        };
        match root.get(DEVICES).and_then(|cell| cell.value.as_ref()) {
            Some(Value::Record(record)) => Ok(record),
            _ => invalid("Workspace devices field is not a Record"),
        }
    }

    fn device_cell(&self, device_id: DeviceId) -> io::Result<Option<&Cell>> {
        Ok(self
            .devices_record()?
            .get(&device_id.to_string())
            .filter(|cell| !cell.is_tombstone()))
    }
}

pub(crate) fn device_path(device_id: DeviceId) -> Vec<PathStep> {
    vec![
        PathStep::new(TypeTag::Record, Segment::Record(DEVICES.into())),
        PathStep::new(TypeTag::Record, Segment::Record(device_id.to_string())),
    ]
}

pub(crate) fn ticket_event(
    author_hlc: Hlc,
    ticket_id: &EnrollmentTicketId,
    ticket: &EnrollmentTicket,
) -> Event {
    Event {
        table_id: "_control".into(),
        primary_key: PrimaryKey::String(CONTROL_KEY.into()),
        path: vec![
            PathStep::new(TypeTag::Record, Segment::Record(TICKETS.into())),
            PathStep::new(TypeTag::Record, Segment::Record(ticket_id.0.clone())),
        ],
        op: Op::Merge {
            cell: ticket.to_cell(author_hlc),
        },
        hlc: author_hlc,
        sync: true,
        signature: Vec::new(),
    }
}

pub(crate) fn delete_ticket_event(author_hlc: Hlc, ticket_id: &EnrollmentTicketId) -> Event {
    Event {
        table_id: "_control".into(),
        primary_key: PrimaryKey::String(CONTROL_KEY.into()),
        path: vec![
            PathStep::new(TypeTag::Record, Segment::Record(TICKETS.into())),
            PathStep::new(TypeTag::Record, Segment::Record(ticket_id.0.clone())),
        ],
        op: Op::Delete,
        hlc: author_hlc,
        sync: true,
        signature: Vec::new(),
    }
}

pub(crate) fn shared_table_event(author_hlc: Hlc, name: &str, create: bool) -> Event {
    Event {
        table_id: "_control".into(),
        primary_key: PrimaryKey::String(CONTROL_KEY.into()),
        path: vec![
            PathStep::new(TypeTag::Record, Segment::Record(SHARED_TABLES.into())),
            PathStep::new(TypeTag::Record, Segment::Record(name.into())),
        ],
        op: if create {
            Op::Merge {
                cell: Cell {
                    value: Some(Value::Bool(true)),
                    hlc: author_hlc,
                    sync: None,
                },
            }
        } else {
            Op::Delete
        },
        hlc: author_hlc,
        sync: true,
        signature: Vec::new(),
    }
}

pub(crate) fn shared_table_target(event: &Event) -> Option<&str> {
    if event.table_id != "_control" || event.path.len() != 2 {
        return None;
    }
    match (&event.path[0].segment, &event.path[1].segment) {
        (Segment::Record(section), Segment::Record(name)) if section == SHARED_TABLES => Some(name),
        _ => None,
    }
}

pub(crate) fn device_field_path(device_id: DeviceId, field: &str) -> Vec<PathStep> {
    let mut path = device_path(device_id);
    path.push(PathStep::new(
        TypeTag::Record,
        Segment::Record(field.into()),
    ));
    path
}

pub(crate) fn role_event(
    author_hlc: Hlc,
    target: DeviceId,
    role: WorkspaceRole,
    add: bool,
) -> Event {
    Event {
        table_id: "_control".into(),
        primary_key: PrimaryKey::String(CONTROL_KEY.into()),
        path: device_field_path(target, "roles"),
        op: Op::Type(TypeOp::Set(if add {
            SetOp::Add {
                key: PrimaryKey::String(role.as_str().into()),
            }
        } else {
            SetOp::Remove {
                key: PrimaryKey::String(role.as_str().into()),
            }
        })),
        hlc: author_hlc,
        sync: true,
        signature: Vec::new(),
    }
}

pub(crate) fn replace_field_event(
    author_hlc: Hlc,
    target: DeviceId,
    field: &str,
    value: Value,
) -> Event {
    Event {
        table_id: "_control".into(),
        primary_key: PrimaryKey::String(CONTROL_KEY.into()),
        path: device_field_path(target, field),
        op: Op::Replace { value },
        hlc: author_hlc,
        sync: true,
        signature: Vec::new(),
    }
}

pub(crate) fn admit_event(author_hlc: Hlc, target: DeviceId, record: DeviceRecord) -> Event {
    Event {
        table_id: "_control".into(),
        primary_key: PrimaryKey::String(CONTROL_KEY.into()),
        path: device_path(target),
        op: Op::Merge {
            cell: record.to_cell(author_hlc),
        },
        hlc: author_hlc,
        sync: true,
        signature: Vec::new(),
    }
}

pub(crate) fn remove_device_event(author_hlc: Hlc, target: DeviceId) -> Event {
    Event {
        table_id: "_control".into(),
        primary_key: PrimaryKey::String(CONTROL_KEY.into()),
        path: device_path(target),
        op: Op::Delete,
        hlc: author_hlc,
        sync: true,
        signature: Vec::new(),
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
                    sync: None,
                },
            )
        },
    )))
}

pub(crate) fn key_ring_value(key_ring: &DeviceKeyRing, at: Hlc) -> io::Result<Value> {
    let wrapper = DeviceRecord {
        name: String::new(),
        key_ring: key_ring.clone(),
        roles: BTreeSet::new(),
        capabilities: BTreeSet::new(),
        replication_frontier: ContiguousFrontier::default(),
    }
    .to_cell(at);
    let Value::Record(device) = wrapper.value.expect("DeviceRecord is live") else {
        unreachable!()
    };
    device
        .get("key_ring")
        .and_then(|cell| cell.value.clone())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing encoded key ring"))
}

pub(crate) fn device_target(path: &[PathStep]) -> Option<DeviceId> {
    let step = path.get(1)?;
    let Segment::Record(id) = &step.segment else {
        return None;
    };
    parse_device_id(id).ok()
}

fn parse_device_id(value: &str) -> io::Result<DeviceId> {
    if value.len() != 32 {
        return invalid("invalid DeviceId in control path");
    }
    let mut bytes = [0; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        bytes[index] = u8::from_str_radix(text, 16)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    Ok(DeviceId::from_bytes(bytes))
}

fn require(device: &DeviceRecord, action: WorkspaceAction) -> io::Result<()> {
    if device.allows(action) {
        Ok(())
    } else {
        denied("Device role does not allow this control mutation")
    }
}

fn denied<T>(message: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::PermissionDenied, message))
}

pub(crate) fn validate_initial_device(device: &DeviceRecord) -> io::Result<()> {
    if !device.roles.is_empty() {
        return denied("new Device records must start with no explicit roles");
    }
    if device.key_ring.phase != zendb_types::DeviceKeyPhase::Stable
        || device.key_ring.secondary_key.is_some()
        || device.key_ring.primary_from_seq != 1
    {
        return invalid("new Device records must contain one stable initial signing key");
    }
    if device.replication_frontier != ContiguousFrontier::default() {
        return invalid("new Device records must start with an empty replication frontier");
    }
    Ok(())
}

fn invalid<T>(message: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, message))
}
