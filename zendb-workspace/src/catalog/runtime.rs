//! Shared Table ownership, direct read guards, and streaming consumers.

use std::{io, ops::Deref, sync::Arc};

use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use zendb_storage::{Change, InsertOutcome, Table, TopicConsumer};
use zendb_types::{Op, Path, PrimaryKey};

use crate::{devices::Devices, Result};

pub(crate) const RECEIPT_CONSUMER: &str = "__zendb_device_receipts";

pub(crate) struct TableEntry {
    pub(crate) table: Arc<RwLock<Table>>,
    pub(crate) receipt: Mutex<TopicConsumer<Change>>,
}

impl TableEntry {
    pub(crate) fn new(table: Table) -> Result<Arc<Self>> {
        let receipt = table.consumer(RECEIPT_CONSUMER)?;
        Ok(Arc::new(Self {
            table: Arc::new(RwLock::new(table)),
            receipt: Mutex::new(receipt),
        }))
    }
}

#[derive(Clone)]
pub struct TableHandle {
    name: String,
    entry: Arc<TableEntry>,
    devices: Devices,
}

impl TableHandle {
    pub(crate) fn new(name: String, entry: Arc<TableEntry>, devices: Devices) -> Self {
        Self {
            name,
            entry,
            devices,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn insert(&self, primary_key: PrimaryKey, path: Path, op: Op) -> Result<InsertOutcome> {
        self.devices
            .insert_application(&self.entry, primary_key, path, op)
    }

    pub fn read(&self) -> TableReadGuard<'_> {
        TableReadGuard {
            table: self.entry.table.read(),
        }
    }

    pub fn consumer(&self, name: &str) -> Result<TableConsumer> {
        let changes = self.entry.table.read().consumer(name)?;
        Ok(TableConsumer {
            table: self.entry.table.clone(),
            changes,
        })
    }
}

pub struct TableReadGuard<'a> {
    table: RwLockReadGuard<'a, Table>,
}

impl Deref for TableReadGuard<'_> {
    type Target = Table;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

pub struct TableConsumer {
    table: Arc<RwLock<Table>>,
    changes: TopicConsumer<Change>,
}

impl TableConsumer {
    pub fn next_change(&mut self) -> io::Result<Option<Change>> {
        self.changes.next().transpose()
    }

    pub fn read(&self) -> TableReadGuard<'_> {
        TableReadGuard {
            table: self.table.read(),
        }
    }

    pub fn commit(&self) -> io::Result<()> {
        self.changes.commit()
    }
}
