//! Table lookup and computation subscription maintenance.

use std::{io, sync::Arc};

use crate::computation::{worker::ComputationInput, Subscription};

use super::{not_found, DatabaseInner, Table};

impl DatabaseInner {
    pub(crate) fn table(self: &Arc<Self>, name: &str) -> io::Result<Table> {
        self.tables
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| not_found("table", name))
    }

    pub(super) fn attach_table_to_all_subscribers(
        &self,
        name: &str,
        table: &Table,
    ) -> io::Result<()> {
        for worker in self.computations.read().values() {
            if worker
                .config
                .subscriptions
                .iter()
                .any(|subscription| matches!(subscription, Subscription::AllTables))
            {
                let reader = table.read().consumer(&worker.name)?;
                worker.inputs.lock().push(ComputationInput {
                    table_name: name.to_owned(),
                    reader,
                });
            }
        }
        Ok(())
    }

    pub(super) fn detach_table(&self, name: &str) {
        for worker in self.computations.read().values() {
            let mut inputs = worker.inputs.lock();
            let mut retained = Vec::with_capacity(inputs.len());
            let mut removed = Vec::new();
            for input in std::mem::take(&mut *inputs) {
                if input.table_name == name {
                    removed.push(input);
                } else {
                    retained.push(input);
                }
            }
            *inputs = retained;
            for input in removed {
                if let Err(error) = input.reader.delete() {
                    log::error!(
                        "failed deleting consumer {:?} from table {name:?}: {error}",
                        worker.name
                    );
                }
            }
        }
    }
}
