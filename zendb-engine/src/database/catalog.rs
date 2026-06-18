//! Durable database resource metadata.

use bincode::{Decode, Encode};
use zendb_storage::core::keydir::KeyDir;
use zendb_storage::frontend::state::StateConfig;

use crate::{ComputationConfig, TableConfig};

#[derive(Debug, Clone, Encode, Decode)]
pub(super) enum CatalogEntry {
    Table(TableConfig),
    Computation(ComputationConfig),
    SharedState {
        owner: String,
        implementation: String,
        config: StateConfig,
    },
}

pub(super) type Catalog = KeyDir<String, CatalogEntry>;
