//! Metadata-owning CRDT values used by the portable `Value` registry.

pub mod blob;
pub mod bool;
pub mod float;
pub mod installation;
pub mod int;
pub mod record;
pub mod string;
pub mod text;
pub mod timestamp;

pub use blob::{Blob, BlobOp};
pub use bool::{Bool, BoolOp};
pub use float::{Float, FloatOp};
pub use installation::{Installation, InstallationOp, InstallationState};
pub use int::{Int, IntOp};
pub use record::{Record, RecordOp};
pub use string::{String, StringOp};
pub use text::{Text, TextError, TextId, TextOp, TextOpError};
pub use timestamp::{Timestamp, TimestampConversionError, TimestampOp};
