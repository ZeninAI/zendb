//! Operator definitions, typed composition, state, and execution.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
//! │   Database       │────►│  OperatorWorker   │────►│    RunLoop       │
//! │                  │     │                   │     │                  │
//! │ • registers ops  │     │ • holds inputs    │     │ • event queue    │
//! │ • manages catalog│     │ • timer inbox     │     │ • shutdown FSM   │
//! │ • spawns workers │     │ • spawn/suspend   │     │ • poll + commit  │
//! └──────────────────┘     └──────────────────┘     └──────────────────┘
//!                                                           │
//!                                                           ▼
//!                                                   ┌──────────────────┐
//!                                                   │    Operator      │
//!                                                   │  (user code)     │
//!                                                   │                  │
//!                                                   │ • create         │
//!                                                   │ • process        │
//!                                                   │ • on_timer       │
//!                                                   │ • on_input_*     │
//!                                                   │ • teardown       │
//!                                                   └──────────────────┘
//! ```
//!
//! # Lifecycle
//!
//! ```text
//! ┌─────────┐
//! │ create  │  ← OperatorContext available; set up state handles and tables
//! └────┬────┘
//!      │  (for each matching table already open)
//!      ▼
//! ┌────────────────┐
//! │ on_input_opened│
//! └────────┬───────┘
//!          │
//!          ▼
//! ┌─────────────────────────────────────────────────────┐
//! │              ACTIVE LOOP                             │
//! │  ┌─────────┐   ┌──────────┐   ┌────────────────┐   │
//! │  │ process │   │ on_timer │   │ on_input_opened │   │
//! │  │ changes │   │  fires   │   │ / _closed       │   │
//! │  └─────────┘   └──────────┘   └────────────────┘   │
//! │                                                     │
//! │  Any method may return Finish ─────────────────────►│
//! └─────────────────────────────────┬───────────────────┘
//!                                   │
//!                                   ▼
//!                          ┌────────────────┐
//!                          │   teardown     │ ← reason: Finished/Failed/Cancelled
//!                          └────────────────┘
//! ```
//!
//! # Ownership
//!
//! | Layer | Owns | Does NOT own |
//! |-------|------|-------------|
//! | `Operator` (user code) | Business logic, state handles, timer payloads | Polling, commit offsets, event ordering |
//! | `OperatorContext` | DB access, timer registration, table/state creation | Lifecycle transitions |
//! | `OperatorWorker` | Input attachment/detachment, timer inbox, spawn | Run loop details |
//! | `RunLoop` | Event queue, shutdown state machine, poll+commit, idle/wake | What the operator does with changes |

mod config;
mod context;
mod lifecycle;
mod macros;
pub mod prelude;
pub(crate) mod run_loop;
mod traits;
pub(crate) mod worker;

use std::{future::Future, pin::Pin};

pub use config::{OperatorRuntimeConfig, Subscription};
pub use context::OperatorContext;
pub use lifecycle::{OperatorDirective, OperatorPhase, TeardownReason};
pub use traits::{DispatchConfig, DispatchOperator, Operator};
pub use zendb_storage::frontend::state::State;
pub use zendb_types::Change;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
