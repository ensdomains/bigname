//! Schema-v2 current-state projection derivation and atomic publication.

mod builders;
mod engine;
mod error;
mod publish;
mod scope;
mod stage;

pub use engine::{BatchOutcome, BatchRequest, Engine, Marker, RunMode};
pub use error::{ErrorKind, ProjectError, Result};

/// Hydration remains owned by the later live-multicall lane. The project phase
/// deliberately publishes only event-derived current state.
pub const HYDRATION_DEFERRED_REASON: &str =
    "project hydration is deferred to the live multicall phase";
