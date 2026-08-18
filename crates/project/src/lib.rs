//! Schema-v2 current-state projection derivation and atomic publication.

mod builders;
mod engine;
mod error;
mod hydration;
mod integrity;
mod publish;
mod resolver_address;
mod scope;
mod stage;

pub use engine::{BatchOutcome, BatchRequest, Engine, Marker, RunMode};
pub use error::{ErrorKind, ProjectError, Result};
pub use hydration::{HydrationOutcome, Hydrator};
pub use integrity::{DUAL_CURRENT_EXACT_NAME_AUTHORITY, GenerationFailureEvidence};
