//! Schema-v2 current-state projection derivation and atomic publication.

mod builders;
mod engine;
mod error;
mod families;
mod hydration;
mod integrity;
#[cfg(test)]
mod profile;
mod publish;
#[cfg(test)]
mod reference;
mod resolver_address;
mod scope;
mod stage;

pub use builders::child_registrations::EXCLUDED_CHILD_REGISTRATION_PARENTS;
pub use engine::{BatchOutcome, BatchRequest, Engine, Marker, RunMode, WriteSummary};
pub use error::{ErrorKind, ProjectError, Result};
pub use hydration::{HydrationOutcome, Hydrator};
pub use integrity::{
    DUAL_CURRENT_CHILD_AUTHORITY, DUAL_CURRENT_EXACT_NAME_AUTHORITY, GenerationFailureEvidence,
};
