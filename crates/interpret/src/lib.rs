//! Schema-v2 interpretation orchestration and its plain derived-data write layer.

mod engine;
mod error;
mod load;
mod write;

pub use engine::{BatchOutcome, BatchRequest, Engine, Marker, RunMode};
pub use error::{ErrorKind, InterpretError, Result};

pub const RECOMPUTE_FLAGS_UNAVAILABLE_REASON: &str = "interpret recompute-flags is unavailable: label flags cannot be published until name-surface visibility and active binding reconciliation are implemented";
