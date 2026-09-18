mod coinbase_sql;
mod engine;
mod error;
mod event_signatures;
mod fetching;
mod manifest;
mod plan;
mod provider;
#[cfg(test)]
#[path = "tests/test_chain.rs"]
mod test_chain;
mod verification;
mod write;

pub use engine::{
    BatchOutcome, BatchRequest, Engine, HeadMarkers, LiveBatchOutcome, LiveBatchRequest,
    LiveContinuation, Marker, SourceCursor, SourceDescriptor, SourceProgress, admit_source_floor,
    plan_live_continuation,
};
pub use error::{ErrorKind, IngestError, REDO_BOUNDARY_DIVERGENCE_PREFIX, Result};
pub use manifest::{WatchFilter, WatchQuery, load_persisted_watch_filter, load_watch_filter};
pub use plan::{BASE_COINBASE_SEAM_BLOCK, enforce_source_floor};
pub use provider::RETH_DB_OPENED_STORAGE_CHILDREN;
pub use verification::{
    VerificationBatch, VerificationLog, VerificationMarker, VerificationProvider,
    VerificationProviderKind,
};

#[cfg(feature = "reth-db")]
mod reth_diagnostics;
#[cfg(feature = "reth-db")]
pub use reth_diagnostics::read_reth_sample;
