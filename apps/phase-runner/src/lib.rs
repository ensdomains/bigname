pub mod capacity;
pub mod cli;
mod completed_phase_recovery;
pub mod config;
pub mod database;
pub mod error;
mod head_finality;
mod head_observed;
pub mod heads;
mod ingest_cursor_config;
pub mod ingest_phase;
mod ingest_progress;
pub mod inspect;
pub mod interpret_phase;
pub mod label_preimages;
pub mod live_phase;
pub mod metrics;
pub mod phase;
pub mod phase_lock;
pub mod project_phase;
mod redo_completion;
mod redo_failure;
mod redo_manifest_attestation;
mod redo_manifest_audit;
mod redo_presence;
mod redo_recompute;
mod redo_stamp;
mod redo_state;
pub mod rewind;
pub mod runner;
mod runner_support;
pub mod schema;
pub mod state;
mod state_heartbeat;
mod state_ingest_progress;
mod state_persistence;
mod state_settlement;
mod supervisor;
mod transitions;
mod verify_compare;
pub mod verify_phase;
mod verify_store;

pub use bigname_content_hash::INTERPRETER_CONTENT_HASH;

pub const SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_SHA: &str = match option_env!("BIGNAME_BUILD_SHA") {
    Some(build_sha) => build_sha,
    None => "unknown",
};
