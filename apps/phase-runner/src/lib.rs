pub mod capacity;
pub mod cli;
mod completed_phase_recovery;
pub mod config;
mod content_hash_refusal;
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
pub mod logging;
pub mod manifest_startup;
pub mod metrics;
pub mod phase;
pub mod phase_lock;
mod progress_monitor;
pub mod project_phase;
mod redo_bounded_adoption;
mod redo_completion;
mod redo_discovery_authorization;
mod redo_extent;
mod redo_failure;
mod redo_first_effect;
mod redo_hash_adoption;
mod redo_manifest_attestation;
mod redo_manifest_audit;
mod redo_manifest_authority;
mod redo_presence;
mod redo_project;
mod redo_recompute;
mod redo_required_boundary;
mod redo_stamp;
mod redo_state;
mod redo_supersede;
pub mod rewind;
pub mod rpc_chain_check;
pub mod runner;
mod runner_support;
pub mod schema;
pub mod shutdown;
pub mod source_transport;
pub mod state;
mod state_heartbeat;
mod state_ingest_progress;
mod state_persistence;
mod state_settlement;
mod supervisor;
mod transitions;
mod verify_compare;
mod verify_level;
pub mod verify_phase;
mod verify_store;

pub use bigname_content_hash::{INTERPRETER_CONTENT_HASH, PROJECTION_CONTENT_HASH};
pub use progress_monitor::RunnerPhaseProgress;
#[doc(hidden)]
pub use redo_bounded_adoption::migration::sender_statement;
#[doc(hidden)]
pub use redo_first_effect::first_effect_statements;
#[doc(hidden)]
pub use redo_stamp::{CREATE_STAMP, EXTEND_STAMP, REQUIRED_REDO_PREFIX};
pub use transitions::expected_content_hash;

pub const SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_SHA: &str = match option_env!("BIGNAME_BUILD_SHA") {
    Some(build_sha) => build_sha,
    None => "unknown",
};

/// The build and both content hashes this binary reports on `build_info`.
pub(crate) const BUILD_INFO: bigname_metrics::BuildInfo<'static> = bigname_metrics::BuildInfo {
    build_sha: BUILD_SHA,
    interpreter_content_hash: INTERPRETER_CONTENT_HASH,
    projection_content_hash: PROJECTION_CONTENT_HASH,
};
