use std::{collections::BTreeSet, path::Path};

use anyhow::{Context, Result};
use bigname_manifests::{
    load_manifest_declared_bootstrap_targets, load_manifest_skipped_bootstrap_targets,
};
use tracing::{info, warn};

use crate::{
    backfill::{
        BackfillAdapterSyncMode, BackfillBlockRange, hash_pinned_backfill_range_specs,
        run_resumable_hash_pinned_backfill_job_concurrently,
        run_resumable_hash_pinned_backfill_job_concurrently_with_heartbeat,
    },
    backfill_lease_expires_at, default_backfill_lease_owner, deployment_profile_from_manifest_root,
    generated_backfill_lease_token,
    provider::{ChainProviderOps, ProviderRegistry},
    reconciliation::{
        HeaderAuditMode, RawFactNormalizedEventReplayRequest,
        RawFactNormalizedEventReplaySelection, log_raw_fact_normalized_event_replay_outcome,
        replay_raw_fact_normalized_events,
    },
    run::startup_heartbeat::{StartupAdapterHeartbeat, StartupHeartbeat},
    runtime::{IntakeChainTask, validate_provider_registry_for_intake_tasks},
};

#[path = "bootstrap_backfill/checkpoints.rs"]
mod checkpoints;
#[path = "bootstrap_backfill/entrypoints.rs"]
mod entrypoints;
#[path = "bootstrap_backfill/identity.rs"]
mod identity;
#[path = "bootstrap_backfill/logging.rs"]
mod logging;
#[path = "bootstrap_backfill/orchestration.rs"]
mod orchestration;
#[path = "bootstrap_backfill/planning.rs"]
mod planning;
#[path = "bootstrap_backfill/progress.rs"]
mod progress;
#[path = "bootstrap_backfill/recovery.rs"]
mod recovery;

use checkpoints::{load_bootstrap_segment_checkpoint, load_bootstrap_target_checkpoint};
#[cfg(test)]
pub(crate) use entrypoints::run_startup_bootstrap_backfills;
pub(crate) use entrypoints::{
    BootstrapBackfillOutcome, run_startup_bootstrap_backfills_with_heartbeat,
};
pub(crate) use identity::bootstrap_backfill_idempotency_key;
use identity::{
    partitioned_bootstrap_backfill_idempotency_key, replay_source_scope_from_source_plan,
};
use orchestration::run_startup_bootstrap_backfills_inner;
use planning::{
    BootstrapBackfillTargetRange, bootstrap_target_range, effective_bootstrap_backfill_worker_count,
};
pub(crate) use planning::{
    bootstrap_finalized_head_block, resolve_bootstrap_backfill_worker_count,
};
use progress::{
    bootstrap_segment_target_ids_with_optional_progress,
    bootstrap_source_identity_with_optional_progress, extend_bootstrap_targets_with_progress,
    load_bootstrap_source_plan_with_optional_progress,
    load_discovery_bootstrap_targets_with_optional_progress,
    load_retained_recovery_targets_with_optional_progress,
    narrow_bootstrap_source_plan_with_optional_progress,
    plan_bootstrap_segments_with_optional_progress, record_bootstrap_progress,
};
#[cfg(test)]
pub(crate) use recovery::install_forced_retention_rotation;
use recovery::{
    BootstrapConvergenceTracker, BootstrapPassStatus, finish_bootstrap_convergence_pass,
    finish_bootstrap_convergence_pass_with_progress,
    load_bootstrap_retention_snapshot_with_progress,
};
pub(crate) use recovery::{
    automatic_backfill_retention_snapshot_is_stable,
    converge_ens_v2_retained_history_through_block, load_bootstrap_retention_snapshot,
};
const BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS: u64 = 300;
pub(crate) const DEFAULT_BOOTSTRAP_BACKFILL_WORKERS: usize = 0;
pub(crate) const DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS: i64 = 50_000;
