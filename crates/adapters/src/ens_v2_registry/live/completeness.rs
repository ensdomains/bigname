use anyhow::{Context, Result, ensure};
use bigname_manifests::{
    RequiredWatchedTuple, load_required_watched_tuples_in_transaction,
    load_required_watched_tuples_in_transaction_with_progress,
};
use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress},
    startup_progress::StartupManifestProgress,
};

const RETAINED_REQUIREMENT_PROGRESS_ROWS: usize = 1_000;

mod finish;
mod guard;
mod requirements;
mod state;

use requirements::{
    ens_v2_closure_source_families, ens_v2_discovery_history_source_families,
    ensure_generation_bound_coverage, ensure_generation_bound_coverage_with_live_selection,
    ensure_generation_bound_coverage_with_live_selection_with_progress,
    ensure_newly_required_generation_bound_coverage, ensure_retained_semantic_witnesses,
    ensure_retained_semantic_witnesses_with_progress, requirement_intervals_not_covered_by,
    requirement_intervals_not_covered_by_with_progress,
};
pub(in crate::ens_v2_registry) use requirements::{
    has_authoritative_ens_v2_closure_through,
    has_authoritative_ens_v2_closure_through_with_progress,
};
use state::{
    ensure_discovery_epoch_row, ensure_retained_history_state_row,
    load_locked_retained_history_state, load_selected_live_block_intervals,
};

/// A retained-history proof tied to the current destructive-retention
/// generation, discovery-admission epoch, and inclusive block boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ens_v2_registry) struct RawLogClosureProof {
    pub(in crate::ens_v2_registry) retention_generation: i64,
    pub(in crate::ens_v2_registry) discovery_admission_epoch: i64,
    pub(in crate::ens_v2_registry) proven_through_block: i64,
}

/// A long-lived per-chain mutation fence plus an `ACCESS SHARE` table lock.
/// Semantic writes for this chain wait on the advisory fence, writes for other
/// chains remain live, and global truncation waits on the table lock.
pub(in crate::ens_v2_registry) struct FullSourceRawLogHistoryGuard {
    transaction: Transaction<'static, Postgres>,
    chain: String,
}

async fn ensure_generation_bound_coverage_with_optional_progress(
    pool: &PgPool,
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if progress.is_none() {
        return ensure_generation_bound_coverage(
            connection,
            chain,
            requirements,
            retention_generation,
        )
        .await;
    }
    for page in requirements.chunks(RETAINED_REQUIREMENT_PROGRESS_ROWS) {
        ensure_generation_bound_coverage(connection, chain, page, retention_generation).await?;
        record_startup_adapter_progress(pool, progress).await?;
    }
    Ok(())
}

async fn ensure_retained_semantic_witnesses_with_optional_progress(
    pool: &PgPool,
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    through_block: i64,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if progress.is_none() {
        return ensure_retained_semantic_witnesses(connection, chain, requirements, through_block)
            .await;
    }
    ensure_retained_semantic_witnesses_with_progress(
        pool,
        connection,
        chain,
        requirements,
        through_block,
        progress
            .as_deref_mut()
            .expect("ENSv2 witness progress was checked above"),
    )
    .await
}
