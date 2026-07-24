use anyhow::{Result, bail};
use sqlx::PgPool;
use tracing::info;

use super::{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, NormalizedReplayHeartbeat};
use crate::reconciliation::{
    FullClosureReplayLockWaitHeartbeat, RawFactNormalizedEventReplayOutcome,
    active_closure_or_dependency_replay_adapters,
    sync_automatic_two_phase_full_closure_normalized_events, unsupported_closure_replay_adapters,
};

#[expect(clippy::too_many_arguments)]
pub(super) async fn replay_full_closure_or_dependency_normalized_events(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    from_block: i64,
    to_block: i64,
    stateless_ranges: &[(i64, i64)],
    max_raw_logs_per_page: usize,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<RawFactNormalizedEventReplayOutcome> {
    let adapters = active_closure_or_dependency_replay_adapters(pool, chain).await?;
    let unsupported = unsupported_closure_replay_adapters(&adapters);
    if !unsupported.is_empty() {
        bail!(
            "normalized-event replay selected closure/context-dependent adapter(s) {}; full closure replay is not implemented for these adapters",
            unsupported.join(", ")
        );
    }
    info!(
        service = "indexer",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        chain,
        from_block,
        to_block,
        stateless_range_count = stateless_ranges.len(),
        stateless_ranges = ?stateless_ranges,
        max_raw_logs_per_page,
        adapter_count = adapters.len(),
        adapters = ?adapters,
        "two-phase full closure normalized-event replay session started"
    );

    let mut lock_wait_heartbeat_handle = progress.as_deref().cloned();
    let mut lock_wait_heartbeat: Option<&mut dyn FullClosureReplayLockWaitHeartbeat> =
        lock_wait_heartbeat_handle
            .as_mut()
            .map(|heartbeat| heartbeat as &mut dyn FullClosureReplayLockWaitHeartbeat);
    let mut stateless_progress = progress.as_deref().cloned();
    let mut closure_progress = progress.as_deref().cloned();
    let mut noop_stateless_progress = NoopNormalizedReplayProgress;
    let mut noop_closure_progress = NoopNormalizedReplayProgress;
    let replay = sync_automatic_two_phase_full_closure_normalized_events(
        pool,
        deployment_profile,
        chain,
        CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        from_block,
        to_block,
        stateless_ranges,
        &adapters,
        max_raw_logs_per_page,
        &mut lock_wait_heartbeat,
        stateless_progress
            .as_mut()
            .map_or(&mut noop_stateless_progress, |progress| progress),
        closure_progress
            .as_mut()
            .map_or(&mut noop_closure_progress, |progress| progress),
    )
    .await?;
    let stateless = replay.stateless;
    let closure = replay.closure;
    let mut stateless_replay_authority = stateless.stateless_replay_authority.clone();
    stateless_replay_authority.add(&closure.stateless_replay_authority);

    Ok(RawFactNormalizedEventReplayOutcome {
        deployment_profile: deployment_profile.to_owned(),
        chain: chain.to_owned(),
        selection_kind: "two_phase_full_closure",
        source_scope_target_count: adapters.len(),
        selected_block_count: stateless.selected_block_count,
        canonical_raw_log_count: stateless.canonical_raw_log_count,
        scanned_raw_log_count: stateless.scanned_raw_log_count + closure.scanned_log_count,
        matched_raw_log_count: stateless.matched_raw_log_count + closure.matched_log_count,
        normalized_event_synced_count: stateless.normalized_event_synced_count
            + closure.total_synced_count,
        normalized_event_inserted_count: stateless.normalized_event_inserted_count
            + closure.total_inserted_count,
        stateless_replay_authority,
    })
}

struct NoopNormalizedReplayProgress;

impl bigname_adapters::StartupAdapterProgress for NoopNormalizedReplayProgress {
    fn record<'a>(
        &'a mut self,
        _pool: &'a PgPool,
    ) -> bigname_adapters::StartupAdapterProgressFuture<'a> {
        Box::pin(std::future::ready(Ok(())))
    }
}

pub(super) async fn record_normalized_replay_progress(
    pool: &PgPool,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        bigname_adapters::StartupAdapterProgress::record(progress, pool).await?;
    }
    Ok(())
}
