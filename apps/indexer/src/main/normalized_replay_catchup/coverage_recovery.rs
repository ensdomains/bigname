use anyhow::Result;
use bigname_storage::RawLogStagingInputVersion;

use super::{
    FullClosureCoverageViolations, NormalizedReplayHeartbeat,
    replay_full_closure_or_dependency_normalized_events,
};
use crate::{
    backfill::{CoinbaseSqlBackfillConfig, CoinbaseSqlSourceRegistry},
    provider::ChainProviderOps,
    reconciliation::{HeaderAuditMode, RawFactNormalizedEventReplayOutcome},
};

#[path = "coverage_recovery/full_closure.rs"]
mod full_closure;

pub(super) use full_closure::sweep_stale_backfill_claims_for_replay;

#[expect(clippy::too_many_arguments)]
pub(super) async fn replay_full_closure_with_coverage_recovery(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    from_block: i64,
    to_block: i64,
    max_raw_logs_per_page: usize,
    provider: Option<&(impl ChainProviderOps + ?Sized)>,
    coinbase_sql_recovery: Option<(&CoinbaseSqlSourceRegistry, &CoinbaseSqlBackfillConfig)>,
    hash_pinned_chunk_blocks: i64,
    max_provider_attempts_per_iteration: usize,
    header_audit_mode: HeaderAuditMode,
    raw_log_input_version: RawLogStagingInputVersion,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<(
    RawFactNormalizedEventReplayOutcome,
    RawLogStagingInputVersion,
)> {
    let stateless_ranges = vec![(from_block, to_block)];
    let replay_result = replay_full_closure_or_dependency_normalized_events(
        pool,
        deployment_profile,
        chain,
        from_block,
        to_block,
        &stateless_ranges,
        max_raw_logs_per_page,
        progress,
    )
    .await;
    let replay_error = match replay_result {
        Ok(outcome) => return Ok((outcome, raw_log_input_version)),
        Err(error) => error,
    };
    if let Some(requirement) = replay_error
        .downcast_ref::<FullClosureCoverageViolations>()
        .cloned()
    {
        let Some(provider) = provider else {
            return Err(replay_error.context(format!(
                "normalized replay cannot recover full-closure coverage violations on {chain}: no provider is configured"
            )));
        };
        let batch = match Box::pin(full_closure::recover_full_closure_coverage_batch(
            pool,
            deployment_profile,
            provider,
            coinbase_sql_recovery,
            hash_pinned_chunk_blocks,
            max_provider_attempts_per_iteration,
            header_audit_mode,
            &requirement,
            progress,
        ))
        .await
        {
            Ok(batch) => batch,
            Err(recovery_error) => {
                return Err(replay_error.context(format!(
                    "automatic full-closure coverage recovery failed: {recovery_error:#}"
                )));
            }
        };
        let remaining_reported = requirement
            .violations
            .len()
            .saturating_sub(batch.completed_count());
        return Err(replay_error.context(format!(
            "auto-enqueued and completed generation-bound full-closure coverage recovery job ids are recorded in batch outcome: {}; at most {max_provider_attempts_per_iteration} provider attempts ran while deferred or terminal violations did not block later reported violations; {remaining_reported} reported violations remain{}; the next bounded catch-up iteration will reload coverage authority",
            batch.failure_record_summary(),
            if requirement.further_violations_elided {
                " and further violations were elided"
            } else {
                ""
            }
        )));
    }
    Err(replay_error)
}
