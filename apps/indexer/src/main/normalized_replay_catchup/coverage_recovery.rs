use anyhow::Result;
use bigname_storage::RawLogStagingInputVersion;
use tracing::info;

use super::{
    CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, NormalizedReplayHeartbeat,
    replay_full_closure_or_dependency_normalized_events,
};
use crate::{
    provider::ChainProviderOps,
    reconciliation::{
        EnsV2LiveCoverageRecoveryStatus, HeaderAuditMode, RawFactNormalizedEventReplayOutcome,
        automatic_stateless_replay_completed, recover_ens_v2_live_coverage_requirement,
        recover_ens_v2_live_coverage_requirement_with_progress,
    },
};

const MAX_COVERAGE_RECOVERY_ATTEMPTS: usize = 32;

pub(crate) async fn recover_ens_v2_live_coverage_requirement_for_replay(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    provider: &(impl ChainProviderOps + ?Sized),
    header_audit_mode: HeaderAuditMode,
    requirement: &bigname_adapters::EnsV2MissingCoverage,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<EnsV2LiveCoverageRecoveryStatus> {
    match progress.as_deref_mut() {
        Some(progress) => {
            recover_ens_v2_live_coverage_requirement_with_progress(
                pool,
                deployment_profile,
                provider,
                header_audit_mode,
                requirement,
                progress,
            )
            .await
        }
        None => {
            recover_ens_v2_live_coverage_requirement(
                pool,
                deployment_profile,
                provider,
                header_audit_mode,
                requirement,
            )
            .await
        }
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn replay_full_closure_with_coverage_recovery(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    from_block: i64,
    to_block: i64,
    max_raw_logs_per_page: usize,
    provider: Option<&(impl ChainProviderOps + ?Sized)>,
    header_audit_mode: HeaderAuditMode,
    mut raw_log_input_version: RawLogStagingInputVersion,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<(
    RawFactNormalizedEventReplayOutcome,
    RawLogStagingInputVersion,
)> {
    let mut recovery_attempt = 0_usize;
    let mut stateless_ranges = vec![(from_block, to_block)];
    loop {
        let replay_error = match replay_full_closure_or_dependency_normalized_events(
            pool,
            deployment_profile,
            chain,
            from_block,
            to_block,
            &stateless_ranges,
            max_raw_logs_per_page,
            progress,
        )
        .await
        {
            Ok(outcome) => return Ok((outcome, raw_log_input_version)),
            Err(error) => error,
        };
        let stateless_replay_completed = automatic_stateless_replay_completed(&replay_error);
        let Some(requirement) = bigname_adapters::ens_v2_missing_coverage(&replay_error).cloned()
        else {
            return Err(replay_error);
        };
        if recovery_attempt >= MAX_COVERAGE_RECOVERY_ATTEMPTS {
            return Err(replay_error.context(format!(
                "normalized replay ENSv2 coverage recovery did not converge within {MAX_COVERAGE_RECOVERY_ATTEMPTS} attempts"
            )));
        }
        let Some(provider) = provider else {
            return Err(replay_error.context(format!(
                "normalized replay cannot recover missing ENSv2 coverage on {chain}: no provider is configured"
            )));
        };

        recovery_attempt += 1;
        let status = match recover_ens_v2_live_coverage_requirement_for_replay(
            pool,
            deployment_profile,
            provider,
            header_audit_mode,
            &requirement,
            progress,
        )
        .await
        {
            Ok(status) => status,
            Err(recovery_error) => {
                return Err(replay_error.context(format!(
                    "provider-backed normalized replay ENSv2 coverage recovery failed: {recovery_error:#}"
                )));
            }
        };
        if status == EnsV2LiveCoverageRecoveryStatus::AuthorityChanged {
            return Err(replay_error.context(
                "ENSv2 retention generation or discovery authority changed during normalized replay coverage recovery; replan the replay from current authority",
            ));
        }

        // Preserve the original full span when preflight validation prevented
        // phase one from running. Once phase one completed, retain only every
        // exact span fetched by later recovery attempts. The stateful adapter
        // pass still restarts over its complete span.
        if stateless_replay_completed {
            stateless_ranges.clear();
        }
        include_stateless_range(
            &mut stateless_ranges,
            requirement.required_from_block,
            requirement.required_to_block,
        );
        #[cfg(test)]
        super::test_hook::pause_after_coverage_recovery(pool, deployment_profile, chain).await;

        let observed_raw_log_input_version =
            bigname_storage::load_raw_log_staging_input_version(pool, chain).await?;
        if observed_raw_log_input_version.retention_generation
            != raw_log_input_version.retention_generation
        {
            return Err(replay_error.context(format!(
                "raw-log retention generation changed during normalized replay coverage recovery: expected {}, observed {}; replan the replay from current authority",
                raw_log_input_version.retention_generation,
                observed_raw_log_input_version.retention_generation,
            )));
        }
        if from_block > 0
            && raw_log_changed_outside_stateless_ranges(
                pool,
                chain,
                raw_log_input_version.revision,
                &stateless_ranges,
                0,
                from_block - 1,
            )
            .await?
        {
            return Err(replay_error.context(format!(
                "raw-log staging input changed below normalized replay range start {from_block} during coverage recovery; replan from the durable cursor"
            )));
        }
        let widened_for_concurrent_input = raw_log_changed_outside_stateless_ranges(
            pool,
            chain,
            raw_log_input_version.revision,
            &stateless_ranges,
            from_block,
            to_block,
        )
        .await?;
        if widened_for_concurrent_input {
            include_stateless_range(&mut stateless_ranges, from_block, to_block);
        }
        raw_log_input_version = observed_raw_log_input_version;
        info!(
            service = "indexer",
            command = "run",
            replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
            chain,
            source_family = %requirement.source_family,
            address = %requirement.address,
            from_block = requirement.required_from_block,
            to_block = requirement.required_to_block,
            retention_generation = requirement.retention_generation,
            recovery_attempt,
            widened_for_concurrent_input,
            stateless_range_count = stateless_ranges.len(),
            stateless_ranges = ?stateless_ranges,
            "retrying unchanged normalized replay after exact generation-bound coverage recovery"
        );
    }
}

async fn raw_log_changed_outside_stateless_ranges(
    pool: &sqlx::PgPool,
    chain: &str,
    revision: i64,
    stateless_ranges: &[(i64, i64)],
    inspected_from_block: i64,
    inspected_to_block: i64,
) -> Result<bool> {
    let mut next_uncovered_block = inspected_from_block;
    for &(range_from_block, range_to_block) in stateless_ranges {
        if range_to_block < next_uncovered_block {
            continue;
        }
        if range_from_block > inspected_to_block {
            break;
        }
        let covered_from_block = range_from_block.max(inspected_from_block);
        if next_uncovered_block < covered_from_block
            && bigname_storage::raw_log_staging_block_range_changed_since(
                pool,
                chain,
                revision,
                next_uncovered_block,
                covered_from_block - 1,
            )
            .await?
        {
            return Ok(true);
        }
        let Some(after_covered_block) = range_to_block.checked_add(1) else {
            return Ok(false);
        };
        next_uncovered_block = next_uncovered_block.max(after_covered_block);
        if next_uncovered_block > inspected_to_block {
            return Ok(false);
        }
    }
    bigname_storage::raw_log_staging_block_range_changed_since(
        pool,
        chain,
        revision,
        next_uncovered_block,
        inspected_to_block,
    )
    .await
}

fn include_stateless_range(ranges: &mut Vec<(i64, i64)>, from_block: i64, to_block: i64) {
    debug_assert!(from_block <= to_block);
    ranges.push((from_block, to_block));
    ranges.sort_unstable();

    let mut merged = Vec::<(i64, i64)>::with_capacity(ranges.len());
    for (from_block, to_block) in ranges.drain(..) {
        if let Some((_, merged_to_block)) = merged.last_mut()
            && (from_block <= *merged_to_block
                || merged_to_block.checked_add(1) == Some(from_block))
        {
            *merged_to_block = (*merged_to_block).max(to_block);
        } else {
            merged.push((from_block, to_block));
        }
    }
    *ranges = merged;
}
