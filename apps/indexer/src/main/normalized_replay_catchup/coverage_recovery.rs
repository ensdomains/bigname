use anyhow::Result;
use bigname_storage::RawLogStagingInputVersion;
use tracing::info;

use super::{
    CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, replay_full_closure_or_dependency_normalized_events,
};
use crate::{
    provider::ChainProviderOps,
    reconciliation::{
        EnsV2LiveCoverageRecoveryStatus, HeaderAuditMode, RawFactNormalizedEventReplayOutcome,
        automatic_stateless_replay_completed, recover_ens_v2_live_coverage_requirement,
    },
};

const MAX_COVERAGE_RECOVERY_ATTEMPTS: usize = 32;

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
        let status = match recover_ens_v2_live_coverage_requirement(
            pool,
            deployment_profile,
            provider,
            header_audit_mode,
            &requirement,
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

        raw_log_input_version =
            bigname_storage::load_raw_log_staging_input_version(pool, chain).await?;
        // Preserve the original full span when preflight validation prevented
        // phase one from running. Once phase one completed, retain only every
        // exact span fetched by later recovery attempts. The stateful closure
        // pass still restarts over its complete span.
        if stateless_replay_completed {
            stateless_ranges.clear();
        }
        include_stateless_range(
            &mut stateless_ranges,
            requirement.required_from_block,
            requirement.required_to_block,
        );
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
            stateless_range_count = stateless_ranges.len(),
            stateless_ranges = ?stateless_ranges,
            "retrying unchanged normalized replay after exact generation-bound coverage recovery"
        );
    }
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
