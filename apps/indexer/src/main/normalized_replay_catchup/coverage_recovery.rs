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
        recover_ens_v2_live_coverage_requirement,
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
    loop {
        let replay_error = match replay_full_closure_or_dependency_normalized_events(
            pool,
            deployment_profile,
            chain,
            from_block,
            to_block,
            max_raw_logs_per_page,
        )
        .await
        {
            Ok(outcome) => return Ok((outcome, raw_log_input_version)),
            Err(error) => error,
        };
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
            "retrying unchanged normalized replay after exact generation-bound coverage recovery"
        );
    }
}
