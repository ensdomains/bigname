use anyhow::{Context, Result, ensure};
use bigname_manifests::{
    UncoveredWatchedTuple, WatchedSourceSelector, WatchedTargetIdentity,
    load_discovery_admission_epoch, load_historical_watched_contracts_for_target,
    resolve_watched_source_selector,
};
use bigname_storage::RawLogStagingInputVersion;
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use std::time::Duration;
use tracing::info;

use super::{
    CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, FullClosureCoverageViolations,
    NormalizedReplayHeartbeat, replay_full_closure_or_dependency_normalized_events,
};
use crate::{
    backfill::{
        BackfillAdapterSyncMode, BackfillBlockRange, BackfillJobRunConfig,
        CoinbaseSqlBackfillConfig, CoinbaseSqlSourceRegistry, STALE_BACKFILL_CLAIM_MAX_AGE_SECS,
        create_verified_coinbase_sql_backfill_job, create_verified_hash_pinned_backfill_job,
        load_backfill_topic_plan, run_precreated_verified_coinbase_sql_backfill_job,
        run_precreated_verified_coinbase_sql_backfill_job_with_progress,
        run_precreated_verified_hash_pinned_backfill_job,
        run_precreated_verified_hash_pinned_backfill_job_with_progress,
        verified_backfill_job_source_identity_payload,
    },
    backfill_lease_expires_at, default_backfill_lease_owner, generated_backfill_lease_token,
    provider::ChainProviderOps,
    reconciliation::{
        EnsV2LiveCoverageRecoveryStatus, HeaderAuditMode, RawFactNormalizedEventReplayOutcome,
        automatic_stateless_replay_completed, recover_ens_v2_live_coverage_requirement,
        recover_ens_v2_live_coverage_requirement_with_progress,
    },
};

const MAX_COVERAGE_RECOVERY_ATTEMPTS: usize = 32;
const MAX_FULL_CLOSURE_COVERAGE_JOBS_PER_ITERATION: usize = 4;
const FULL_CLOSURE_COVERAGE_RECOVERY_LEASE_DURATION_SECS: u64 = 300;
pub(super) async fn sweep_stale_backfill_claims_for_replay(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<()> {
    let stale_job_ids = bigname_storage::sweep_stale_backfill_claims(
        pool,
        chain,
        OffsetDateTime::now_utc() - Duration::from_secs(STALE_BACKFILL_CLAIM_MAX_AGE_SECS as u64),
    )
    .await?;
    if !stale_job_ids.is_empty() {
        info!(
            service = "indexer",
            command = "run",
            replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
            chain,
            backfill_job_ids = ?stale_job_ids,
            stale_after_secs = STALE_BACKFILL_CLAIM_MAX_AGE_SECS,
            "released stale backfill claims for ordinary lease re-claim"
        );
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FullClosureRecoveryAuthority {
    retention_generation: i64,
    raw_log_input_revision: i64,
    discovery_admission_epoch: i64,
}

async fn load_full_closure_recovery_authority(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<FullClosureRecoveryAuthority> {
    let input_version = bigname_storage::load_raw_log_staging_input_version(pool, chain).await?;
    Ok(FullClosureRecoveryAuthority {
        retention_generation: input_version.retention_generation,
        raw_log_input_revision: input_version.revision,
        discovery_admission_epoch: load_discovery_admission_epoch(pool, chain).await?,
    })
}

async fn resolve_exact_coverage_recovery_source_plan(
    pool: &sqlx::PgPool,
    chain: &str,
    source_family: &str,
    address: &str,
    range: BackfillBlockRange,
) -> Result<bigname_manifests::WatchedSourceSelectorPlan> {
    let historical_contracts =
        load_historical_watched_contracts_for_target(pool, chain, source_family, address)
            .await?
            .into_iter()
            .filter(|contract| {
                contract
                    .active_from_block_number
                    .is_none_or(|from| from <= range.to_block)
                    && contract
                        .active_to_block_number
                        .is_none_or(|to| to >= range.from_block)
            })
            .collect::<Vec<_>>();
    ensure!(
        !historical_contracts.is_empty(),
        "coverage recovery cannot resolve watched target {source_family} {address} on {chain} over {}..={}",
        range.from_block,
        range.to_block
    );
    let selected_targets = historical_contracts
        .iter()
        .map(|contract| WatchedTargetIdentity {
            contract_instance_id: contract.contract_instance_id,
        })
        .collect::<Vec<_>>();
    let source_plan = resolve_watched_source_selector(
        &historical_contracts,
        chain,
        WatchedSourceSelector::WatchedTargetSet(selected_targets),
        range.from_block,
        range.to_block,
    )?;
    ensure!(
        source_plan.selected_targets.iter().all(|target| {
            target.source_family == source_family
                && target.address.eq_ignore_ascii_case(address)
                && target.effective_from_block >= range.from_block
                && target.effective_to_block <= range.to_block
        }),
        "coverage recovery selected authority outside exact target {source_family} {address} over {}..={}",
        range.from_block,
        range.to_block
    );
    Ok(source_plan)
}

#[expect(clippy::too_many_arguments)]
async fn recover_full_closure_coverage_batch(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    provider: &(impl ChainProviderOps + ?Sized),
    coinbase_sql_recovery: Option<(&CoinbaseSqlSourceRegistry, &CoinbaseSqlBackfillConfig)>,
    hash_pinned_chunk_blocks: i64,
    header_audit_mode: HeaderAuditMode,
    requirement: &FullClosureCoverageViolations,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<Vec<i64>> {
    ensure!(
        !requirement.violations.is_empty(),
        "full-closure coverage recovery received an empty violation set"
    );
    let initial_authority = load_full_closure_recovery_authority(pool, &requirement.chain).await?;
    ensure!(
        initial_authority.retention_generation == requirement.retention_generation,
        "full-closure coverage recovery authority changed before job creation on {}: expected retention generation {}, observed {}",
        requirement.chain,
        requirement.retention_generation,
        initial_authority.retention_generation
    );

    let mut job_ids = Vec::new();
    for violation in requirement
        .violations
        .iter()
        .take(MAX_FULL_CLOSURE_COVERAGE_JOBS_PER_ITERATION)
    {
        let job_id = recover_one_full_closure_violation(
            pool,
            deployment_profile,
            provider,
            coinbase_sql_recovery,
            hash_pinned_chunk_blocks,
            header_audit_mode,
            requirement,
            violation,
            initial_authority.raw_log_input_revision,
            progress,
        )
        .await
        .with_context(|| {
            format!("full-closure coverage recovery failed after enqueueing job ids {job_ids:?}")
        })?;
        job_ids.push(job_id);
    }

    let final_authority = load_full_closure_recovery_authority(pool, &requirement.chain).await?;
    ensure!(
        final_authority.retention_generation == initial_authority.retention_generation
            && final_authority.discovery_admission_epoch
                == initial_authority.discovery_admission_epoch,
        "full-closure retention generation or discovery authority changed while recovery jobs {job_ids:?} ran on {}; replan from current authority",
        requirement.chain
    );
    Ok(job_ids)
}

#[expect(clippy::too_many_arguments)]
async fn recover_one_full_closure_violation(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    provider: &(impl ChainProviderOps + ?Sized),
    coinbase_sql_recovery: Option<(&CoinbaseSqlSourceRegistry, &CoinbaseSqlBackfillConfig)>,
    hash_pinned_chunk_blocks: i64,
    header_audit_mode: HeaderAuditMode,
    requirement: &FullClosureCoverageViolations,
    violation: &UncoveredWatchedTuple,
    recovery_raw_log_input_revision: i64,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<i64> {
    let range =
        BackfillBlockRange::new(violation.required_from_block, violation.required_to_block)?;
    let source_plan = resolve_exact_coverage_recovery_source_plan(
        pool,
        &requirement.chain,
        &violation.source_family,
        &violation.address,
        range,
    )
    .await?;
    let topic_plan = load_backfill_topic_plan(pool, &source_plan).await?;
    let coinbase_config = coinbase_sql_recovery.and_then(|(registry, config)| {
        registry
            .has_source_for(&requirement.chain)
            .then_some(config)
    });
    let uses_coinbase_sql = coinbase_config.is_some();
    let source_identity =
        verified_backfill_job_source_identity_payload(&source_plan, &topic_plan, coinbase_config)?;
    let source_identity_hash = source_identity
        .get("source_identity_hash")
        .and_then(Value::as_str)
        .context("full-closure recovery source identity is missing its hash")?;
    let config = BackfillJobRunConfig {
        deployment_profile: deployment_profile.to_owned(),
        idempotency_key: format!(
            "indexer-full-closure-coverage-recovery:v1:deployment_profile={deployment_profile}:chain={}:source_identity_hash={source_identity_hash}:from={}:to={}:raw_log_input_revision={recovery_raw_log_input_revision}",
            requirement.chain, range.from_block, range.to_block,
        ),
        scope_idempotency_to_raw_log_retention_generation: true,
        range,
        lease_owner: format!(
            "{}:full-closure-coverage-recovery",
            default_backfill_lease_owner()
        ),
        lease_token: generated_backfill_lease_token()?,
        lease_expires_at: backfill_lease_expires_at(
            FULL_CLOSURE_COVERAGE_RECOVERY_LEASE_DURATION_SECS,
        )?,
        hash_pinned_chunk_blocks,
        adapter_sync_mode: BackfillAdapterSyncMode::RawOnly,
        header_audit_mode,
    };

    let job_id = if uses_coinbase_sql {
        let (registry, coinbase_config) =
            coinbase_sql_recovery.expect("Coinbase SQL registry was checked");
        let coinbase_config = coinbase_config.clone();
        let record = create_verified_coinbase_sql_backfill_job(
            pool,
            &source_plan,
            &config,
            &coinbase_config,
            &topic_plan,
        )
        .await?;
        ensure!(
            record.job.raw_log_retention_generation == requirement.retention_generation,
            "full-closure recovery job {} captured retention generation {}, expected {}",
            record.job.backfill_job_id,
            record.job.raw_log_retention_generation,
            requirement.retention_generation
        );
        let job_id = record.job.backfill_job_id;
        let historical_source = registry
            .source_for(&requirement.chain)?
            .context("configured Coinbase SQL recovery source disappeared")?
            .with_query_attempt_recorder(pool.clone(), job_id);
        let execution_result = match progress.as_deref_mut() {
            Some(heartbeat) => {
                run_precreated_verified_coinbase_sql_backfill_job_with_progress(
                    pool,
                    &source_plan,
                    provider,
                    &historical_source,
                    config,
                    coinbase_config,
                    topic_plan,
                    record,
                    heartbeat,
                )
                .await
            }
            None => {
                run_precreated_verified_coinbase_sql_backfill_job(
                    pool,
                    &source_plan,
                    provider,
                    &historical_source,
                    config,
                    coinbase_config,
                    topic_plan,
                    record,
                )
                .await
            }
        };
        execution_result.with_context(|| {
            format!(
                "enqueued full-closure Coinbase SQL coverage recovery job id {job_id} failed for {} {} over {}..={}",
                violation.source_family,
                violation.address,
                range.from_block,
                range.to_block
            )
        })?;
        job_id
    } else {
        let record =
            create_verified_hash_pinned_backfill_job(pool, &source_plan, &config, &topic_plan)
                .await?;
        ensure!(
            record.job.raw_log_retention_generation == requirement.retention_generation,
            "full-closure recovery job {} captured retention generation {}, expected {}",
            record.job.backfill_job_id,
            record.job.raw_log_retention_generation,
            requirement.retention_generation
        );
        let job_id = record.job.backfill_job_id;
        let execution_result = match progress.as_deref_mut() {
            Some(heartbeat) => {
                run_precreated_verified_hash_pinned_backfill_job_with_progress(
                    pool,
                    &source_plan,
                    provider,
                    config,
                    topic_plan,
                    record,
                    heartbeat,
                )
                .await
            }
            None => {
                run_precreated_verified_hash_pinned_backfill_job(
                    pool,
                    &source_plan,
                    provider,
                    config,
                    topic_plan,
                    record,
                )
                .await
            }
        };
        execution_result.with_context(|| {
            format!(
                "enqueued full-closure hash-pinned coverage recovery job id {job_id} failed for {} {} over {}..={}",
                violation.source_family,
                violation.address,
                range.from_block,
                range.to_block
            )
        })?;
        job_id
    };

    Ok(job_id)
}

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
    coinbase_sql_recovery: Option<(&CoinbaseSqlSourceRegistry, &CoinbaseSqlBackfillConfig)>,
    hash_pinned_chunk_blocks: i64,
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
        if let Some(requirement) = replay_error
            .downcast_ref::<FullClosureCoverageViolations>()
            .cloned()
        {
            let Some(provider) = provider else {
                return Err(replay_error.context(format!(
                    "normalized replay cannot recover full-closure coverage violations on {chain}: no provider is configured"
                )));
            };
            let job_ids = match Box::pin(recover_full_closure_coverage_batch(
                pool,
                deployment_profile,
                provider,
                coinbase_sql_recovery,
                hash_pinned_chunk_blocks,
                header_audit_mode,
                &requirement,
                progress,
            ))
            .await
            {
                Ok(job_ids) => job_ids,
                Err(recovery_error) => {
                    return Err(replay_error.context(format!(
                        "automatic full-closure coverage recovery failed: {recovery_error:#}"
                    )));
                }
            };
            let remaining_reported = requirement.violations.len().saturating_sub(job_ids.len());
            return Err(replay_error.context(format!(
                "auto-enqueued and completed generation-bound full-closure coverage recovery job ids {job_ids:?}; processed at most {MAX_FULL_CLOSURE_COVERAGE_JOBS_PER_ITERATION} violations this iteration, {remaining_reported} reported violations remain{}; the next bounded catch-up iteration will reload coverage authority",
                if requirement.further_violations_elided {
                    " and further violations were elided"
                } else {
                    ""
                }
            )));
        }
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
