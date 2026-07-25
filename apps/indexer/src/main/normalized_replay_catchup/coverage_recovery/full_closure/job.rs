#[path = "job/failure.rs"]
mod failure;
#[path = "job/reuse.rs"]
mod reuse;

use anyhow::{Context, Result, ensure};
use bigname_manifests::{
    UncoveredWatchedTuple, WatchedSourceSelector, WatchedTargetIdentity,
    load_historical_watched_contracts_for_target, resolve_watched_source_selector,
};
use bigname_storage::{
    BackfillJobRecord, BackfillLifecycleStatus, CoverageRecoveryFailureKey,
    CoverageRecoveryFailureRecord, CoverageRecoveryFailureState, CoverageRecoveryReservationFence,
};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;

use self::failure::{
    MAX_ATTEMPTS_PER_VIOLATION_GENERATION, persist_attempt_failure,
    reconcile_bound_unjournaled_attempt, reconcile_selected_failed_job, truncate_error,
};
use self::reuse::reusable_incomplete_job;
use super::super::super::{FullClosureCoverageViolations, NormalizedReplayHeartbeat};
use super::load_authority;
use crate::{
    backfill::{
        BackfillAdapterSyncMode, BackfillBlockRange, BackfillJobRunConfig, BackfillTopicPlan,
        CoinbaseSqlBackfillConfig, CoinbaseSqlSourceRegistry,
        create_verified_coinbase_sql_backfill_job, create_verified_hash_pinned_backfill_job,
        load_backfill_topic_plan, run_precreated_verified_coinbase_sql_backfill_job,
        run_precreated_verified_coinbase_sql_backfill_job_with_progress,
        run_precreated_verified_hash_pinned_backfill_job,
        run_precreated_verified_hash_pinned_backfill_job_with_progress,
        verified_backfill_job_source_identity_payload,
    },
    backfill_lease_expires_at, default_backfill_lease_owner, generated_backfill_lease_token,
    provider::ChainProviderOps,
    reconciliation::HeaderAuditMode,
};

const FULL_CLOSURE_JOB_KEY_PREFIX: &str = "indexer-full-closure-coverage-recovery:v2:";
const LEASE_DURATION_SECS: u64 = 300;

pub(super) enum ViolationRecoveryOutcome {
    Completed {
        job_id: i64,
        attempted: bool,
    },
    Failed {
        job_id: i64,
        error: String,
    },
    Deferred {
        job_id: Option<i64>,
    },
    Terminal {
        job_id: Option<i64>,
        cause: String,
        attempted: bool,
    },
    Pending {
        job_id: i64,
    },
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn recover_one_violation(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    provider: &(impl ChainProviderOps + ?Sized),
    coinbase_sql_recovery: Option<(&CoinbaseSqlSourceRegistry, &CoinbaseSqlBackfillConfig)>,
    hash_pinned_chunk_blocks: i64,
    header_audit_mode: HeaderAuditMode,
    requirement: &FullClosureCoverageViolations,
    violation: &UncoveredWatchedTuple,
    failure_key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
    persisted_failure: Option<&CoverageRecoveryFailureRecord>,
    allow_provider_attempt: bool,
    provider_attempted: &mut bool,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<ViolationRecoveryOutcome> {
    if let Some(outcome) =
        reconcile_bound_unjournaled_attempt(pool, failure_key, expected_epoch).await?
    {
        return Ok(outcome);
    }
    let range =
        BackfillBlockRange::new(violation.required_from_block, violation.required_to_block)?;
    let source_plan = resolve_exact_source_plan(
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
    let source_identity =
        verified_backfill_job_source_identity_payload(&source_plan, &topic_plan, coinbase_config)?;
    let source_identity_hash = source_identity
        .get("source_identity_hash")
        .and_then(Value::as_str)
        .context("full-closure recovery source identity is missing its hash")?;
    let range_input_revision = load_range_input_revision(pool, &requirement.chain, range).await?;
    let mut config = BackfillJobRunConfig {
        deployment_profile: deployment_profile.to_owned(),
        idempotency_key: format!(
            "{FULL_CLOSURE_JOB_KEY_PREFIX}deployment_profile={deployment_profile}:chain={}:source_identity_hash={source_identity_hash}:from={}:to={}:coverage_recovery_write_epoch={expected_epoch}:range_raw_log_input_revision={range_input_revision}",
            requirement.chain, range.from_block, range.to_block,
        ),
        scope_idempotency_to_raw_log_retention_generation: true,
        coverage_recovery_reservation_fence: None,
        range,
        lease_owner: format!(
            "{}:full-closure-coverage-recovery",
            default_backfill_lease_owner()
        ),
        lease_token: generated_backfill_lease_token()?,
        lease_expires_at: backfill_lease_expires_at(LEASE_DURATION_SECS)?,
        hash_pinned_chunk_blocks,
        adapter_sync_mode: BackfillAdapterSyncMode::RawOnly,
        header_audit_mode,
    };
    let uses_coinbase_sql = coinbase_config.is_some();
    let record = match reusable_incomplete_job(
        pool,
        persisted_failure,
        deployment_profile,
        requirement,
        range,
        &source_identity,
        expected_epoch,
    )
    .await?
    {
        Some(record) => record,
        None => create_job(pool, &source_plan, &config, &topic_plan, coinbase_config).await?,
    };
    bigname_storage::bind_coverage_recovery_job_write_epoch(
        pool,
        failure_key,
        expected_epoch,
        record.job.backfill_job_id,
    )
    .await?;
    config.coverage_recovery_reservation_fence = Some(CoverageRecoveryReservationFence {
        key: failure_key.clone(),
        expected_write_epoch: expected_epoch,
        expected_failure_attempt_count: persisted_failure
            .map_or(0, |failure| failure.attempt_count),
        expected_job_attempt_count: maximum_job_attempt_count(&record),
    });
    config
        .idempotency_key
        .clone_from(&record.job.idempotency_key);
    ensure!(
        record.job.raw_log_retention_generation == requirement.retention_generation,
        "full-closure recovery job {} captured retention generation {}, expected {}",
        record.job.backfill_job_id,
        record.job.raw_log_retention_generation,
        requirement.retention_generation
    );
    let observed_authority = load_authority(pool, &requirement.chain).await?;
    if observed_authority.retention_generation != requirement.retention_generation {
        if record.job.status != BackfillLifecycleStatus::Completed {
            bigname_storage::fail_backfill_job(
                pool,
                record.job.backfill_job_id,
                "full-closure coverage recovery retention generation changed before execution",
                json!({
                    "phase": "coverage_recovery_creation_fence",
                    "state": "terminal",
                    "cause": "obsolete_retention_generation",
                    "expected_retention_generation": requirement.retention_generation,
                    "observed_retention_generation": observed_authority.retention_generation,
                }),
            )
            .await?;
        }
        anyhow::bail!(
            "retention generation changed after creating recovery job {}: expected {}, observed {}",
            record.job.backfill_job_id,
            requirement.retention_generation,
            observed_authority.retention_generation
        );
    }
    let observed_epoch = bigname_storage::load_coverage_recovery_epoch(pool, failure_key).await?;
    if observed_epoch != expected_epoch {
        anyhow::bail!(
            "coverage recovery write epoch changed after preparing job {}: expected {}, observed {}",
            record.job.backfill_job_id,
            expected_epoch,
            observed_epoch
        );
    }

    if !topic_plan.source_family_has_topics(&violation.source_family) {
        let reason = format!(
            "full-closure coverage recovery cannot fetch source family {} because it has no active event topic0 values",
            violation.source_family
        );
        let metadata = json!({
            "phase": "coverage_recovery_prepare",
            "state": "terminal",
            "cause": "source_family_without_active_event_topic0",
            "attempt_count": maximum_job_attempt_count(&record),
        });
        bigname_storage::record_coverage_recovery_terminal_failure(
            pool,
            failure_key,
            expected_epoch,
            Some(record.job.backfill_job_id),
            maximum_job_attempt_count(&record),
            &reason,
            metadata,
        )
        .await?;
        return Ok(ViolationRecoveryOutcome::Terminal {
            job_id: Some(record.job.backfill_job_id),
            cause: reason,
            attempted: false,
        });
    }

    if record.job.status == BackfillLifecycleStatus::Completed {
        bigname_storage::clear_coverage_recovery_failure(pool, failure_key, expected_epoch).await?;
        return Ok(ViolationRecoveryOutcome::Completed {
            job_id: record.job.backfill_job_id,
            attempted: false,
        });
    }
    if let Some(outcome) =
        reconcile_selected_failed_job(pool, failure_key, expected_epoch, &record).await?
    {
        return Ok(outcome);
    }
    if !allow_provider_attempt {
        return Ok(ViolationRecoveryOutcome::Pending {
            job_id: record.job.backfill_job_id,
        });
    }
    if record.ranges.iter().any(|range| {
        matches!(
            range.status,
            BackfillLifecycleStatus::Reserved | BackfillLifecycleStatus::Running
        ) && range
            .lease_expires_at
            .is_some_and(|expires_at| expires_at > OffsetDateTime::now_utc())
    }) {
        return Ok(ViolationRecoveryOutcome::Deferred {
            job_id: Some(record.job.backfill_job_id),
        });
    }

    #[cfg(test)]
    crate::normalized_replay_catchup::test_hook::pause_before_coverage_attempt(
        pool,
        deployment_profile,
        &requirement.chain,
    )
    .await;

    *provider_attempted = true;
    let outcome = execute_job(
        pool,
        provider,
        coinbase_sql_recovery,
        requirement,
        failure_key,
        expected_epoch,
        source_plan,
        topic_plan,
        config,
        record,
        uses_coinbase_sql,
        progress,
    )
    .await;
    if matches!(&outcome, Ok(ViolationRecoveryOutcome::Deferred { .. })) {
        *provider_attempted = false;
    }
    outcome
}

#[expect(clippy::too_many_arguments)]
async fn execute_job(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    coinbase_sql_recovery: Option<(&CoinbaseSqlSourceRegistry, &CoinbaseSqlBackfillConfig)>,
    requirement: &FullClosureCoverageViolations,
    failure_key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
    source_plan: bigname_manifests::WatchedSourceSelectorPlan,
    topic_plan: BackfillTopicPlan,
    config: BackfillJobRunConfig,
    record: BackfillJobRecord,
    uses_coinbase_sql: bool,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<ViolationRecoveryOutcome> {
    let job_id = record.job.backfill_job_id;
    let attempted_lease_token = config.lease_token.clone();
    let mut failure_record = record.clone();
    let execution_result = if uses_coinbase_sql {
        let (registry, coinbase_config) =
            coinbase_sql_recovery.context("Coinbase SQL registry disappeared")?;
        let historical_source = registry
            .source_for(&requirement.chain)?
            .context("configured Coinbase SQL recovery source disappeared")?
            .with_query_attempt_recorder(pool.clone(), job_id);
        match progress.as_deref_mut() {
            Some(heartbeat) => {
                run_precreated_verified_coinbase_sql_backfill_job_with_progress(
                    pool,
                    &source_plan,
                    provider,
                    &historical_source,
                    config,
                    coinbase_config.clone(),
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
                    coinbase_config.clone(),
                    topic_plan,
                    record,
                )
                .await
            }
        }
    } else {
        match progress.as_deref_mut() {
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
        }
    };
    match execution_result {
        Ok(_) => {
            bigname_storage::clear_coverage_recovery_failure(pool, failure_key, expected_epoch)
                .await?;
            Ok(ViolationRecoveryOutcome::Completed {
                job_id,
                attempted: true,
            })
        }
        Err(error) => {
            if error
                .downcast_ref::<bigname_storage::CoverageRecoveryReservationConflict>()
                .is_some()
            {
                return Ok(ViolationRecoveryOutcome::Deferred {
                    job_id: Some(job_id),
                });
            }
            failure_record.ranges = bigname_storage::load_backfill_ranges(pool, job_id).await?;
            if failure_record.ranges.iter().any(|range| {
                matches!(
                    range.status,
                    BackfillLifecycleStatus::Reserved | BackfillLifecycleStatus::Running
                ) && range
                    .lease_expires_at
                    .is_some_and(|expires_at| expires_at > OffsetDateTime::now_utc())
                    && range.lease_token.as_deref() != Some(attempted_lease_token.as_str())
            }) {
                return Ok(ViolationRecoveryOutcome::Deferred {
                    job_id: Some(job_id),
                });
            }
            let current_job = bigname_storage::load_backfill_job(pool, job_id)
                .await?
                .context("coverage recovery job disappeared after execution")?;
            if current_job.status == BackfillLifecycleStatus::Completed {
                bigname_storage::clear_coverage_recovery_failure(pool, failure_key, expected_epoch)
                    .await?;
                return Ok(ViolationRecoveryOutcome::Completed {
                    job_id,
                    attempted: false,
                });
            }
            let error = format!("{error:#}");
            let failure = persist_attempt_failure(
                pool,
                failure_key,
                expected_epoch,
                &failure_record,
                "provider-backed recovery attempt failed",
                &error,
            )
            .await?;
            let error = format!(
                "{} (attempt {} of {}, state {})",
                truncate_error(&error),
                failure.attempt_count,
                MAX_ATTEMPTS_PER_VIOLATION_GENERATION,
                match failure.state {
                    CoverageRecoveryFailureState::RetryBackoff => "retry_backoff",
                    CoverageRecoveryFailureState::Terminal => "terminal",
                }
            );
            Ok(match failure.state {
                CoverageRecoveryFailureState::RetryBackoff => {
                    ViolationRecoveryOutcome::Failed { job_id, error }
                }
                CoverageRecoveryFailureState::Terminal => ViolationRecoveryOutcome::Terminal {
                    job_id: Some(job_id),
                    cause: failure.failure_reason,
                    attempted: true,
                },
            })
        }
    }
}

async fn create_job(
    pool: &sqlx::PgPool,
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    topic_plan: &BackfillTopicPlan,
    coinbase_config: Option<&CoinbaseSqlBackfillConfig>,
) -> Result<BackfillJobRecord> {
    match coinbase_config {
        Some(coinbase_config) => {
            create_verified_coinbase_sql_backfill_job(
                pool,
                source_plan,
                config,
                coinbase_config,
                topic_plan,
            )
            .await
        }
        None => {
            create_verified_hash_pinned_backfill_job(pool, source_plan, config, topic_plan).await
        }
    }
}

async fn load_range_input_revision(
    pool: &sqlx::PgPool,
    chain: &str,
    range: BackfillBlockRange,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COALESCE(MAX(revision), 0)::BIGINT
        FROM raw_log_staging_block_revisions
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
        "#,
    )
    .bind(chain)
    .bind(range.from_block)
    .bind(range.to_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load in-range raw-log revision for {chain} over {}..={}",
            range.from_block, range.to_block
        )
    })
}

async fn resolve_exact_source_plan(
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
    let source_plan = resolve_watched_source_selector(
        &historical_contracts,
        chain,
        WatchedSourceSelector::WatchedTargetSet(
            historical_contracts
                .iter()
                .map(|contract| WatchedTargetIdentity {
                    contract_instance_id: contract.contract_instance_id,
                })
                .collect(),
        ),
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

pub(super) fn maximum_job_attempt_count(record: &BackfillJobRecord) -> i64 {
    record
        .ranges
        .iter()
        .map(|range| range.attempt_count)
        .max()
        .unwrap_or(0)
}
