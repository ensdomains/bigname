use std::time::Duration;

use anyhow::Result;
use bigname_storage::{
    BackfillJobRecord, CoverageRecoveryFailureKey, CoverageRecoveryFailureRecord,
    CoverageRecoveryFailureState,
};
use serde_json::json;

use super::{ViolationRecoveryOutcome, maximum_job_attempt_count};

pub(super) const MAX_ATTEMPTS_PER_VIOLATION_GENERATION: i64 = 32;
const INITIAL_RETRY_BACKOFF_SECS: u64 = 5;
const MAXIMUM_RETRY_BACKOFF_SECS: u64 = 300;
const MAX_RECORDED_ERROR_CHARS: usize = 2_048;

pub(super) async fn persist_attempt_failure(
    pool: &sqlx::PgPool,
    failure_key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
    record: &BackfillJobRecord,
    reason: &str,
    error: &str,
) -> Result<CoverageRecoveryFailureRecord> {
    let job_attempt_count = maximum_job_attempt_count(record);
    if job_attempt_count == 0 {
        let terminal_reason =
            "full-closure coverage recovery failed before reserving a persisted attempt";
        let metadata = json!({
            "phase": "coverage_recovery_execute",
            "state": "terminal",
            "cause": "attempt_not_reserved",
            "error": truncate_error(error),
        });
        let failure = bigname_storage::record_coverage_recovery_terminal_failure(
            pool,
            failure_key,
            expected_epoch,
            Some(record.job.backfill_job_id),
            0,
            terminal_reason,
            metadata,
        )
        .await?;
        return Ok(failure);
    }
    let terminal_reason = format!(
        "full-closure coverage recovery exhausted its {}-attempt budget",
        MAX_ATTEMPTS_PER_VIOLATION_GENERATION
    );
    let failure = bigname_storage::record_coverage_recovery_attempt_failure(
        pool,
        failure_key,
        expected_epoch,
        record.job.backfill_job_id,
        job_attempt_count,
        MAX_ATTEMPTS_PER_VIOLATION_GENERATION,
        Duration::from_secs(INITIAL_RETRY_BACKOFF_SECS),
        Duration::from_secs(MAXIMUM_RETRY_BACKOFF_SECS),
        reason,
        &terminal_reason,
        json!({
            "phase": "coverage_recovery_execute",
            "error": truncate_error(error),
            "maximum_attempts": MAX_ATTEMPTS_PER_VIOLATION_GENERATION,
        }),
    )
    .await?;
    if failure.state == CoverageRecoveryFailureState::Terminal {
        #[cfg(test)]
        crate::normalized_replay_catchup::test_hook::pause_after_terminal_failure_record(
            pool,
            &failure_key.deployment_profile,
            &failure_key.chain_id,
        )
        .await;
    }
    Ok(failure)
}

pub(super) async fn reconcile_bound_unjournaled_attempt(
    pool: &sqlx::PgPool,
    failure_key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
) -> Result<Option<ViolationRecoveryOutcome>> {
    let Some(record) = bigname_storage::load_bound_coverage_recovery_job_with_unjournaled_attempt(
        pool,
        failure_key,
        expected_epoch,
    )
    .await?
    else {
        return Ok(None);
    };
    let reason = record
        .job
        .failure_reason
        .as_deref()
        .unwrap_or("stale coverage recovery attempt failed before its journal was written");
    let failure = persist_attempt_failure(
        pool,
        failure_key,
        expected_epoch,
        &record,
        reason,
        &record.job.failure_metadata.to_string(),
    )
    .await?;
    Ok(Some(outcome_for_persisted_failure(failure)))
}

pub(super) async fn reconcile_selected_failed_job(
    pool: &sqlx::PgPool,
    failure_key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
    record: &BackfillJobRecord,
) -> Result<Option<ViolationRecoveryOutcome>> {
    if record.job.status != bigname_storage::BackfillLifecycleStatus::Failed {
        return Ok(None);
    }
    let job_attempt_count = maximum_job_attempt_count(record);
    let observed_attempt_count = bigname_storage::load_coverage_recovery_job_attempt_watermark(
        pool,
        failure_key,
        record.job.backfill_job_id,
    )
    .await?;
    if job_attempt_count == 0 || job_attempt_count <= observed_attempt_count {
        return Ok(None);
    }
    let reason = record
        .job
        .failure_reason
        .as_deref()
        .unwrap_or("reclaimed coverage recovery job failed before retry");
    let failure = persist_attempt_failure(
        pool,
        failure_key,
        expected_epoch,
        record,
        reason,
        &record.job.failure_metadata.to_string(),
    )
    .await?;
    Ok(Some(outcome_for_persisted_failure(failure)))
}

pub(super) fn outcome_for_persisted_failure(
    failure: CoverageRecoveryFailureRecord,
) -> ViolationRecoveryOutcome {
    match failure.state {
        CoverageRecoveryFailureState::RetryBackoff => ViolationRecoveryOutcome::Deferred {
            job_id: failure.last_backfill_job_id,
        },
        CoverageRecoveryFailureState::Terminal => ViolationRecoveryOutcome::Terminal {
            job_id: failure.last_backfill_job_id,
            cause: failure.failure_reason,
            attempted: false,
        },
    }
}

pub(super) fn truncate_error(error: &str) -> String {
    let mut truncated = error
        .chars()
        .take(MAX_RECORDED_ERROR_CHARS)
        .collect::<String>();
    if error.chars().count() > MAX_RECORDED_ERROR_CHARS {
        truncated.push_str("...[truncated]");
    }
    truncated
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::{Context, Result};
    use bigname_storage::{BackfillJobCreate, BackfillRangeSpec};
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn crashed_bound_attempt_is_journaled_before_identity_switch() -> Result<()> {
        for existing_failure in [false, true] {
            assert_crashed_attempt_can_switch_identity(existing_failure).await?;
        }
        Ok(())
    }

    async fn assert_crashed_attempt_can_switch_identity(existing_failure: bool) -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("indexer_coverage_recovery_crashed_identity_switch"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for coverage recovery crash test",
        )
        .await?;
        let key = CoverageRecoveryFailureKey {
            deployment_profile: "mainnet".to_owned(),
            chain_id: "eth-mainnet".to_owned(),
            raw_log_retention_generation: 0,
            source_family: "ens_v1_registry_l1".to_owned(),
            emitting_address: "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            required_from_block: 100,
            required_to_block: 120,
        };
        let first = bigname_storage::create_generation_scoped_backfill_job(
            database.pool(),
            &recovery_job(&key, "identity-a", "0xtopic-a"),
        )
        .await?;
        bigname_storage::bind_coverage_recovery_job_write_epoch(
            database.pool(),
            &key,
            0,
            first.job.backfill_job_id,
        )
        .await?;

        let crashed_attempt = if existing_failure { 2 } else { 1 };
        mark_job_failed(
            database.pool(),
            first.job.backfill_job_id,
            if existing_failure { 1 } else { crashed_attempt },
        )
        .await?;
        if existing_failure {
            bigname_storage::record_coverage_recovery_attempt_failure(
                database.pool(),
                &key,
                0,
                first.job.backfill_job_id,
                1,
                MAX_ATTEMPTS_PER_VIOLATION_GENERATION,
                Duration::from_secs(5),
                Duration::from_secs(300),
                "first identity failed",
                "coverage recovery attempt budget exhausted",
                json!({"phase": "coverage_recovery_execute"}),
            )
            .await?;
            mark_job_failed(database.pool(), first.job.backfill_job_id, crashed_attempt).await?;
        }
        let second = bigname_storage::create_generation_scoped_backfill_job(
            database.pool(),
            &recovery_job(&key, "identity-b", "0xtopic-b"),
        )
        .await?;

        let outcome = reconcile_bound_unjournaled_attempt(database.pool(), &key, 0).await?;
        assert!(
            matches!(
                outcome,
                Some(ViolationRecoveryOutcome::Deferred { job_id: Some(job_id) })
                    if job_id == first.job.backfill_job_id
            ),
            "the stale attempt must be journaled and deferred before choosing the new identity"
        );
        let failure = bigname_storage::load_coverage_recovery_failure(database.pool(), &key)
            .await?
            .context("crashed attempt was not journaled")?;
        assert_eq!(failure.attempt_count, crashed_attempt);
        assert_eq!(
            bigname_storage::load_coverage_recovery_job_attempt_watermark(
                database.pool(),
                &key,
                first.job.backfill_job_id,
            )
            .await?,
            crashed_attempt
        );

        bigname_storage::bind_coverage_recovery_job_write_epoch(
            database.pool(),
            &key,
            0,
            second.job.backfill_job_id,
        )
        .await?;
        let bindings = sqlx::query_as::<_, (i64, Option<i64>)>(
            r#"
            SELECT backfill_job_id, coverage_recovery_write_epoch
            FROM backfill_jobs
            WHERE backfill_job_id IN ($1, $2)
            ORDER BY backfill_job_id
            "#,
        )
        .bind(first.job.backfill_job_id)
        .bind(second.job.backfill_job_id)
        .fetch_all(database.pool())
        .await?;
        assert_eq!(
            bindings,
            vec![
                (first.job.backfill_job_id, None),
                (second.job.backfill_job_id, Some(0)),
            ],
            "journaling the crashed attempt must let the current identity supersede the old revision"
        );
        database.cleanup().await
    }

    fn recovery_job(
        key: &CoverageRecoveryFailureKey,
        identity: &str,
        topic: &str,
    ) -> BackfillJobCreate {
        BackfillJobCreate {
            deployment_profile: key.deployment_profile.clone(),
            chain_id: key.chain_id.clone(),
            source_identity: json!({
                "identity": identity,
                "selected_targets": [{
                    "source_family": key.source_family,
                    "address": key.emitting_address,
                    "effective_from_block": key.required_from_block,
                    "effective_to_block": key.required_to_block,
                }],
                "topic0s_by_source_family": {
                    key.source_family.clone(): [topic]
                },
            }),
            scan_mode: "hash_pinned_block".to_owned(),
            range_start_block_number: key.required_from_block,
            range_end_block_number: key.required_to_block,
            idempotency_key: format!(
                "indexer-full-closure-coverage-recovery:v2:{identity}:coverage_recovery_write_epoch=0"
            ),
            ranges: vec![BackfillRangeSpec {
                range_start_block_number: key.required_from_block,
                range_end_block_number: key.required_to_block,
            }],
        }
    }

    async fn mark_job_failed(
        pool: &sqlx::PgPool,
        backfill_job_id: i64,
        attempt_count: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE backfill_ranges
            SET status = 'failed'::backfill_lifecycle_status,
                attempt_count = $2,
                failure_reason = 'stale coverage recovery claim',
                failure_metadata = '{"phase":"stale_claim_sweep"}'::jsonb,
                lease_token = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = now()
            WHERE backfill_job_id = $1
            "#,
        )
        .bind(backfill_job_id)
        .bind(attempt_count)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            UPDATE backfill_jobs
            SET status = 'failed'::backfill_lifecycle_status,
                failure_reason = 'stale coverage recovery claim',
                failure_metadata = '{"phase":"stale_claim_sweep"}'::jsonb,
                updated_at = now()
            WHERE backfill_job_id = $1
            "#,
        )
        .bind(backfill_job_id)
        .execute(pool)
        .await?;
        Ok(())
    }
}
