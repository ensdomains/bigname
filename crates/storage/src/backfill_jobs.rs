mod complete;
mod coverage_facts;
mod create;
mod decode;
mod fail;
mod lease;
mod read;
mod sql;
mod topic_evidence;
mod types;
mod validate;

use anyhow::{Context, Result, ensure};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgConnection, PgPool, Row};

const STALE_CLAIM_REASON: &str = "stale backfill claim";

/// Durable count/digest evidence captured while the chain's raw-log mutation
/// fence is held. The caller owns validation of the selected log set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillStoredVerification {
    pub raw_log_input_revision: i64,
    pub verified_from_block: i64,
    pub verified_to_block: i64,
    pub selected_log_count: i64,
    pub selected_log_digest: String,
}

/// Keep the largest lower-bound pre-fetch estimate seen for a resumable job.
/// The caller includes required aggregate verification and initial row-window
/// requests. Pagination, retries, and filter-pack splits are unknowable before
/// the source responds and remain actuals.
pub async fn record_backfill_job_projected_minimum_provider_queries(
    pool: &PgPool,
    backfill_job_id: i64,
    projected_minimum_query_count: i64,
) -> Result<()> {
    ensure!(
        projected_minimum_query_count >= 0,
        "projected minimum provider query count must not be negative"
    );
    let result = sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            projected_minimum_provider_query_count = GREATEST(
                projected_minimum_provider_query_count,
                $2
            ),
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .bind(projected_minimum_query_count)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to record projected minimum provider queries for backfill job {backfill_job_id}"
        )
    })?;
    ensure!(
        result.rows_affected() == 1,
        "missing backfill job {backfill_job_id} while recording projected minimum provider queries"
    );
    Ok(())
}

/// Add provider request attempts immediately after a paid query returns.
/// Recording before validation preserves useful actuals when later work fails.
pub async fn add_backfill_job_actual_provider_queries(
    pool: &PgPool,
    backfill_job_id: i64,
    actual_query_count: i64,
) -> Result<()> {
    ensure!(
        actual_query_count >= 0,
        "actual provider query count must not be negative"
    );
    let result = sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            actual_provider_query_count = actual_provider_query_count + $2,
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .bind(actual_query_count)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to add provider query actuals for backfill job {backfill_job_id}")
    })?;
    ensure!(
        result.rows_affected() == 1,
        "missing backfill job {backfill_job_id} while recording provider query actuals"
    );
    Ok(())
}

/// Bind a coverage-recovery job to the exact fenced raw-log snapshot it
/// inspected. This is called through the guard's connection so the evidence
/// and revision observation commit together.
pub async fn record_backfill_job_stored_verification(
    connection: &mut PgConnection,
    backfill_job_id: i64,
    expected_retention_generation: i64,
    verification: &BackfillStoredVerification,
) -> Result<()> {
    ensure!(
        expected_retention_generation >= 0,
        "stored verification retention generation must not be negative"
    );
    ensure!(
        verification.raw_log_input_revision >= 0,
        "stored verification raw-log input revision must not be negative"
    );
    ensure!(
        verification.verified_from_block >= 0
            && verification.verified_from_block <= verification.verified_to_block,
        "stored verification block range is invalid"
    );
    ensure!(
        verification.selected_log_count >= 0,
        "stored verification selected log count must not be negative"
    );
    ensure!(
        verification.selected_log_digest.len() == 32
            && verification
                .selected_log_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "stored verification digest must be 32 lowercase hexadecimal characters"
    );

    let result = sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            stored_verification_raw_log_input_revision = $3,
            stored_verification_from_block = $4,
            stored_verification_to_block = $5,
            stored_verification_log_count = $6,
            stored_verification_digest = $7,
            updated_at = now()
        FROM raw_log_staging_input_revisions retained
        WHERE backfill_jobs.backfill_job_id = $1
          AND backfill_jobs.raw_log_retention_generation = $2
          AND retained.chain_id = backfill_jobs.chain_id
          AND retained.retention_generation = $2
          AND retained.revision = $3
          AND backfill_jobs.range_start_block_number <= $4
          AND backfill_jobs.range_end_block_number >= $5
          AND backfill_jobs.status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .bind(expected_retention_generation)
    .bind(verification.raw_log_input_revision)
    .bind(verification.verified_from_block)
    .bind(verification.verified_to_block)
    .bind(verification.selected_log_count)
    .bind(&verification.selected_log_digest)
    .execute(connection)
    .await
    .with_context(|| {
        format!("failed to record stored verification for backfill job {backfill_job_id}")
    })?;
    ensure!(
        result.rows_affected() == 1,
        "backfill job {backfill_job_id} no longer matches its stored verification generation, range, or lifecycle"
    );
    Ok(())
}

/// Report whether a previously recorded exact-window verification still
/// matches current retention and per-block mutation evidence.
pub async fn backfill_job_stored_verification_is_current(
    pool: &PgPool,
    backfill_job_id: i64,
    chain_id: &str,
    verified_from_block: i64,
    verified_to_block: i64,
) -> Result<bool> {
    ensure!(
        backfill_job_id > 0,
        "stored verification backfill job id must be positive"
    );
    ensure!(
        !chain_id.trim().is_empty(),
        "stored verification chain must not be empty"
    );
    ensure!(
        verified_from_block >= 0 && verified_from_block <= verified_to_block,
        "stored verification query range is invalid"
    );
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT COALESCE((
            job.raw_log_retention_generation = retained.retention_generation
            AND job.stored_verification_raw_log_input_revision IS NOT NULL
            AND job.stored_verification_raw_log_input_revision <= retained.revision
            AND job.stored_verification_from_block <= $3
            AND job.stored_verification_to_block >= $4
            AND NOT EXISTS (
                SELECT 1
                FROM raw_log_staging_block_revisions changed
                WHERE changed.chain_id = job.chain_id
                  AND changed.revision
                      > job.stored_verification_raw_log_input_revision
                  AND changed.block_number BETWEEN $3 AND $4
            )
        ), FALSE)
        FROM backfill_jobs job
        JOIN raw_log_staging_input_revisions retained
          ON retained.chain_id = job.chain_id
        WHERE job.backfill_job_id = $1
          AND job.chain_id = $2
        "#,
    )
    .bind(backfill_job_id)
    .bind(chain_id)
    .bind(verified_from_block)
    .bind(verified_to_block)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to inspect stored verification for backfill job {backfill_job_id}")
    })
    .map(|current| current.unwrap_or(false))
}

/// Clear abandoned range leases whose normal heartbeat has not moved the row
/// before `stale_before`. Failed ranges are already reservable by the ordinary
/// lease path, so no parallel claim lifecycle is introduced.
pub async fn sweep_stale_backfill_claims(
    pool: &PgPool,
    chain_id: &str,
    stale_before: OffsetDateTime,
) -> Result<Vec<i64>> {
    ensure!(
        !chain_id.trim().is_empty(),
        "stale backfill claim sweep chain must not be empty"
    );
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open stale backfill claim sweep transaction")?;
    let rows = sqlx::query(
        r#"
        WITH stale_jobs AS MATERIALIZED (
            SELECT backfill_job_id
            FROM backfill_jobs
            WHERE chain_id = $1
              AND status IN (
                  'reserved'::backfill_lifecycle_status,
                  'running'::backfill_lifecycle_status
              )
              AND updated_at < $2
              AND NOT EXISTS (
                  SELECT 1
                  FROM backfill_ranges active
                  WHERE active.backfill_job_id = backfill_jobs.backfill_job_id
                    AND active.status IN (
                        'reserved'::backfill_lifecycle_status,
                        'running'::backfill_lifecycle_status
                    )
                    AND active.updated_at >= $2
              )
            FOR UPDATE SKIP LOCKED
        ),
        stale_ranges AS MATERIALIZED (
            SELECT ranges.backfill_range_id, ranges.backfill_job_id
            FROM backfill_ranges ranges
            JOIN stale_jobs jobs USING (backfill_job_id)
            WHERE ranges.status IN (
                    'reserved'::backfill_lifecycle_status,
                    'running'::backfill_lifecycle_status
                )
              AND ranges.updated_at < $2
            FOR UPDATE OF ranges SKIP LOCKED
        ),
        reclaimed AS (
            UPDATE backfill_ranges ranges
            SET
                status = 'failed'::backfill_lifecycle_status,
                lease_token = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL,
                failure_reason = $3,
                failure_metadata = jsonb_build_object(
                    'phase', 'stale_claim_sweep',
                    'stale_before', $2
                ),
                updated_at = now()
            FROM stale_ranges
            WHERE ranges.backfill_range_id = stale_ranges.backfill_range_id
            RETURNING stale_ranges.backfill_job_id
        )
        SELECT DISTINCT backfill_job_id
        FROM reclaimed
        ORDER BY backfill_job_id
        "#,
    )
    .bind(chain_id)
    .bind(stale_before)
    .bind(STALE_CLAIM_REASON)
    .fetch_all(&mut *transaction)
    .await
    .with_context(|| format!("failed to sweep stale backfill ranges for {chain_id}"))?;
    let job_ids = rows
        .into_iter()
        .map(|row| row.try_get("backfill_job_id").map_err(Into::into))
        .collect::<Result<Vec<i64>>>()?;

    if !job_ids.is_empty() {
        sqlx::query(
            r#"
            UPDATE backfill_jobs
            SET
                status = 'failed'::backfill_lifecycle_status,
                failure_reason = $2,
                failure_metadata = jsonb_build_object(
                    'phase', 'stale_claim_sweep',
                    'stale_before', $3
                ),
                completed_at = NULL,
                updated_at = now()
            WHERE backfill_job_id = ANY($1::BIGINT[])
              AND status <> 'completed'::backfill_lifecycle_status
            "#,
        )
        .bind(&job_ids)
        .bind(STALE_CLAIM_REASON)
        .bind(stale_before)
        .execute(&mut *transaction)
        .await
        .with_context(|| {
            format!("failed to mark swept backfill jobs reclaimable for {chain_id}")
        })?;
    }

    transaction
        .commit()
        .await
        .context("failed to commit stale backfill claim sweep")?;
    Ok(job_ids)
}

pub use complete::{
    complete_backfill_job, complete_backfill_range, complete_backfill_range_recording_coverage,
    complete_backfill_range_recording_coverage_with_progress,
};
pub use coverage_facts::{
    BackfillCoverageFactDerivation, BackfillCoverageFactScope, BackfillCoverageFactStreamItem,
    BackfillCoverageFactWrite, BackfillCoverageProgress, BackfillCoverageProgressFuture,
    load_backfill_coverage_fact_counts, write_backfill_coverage_facts,
};
pub use create::{
    create_backfill_job, create_generation_scoped_backfill_job,
    ensure_and_load_raw_log_retention_generation,
};
pub use fail::{fail_backfill_job, fail_backfill_range};
pub use lease::{advance_backfill_range, reserve_backfill_range};
pub use read::{
    load_backfill_job, load_backfill_ranges, load_completed_backfill_jobs_intersecting_range,
};
pub use topic_evidence::{
    BackfillTopicCoverageRequirement, BackfillTopicCoverageViolation,
    MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS, find_backfill_topic_coverage_violations,
    materialize_completed_backfill_topic_evidence,
};
pub use types::{
    BackfillJob, BackfillJobCreate, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange,
    BackfillRangeSpec,
};

#[cfg(test)]
mod tests;
