use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Postgres};

use super::{
    decode::{decode_backfill_job, decode_backfill_range},
    read::{
        load_backfill_job_for_update, load_backfill_range_for_update, load_backfill_range_job_id,
    },
    sql::{backfill_job_returning_sql, backfill_range_returning_sql},
    types::{BackfillJob, BackfillLifecycleStatus, BackfillRange},
    validate::{ensure_lease_matches, validate_failure, validate_non_empty},
};

/// Mark a leased range failed without rewinding its checkpoint.
pub async fn fail_backfill_range(
    pool: &PgPool,
    backfill_range_id: i64,
    lease_token: &str,
    failure_reason: &str,
    failure_metadata: Value,
) -> Result<BackfillRange> {
    validate_non_empty("lease_token", lease_token)?;
    validate_failure(failure_reason, &failure_metadata)?;
    let failure_metadata_text = serde_json::to_string(&failure_metadata)
        .context("failed to serialize backfill range failure metadata")?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range failure")?;

    // Job lock before range lock (see load_backfill_job_for_update): this
    // writer also marks the job row failed below.
    let backfill_job_id = load_backfill_range_job_id(&mut *transaction, backfill_range_id)
        .await?
        .with_context(|| format!("missing backfill range {backfill_range_id}"))?;
    load_backfill_job_for_update(&mut *transaction, backfill_job_id)
        .await?
        .with_context(|| {
            format!("missing backfill job {backfill_job_id} for range {backfill_range_id}")
        })?;

    let current = load_backfill_range_for_update(&mut *transaction, backfill_range_id)
        .await?
        .with_context(|| format!("missing backfill range {backfill_range_id}"))?;
    if current.status == BackfillLifecycleStatus::Completed
        || current.status == BackfillLifecycleStatus::Failed
    {
        transaction
            .commit()
            .await
            .context("failed to commit terminal backfill range failure no-op")?;
        return Ok(current);
    }
    ensure_lease_matches(&current, lease_token)?;

    let fail_sql = backfill_range_returning_sql(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'failed'::backfill_lifecycle_status,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            failure_reason = $2,
            failure_metadata = $3::jsonb,
            updated_at = now()
        WHERE backfill_range_id = $1
        "#,
    );
    let range = sqlx::query(&fail_sql)
        .bind(backfill_range_id)
        .bind(failure_reason)
        .bind(failure_metadata_text)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to mark backfill range {backfill_range_id} failed"))?;
    let range = decode_backfill_range(range)?;

    set_backfill_job_failed(
        &mut transaction,
        range.backfill_job_id,
        failure_reason,
        &failure_metadata,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill range failure")?;

    Ok(range)
}

/// Mark a job and every incomplete child range failed without rewinding range
/// checkpoints.
pub async fn fail_backfill_job(
    pool: &PgPool,
    backfill_job_id: i64,
    failure_reason: &str,
    failure_metadata: Value,
) -> Result<BackfillJob> {
    validate_failure(failure_reason, &failure_metadata)?;
    let failure_metadata_text = serde_json::to_string(&failure_metadata)
        .context("failed to serialize backfill job failure metadata")?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill job failure")?;

    let current = load_backfill_job_for_update(&mut *transaction, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    if current.status == BackfillLifecycleStatus::Completed {
        transaction
            .commit()
            .await
            .context("failed to commit completed backfill job failure no-op")?;
        return Ok(current);
    }

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'failed'::backfill_lifecycle_status,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            failure_reason = $2,
            failure_metadata = $3::jsonb,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .bind(failure_reason)
    .bind(failure_metadata_text)
    .execute(&mut *transaction)
    .await
    .with_context(|| format!("failed to mark ranges failed for backfill job {backfill_job_id}"))?;

    let job = set_backfill_job_failed(
        &mut transaction,
        backfill_job_id,
        failure_reason,
        &failure_metadata,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill job failure")?;

    Ok(job)
}

async fn set_backfill_job_failed(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
    failure_reason: &str,
    failure_metadata: &Value,
) -> Result<BackfillJob> {
    let failure_metadata_text = serde_json::to_string(failure_metadata)
        .context("failed to serialize backfill job failure metadata")?;
    let fail_sql = backfill_job_returning_sql(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'failed'::backfill_lifecycle_status,
            failure_reason = $2,
            failure_metadata = $3::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    );
    let row = sqlx::query(&fail_sql)
        .bind(backfill_job_id)
        .bind(failure_reason)
        .bind(failure_metadata_text)
        .fetch_one(&mut **executor)
        .await
        .with_context(|| format!("failed to mark backfill job {backfill_job_id} failed"))?;

    decode_backfill_job(row)
}
