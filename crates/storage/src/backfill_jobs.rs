use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

/// Persisted lifecycle state for backfill jobs and range checkpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackfillLifecycleStatus {
    Pending,
    Reserved,
    Running,
    Completed,
    Failed,
}

impl BackfillLifecycleStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Reserved => "reserved",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "reserved" => Ok(Self::Reserved),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => bail!("unknown backfill lifecycle status {value}"),
        }
    }
}

/// Child range bounds for a bounded backfill job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillRangeSpec {
    pub range_start_block_number: i64,
    pub range_end_block_number: i64,
}

/// Immutable job creation contract. Empty `ranges` creates one range covering
/// the declared job bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillJobCreate {
    pub deployment_profile: String,
    pub chain_id: String,
    pub source_identity: Value,
    pub scan_mode: String,
    pub range_start_block_number: i64,
    pub range_end_block_number: i64,
    pub idempotency_key: String,
    pub ranges: Vec<BackfillRangeSpec>,
}

/// Persisted backfill job snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillJob {
    pub backfill_job_id: i64,
    pub deployment_profile: String,
    pub chain_id: String,
    pub source_identity: Value,
    pub scan_mode: String,
    pub range_start_block_number: i64,
    pub range_end_block_number: i64,
    pub idempotency_key: String,
    pub status: BackfillLifecycleStatus,
    pub failure_reason: Option<String>,
    pub failure_metadata: Value,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
}

/// Persisted child range checkpoint snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillRange {
    pub backfill_range_id: i64,
    pub backfill_job_id: i64,
    pub range_start_block_number: i64,
    pub range_end_block_number: i64,
    pub checkpoint_block_number: i64,
    pub status: BackfillLifecycleStatus,
    pub lease_token: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<OffsetDateTime>,
    pub attempt_count: i64,
    pub failure_reason: Option<String>,
    pub failure_metadata: Value,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
}

/// Job plus child ranges returned by idempotent creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillJobRecord {
    pub job: BackfillJob,
    pub ranges: Vec<BackfillRange>,
}

/// Insert a bounded backfill job and its child ranges, or return the existing
/// matching job for the same idempotency key.
pub async fn create_backfill_job(
    pool: &PgPool,
    request: &BackfillJobCreate,
) -> Result<BackfillJobRecord> {
    let range_specs = validate_backfill_job_create(request)?;
    let source_identity = serde_json::to_string(&request.source_identity)
        .context("failed to serialize source_identity")?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill job creation")?;

    let inserted = sqlx::query(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key
        )
        VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING
            backfill_job_id,
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status::TEXT AS status,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        "#,
    )
    .bind(&request.deployment_profile)
    .bind(&request.chain_id)
    .bind(source_identity)
    .bind(&request.scan_mode)
    .bind(request.range_start_block_number)
    .bind(request.range_end_block_number)
    .bind(&request.idempotency_key)
    .fetch_optional(&mut *transaction)
    .await
    .with_context(|| {
        format!(
            "failed to insert backfill job for idempotency key {}",
            request.idempotency_key
        )
    })?;

    let job = match inserted {
        Some(row) => {
            let job = decode_backfill_job(row)?;
            for spec in &range_specs {
                insert_backfill_range(&mut transaction, job.backfill_job_id, spec).await?;
            }
            job
        }
        None => {
            let job = load_backfill_job_by_idempotency_key_internal(
                &mut *transaction,
                &request.idempotency_key,
            )
            .await?
            .with_context(|| {
                format!(
                    "backfill job idempotency key {} conflicted but no row was found",
                    request.idempotency_key
                )
            })?;
            ensure_existing_job_matches_request(&job, request)?;
            job
        }
    };

    let ranges = load_backfill_ranges_internal(&mut *transaction, job.backfill_job_id).await?;
    ensure_existing_ranges_match_specs(job.backfill_job_id, &ranges, &range_specs)?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill job creation")?;

    Ok(BackfillJobRecord { job, ranges })
}

/// Load one backfill job by stable row identity.
pub async fn load_backfill_job(pool: &PgPool, backfill_job_id: i64) -> Result<Option<BackfillJob>> {
    load_backfill_job_internal(pool, backfill_job_id).await
}

/// Load child ranges for one backfill job in declared range order.
pub async fn load_backfill_ranges(
    pool: &PgPool,
    backfill_job_id: i64,
) -> Result<Vec<BackfillRange>> {
    load_backfill_ranges_internal(pool, backfill_job_id).await
}

/// Atomically reserve the next pending, failed, or expired range for a job.
/// Reusing the same active lease token and owner returns the existing range.
pub async fn reserve_backfill_range(
    pool: &PgPool,
    backfill_job_id: i64,
    lease_owner: &str,
    lease_token: &str,
    lease_expires_at: OffsetDateTime,
) -> Result<Option<BackfillRange>> {
    validate_lease(lease_owner, lease_token, lease_expires_at)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range reservation")?;

    let job = load_backfill_job_for_update(&mut *transaction, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    if job.status == BackfillLifecycleStatus::Completed {
        transaction
            .commit()
            .await
            .context("failed to commit completed backfill job reservation no-op")?;
        return Ok(None);
    }

    if let Some(existing) = load_active_backfill_range_by_lease(
        &mut *transaction,
        backfill_job_id,
        lease_owner,
        lease_token,
    )
    .await?
    {
        transaction
            .commit()
            .await
            .context("failed to commit duplicate backfill range reservation")?;
        return Ok(Some(existing));
    }

    let candidate = sqlx::query(
        r#"
        SELECT backfill_range_id
        FROM backfill_ranges
        WHERE backfill_job_id = $1
          AND (
            status IN ('pending'::backfill_lifecycle_status, 'failed'::backfill_lifecycle_status)
            OR (
              status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status)
              AND lease_expires_at <= now()
            )
          )
        ORDER BY
          CASE
            WHEN status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status) THEN 0
            WHEN status = 'pending'::backfill_lifecycle_status THEN 1
            WHEN status = 'failed'::backfill_lifecycle_status THEN 2
            ELSE 3
          END,
          range_start_block_number,
          range_end_block_number
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(backfill_job_id)
    .fetch_optional(&mut *transaction)
    .await
    .with_context(|| format!("failed to select reservable range for backfill job {backfill_job_id}"))?;

    let Some(candidate) = candidate else {
        transaction
            .commit()
            .await
            .context("failed to commit empty backfill range reservation")?;
        return Ok(None);
    };
    let backfill_range_id = candidate
        .try_get::<i64, _>("backfill_range_id")
        .context("missing backfill_range_id from reservable range row")?;

    let reserve_sql = backfill_range_returning_sql(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'reserved'::backfill_lifecycle_status,
            lease_token = $2,
            lease_owner = $3,
            lease_expires_at = $4,
            attempt_count = attempt_count + 1,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_range_id = $1
        "#,
    );
    let range = sqlx::query(&reserve_sql)
        .bind(backfill_range_id)
        .bind(lease_token)
        .bind(lease_owner)
        .bind(lease_expires_at)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to reserve backfill range {backfill_range_id}"))?;
    let range = decode_backfill_range(range)?;

    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'reserved'::backfill_lifecycle_status,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .execute(&mut *transaction)
    .await
    .with_context(|| format!("failed to mark backfill job {backfill_job_id} reserved"))?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill range reservation")?;

    Ok(Some(range))
}

/// Move a leased range checkpoint forward monotonically.
pub async fn advance_backfill_range(
    pool: &PgPool,
    backfill_range_id: i64,
    lease_token: &str,
    checkpoint_block_number: i64,
) -> Result<BackfillRange> {
    validate_non_empty("lease_token", lease_token)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range advance")?;

    let current = load_backfill_range_for_update(&mut *transaction, backfill_range_id)
        .await?
        .with_context(|| format!("missing backfill range {backfill_range_id}"))?;

    if current.status == BackfillLifecycleStatus::Completed {
        if checkpoint_block_number <= current.checkpoint_block_number {
            transaction
                .commit()
                .await
                .context("failed to commit completed backfill range advance no-op")?;
            return Ok(current);
        }
        bail!("completed backfill range {backfill_range_id} cannot advance beyond completion");
    }
    if current.status == BackfillLifecycleStatus::Failed {
        bail!("failed backfill range {backfill_range_id} must be reserved again before advancing");
    }
    ensure_lease_matches(&current, lease_token)?;
    if checkpoint_block_number > current.range_end_block_number {
        bail!(
            "backfill range {backfill_range_id} checkpoint {checkpoint_block_number} is beyond declared range end {}",
            current.range_end_block_number
        );
    }
    if checkpoint_block_number < current.checkpoint_block_number {
        transaction
            .commit()
            .await
            .context("failed to commit stale backfill range advance no-op")?;
        return Ok(current);
    }

    let advance_sql = backfill_range_returning_sql(
        r#"
        UPDATE backfill_ranges
        SET
            checkpoint_block_number = $2,
            status = 'running'::backfill_lifecycle_status,
            updated_at = now()
        WHERE backfill_range_id = $1
        "#,
    );
    let range = sqlx::query(&advance_sql)
        .bind(backfill_range_id)
        .bind(checkpoint_block_number)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to advance backfill range {backfill_range_id}"))?;
    let range = decode_backfill_range(range)?;

    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'running'::backfill_lifecycle_status,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(range.backfill_job_id)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!(
            "failed to mark backfill job {} running",
            range.backfill_job_id
        )
    })?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill range advance")?;

    Ok(range)
}

/// Complete a leased range after its checkpoint reaches the declared end.
pub async fn complete_backfill_range(
    pool: &PgPool,
    backfill_range_id: i64,
    lease_token: &str,
) -> Result<BackfillRange> {
    validate_non_empty("lease_token", lease_token)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range completion")?;

    let current = load_backfill_range_for_update(&mut *transaction, backfill_range_id)
        .await?
        .with_context(|| format!("missing backfill range {backfill_range_id}"))?;
    if current.status == BackfillLifecycleStatus::Completed {
        transaction
            .commit()
            .await
            .context("failed to commit completed backfill range no-op")?;
        return Ok(current);
    }
    if current.status == BackfillLifecycleStatus::Failed {
        bail!("failed backfill range {backfill_range_id} must be reserved again before completion");
    }
    ensure_lease_matches(&current, lease_token)?;
    if current.checkpoint_block_number != current.range_end_block_number {
        bail!(
            "backfill range {backfill_range_id} checkpoint {} has not reached declared range end {}",
            current.checkpoint_block_number,
            current.range_end_block_number
        );
    }

    let complete_sql = backfill_range_returning_sql(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'completed'::backfill_lifecycle_status,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = COALESCE(completed_at, now()),
            updated_at = now()
        WHERE backfill_range_id = $1
        "#,
    );
    let range = sqlx::query(&complete_sql)
        .bind(backfill_range_id)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to complete backfill range {backfill_range_id}"))?;
    let range = decode_backfill_range(range)?;

    maybe_complete_backfill_job(&mut transaction, range.backfill_job_id).await?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill range completion")?;

    Ok(range)
}

/// Complete a job when all child range checkpoints have reached their declared
/// ends. This is idempotent when the job is already complete.
pub async fn complete_backfill_job(pool: &PgPool, backfill_job_id: i64) -> Result<BackfillJob> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill job completion")?;

    let current = load_backfill_job_for_update(&mut *transaction, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    if current.status == BackfillLifecycleStatus::Completed {
        transaction
            .commit()
            .await
            .context("failed to commit completed backfill job no-op")?;
        return Ok(current);
    }

    ensure_all_ranges_reached_end(&mut *transaction, backfill_job_id).await?;

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'completed'::backfill_lifecycle_status,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = COALESCE(completed_at, now()),
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .execute(&mut *transaction)
    .await
    .with_context(|| format!("failed to complete ranges for backfill job {backfill_job_id}"))?;

    let job = set_backfill_job_completed(&mut transaction, backfill_job_id).await?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill job completion")?;

    Ok(job)
}

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

async fn insert_backfill_range(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
    spec: &BackfillRangeSpec,
) -> Result<BackfillRange> {
    let insert_sql = backfill_range_returning_sql(
        r#"
        INSERT INTO backfill_ranges (
            backfill_job_id,
            range_start_block_number,
            range_end_block_number,
            checkpoint_block_number
        )
        VALUES ($1, $2, $3, $2)
        "#,
    );
    let row = sqlx::query(&insert_sql)
        .bind(backfill_job_id)
        .bind(spec.range_start_block_number)
        .bind(spec.range_end_block_number)
        .fetch_one(&mut **executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert backfill range {}..={} for job {backfill_job_id}",
                spec.range_start_block_number, spec.range_end_block_number
            )
        })?;

    decode_backfill_range(row)
}

async fn maybe_complete_backfill_job(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
) -> Result<()> {
    let incomplete_count = incomplete_range_count(&mut **executor, backfill_job_id).await?;
    if incomplete_count != 0 {
        return Ok(());
    }

    set_backfill_job_completed(executor, backfill_job_id).await?;
    Ok(())
}

async fn set_backfill_job_completed(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
) -> Result<BackfillJob> {
    let complete_sql = backfill_job_returning_sql(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'completed'::backfill_lifecycle_status,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = COALESCE(completed_at, now()),
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    );
    let row = sqlx::query(&complete_sql)
        .bind(backfill_job_id)
        .fetch_one(&mut **executor)
        .await
        .with_context(|| format!("failed to complete backfill job {backfill_job_id}"))?;

    decode_backfill_job(row)
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

async fn ensure_all_ranges_reached_end<'e, E>(executor: E, backfill_job_id: i64) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    let count = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS incomplete_count
        FROM backfill_ranges
        WHERE backfill_job_id = $1
          AND checkpoint_block_number <> range_end_block_number
        "#,
    )
    .bind(backfill_job_id)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!("failed to count incomplete ranges for backfill job {backfill_job_id}")
    })?
    .try_get::<i64, _>("incomplete_count")
    .context("missing incomplete_count")?;

    if count != 0 {
        bail!(
            "backfill job {backfill_job_id} has {count} range checkpoints that have not reached their declared ends"
        );
    }

    Ok(())
}

async fn incomplete_range_count<'e, E>(executor: E, backfill_job_id: i64) -> Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS incomplete_count
        FROM backfill_ranges
        WHERE backfill_job_id = $1
          AND (
            status <> 'completed'::backfill_lifecycle_status
            OR checkpoint_block_number <> range_end_block_number
          )
        "#,
    )
    .bind(backfill_job_id)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!("failed to count incomplete ranges for backfill job {backfill_job_id}")
    })?
    .try_get::<i64, _>("incomplete_count")
    .context("missing incomplete_count")
}

async fn load_backfill_job_internal<'e, E>(
    executor: E,
    backfill_job_id: i64,
) -> Result<Option<BackfillJob>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_job_select_sql("WHERE backfill_job_id = $1", "");
    let row = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_optional(executor)
        .await
        .with_context(|| format!("failed to load backfill job {backfill_job_id}"))?;

    row.map(decode_backfill_job).transpose()
}

async fn load_backfill_job_by_idempotency_key_internal<'e, E>(
    executor: E,
    idempotency_key: &str,
) -> Result<Option<BackfillJob>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_job_select_sql("WHERE idempotency_key = $1", "");
    let row = sqlx::query(&select_sql)
        .bind(idempotency_key)
        .fetch_optional(executor)
        .await
        .with_context(|| {
            format!("failed to load backfill job for idempotency key {idempotency_key}")
        })?;

    row.map(decode_backfill_job).transpose()
}

async fn load_backfill_job_for_update<'e, E>(
    executor: E,
    backfill_job_id: i64,
) -> Result<Option<BackfillJob>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_job_select_sql("WHERE backfill_job_id = $1", "FOR UPDATE");
    let row = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_optional(executor)
        .await
        .with_context(|| format!("failed to lock backfill job {backfill_job_id}"))?;

    row.map(decode_backfill_job).transpose()
}

async fn load_backfill_ranges_internal<'e, E>(
    executor: E,
    backfill_job_id: i64,
) -> Result<Vec<BackfillRange>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_range_select_sql(
        "WHERE backfill_job_id = $1",
        "ORDER BY range_start_block_number, range_end_block_number",
    );
    let rows = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_all(executor)
        .await
        .with_context(|| format!("failed to load ranges for backfill job {backfill_job_id}"))?;

    rows.into_iter().map(decode_backfill_range).collect()
}

async fn load_backfill_range_for_update<'e, E>(
    executor: E,
    backfill_range_id: i64,
) -> Result<Option<BackfillRange>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_range_select_sql("WHERE backfill_range_id = $1", "FOR UPDATE");
    let row = sqlx::query(&select_sql)
        .bind(backfill_range_id)
        .fetch_optional(executor)
        .await
        .with_context(|| format!("failed to lock backfill range {backfill_range_id}"))?;

    row.map(decode_backfill_range).transpose()
}

async fn load_active_backfill_range_by_lease<'e, E>(
    executor: E,
    backfill_job_id: i64,
    lease_owner: &str,
    lease_token: &str,
) -> Result<Option<BackfillRange>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_range_select_sql(
        r#"
        WHERE backfill_job_id = $1
          AND lease_owner = $2
          AND lease_token = $3
          AND status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status)
        "#,
        "FOR UPDATE",
    );
    let row = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .bind(lease_owner)
        .bind(lease_token)
        .fetch_optional(executor)
        .await
        .with_context(|| {
            format!("failed to load active lease {lease_token} for backfill job {backfill_job_id}")
        })?;

    row.map(decode_backfill_range).transpose()
}

fn validate_backfill_job_create(request: &BackfillJobCreate) -> Result<Vec<BackfillRangeSpec>> {
    validate_non_empty("deployment_profile", &request.deployment_profile)?;
    validate_non_empty("chain_id", &request.chain_id)?;
    validate_non_empty("scan_mode", &request.scan_mode)?;
    validate_non_empty("idempotency_key", &request.idempotency_key)?;
    validate_range_bounds(
        request.range_start_block_number,
        request.range_end_block_number,
        "backfill job",
    )?;
    match &request.source_identity {
        Value::Object(_) | Value::Array(_) => {}
        _ => bail!("backfill job source_identity must be a JSON object or array"),
    }

    let mut specs = if request.ranges.is_empty() {
        vec![BackfillRangeSpec {
            range_start_block_number: request.range_start_block_number,
            range_end_block_number: request.range_end_block_number,
        }]
    } else {
        request.ranges.clone()
    };
    specs.sort_by_key(|spec| (spec.range_start_block_number, spec.range_end_block_number));

    let mut expected_start = request.range_start_block_number;
    for spec in &specs {
        validate_range_bounds(
            spec.range_start_block_number,
            spec.range_end_block_number,
            "backfill range",
        )?;
        if spec.range_start_block_number != expected_start {
            bail!(
                "backfill ranges must partition the declared job range contiguously; expected range start {expected_start}, got {}",
                spec.range_start_block_number
            );
        }
        if spec.range_end_block_number > request.range_end_block_number {
            bail!(
                "backfill range {}..={} exceeds declared job range end {}",
                spec.range_start_block_number,
                spec.range_end_block_number,
                request.range_end_block_number
            );
        }
        expected_start = spec
            .range_end_block_number
            .checked_add(1)
            .context("backfill range end overflowed while validating contiguous ranges")?;
    }

    if expected_start - 1 != request.range_end_block_number {
        bail!(
            "backfill ranges must cover the declared job range through end {}; covered through {}",
            request.range_end_block_number,
            expected_start - 1
        );
    }

    Ok(specs)
}

fn validate_range_bounds(start: i64, end: i64, label: &str) -> Result<()> {
    if start < 0 {
        bail!("{label} has negative range start {start}");
    }
    if end < start {
        bail!("{label} range end {end} is before range start {start}");
    }
    Ok(())
}

fn validate_lease(
    lease_owner: &str,
    lease_token: &str,
    lease_expires_at: OffsetDateTime,
) -> Result<()> {
    validate_non_empty("lease_owner", lease_owner)?;
    validate_non_empty("lease_token", lease_token)?;
    if lease_expires_at <= OffsetDateTime::now_utc() {
        bail!("lease_expires_at must be in the future");
    }
    Ok(())
}

fn validate_failure(failure_reason: &str, failure_metadata: &Value) -> Result<()> {
    validate_non_empty("failure_reason", failure_reason)?;
    if !failure_metadata.is_object() {
        bail!("failure_metadata must be a JSON object");
    }
    Ok(())
}

fn validate_non_empty(field_name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field_name} must not be empty");
    }
    Ok(())
}

fn ensure_existing_job_matches_request(
    existing: &BackfillJob,
    request: &BackfillJobCreate,
) -> Result<()> {
    if existing.deployment_profile != request.deployment_profile
        || existing.chain_id != request.chain_id
        || existing.source_identity != request.source_identity
        || existing.scan_mode != request.scan_mode
        || existing.range_start_block_number != request.range_start_block_number
        || existing.range_end_block_number != request.range_end_block_number
        || existing.idempotency_key != request.idempotency_key
    {
        bail!(
            "existing backfill job for idempotency key {} does not match requested immutable job identity",
            request.idempotency_key
        );
    }

    Ok(())
}

fn ensure_existing_ranges_match_specs(
    backfill_job_id: i64,
    ranges: &[BackfillRange],
    specs: &[BackfillRangeSpec],
) -> Result<()> {
    let existing = ranges
        .iter()
        .map(|range| BackfillRangeSpec {
            range_start_block_number: range.range_start_block_number,
            range_end_block_number: range.range_end_block_number,
        })
        .collect::<Vec<_>>();
    if existing != specs {
        bail!("existing ranges for backfill job {backfill_job_id} do not match requested ranges");
    }

    Ok(())
}

fn ensure_lease_matches(range: &BackfillRange, lease_token: &str) -> Result<()> {
    if range.lease_token.as_deref() != Some(lease_token) {
        bail!(
            "backfill range {} is not held by lease token {}",
            range.backfill_range_id,
            lease_token
        );
    }

    Ok(())
}

fn decode_backfill_job(row: PgRow) -> Result<BackfillJob> {
    Ok(BackfillJob {
        backfill_job_id: row
            .try_get("backfill_job_id")
            .context("missing backfill_job_id")?,
        deployment_profile: row
            .try_get("deployment_profile")
            .context("missing deployment_profile")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        source_identity: row
            .try_get("source_identity")
            .context("missing source_identity")?,
        scan_mode: row.try_get("scan_mode").context("missing scan_mode")?,
        range_start_block_number: row
            .try_get("range_start_block_number")
            .context("missing range_start_block_number")?,
        range_end_block_number: row
            .try_get("range_end_block_number")
            .context("missing range_end_block_number")?,
        idempotency_key: row
            .try_get("idempotency_key")
            .context("missing idempotency_key")?,
        status: BackfillLifecycleStatus::parse(
            &row.try_get::<String, _>("status")
                .context("missing status")?,
        )?,
        failure_reason: row
            .try_get("failure_reason")
            .context("missing failure_reason")?,
        failure_metadata: row
            .try_get("failure_metadata")
            .context("missing failure_metadata")?,
        created_at: row.try_get("created_at").context("missing created_at")?,
        updated_at: row.try_get("updated_at").context("missing updated_at")?,
        completed_at: row
            .try_get("completed_at")
            .context("missing completed_at")?,
    })
}

fn decode_backfill_range(row: PgRow) -> Result<BackfillRange> {
    Ok(BackfillRange {
        backfill_range_id: row
            .try_get("backfill_range_id")
            .context("missing backfill_range_id")?,
        backfill_job_id: row
            .try_get("backfill_job_id")
            .context("missing backfill_job_id")?,
        range_start_block_number: row
            .try_get("range_start_block_number")
            .context("missing range_start_block_number")?,
        range_end_block_number: row
            .try_get("range_end_block_number")
            .context("missing range_end_block_number")?,
        checkpoint_block_number: row
            .try_get("checkpoint_block_number")
            .context("missing checkpoint_block_number")?,
        status: BackfillLifecycleStatus::parse(
            &row.try_get::<String, _>("status")
                .context("missing status")?,
        )?,
        lease_token: row.try_get("lease_token").context("missing lease_token")?,
        lease_owner: row.try_get("lease_owner").context("missing lease_owner")?,
        lease_expires_at: row
            .try_get("lease_expires_at")
            .context("missing lease_expires_at")?,
        attempt_count: row
            .try_get("attempt_count")
            .context("missing attempt_count")?,
        failure_reason: row
            .try_get("failure_reason")
            .context("missing failure_reason")?,
        failure_metadata: row
            .try_get("failure_metadata")
            .context("missing failure_metadata")?,
        created_at: row.try_get("created_at").context("missing created_at")?,
        updated_at: row.try_get("updated_at").context("missing updated_at")?,
        completed_at: row
            .try_get("completed_at")
            .context("missing completed_at")?,
    })
}

fn backfill_job_select_sql(where_clause: &str, suffix: &str) -> String {
    format!(
        r#"
        SELECT
            backfill_job_id,
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status::TEXT AS status,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        FROM backfill_jobs
        {where_clause}
        {suffix}
        "#
    )
}

fn backfill_job_returning_sql(prefix: &str) -> String {
    format!(
        r#"
        {prefix}
        RETURNING
            backfill_job_id,
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status::TEXT AS status,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        "#
    )
}

fn backfill_range_select_sql(where_clause: &str, suffix: &str) -> String {
    format!(
        r#"
        SELECT
            backfill_range_id,
            backfill_job_id,
            range_start_block_number,
            range_end_block_number,
            checkpoint_block_number,
            status::TEXT AS status,
            lease_token,
            lease_owner,
            lease_expires_at,
            attempt_count,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        FROM backfill_ranges
        {where_clause}
        {suffix}
        "#
    )
}

fn backfill_range_returning_sql(prefix: &str) -> String {
    format!(
        r#"
        {prefix}
        RETURNING
            backfill_range_id,
            backfill_job_id,
            range_start_block_number,
            range_end_block_number,
            checkpoint_block_number,
            status::TEXT AS status,
            lease_token,
            lease_owner,
            lease_expires_at,
            attempt_count,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        "#
    )
}

#[cfg(test)]
mod tests;
