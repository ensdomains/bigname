use anyhow::{Context, Result, ensure};
use sqlx::{PgPool, Postgres};

use super::{
    decode::{decode_backfill_job, decode_backfill_range},
    read::{load_backfill_job_by_idempotency_key_internal, load_backfill_ranges_internal},
    sql::backfill_range_returning_sql,
    types::{BackfillJobCreate, BackfillJobRecord, BackfillRange, BackfillRangeSpec},
    validate::{
        ensure_existing_job_matches_request, ensure_existing_ranges_match_specs,
        validate_backfill_job_create, validate_non_empty,
    },
};

const RAW_LOG_RETENTION_GENERATION_KEY_SUFFIX: &str = ":raw_log_retention_generation=";

/// Ensure a chain has raw-log retention state and load its current generation.
///
/// Callers may use this value to distinguish automatically planned jobs. Job
/// creation still captures the generation again under its own transaction
/// lock, and that persisted value is authoritative if retention changes
/// between planning and creation.
pub async fn ensure_and_load_raw_log_retention_generation(
    pool: &PgPool,
    chain_id: &str,
) -> Result<i64> {
    validate_non_empty("chain_id", chain_id)?;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw-log retention generation load")?;
    let generation = lock_raw_log_retention_generation(&mut transaction, chain_id).await?;
    transaction
        .commit()
        .await
        .context("failed to commit raw-log retention generation load")?;
    Ok(generation)
}

/// Insert a bounded backfill job and its child ranges, or return the existing
/// matching job for the same idempotency key.
pub async fn create_backfill_job(
    pool: &PgPool,
    request: &BackfillJobCreate,
) -> Result<BackfillJobRecord> {
    create_backfill_job_with_key_scope(pool, request, false).await
}

/// Create an automatically planned job whose effective idempotency key is
/// scoped to the raw-log retention generation captured by the job.
///
/// `request.idempotency_key` is the logical base key. The generation suffix is
/// appended only after creation has locked the chain's retention state, so a
/// compaction cannot make the call reuse a completed job from an older
/// generation.
pub async fn create_generation_scoped_backfill_job(
    pool: &PgPool,
    request: &BackfillJobCreate,
) -> Result<BackfillJobRecord> {
    ensure!(
        !request
            .idempotency_key
            .contains(RAW_LOG_RETENTION_GENERATION_KEY_SUFFIX),
        "generation-scoped backfill job requires a logical base idempotency key without a raw-log retention generation suffix"
    );
    create_backfill_job_with_key_scope(pool, request, true).await
}

async fn create_backfill_job_with_key_scope(
    pool: &PgPool,
    request: &BackfillJobCreate,
    scope_key_to_raw_log_retention_generation: bool,
) -> Result<BackfillJobRecord> {
    let range_specs = validate_backfill_job_create(request)?;
    let source_identity = serde_json::to_string(&request.source_identity)
        .context("failed to serialize source_identity")?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill job creation")?;
    let raw_log_retention_generation =
        lock_raw_log_retention_generation(&mut transaction, &request.chain_id).await?;
    let mut effective_request = request.clone();
    if scope_key_to_raw_log_retention_generation {
        effective_request.idempotency_key = format!(
            "{}{RAW_LOG_RETENTION_GENERATION_KEY_SUFFIX}{raw_log_retention_generation}",
            request.idempotency_key
        );
    }

    let inserted = sqlx::query(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key
        )
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING
            backfill_job_id,
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
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
    .bind(&effective_request.deployment_profile)
    .bind(&effective_request.chain_id)
    .bind(raw_log_retention_generation)
    .bind(source_identity)
    .bind(&effective_request.scan_mode)
    .bind(effective_request.range_start_block_number)
    .bind(effective_request.range_end_block_number)
    .bind(&effective_request.idempotency_key)
    .fetch_optional(&mut *transaction)
    .await
    .with_context(|| {
        format!(
            "failed to insert backfill job for idempotency key {}",
            effective_request.idempotency_key
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
                &effective_request.idempotency_key,
            )
            .await?
            .with_context(|| {
                format!(
                    "backfill job idempotency key {} conflicted but no row was found",
                    effective_request.idempotency_key
                )
            })?;
            ensure_existing_job_matches_request(&job, &effective_request)?;
            job
        }
    };
    if scope_key_to_raw_log_retention_generation {
        ensure!(
            job.raw_log_retention_generation == raw_log_retention_generation,
            "existing generation-scoped backfill job {} captured raw-log retention generation {}, expected {} from its effective idempotency key",
            job.backfill_job_id,
            job.raw_log_retention_generation,
            raw_log_retention_generation
        );
    }

    let ranges = load_backfill_ranges_internal(&mut *transaction, job.backfill_job_id).await?;
    ensure_existing_ranges_match_specs(job.backfill_job_id, &ranges, &range_specs)?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill job creation")?;

    Ok(BackfillJobRecord { job, ranges })
}

async fn lock_raw_log_retention_generation(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<i64> {
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since
        )
        VALUES ($1, 0, 0, false, clock_timestamp())
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to ensure raw-log retention state for backfill chain {chain_id}")
    })?;

    sqlx::query_scalar(
        r#"
        SELECT COALESCE(retention_generation, 0)::BIGINT
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        FOR SHARE
        "#,
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to lock raw-log retention state for backfill chain {chain_id}")
    })
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
        VALUES ($1, $2, $3, $2 - 1)
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
