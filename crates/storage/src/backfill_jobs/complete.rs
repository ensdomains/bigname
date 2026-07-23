use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Postgres};
use tracing::warn;

use super::{
    coverage_facts::{
        BackfillCoverageFactDerivation, BackfillCoverageFactStreamItem, BackfillCoverageFactWrite,
        BackfillCoverageProgress, write_backfill_coverage_fact_stream,
        write_backfill_coverage_facts_from_iter,
    },
    decode::{decode_backfill_job, decode_backfill_range},
    read::{
        incomplete_range_count, load_backfill_job_for_update, load_backfill_range_for_update,
        load_backfill_range_job_id, load_backfill_ranges_for_update,
    },
    sql::{backfill_job_returning_sql, backfill_range_returning_sql},
    types::{BackfillJob, BackfillLifecycleStatus, BackfillRange},
    validate::{ensure_lease_matches, ensure_ranges_ready_for_job_completion, validate_non_empty},
};

/// Complete a leased range after its checkpoint reaches the declared end.
pub async fn complete_backfill_range(
    pool: &PgPool,
    backfill_range_id: i64,
    lease_token: &str,
) -> Result<BackfillRange> {
    complete_backfill_range_recording_coverage(pool, backfill_range_id, lease_token, |_| {
        std::iter::empty()
    })
    .await
}

/// Complete a leased range and, when this range completion also completes the
/// parent job, record the job's coverage facts in the same transaction as the
/// job status flip. `coverage_facts` is invoked at most once, with the
/// completed job row, and must derive facts from the executor's in-memory plan
/// (never by reloading the watch set, which can drift during long backfills).
pub async fn complete_backfill_range_recording_coverage<F, I>(
    pool: &PgPool,
    backfill_range_id: i64,
    lease_token: &str,
    coverage_facts: F,
) -> Result<BackfillRange>
where
    F: FnOnce(&BackfillJob) -> I,
    I: Iterator<Item = BackfillCoverageFactWrite>,
{
    validate_non_empty("lease_token", lease_token)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range completion")?;

    // Lock the job before the range (matching complete_backfill_job's lock
    // order) so concurrent completions of the final two ranges serialize.
    // Otherwise, under READ COMMITTED, neither transaction's incomplete-range
    // count reaches zero, and the job is later flipped by the reservation path
    // without coverage facts.
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

    maybe_complete_backfill_job(&mut transaction, range.backfill_job_id, coverage_facts).await?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill range completion")?;

    Ok(range)
}

/// Progress-aware completion for whole-active plans. The stream may emit
/// progress markers for examined inputs that do not produce facts; database
/// insert chunks also trigger the supplied hook.
pub async fn complete_backfill_range_recording_coverage_with_progress<F, I>(
    pool: &PgPool,
    backfill_range_id: i64,
    lease_token: &str,
    coverage_facts: F,
    progress: &mut dyn BackfillCoverageProgress,
) -> Result<BackfillRange>
where
    F: FnOnce(&BackfillJob) -> I,
    I: Iterator<Item = BackfillCoverageFactStreamItem>,
{
    validate_non_empty("lease_token", lease_token)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range completion")?;
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

    maybe_complete_backfill_job_with_progress(
        &mut transaction,
        range.backfill_job_id,
        coverage_facts,
        progress,
    )
    .await?;
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

    let ranges = load_backfill_ranges_for_update(&mut *transaction, backfill_job_id).await?;
    ensure_ranges_ready_for_job_completion(backfill_job_id, &ranges)?;

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
    warn_backfill_job_completed_without_coverage_facts(&job, "complete_backfill_job");

    transaction
        .commit()
        .await
        .context("failed to commit backfill job completion")?;

    Ok(job)
}

/// Every job completed without coverage facts leaves a durable gap that
/// checkpoint promotion cannot prove over; make those flips loud so operators
/// can re-derive (or rerun) before promotion stalls on the missing tuples.
pub(super) fn warn_backfill_job_completed_without_coverage_facts(
    job: &BackfillJob,
    completion_path: &str,
) {
    warn_backfill_job_coverage_fact_gap(
        job,
        completion_path,
        "backfill job completed without coverage facts",
    );
}

fn warn_backfill_job_coverage_fact_gap(job: &BackfillJob, completion_path: &str, headline: &str) {
    let payload_format = job
        .source_identity
        .get("source_identity_payload_format")
        .and_then(Value::as_str);
    let compact_digest_identity = matches!(
        payload_format,
        Some("selected_targets_digest_v1")
            | Some("selected_targets_digest_with_generic_topic_scans_v1")
    );
    // Only recommend the repair command for identity shapes it derives
    // soundly: plain verbatim selected_targets payloads. Generic-scan and
    // family-scan-only shapes do not persist the scanned family's target
    // spans, so their coverage is unrecoverable.
    let guidance = if compact_digest_identity {
        "; its compact digest identity makes coverage unrecoverable without re-running the job on fact-writing code"
    } else if payload_format.is_none() {
        "; run repair derive-backfill-coverage-facts to derive coverage from its verbatim selected_targets"
    } else {
        "; its identity does not persist the family target spans needed for sound coverage facts, so re-run the job on fact-writing code"
    };
    warn!(
        backfill_job_id = job.backfill_job_id,
        chain_id = %job.chain_id,
        completion_path,
        source_identity_payload_format = payload_format.unwrap_or_default(),
        compact_digest_identity,
        "{headline}{guidance}",
    );
}

async fn maybe_complete_backfill_job<F, I>(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
    coverage_facts: F,
) -> Result<()>
where
    F: FnOnce(&BackfillJob) -> I,
    I: Iterator<Item = BackfillCoverageFactWrite>,
{
    // The caller holds the job row lock, so this count cannot race a
    // concurrent completion of another range of the same job.
    let incomplete_count = incomplete_range_count(&mut **executor, backfill_job_id).await?;
    if incomplete_count != 0 {
        return Ok(());
    }

    let job = set_backfill_job_completed(executor, backfill_job_id).await?;
    let inserted_fact_count = write_backfill_coverage_facts_from_iter(
        executor,
        job.backfill_job_id,
        BackfillCoverageFactDerivation::JobCompletion,
        coverage_facts(&job),
    )
    .await?;
    // Catches every flip that ends up fact-less through this path: the
    // exported complete_backfill_range's empty iterator, and recording
    // completions whose derivation yielded nothing.
    if inserted_fact_count == 0 {
        warn_backfill_job_coverage_fact_gap(
            &job,
            "complete_backfill_range",
            "backfill job completion recorded zero coverage facts",
        );
    }
    Ok(())
}

async fn maybe_complete_backfill_job_with_progress<F, I>(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
    coverage_facts: F,
    progress: &mut dyn BackfillCoverageProgress,
) -> Result<()>
where
    F: FnOnce(&BackfillJob) -> I,
    I: Iterator<Item = BackfillCoverageFactStreamItem>,
{
    let incomplete_count = incomplete_range_count(&mut **executor, backfill_job_id).await?;
    if incomplete_count != 0 {
        return Ok(());
    }

    let job = set_backfill_job_completed(executor, backfill_job_id).await?;
    let inserted_fact_count = write_backfill_coverage_fact_stream(
        executor,
        job.backfill_job_id,
        BackfillCoverageFactDerivation::JobCompletion,
        coverage_facts(&job),
        &mut Some(progress),
    )
    .await?;
    if inserted_fact_count == 0 {
        warn_backfill_job_coverage_fact_gap(
            &job,
            "complete_backfill_range",
            "backfill job completion recorded zero coverage facts",
        );
    }
    Ok(())
}

pub(super) async fn set_backfill_job_completed(
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
