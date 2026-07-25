use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Row, types::time::OffsetDateTime};

mod attempts;
pub(crate) mod fence;
mod rearm;
mod terminal_job;

pub use attempts::load_coverage_recovery_job_attempt_watermark;
pub use rearm::rearm_terminal_coverage_recovery_failure;
pub use terminal_job::record_coverage_recovery_terminal_failure;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageRecoveryFailureKey {
    pub deployment_profile: String,
    pub chain_id: String,
    pub raw_log_retention_generation: i64,
    pub source_family: String,
    pub emitting_address: String,
    pub required_from_block: i64,
    pub required_to_block: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageRecoveryReservationFence {
    pub key: CoverageRecoveryFailureKey,
    pub expected_write_epoch: i64,
    pub expected_failure_attempt_count: i64,
    pub expected_job_attempt_count: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageRecoveryFailureState {
    RetryBackoff,
    Terminal,
}

impl CoverageRecoveryFailureState {
    fn as_str(self) -> &'static str {
        match self {
            Self::RetryBackoff => "retry_backoff",
            Self::Terminal => "terminal",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "retry_backoff" => Ok(Self::RetryBackoff),
            "terminal" => Ok(Self::Terminal),
            _ => anyhow::bail!("unknown coverage recovery failure state {value}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageRecoveryFailureRecord {
    pub key: CoverageRecoveryFailureKey,
    pub state: CoverageRecoveryFailureState,
    pub attempt_count: i64,
    pub retry_not_before: Option<OffsetDateTime>,
    pub last_backfill_job_id: Option<i64>,
    pub last_job_attempt_count: i64,
    pub failure_reason: String,
    pub failure_metadata: Value,
}

#[expect(clippy::too_many_arguments)]
pub async fn record_coverage_recovery_attempt_failure(
    pool: &PgPool,
    key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
    backfill_job_id: i64,
    job_attempt_count: i64,
    maximum_attempts: i64,
    initial_backoff: Duration,
    maximum_backoff: Duration,
    failure_reason: &str,
    terminal_failure_reason: &str,
    mut failure_metadata: Value,
) -> Result<CoverageRecoveryFailureRecord> {
    validate_key(key)?;
    ensure!(
        backfill_job_id > 0,
        "coverage recovery job id must be positive"
    );
    ensure!(
        job_attempt_count > 0,
        "coverage recovery job attempt count must be positive"
    );
    ensure!(
        maximum_attempts > 0,
        "coverage recovery maximum attempts must be positive"
    );
    ensure!(
        !initial_backoff.is_zero() && maximum_backoff >= initial_backoff,
        "coverage recovery backoff bounds are invalid"
    );
    validate_failure(failure_reason, &failure_metadata)?;
    ensure!(
        !terminal_failure_reason.trim().is_empty(),
        "terminal coverage recovery failure reason must not be empty"
    );

    let mut transaction = pool
        .begin()
        .await
        .context("failed to start coverage recovery failure transaction")?;
    lock_failure_key(&mut transaction, key).await?;
    fence::validate_expected_epoch(&mut transaction, key, expected_epoch).await?;
    let persisted_job_attempt_count = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MAX(attempt_count)::BIGINT
        FROM backfill_ranges
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to validate persisted coverage recovery job attempts")?
    .context("coverage recovery job has no persisted child ranges")?;
    ensure!(
        persisted_job_attempt_count >= job_attempt_count,
        "coverage recovery job {backfill_job_id} now has persisted attempt count {persisted_job_attempt_count}, which is behind cached failure observation {job_attempt_count}; the interval may have been operator re-armed"
    );
    let existing = load_failure_for_update(&mut transaction, key).await?;
    let observed_job_attempt_count =
        attempts::load_job_attempt_watermark(&mut transaction, key, backfill_job_id).await?;
    if observed_job_attempt_count >= job_attempt_count {
        let existing = existing.context(
            "coverage recovery job attempt watermark exists without its parent failure record",
        )?;
        let record = attempts::point_failure_at_observed_job(
            &mut transaction,
            key,
            &existing,
            backfill_job_id,
            observed_job_attempt_count,
        )
        .await?;
        attempts::update_bound_job_attempt_count(
            &mut transaction,
            backfill_job_id,
            expected_epoch,
            observed_job_attempt_count,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit repeated coverage recovery failure observation")?;
        return Ok(record);
    }

    let prior_attempt_count = existing.as_ref().map_or(0, |record| record.attempt_count);
    let newly_observed_attempts = job_attempt_count - observed_job_attempt_count;
    let attempt_count = prior_attempt_count
        .checked_add(newly_observed_attempts)
        .context("coverage recovery attempt count overflowed")?
        .min(maximum_attempts);
    let state = if attempt_count >= maximum_attempts {
        CoverageRecoveryFailureState::Terminal
    } else {
        CoverageRecoveryFailureState::RetryBackoff
    };
    let retry_after_seconds = exponential_backoff_seconds(
        attempt_count,
        initial_backoff.as_secs(),
        maximum_backoff.as_secs(),
    );
    let retry_not_before = if state == CoverageRecoveryFailureState::RetryBackoff {
        Some(OffsetDateTime::now_utc() + Duration::from_secs(retry_after_seconds))
    } else {
        None
    };
    let metadata = failure_metadata
        .as_object_mut()
        .expect("failure metadata was validated as an object");
    metadata.insert("attempt_count".to_owned(), Value::from(attempt_count));
    metadata.insert("maximum_attempts".to_owned(), Value::from(maximum_attempts));
    metadata.insert("state".to_owned(), Value::from(state.as_str()));
    if state == CoverageRecoveryFailureState::Terminal {
        metadata.insert("cause".to_owned(), Value::from("attempt_budget_exhausted"));
    }
    metadata.insert(
        "retry_after_seconds".to_owned(),
        if state == CoverageRecoveryFailureState::RetryBackoff {
            Value::from(retry_after_seconds)
        } else {
            Value::Null
        },
    );
    let failure_metadata = serde_json::to_string(&failure_metadata)
        .context("failed to serialize coverage recovery failure metadata")?;
    let persisted_failure_reason = if state == CoverageRecoveryFailureState::Terminal {
        terminal_failure_reason
    } else {
        failure_reason
    };
    let row = sqlx::query(
        r#"
        INSERT INTO normalized_replay_coverage_recovery_failures (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_family,
            emitting_address,
            required_from_block,
            required_to_block,
            state,
            attempt_count,
            retry_not_before,
            last_backfill_job_id,
            last_job_attempt_count,
            failure_reason,
            failure_metadata
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb
        )
        ON CONFLICT (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_family,
            emitting_address,
            required_from_block,
            required_to_block
        ) DO UPDATE
        SET state = EXCLUDED.state,
            attempt_count = EXCLUDED.attempt_count,
            retry_not_before = EXCLUDED.retry_not_before,
            last_backfill_job_id = EXCLUDED.last_backfill_job_id,
            last_job_attempt_count = EXCLUDED.last_job_attempt_count,
            failure_reason = EXCLUDED.failure_reason,
            failure_metadata = EXCLUDED.failure_metadata,
            last_failed_at = now(),
            updated_at = now()
        RETURNING
            state,
            attempt_count,
            retry_not_before,
            last_backfill_job_id,
            last_job_attempt_count,
            failure_reason,
            failure_metadata
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .bind(state.as_str())
    .bind(attempt_count)
    .bind(retry_not_before)
    .bind(backfill_job_id)
    .bind(job_attempt_count)
    .bind(persisted_failure_reason)
    .bind(failure_metadata)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to persist coverage recovery attempt failure")?;
    let record = decode_failure(key.clone(), row)?;
    attempts::upsert_job_attempt_watermark(
        &mut transaction,
        key,
        backfill_job_id,
        job_attempt_count,
    )
    .await?;
    attempts::update_bound_job_attempt_count(
        &mut transaction,
        backfill_job_id,
        expected_epoch,
        job_attempt_count,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit coverage recovery attempt failure")?;
    Ok(record)
}

pub(crate) async fn lock_failure_key(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
) -> Result<()> {
    // A missing row cannot be protected by SELECT ... FOR UPDATE. Serialize
    // the natural key as well so concurrent first failures cannot both derive
    // their cumulative count from an absent record.
    let lock_identity = serde_json::to_string(&(
        &key.deployment_profile,
        &key.chain_id,
        key.raw_log_retention_generation,
        &key.source_family,
        &key.emitting_address,
        key.required_from_block,
        key.required_to_block,
    ))
    .context("failed to encode coverage recovery failure lock identity")?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(lock_identity)
        .execute(&mut **transaction)
        .await
        .context("failed to lock coverage recovery failure key")?;
    Ok(())
}

pub async fn load_coverage_recovery_failure(
    pool: &PgPool,
    key: &CoverageRecoveryFailureKey,
) -> Result<Option<CoverageRecoveryFailureRecord>> {
    validate_key(key)?;
    let row = sqlx::query(
        r#"
        SELECT
            state,
            attempt_count,
            retry_not_before,
            last_backfill_job_id,
            last_job_attempt_count,
            failure_reason,
            failure_metadata
        FROM normalized_replay_coverage_recovery_failures
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND raw_log_retention_generation = $3
          AND source_family = $4
          AND emitting_address = $5
          AND required_from_block = $6
          AND required_to_block = $7
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .fetch_optional(pool)
    .await
    .context("failed to load coverage recovery failure")?;
    row.map(|row| decode_failure(key.clone(), row)).transpose()
}

pub async fn clear_coverage_recovery_failure(
    pool: &PgPool,
    key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
) -> Result<()> {
    validate_key(key)?;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start resolved coverage recovery transaction")?;
    lock_failure_key(&mut transaction, key).await?;
    fence::validate_expected_epoch(&mut transaction, key, expected_epoch).await?;
    fence::advance_epoch(&mut transaction, key).await?;
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_coverage_recovery_failures
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND raw_log_retention_generation = $3
          AND source_family = $4
          AND emitting_address = $5
          AND required_from_block = $6
          AND required_to_block = $7
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .execute(&mut *transaction)
    .await
    .context("failed to clear resolved coverage recovery failure")?;
    transaction
        .commit()
        .await
        .context("failed to commit resolved coverage recovery failure")?;
    Ok(())
}

pub(crate) async fn load_failure_for_update(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
) -> Result<Option<CoverageRecoveryFailureRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            state,
            attempt_count,
            retry_not_before,
            last_backfill_job_id,
            last_job_attempt_count,
            failure_reason,
            failure_metadata
        FROM normalized_replay_coverage_recovery_failures
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND raw_log_retention_generation = $3
          AND source_family = $4
          AND emitting_address = $5
          AND required_from_block = $6
          AND required_to_block = $7
        FOR UPDATE
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to lock coverage recovery failure")?;
    row.map(|row| decode_failure(key.clone(), row)).transpose()
}

fn decode_failure(
    key: CoverageRecoveryFailureKey,
    row: sqlx::postgres::PgRow,
) -> Result<CoverageRecoveryFailureRecord> {
    Ok(CoverageRecoveryFailureRecord {
        key,
        state: CoverageRecoveryFailureState::parse(row.try_get("state")?)?,
        attempt_count: row.try_get("attempt_count")?,
        retry_not_before: row.try_get("retry_not_before")?,
        last_backfill_job_id: row.try_get("last_backfill_job_id")?,
        last_job_attempt_count: row.try_get("last_job_attempt_count")?,
        failure_reason: row.try_get("failure_reason")?,
        failure_metadata: row.try_get("failure_metadata")?,
    })
}

fn exponential_backoff_seconds(attempt_count: i64, initial: u64, maximum: u64) -> u64 {
    let exponent = u32::try_from(attempt_count.saturating_sub(1))
        .unwrap_or(u32::MAX)
        .min(63);
    initial.saturating_mul(1_u64 << exponent).min(maximum)
}

pub(crate) fn validate_key(key: &CoverageRecoveryFailureKey) -> Result<()> {
    ensure!(
        !key.deployment_profile.trim().is_empty()
            && !key.chain_id.trim().is_empty()
            && !key.source_family.trim().is_empty()
            && !key.emitting_address.trim().is_empty(),
        "coverage recovery failure key text fields must not be empty"
    );
    ensure!(
        key.raw_log_retention_generation >= 0,
        "coverage recovery failure generation must not be negative"
    );
    ensure!(
        key.required_from_block >= 0 && key.required_to_block >= key.required_from_block,
        "coverage recovery failure range is invalid"
    );
    Ok(())
}

fn validate_failure(reason: &str, metadata: &Value) -> Result<()> {
    ensure!(
        !reason.trim().is_empty(),
        "coverage recovery failure reason must not be empty"
    );
    ensure!(
        metadata.is_object(),
        "coverage recovery failure metadata must be an object"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::exponential_backoff_seconds;

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        assert_eq!(exponential_backoff_seconds(1, 5, 300), 5);
        assert_eq!(exponential_backoff_seconds(2, 5, 300), 10);
        assert_eq!(exponential_backoff_seconds(6, 5, 300), 160);
        assert_eq!(exponential_backoff_seconds(7, 5, 300), 300);
        assert_eq!(exponential_backoff_seconds(32, 5, 300), 300);
    }
}
