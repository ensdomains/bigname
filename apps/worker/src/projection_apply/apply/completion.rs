use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::projection_apply::dead_letters::dead_letter_invalidation;

use super::{ClaimedInvalidation, MAX_PROJECTION_INVALIDATION_ATTEMPTS};

pub(super) async fn complete_invalidation(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        DELETE FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
          AND generation = $3
          AND claim_token = $4
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.generation)
    .bind(invalidation.claim_token)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to complete projection invalidation {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;
    if result.rows_affected() == 0 {
        release_superseded_claim(pool, invalidation).await?;
    }

    Ok(())
}

pub(super) async fn fail_invalidation(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    error: &anyhow::Error,
) -> Result<()> {
    let failure_reason = postgres_text_safe(&format!("{error:#}"));
    let failed_attempt_count = invalidation.attempt_count + 1;
    let should_dead_letter = failed_attempt_count >= MAX_PROJECTION_INVALIDATION_ATTEMPTS;
    let rows_affected = if should_dead_letter {
        dead_letter_invalidation(pool, invalidation, &failure_reason, failed_attempt_count).await?
    } else {
        sqlx::query(
            r#"
        UPDATE projection_invalidations
        SET
            claim_token = NULL,
            claimed_at = NULL,
            attempt_count = $6,
            last_failure_reason = $5,
            last_failure_at = now()
        WHERE projection = $1
          AND projection_key = $2
          AND generation = $3
          AND claim_token = $4
        "#,
        )
        .bind(&invalidation.projection)
        .bind(&invalidation.projection_key)
        .bind(invalidation.generation)
        .bind(invalidation.claim_token)
        .bind(&failure_reason)
        .bind(failed_attempt_count)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to record projection invalidation failure {}:{}",
                invalidation.projection, invalidation.projection_key
            )
        })?
        .rows_affected()
    };
    if should_dead_letter && rows_affected > 0 {
        tracing::warn!(
            projection = %invalidation.projection,
            projection_key = %invalidation.projection_key,
            generation = invalidation.generation,
            failed_attempt_count,
            max_attempts = MAX_PROJECTION_INVALIDATION_ATTEMPTS,
            failure_reason = %failure_reason,
            "moved projection invalidation to dead letter"
        );
    }
    if rows_affected == 0 {
        release_superseded_claim(pool, invalidation).await?;
    }

    Ok(())
}

async fn release_superseded_claim(pool: &PgPool, invalidation: &ClaimedInvalidation) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET claim_token = NULL,
            claimed_at = NULL
        WHERE projection = $1
          AND projection_key = $2
          AND generation > $3
          AND claim_token = $4
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.generation)
    .bind(invalidation.claim_token)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to release superseded projection invalidation claim {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;

    Ok(())
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}
