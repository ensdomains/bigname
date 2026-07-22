use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, Transaction};

use super::{NORMALIZED_EVENT_CURSOR, NormalizedEventChangeCursor};
use crate::primary_name::rebuild_heartbeat::LoopHeartbeat;
use crate::projection_apply::derive_queries::{INVALIDATION_QUERY_PREFIXES, UPSERT_SUFFIX};

const INVALIDATION_DERIVE_PROGRESS_CHANGES: i64 = 500;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProjectionInvalidationDeriveSummary {
    pub(crate) scanned_event_count: i64,
    pub(crate) enqueued_invalidation_count: u64,
}

pub(crate) async fn normalized_event_cursor_exists(pool: &PgPool) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM projection_apply_cursors
            WHERE cursor_name = $1
        )
        "#,
    )
    .bind(NORMALIZED_EVENT_CURSOR)
    .fetch_one(pool)
    .await
    .context("failed to inspect normalized-event projection apply cursor")
}

pub(crate) async fn seed_normalized_event_cursor_if_absent(
    pool: &PgPool,
    watermark: NormalizedEventChangeCursor,
) -> Result<bool> {
    let inserted = sqlx::query_scalar::<_, i64>(
        r#"
        WITH inserted AS (
            INSERT INTO projection_apply_cursors (
                cursor_name,
                last_change_id,
                updated_at
            )
            VALUES ($1, $2, now())
            ON CONFLICT (cursor_name) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*)::BIGINT FROM inserted
        "#,
    )
    .bind(NORMALIZED_EVENT_CURSOR)
    .bind(watermark.change_id)
    .fetch_one(pool)
    .await
    .context("failed to seed normalized-event projection apply cursor")?;

    Ok(inserted > 0)
}

#[cfg(test)]
pub(super) async fn derive_normalized_event_invalidations(
    pool: &PgPool,
    batch_limit: i64,
) -> Result<ProjectionInvalidationDeriveSummary> {
    if batch_limit <= 0 {
        bail!("projection apply derive batch limit must be positive, got {batch_limit}");
    }

    let complete_upper = capture_normalized_event_change_watermark(pool).await?;
    derive_normalized_event_invalidations_through_inner(pool, batch_limit, complete_upper, None)
        .await
}

pub(super) async fn derive_normalized_event_invalidations_with_heartbeat(
    pool: &PgPool,
    batch_limit: i64,
    loop_heartbeat: &mut LoopHeartbeat,
) -> Result<ProjectionInvalidationDeriveSummary> {
    if batch_limit <= 0 {
        bail!("projection apply derive batch limit must be positive, got {batch_limit}");
    }

    let complete_upper = capture_normalized_event_change_watermark(pool).await?;
    derive_normalized_event_invalidations_through_inner(
        pool,
        batch_limit,
        complete_upper,
        Some(loop_heartbeat),
    )
    .await
}

pub(crate) async fn capture_normalized_event_change_watermark(
    pool: &PgPool,
) -> Result<NormalizedEventChangeCursor> {
    sqlx::query_scalar::<_, i64>(
        "SELECT public.capture_projection_normalized_event_change_watermark()",
    )
    .fetch_one(pool)
    .await
    .context("failed to capture complete normalized-event projection change watermark")
    .map(|change_id| NormalizedEventChangeCursor { change_id })
}

#[cfg(test)]
pub(super) async fn derive_normalized_event_invalidations_through(
    pool: &PgPool,
    batch_limit: i64,
    complete_upper: NormalizedEventChangeCursor,
) -> Result<ProjectionInvalidationDeriveSummary> {
    derive_normalized_event_invalidations_through_inner(pool, batch_limit, complete_upper, None)
        .await
}

async fn derive_normalized_event_invalidations_through_inner(
    pool: &PgPool,
    batch_limit: i64,
    complete_upper: NormalizedEventChangeCursor,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<ProjectionInvalidationDeriveSummary> {
    if batch_limit <= 0 {
        bail!("projection apply derive batch limit must be positive, got {batch_limit}");
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open projection invalidation transaction")?;
    let lower = load_cursor(&mut transaction).await?;
    let Some(upper) =
        load_batch_watermark(&mut transaction, lower, complete_upper, batch_limit).await?
    else {
        transaction
            .commit()
            .await
            .context("failed to commit idle projection invalidation transaction")?;
        return Ok(ProjectionInvalidationDeriveSummary::default());
    };

    let mut scanned_event_count = 0;
    let mut enqueued_invalidation_count = 0u64;
    let mut progress_lower = lower;
    while progress_lower.change_id < upper.change_id {
        let progress_upper = load_batch_watermark(
            &mut transaction,
            progress_lower,
            upper,
            INVALIDATION_DERIVE_PROGRESS_CHANGES,
        )
        .await?
        .context("projection invalidation progress range unexpectedly had no changes")?;
        scanned_event_count +=
            count_changes(&mut transaction, progress_lower, progress_upper).await?;
        record_derivation_progress(pool, &mut loop_heartbeat).await;
        for query_prefix in INVALIDATION_QUERY_PREFIXES {
            let query = format!("{query_prefix}{UPSERT_SUFFIX}");
            enqueued_invalidation_count +=
                enqueue_invalidations(&mut transaction, &query, progress_lower, progress_upper)
                    .await?;
            record_derivation_progress(pool, &mut loop_heartbeat).await;
        }
        progress_lower = progress_upper;
    }
    store_cursor(&mut transaction, upper).await?;
    record_derivation_progress(pool, &mut loop_heartbeat).await;
    transaction
        .commit()
        .await
        .context("failed to commit projection invalidation transaction")?;

    Ok(ProjectionInvalidationDeriveSummary {
        scanned_event_count,
        enqueued_invalidation_count,
    })
}

async fn record_derivation_progress(
    pool: &PgPool,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) {
    if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
        loop_heartbeat.record_if_due(pool).await;
    }
}

async fn load_cursor(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<NormalizedEventChangeCursor> {
    let last_change_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT last_change_id
        FROM projection_apply_cursors
        WHERE cursor_name = $1
        FOR UPDATE
        "#,
    )
    .bind(NORMALIZED_EVENT_CURSOR)
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to load normalized-event projection apply cursor")?
    .unwrap_or(0);

    Ok(NormalizedEventChangeCursor {
        change_id: last_change_id,
    })
}

async fn load_batch_watermark(
    transaction: &mut Transaction<'_, Postgres>,
    lower: NormalizedEventChangeCursor,
    complete_upper: NormalizedEventChangeCursor,
    batch_limit: i64,
) -> Result<Option<NormalizedEventChangeCursor>> {
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        WITH batch AS (
            SELECT change_id
            FROM projection_normalized_event_changes
            WHERE change_id > $1
              AND change_id <= $2
            ORDER BY change_id ASC
            LIMIT $3
        )
        SELECT MAX(change_id)
        FROM batch
        "#,
    )
    .bind(lower.change_id)
    .bind(complete_upper.change_id)
    .bind(batch_limit)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to load normalized-event projection apply batch watermark")
    .map(|change_id| change_id.map(|change_id| NormalizedEventChangeCursor { change_id }))
}

async fn count_changes(
    transaction: &mut Transaction<'_, Postgres>,
    lower: NormalizedEventChangeCursor,
    upper: NormalizedEventChangeCursor,
) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM projection_normalized_event_changes
        WHERE change_id > $1
          AND change_id <= $2
        "#,
    )
    .bind(lower.change_id)
    .bind(upper.change_id)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to count normalized-event projection apply batch")
}

async fn store_cursor(
    transaction: &mut Transaction<'_, Postgres>,
    cursor: NormalizedEventChangeCursor,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_apply_cursors (
            cursor_name,
            last_change_id,
            updated_at
        )
        VALUES ($1, $2, now())
        ON CONFLICT (cursor_name)
        DO UPDATE SET
            last_change_id = EXCLUDED.last_change_id,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(NORMALIZED_EVENT_CURSOR)
    .bind(cursor.change_id)
    .execute(&mut **transaction)
    .await
    .context("failed to store normalized-event projection apply cursor")?;

    Ok(())
}

async fn enqueue_invalidations(
    transaction: &mut Transaction<'_, Postgres>,
    query: &str,
    lower: NormalizedEventChangeCursor,
    upper: NormalizedEventChangeCursor,
) -> Result<u64> {
    sqlx::query(query)
        .bind(lower.change_id)
        .bind(upper.change_id)
        .execute(&mut **transaction)
        .await
        .context("failed to enqueue projection invalidations")
        .map(|result| result.rows_affected())
}
