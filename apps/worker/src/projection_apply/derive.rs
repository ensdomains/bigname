use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction, types::Uuid};

use super::{NORMALIZED_EVENT_CURSOR, NormalizedEventChangeCursor};
use crate::projection_apply::derive_queries::{
    INVALIDATION_QUERY_PREFIXES, UPSERT_SUFFIX, current_projection_invalidation_prefixes,
};

mod staging_inputs;

use staging_inputs::{
    DIRECT_INVALIDATION_REVISIONS_PREFIX, PERMISSIONS_RESOURCE_INPUT_REVISIONS_PREFIX,
    children_parent_changed_requires_full_restage,
};
pub(crate) use staging_inputs::{
    ProjectionStagingInputWatermark, capture_projection_staging_input_watermark_in_transaction,
};

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

pub(crate) async fn seed_normalized_event_cursor_if_absent_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
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
    .fetch_one(&mut **transaction)
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
    derive_normalized_event_invalidations_through(pool, batch_limit, complete_upper).await
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

pub(crate) async fn capture_normalized_event_change_watermark_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<NormalizedEventChangeCursor> {
    sqlx::query_scalar::<_, i64>(
        "SELECT public.capture_projection_normalized_event_change_watermark()",
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to capture complete normalized-event projection change watermark")
    .map(|change_id| NormalizedEventChangeCursor { change_id })
}

pub(crate) async fn completed_projection_sources_changed(
    transaction: &mut Transaction<'_, Postgres>,
    projection: &str,
    lower: ProjectionStagingInputWatermark,
    upper: ProjectionStagingInputWatermark,
    completed_range: CompletedProjectionSourceRange<'_>,
) -> Result<bool> {
    let prefixes = current_projection_invalidation_prefixes(projection)
        .with_context(|| format!("unsupported staged projection {projection}"))?;
    if projection == "children_current"
        && children_parent_changed_requires_full_restage(
            transaction,
            lower.normalized_change_id,
            upper.normalized_change_id,
        )
        .await?
    {
        return Ok(true);
    }
    for prefix in prefixes {
        if completed_candidate_keys_changed(
            transaction,
            projection,
            lower.normalized_change_id,
            upper.normalized_change_id,
            completed_range,
            prefix,
        )
        .await?
        {
            return Ok(true);
        }
    }
    if completed_candidate_keys_changed(
        transaction,
        projection,
        lower.direct_invalidation_revision,
        upper.direct_invalidation_revision,
        completed_range,
        DIRECT_INVALIDATION_REVISIONS_PREFIX,
    )
    .await?
    {
        return Ok(true);
    }
    if projection == "permissions_current" {
        return completed_candidate_keys_changed(
            transaction,
            projection,
            lower.permissions_resource_revision,
            upper.permissions_resource_revision,
            completed_range,
            PERMISSIONS_RESOURCE_INPUT_REVISIONS_PREFIX,
        )
        .await;
    }
    Ok(false)
}

async fn completed_candidate_keys_changed(
    transaction: &mut Transaction<'_, Postgres>,
    projection: &str,
    lower_revision: i64,
    upper_revision: i64,
    completed_range: CompletedProjectionSourceRange<'_>,
    prefix: &str,
) -> Result<bool> {
    if upper_revision <= lower_revision {
        return Ok(false);
    }
    let last_source_key = match completed_range {
        CompletedProjectionSourceRange::Through(last_source_key) => last_source_key,
        CompletedProjectionSourceRange::Full => {
            let query = completed_change_query(prefix, "TRUE");
            return sqlx::query_scalar::<_, bool>(&query)
                .bind(lower_revision)
                .bind(upper_revision)
                .bind(projection)
                .fetch_one(&mut **transaction)
                .await
                .context("failed to detect a change in the completed projection source range");
        }
    };
    let changed = match projection {
        "name_current" => {
            let cursor = json_string(last_source_key, projection)?;
            let query = completed_change_query(prefix, "key_payload ->> 'logical_name_id' <= $4");
            sqlx::query_scalar::<_, bool>(&query)
                .bind(lower_revision)
                .bind(upper_revision)
                .bind(projection)
                .bind(cursor)
                .fetch_one(&mut **transaction)
                .await?
        }
        "children_current" => {
            let cursor = json_string_array(last_source_key, 3, projection)?;
            let query =
                completed_change_query(prefix, "key_payload ->> 'parent_logical_name_id' <= $4");
            sqlx::query_scalar::<_, bool>(&query)
                .bind(lower_revision)
                .bind(upper_revision)
                .bind(projection)
                .bind(&cursor[0])
                .fetch_one(&mut **transaction)
                .await?
        }
        "permissions_current" | "record_inventory_current" => {
            let cursor = Uuid::parse_str(json_string(last_source_key, projection)?)?;
            let query =
                completed_change_query(prefix, "(key_payload ->> 'resource_id')::UUID <= $4");
            sqlx::query_scalar::<_, bool>(&query)
                .bind(lower_revision)
                .bind(upper_revision)
                .bind(projection)
                .bind(cursor)
                .fetch_one(&mut **transaction)
                .await?
        }
        "resolver_current" => {
            let cursor = json_string_array(last_source_key, 2, projection)?;
            let query = completed_change_query(
                prefix,
                "(key_payload ->> 'chain_id', key_payload ->> 'resolver_address') <= ($4, $5)",
            );
            sqlx::query_scalar::<_, bool>(&query)
                .bind(lower_revision)
                .bind(upper_revision)
                .bind(projection)
                .bind(&cursor[0])
                .bind(&cursor[1])
                .fetch_one(&mut **transaction)
                .await?
        }
        "address_names_current" => {
            let cursor = json_string_array(last_source_key, 2, projection)?;
            let query = completed_change_query(
                prefix,
                "key_payload ->> 'logical_name_id' IS NULL OR key_payload ->> 'logical_name_id' <= $4",
            );
            sqlx::query_scalar::<_, bool>(&query)
                .bind(lower_revision)
                .bind(upper_revision)
                .bind(projection)
                .bind(&cursor[0])
                .fetch_one(&mut **transaction)
                .await?
        }
        "primary_names_current" => {
            let cursor = json_string_array(last_source_key, 3, projection)?;
            let query = completed_change_query(
                prefix,
                "(key_payload ->> 'address', key_payload ->> 'namespace', key_payload ->> 'coin_type') <= ($4, $5, $6)",
            );
            sqlx::query_scalar::<_, bool>(&query)
                .bind(lower_revision)
                .bind(upper_revision)
                .bind(projection)
                .bind(&cursor[0])
                .bind(&cursor[1])
                .bind(&cursor[2])
                .fetch_one(&mut **transaction)
                .await?
        }
        _ => unreachable!("projection prefix was accepted above"),
    };
    Ok(changed)
}

#[derive(Clone, Copy)]
pub(crate) enum CompletedProjectionSourceRange<'a> {
    Through(&'a Value),
    Full,
}

fn completed_change_query(prefix: &str, completed_predicate: &str) -> String {
    format!(
        "{prefix} SELECT EXISTS (SELECT 1 FROM candidate_keys WHERE projection = $3 AND projection_key IS NOT NULL AND btrim(projection_key) <> '' AND ({completed_predicate}))"
    )
}

fn json_string<'a>(value: &'a Value, projection: &str) -> Result<&'a str> {
    value
        .as_str()
        .with_context(|| format!("{projection} staging source cursor must be a string"))
}

fn json_string_array(value: &Value, len: usize, projection: &str) -> Result<Vec<String>> {
    let parts = value
        .as_array()
        .with_context(|| format!("{projection} staging source cursor must be an array"))?;
    if parts.len() != len {
        bail!("{projection} staging source cursor must contain {len} strings");
    }
    parts
        .iter()
        .map(|part| {
            part.as_str()
                .map(str::to_owned)
                .with_context(|| format!("{projection} staging source cursor must contain strings"))
        })
        .collect()
}

pub(super) async fn derive_normalized_event_invalidations_through(
    pool: &PgPool,
    batch_limit: i64,
    complete_upper: NormalizedEventChangeCursor,
) -> Result<ProjectionInvalidationDeriveSummary> {
    if batch_limit <= 0 {
        bail!("projection apply derive batch limit must be positive, got {batch_limit}");
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open projection invalidation transaction")?;
    sqlx::query(
        r#"
        SELECT set_config(
            'bigname.normalized_projection_invalidation_derive',
            'on',
            true
        )
        "#,
    )
    .execute(&mut *transaction)
    .await
    .context("failed to identify normalized-event projection invalidation derive")?;
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

    let scanned_event_count = count_changes(&mut transaction, lower, upper).await?;
    let mut enqueued_invalidation_count = 0u64;
    for query_prefix in INVALIDATION_QUERY_PREFIXES {
        let query = format!("{query_prefix}{UPSERT_SUFFIX}");
        enqueued_invalidation_count +=
            enqueue_invalidations(&mut transaction, &query, lower, upper).await?;
    }
    store_cursor(&mut transaction, upper).await?;
    transaction
        .commit()
        .await
        .context("failed to commit projection invalidation transaction")?;

    Ok(ProjectionInvalidationDeriveSummary {
        scanned_event_count,
        enqueued_invalidation_count,
    })
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
