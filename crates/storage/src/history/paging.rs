use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use super::{HistoryEvent, decoders::decode_history_event, selectors::HistorySelector};

pub(super) async fn load_history(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history_internal(pool, selector, canonical_only, false).await
}

pub(super) async fn load_history_head(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Option<HistoryEvent>> {
    let mut rows = load_history_internal(pool, selector, canonical_only, true).await?;
    Ok(rows.drain(..).next())
}

async fn load_history_internal(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
    head_only: bool,
) -> Result<Vec<HistoryEvent>> {
    if matches!(selector, HistorySelector::None) {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_identity,
            ne.namespace,
            ne.logical_name_id,
            ne.resource_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.transaction_hash,
            ne.log_index,
            ne.raw_fact_ref,
            ne.derivation_kind,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.before_state,
            ne.after_state,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                        THEN ne.after_state -> 'provenance'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                        THEN ne.before_state -> 'provenance'
                END,
                '{}'::jsonb
            ) AS provenance,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'coverage') = 'object'
                        THEN ne.after_state -> 'coverage'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'coverage') = 'object'
                        THEN ne.before_state -> 'coverage'
                END,
                '{}'::jsonb
            ) AS coverage
        FROM normalized_events ne
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE
        "#,
    );

    match &selector {
        HistorySelector::LogicalNames(logical_name_ids) => {
            push_string_filter(&mut builder, "ne.logical_name_id", logical_name_ids);
        }
        HistorySelector::Resources(resource_ids) => {
            push_uuid_filter(&mut builder, "ne.resource_id", resource_ids);
        }
        HistorySelector::LogicalNamesOrResources {
            logical_name_ids,
            resource_ids,
        } => {
            builder.push("(");
            push_string_filter(&mut builder, "ne.logical_name_id", logical_name_ids);
            builder.push(" OR ");
            push_uuid_filter(&mut builder, "ne.resource_id", resource_ids);
            builder.push(")");
        }
        HistorySelector::None => unreachable!("none selector handled before query build"),
    }

    if canonical_only {
        builder.push(
            r#"
            AND ne.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            "#,
        );
    }

    builder.push(
        r#"
        ORDER BY
            CASE WHEN ne.block_number IS NULL THEN 1 ELSE 0 END,
            ne.block_number DESC,
            CASE WHEN ne.chain_id IS NULL THEN 1 ELSE 0 END,
            ne.chain_id ASC,
            CASE WHEN ne.block_hash IS NULL THEN 1 ELSE 0 END,
            ne.block_hash DESC,
            CASE WHEN ne.transaction_hash IS NULL THEN 1 ELSE 0 END,
            ne.transaction_hash DESC,
            COALESCE(ne.log_index, -1) DESC,
            ne.event_identity DESC
        "#,
    );

    if head_only {
        builder.push(" LIMIT 1");
    }

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch normalized-event history rows")?;

    rows.into_iter().map(decode_history_event).collect()
}

fn push_string_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [String],
) {
    builder.push(column);
    push_string_filter_tail(builder, values);
}

fn push_string_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [String]) {
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
}

fn push_uuid_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [Uuid],
) {
    builder.push(column);
    push_uuid_filter_tail(builder, values);
}

fn push_uuid_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [Uuid]) {
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
}
