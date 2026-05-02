use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::canonicality::parse_canonicality_state;
use super::types::RelevantEvent;
use super::{CANONICAL_STATE_FILTER, EVENT_KIND_PERMISSION_CHANGED};

pub(super) fn stream_target_resource_ids<'a>(
    pool: &'a PgPool,
) -> impl Stream<Item = Result<Uuid>> + 'a {
    sqlx::query(
        r#"
        SELECT DISTINCT resource_id
        FROM normalized_events
        WHERE event_kind = $1
          AND resource_id IS NOT NULL
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY resource_id
        "#,
    )
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .fetch(pool)
    .map(|row| {
        row.context("failed to stream resource_ids for permissions_current rebuild")
            .and_then(|row| row.try_get("resource_id").context("missing resource_id"))
    })
}

pub(super) async fn load_permission_events(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<RelevantEvent>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.resource_id,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            ne.log_index,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state
        FROM normalized_events ne
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.event_kind = $1
          AND ne.resource_id = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number ASC NULLS FIRST,
            ne.log_index ASC NULLS FIRST,
            ne.normalized_event_id ASC
        "#
    ))
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load canonical PermissionChanged events for resource_id {resource_id}")
    })?;

    rows.into_iter().map(decode_relevant_event).collect()
}

fn decode_relevant_event(row: sqlx::postgres::PgRow) -> Result<RelevantEvent> {
    Ok(RelevantEvent {
        normalized_event_id: row.try_get("normalized_event_id")?,
        source_family: row.try_get("source_family")?,
        manifest_version: row.try_get("manifest_version")?,
        source_manifest_id: row.try_get("source_manifest_id")?,
        chain_id: row
            .try_get::<Option<String>, _>("chain_id")?
            .context("PermissionChanged rows must include chain_id")?,
        block_number: row
            .try_get::<Option<i64>, _>("block_number")?
            .context("PermissionChanged rows must include block_number")?,
        block_hash: row
            .try_get::<Option<String>, _>("block_hash")?
            .context("PermissionChanged rows must include block_hash")?,
        block_timestamp: row.try_get("block_timestamp")?,
        raw_fact_ref: row.try_get("raw_fact_ref")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")?,
        )?,
        after_state: row.try_get("after_state")?,
    })
}
