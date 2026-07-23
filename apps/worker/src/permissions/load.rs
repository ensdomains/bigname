use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::types::{RelevantEvent, ResourceProjectionContext};
use super::{
    CANONICAL_STATE_FILTER, EVENT_KIND_AUTHORITY_EPOCH_CHANGED, EVENT_KIND_PERMISSION_CHANGED,
    EVENT_KIND_PERMISSION_SCOPE_CHANGED, EVENT_KIND_REGISTRATION_GRANTED,
    EVENT_KIND_ROOT_PERMISSION_CHANGED, EVENT_KIND_TOKEN_RESOURCE_LINKED,
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_ROOT_L1,
};

pub(super) fn stream_target_resource_ids_after(
    pool: &PgPool,
    after_resource_id: Option<Uuid>,
    limit: i64,
) -> impl Stream<Item = Result<Uuid>> + '_ {
    sqlx::query(
        r#"
        SELECT targets.resource_id
        FROM (
            SELECT ne.resource_id
            FROM normalized_events ne
            JOIN resources resource
              ON resource.resource_id = ne.resource_id
             AND resource.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            WHERE (
                  ne.event_kind IN ($1, $2, $3, $4)
                  OR (
                      ne.event_kind IN ($5, $6)
                      AND ne.source_family IN ($7, $8)
                  )
              )
              AND ne.resource_id IS NOT NULL
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )

            UNION

            SELECT resource.resource_id
            FROM resources resource
            WHERE resource.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
              AND (
                  (
                      NULLIF(BTRIM(resource.provenance ->> 'source_family'), '') IS NOT NULL
                      AND jsonb_typeof(resource.provenance -> 'manifest_version') = 'number'
                      AND (resource.provenance ->> 'manifest_version')::NUMERIC > 0
                      AND (resource.provenance ->> 'manifest_version')::NUMERIC <= 9223372036854775807
                  )
                  OR (
                      NULLIF(BTRIM(resource.provenance ->> 'binding_source_family'), '') IS NOT NULL
                      AND jsonb_typeof(resource.provenance -> 'binding_manifest_version') = 'number'
                      AND (resource.provenance ->> 'binding_manifest_version')::NUMERIC > 0
                      AND (resource.provenance ->> 'binding_manifest_version')::NUMERIC <= 9223372036854775807
                  )
              )
        ) targets
        WHERE ($9::UUID IS NULL OR targets.resource_id > $9)
        ORDER BY targets.resource_id
        LIMIT $10
        "#,
    )
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .bind(EVENT_KIND_ROOT_PERMISSION_CHANGED)
    .bind(EVENT_KIND_PERMISSION_SCOPE_CHANGED)
    .bind(EVENT_KIND_AUTHORITY_EPOCH_CHANGED)
    .bind(EVENT_KIND_REGISTRATION_GRANTED)
    .bind(EVENT_KIND_TOKEN_RESOURCE_LINKED)
    .bind(SOURCE_FAMILY_ENS_V2_REGISTRY_L1)
    .bind(SOURCE_FAMILY_ENS_V2_ROOT_L1)
    .bind(after_resource_id)
    .bind(limit)
    .fetch(pool)
    .map(|row| {
        row.context("failed to stream resource_ids for permissions_current rebuild")
            .and_then(|row| row.try_get("resource_id").context("missing resource_id"))
    })
}

pub(super) async fn load_resource_projection_context(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Option<ResourceProjectionContext>> {
    sqlx::query_as::<_, ResourceProjectionContext>(
        r#"
        SELECT
            resource.resource_id,
            resource.chain_id,
            resource.block_number,
            resource.block_hash,
            resource.provenance,
            resource.canonicality_state::TEXT AS canonicality_state,
            lineage.block_timestamp
        FROM resources resource
        LEFT JOIN chain_lineage lineage
          ON lineage.chain_id = resource.chain_id
         AND lineage.block_hash = resource.block_hash
        WHERE resource.resource_id = $1
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(resource_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load permissions resource projection context for {resource_id}")
    })
}

pub(super) async fn load_permission_events(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<RelevantEvent>> {
    let rows = sqlx::query_as::<_, RelevantEvent>(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.resource_id,
            ne.event_kind,
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
        JOIN resources resource
          ON resource.resource_id = ne.resource_id
         AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE (
              ne.event_kind IN ($1, $2, $3, $4)
              OR (
                  ne.event_kind IN ($5, $6)
                  AND ne.source_family IN ($7, $8)
              )
          )
          AND ne.resource_id = $9
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number ASC NULLS FIRST,
            ne.log_index ASC NULLS FIRST,
            ne.normalized_event_id ASC
        "#
    ))
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .bind(EVENT_KIND_ROOT_PERMISSION_CHANGED)
    .bind(EVENT_KIND_PERMISSION_SCOPE_CHANGED)
    .bind(EVENT_KIND_AUTHORITY_EPOCH_CHANGED)
    .bind(EVENT_KIND_REGISTRATION_GRANTED)
    .bind(EVENT_KIND_TOKEN_RESOURCE_LINKED)
    .bind(SOURCE_FAMILY_ENS_V2_REGISTRY_L1)
    .bind(SOURCE_FAMILY_ENS_V2_ROOT_L1)
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load canonical permission events for resource_id {resource_id}")
    })?;

    Ok(rows)
}
