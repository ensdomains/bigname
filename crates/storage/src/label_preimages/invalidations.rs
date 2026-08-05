use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, Transaction};

const CANDIDATE_KEYS_FROM_EXISTING: &str = r#"
    SELECT DISTINCT
        'children_current'::TEXT AS projection,
        parent.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', parent.logical_name_id) AS key_payload
    FROM label_preimages preimage
    JOIN normalized_events ne
      ON lower(ne.after_state ->> 'labelhash') = preimage.labelhash
    JOIN chain_lineage event_lineage
      ON event_lineage.chain_id = ne.chain_id
     AND event_lineage.block_hash = ne.block_hash
     AND event_lineage.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
     )
    JOIN name_surfaces parent
      ON parent.namehash = ne.after_state ->> 'parent_node'
     AND parent.namespace = ne.namespace
     AND parent.chain_id = ne.chain_id
     AND parent.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
     )
    JOIN chain_lineage parent_lineage
      ON parent_lineage.chain_id = parent.chain_id
     AND parent_lineage.block_hash = parent.block_hash
     AND parent_lineage.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
     )
    WHERE ne.event_kind = 'SubregistryChanged'
      AND ne.derivation_kind = 'ens_v1_subregistry_changed'
      AND ne.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
      AND ne.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND ne.after_state ->> 'parent_node' IS NOT NULL
      AND ne.after_state ->> 'child_node' IS NOT NULL
      AND ne.after_state ->> 'labelhash' IS NOT NULL
"#;

const UPSERT_INVALIDATIONS_SUFFIX: &str = r#"
)
INSERT INTO projection_invalidations (
    projection,
    projection_key,
    key_payload,
    invalidated_at,
    last_changed_at
)
SELECT projection, projection_key, key_payload, now(), now()
FROM candidate_keys
ON CONFLICT (projection, projection_key)
DO UPDATE SET
    key_payload = EXCLUDED.key_payload,
    generation = projection_invalidations.generation + 1,
    invalidated_at = EXCLUDED.invalidated_at,
    last_changed_at = EXCLUDED.last_changed_at,
    claim_token = NULL,
    claimed_at = NULL,
    last_failure_reason = NULL,
    last_failure_at = NULL
"#;

pub(super) async fn enqueue_children_invalidations_for_existing_label_preimages(
    pool: &PgPool,
) -> Result<u64> {
    let query = format!(
        "WITH candidate_keys AS ({CANDIDATE_KEYS_FROM_EXISTING}{UPSERT_INVALIDATIONS_SUFFIX}"
    );
    sqlx::query(&query)
        .execute(pool)
        .await
        .context("failed to enqueue children_current invalidations for existing label preimages")
        .map(|result| result.rows_affected())
}

pub(super) async fn enqueue_children_invalidations_for_labelhashes(
    transaction: &mut Transaction<'_, Postgres>,
    labelhashes: &[String],
) -> Result<u64> {
    if labelhashes.is_empty() {
        return Ok(0);
    }

    let candidates = CANDIDATE_KEYS_FROM_EXISTING.replace(
        "FROM label_preimages preimage\n    JOIN normalized_events ne\n      ON lower(ne.after_state ->> 'labelhash') = preimage.labelhash",
        "FROM changed_labelhashes changed\n    JOIN normalized_events ne\n      ON lower(ne.after_state ->> 'labelhash') = changed.labelhash",
    );
    let query = format!(
        "WITH changed_labelhashes AS (\n\
             SELECT DISTINCT lower(labelhash) AS labelhash\n\
             FROM unnest($1::TEXT[]) AS input(labelhash)\n\
         ), candidate_keys AS ({candidates}{UPSERT_INVALIDATIONS_SUFFIX}"
    );
    sqlx::query(&query)
        .bind(labelhashes)
        .execute(&mut **transaction)
        .await
        .context("failed to enqueue children_current invalidations for label preimages")
        .map(|result| result.rows_affected())
}
