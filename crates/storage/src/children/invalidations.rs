use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};

use crate::{CanonicalityState, identity::NameSurface};

pub(crate) async fn enqueue_children_current_invalidations_for_parent_surfaces(
    transaction: &mut Transaction<'_, Postgres>,
    name_surfaces: &[NameSurface],
) -> Result<u64> {
    let readable_surfaces = name_surfaces
        .iter()
        .filter(|surface| is_readable_canonicality(surface.canonicality_state))
        .collect::<Vec<_>>();
    if readable_surfaces.is_empty() {
        return Ok(0);
    }

    let logical_name_ids = readable_surfaces
        .iter()
        .map(|surface| surface.logical_name_id.clone())
        .collect::<Vec<_>>();
    let namespaces = readable_surfaces
        .iter()
        .map(|surface| surface.namespace.clone())
        .collect::<Vec<_>>();
    let chain_ids = readable_surfaces
        .iter()
        .map(|surface| surface.chain_id.clone())
        .collect::<Vec<_>>();
    let namehashes = readable_surfaces
        .iter()
        .map(|surface| surface.namehash.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        WITH input_surfaces AS (
            SELECT DISTINCT
                input.logical_name_id,
                input.namespace,
                input.chain_id,
                input.namehash
            FROM unnest(
                $1::TEXT[],
                $2::TEXT[],
                $3::TEXT[],
                $4::TEXT[]
            ) AS input(logical_name_id, namespace, chain_id, namehash)
        ),
        candidate_keys AS (
            SELECT DISTINCT
                'children_current'::TEXT AS projection,
                input.logical_name_id AS projection_key,
                jsonb_build_object('parent_logical_name_id', input.logical_name_id) AS key_payload
            FROM input_surfaces input
            JOIN normalized_events ne
              ON ne.after_state ->> 'parent_node' = input.namehash
             AND ne.namespace = input.namespace
             AND ne.chain_id = input.chain_id
            WHERE ne.event_kind = 'SubregistryChanged'
              AND ne.derivation_kind = 'ens_v1_subregistry_changed'
              AND ne.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
              AND ne.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND ne.after_state ->> 'child_node' IS NOT NULL

            UNION

            SELECT DISTINCT
                'children_current'::TEXT AS projection,
                input.logical_name_id AS projection_key,
                jsonb_build_object('parent_logical_name_id', input.logical_name_id) AS key_payload
            FROM input_surfaces input
            JOIN normalized_events ne
              ON ne.logical_name_id = input.logical_name_id
             AND ne.namespace = input.namespace
             AND ne.chain_id = input.chain_id
            WHERE ne.event_kind IN ('SubregistryChanged', 'ParentChanged')
              AND ne.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        )
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            invalidated_at,
            last_changed_at
        )
        SELECT
            projection,
            projection_key,
            key_payload,
            now(),
            now()
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
        "#,
    )
    .bind(&logical_name_ids)
    .bind(&namespaces)
    .bind(&chain_ids)
    .bind(&namehashes)
    .execute(&mut **transaction)
    .await
    .context("failed to enqueue children_current invalidations for parent surfaces")
    .map(|result| result.rows_affected())
}

fn is_readable_canonicality(canonicality_state: CanonicalityState) -> bool {
    matches!(
        canonicality_state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    )
}
