use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::normalized_event_identity_differences;

pub(crate) async fn repair_ens_v1_unwrapped_authority_renewal_resource_ids(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_resource_ids = Vec::new();
    let mut new_resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut min_block_numbers = Vec::new();
    let mut labelhashes = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_renewal_resource_id_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let (Some(old_resource_id), Some(new_resource_id)) =
            (existing.resource_id, event.resource_id)
        else {
            continue;
        };
        let (Some(logical_name_id), Some(min_block_number)) =
            (existing.logical_name_id.as_ref(), existing.block_number)
        else {
            continue;
        };
        let Some(labelhash) = existing
            .after_state
            .get("labelhash")
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
        else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        old_resource_ids.push(old_resource_id);
        new_resource_ids.push(new_resource_id);
        logical_name_ids.push(logical_name_id.clone());
        min_block_numbers.push(min_block_number);
        labelhashes.push(labelhash.to_ascii_lowercase());
    }

    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let repaired = sqlx::query_scalar::<_, String>(
        r#"
        WITH input AS (
            SELECT *
            FROM unnest(
                $1::TEXT[],
                $2::UUID[],
                $3::UUID[],
                $4::TEXT[],
                $5::BIGINT[],
                $6::TEXT[]
            ) AS input(
                event_identity,
                old_resource_id,
                new_resource_id,
                logical_name_id,
                min_block_number,
                labelhash
            )
        ),
        repair_map AS (
            SELECT DISTINCT ON (input.old_resource_id, input.new_resource_id)
                input.*,
                old_resource.provenance->>'authority_key' AS old_authority_key,
                new_resource.provenance->>'authority_key' AS new_authority_key,
                CASE
                    WHEN new_resource.provenance->>'expiry' ~ '^-?[0-9]+$'
                    THEN (new_resource.provenance->>'expiry')::BIGINT
                    ELSE NULL
                END AS repaired_expiry
            FROM input
            JOIN resources old_resource
              ON old_resource.resource_id = input.old_resource_id
             AND old_resource.chain_id = 'ethereum-mainnet'
             AND old_resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND old_resource.provenance->>'authority_kind' = 'registrar'
             AND old_resource.provenance->>'logical_name_id' = input.logical_name_id
             AND lower(old_resource.provenance->>'labelhash') = input.labelhash
             AND NULLIF(old_resource.provenance->>'authority_key', '') IS NOT NULL
             AND old_resource.block_number <= input.min_block_number
            JOIN resources new_resource
              ON new_resource.resource_id = input.new_resource_id
             AND new_resource.resource_id <> old_resource.resource_id
             AND new_resource.chain_id = 'ethereum-mainnet'
             AND new_resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND new_resource.provenance->>'authority_kind' = 'registrar'
             AND new_resource.provenance->>'logical_name_id' = input.logical_name_id
             AND lower(new_resource.provenance->>'labelhash') = input.labelhash
             AND NULLIF(new_resource.provenance->>'authority_key', '') IS NOT NULL
             AND new_resource.block_number <= input.min_block_number
            ORDER BY input.old_resource_id, input.new_resource_id, input.min_block_number
        ),
        repointed_candidates AS (
            SELECT DISTINCT ON (event.normalized_event_id)
                event.normalized_event_id,
                event.canonicality_state,
                event.event_kind,
                event.event_identity AS old_event_identity,
                CASE
                    WHEN event.event_kind = 'RegistrationReleased'
                     AND repair.old_authority_key IS NOT NULL
                     AND repair.new_authority_key IS NOT NULL
                    THEN replace(
                        event.event_identity,
                        repair.old_authority_key,
                        repair.new_authority_key
                    )
                    ELSE event.event_identity
                END AS repaired_event_identity,
                event.resource_id AS old_resource_id,
                repair.new_resource_id,
                event.before_state,
                before_revocation_state.repaired_before_state,
                event.after_state,
                after_revocation_state.repaired_after_state
            FROM repair_map repair
            JOIN normalized_events event
              ON event.resource_id = repair.old_resource_id
            CROSS JOIN LATERAL (
                SELECT
                    CASE
                        WHEN event.event_kind IN ('RegistrationRenewed', 'ExpiryChanged')
                         AND repair.repaired_expiry IS NOT NULL
                         AND event.before_state ? 'expiry'
                        THEN jsonb_set(
                            event.before_state,
                            '{expiry}',
                            to_jsonb(repair.repaired_expiry),
                            true
                        )
                        WHEN event.event_kind = 'PermissionChanged'
                         AND repair.old_authority_key IS NOT NULL
                         AND repair.new_authority_key IS NOT NULL
                         AND event.before_state #>> '{grant_source,authority_key}' =
                             repair.old_authority_key
                        THEN jsonb_set(
                            event.before_state,
                            '{grant_source,authority_key}',
                            to_jsonb(repair.new_authority_key),
                            false
                        )
                        ELSE event.before_state
                    END AS repaired_before_state
            ) before_grant_state
            CROSS JOIN LATERAL (
                SELECT
                    CASE
                        WHEN event.event_kind = 'PermissionChanged'
                         AND repair.old_authority_key IS NOT NULL
                         AND repair.new_authority_key IS NOT NULL
                         AND before_grant_state.repaired_before_state
                             #>> '{revocation_source,authority_key}' =
                             repair.old_authority_key
                        THEN jsonb_set(
                            before_grant_state.repaired_before_state,
                            '{revocation_source,authority_key}',
                            to_jsonb(repair.new_authority_key),
                            false
                        )
                        ELSE before_grant_state.repaired_before_state
                    END AS repaired_before_state
            ) before_revocation_state
            CROSS JOIN LATERAL (
                SELECT
                    CASE
                        WHEN event.event_kind = 'PermissionChanged'
                         AND repair.old_authority_key IS NOT NULL
                         AND repair.new_authority_key IS NOT NULL
                         AND event.after_state #>> '{grant_source,authority_key}' =
                             repair.old_authority_key
                        THEN jsonb_set(
                            event.after_state,
                            '{grant_source,authority_key}',
                            to_jsonb(repair.new_authority_key),
                            false
                        )
                        ELSE event.after_state
                    END AS repaired_after_state
            ) after_grant_state
            CROSS JOIN LATERAL (
                SELECT
                    CASE
                        WHEN event.event_kind = 'PermissionChanged'
                         AND repair.old_authority_key IS NOT NULL
                         AND repair.new_authority_key IS NOT NULL
                         AND after_grant_state.repaired_after_state
                             #>> '{revocation_source,authority_key}' =
                             repair.old_authority_key
                        THEN jsonb_set(
                            after_grant_state.repaired_after_state,
                            '{revocation_source,authority_key}',
                            to_jsonb(repair.new_authority_key),
                            false
                        )
                        ELSE after_grant_state.repaired_after_state
                    END AS repaired_after_state
            ) after_revocation_state
            WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
              AND event.chain_id = 'ethereum-mainnet'
              AND event.logical_name_id = repair.logical_name_id
              AND event.block_number >= repair.min_block_number
              AND event.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND event.event_kind IN (
                  'RegistrationRenewed',
                  'ExpiryChanged',
                  'TokenControlTransferred',
                  'ResolverChanged',
                  'RecordChanged',
                  'RecordVersionChanged',
                  'PermissionChanged',
                  'RegistrationReleased'
              )
              AND (
                  event.event_kind <> 'ResolverChanged'
                  OR COALESCE(event.after_state->>'source_event', '') <>
                      'AuthorityEpochChanged'
            )
            ORDER BY event.normalized_event_id, repair.min_block_number
        ),
        release_identity_collisions AS (
            SELECT DISTINCT
                repair.normalized_event_id,
                repair.canonicality_state
            FROM repointed_candidates repair
            JOIN normalized_events existing
              ON existing.event_identity = repair.repaired_event_identity
             AND existing.normalized_event_id <> repair.normalized_event_id
            WHERE repair.event_kind = 'RegistrationReleased'
              AND repair.repaired_event_identity <> repair.old_event_identity
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                event_identity = repair.repaired_event_identity,
                resource_id = repair.new_resource_id,
                before_state = repair.repaired_before_state,
                after_state = repair.repaired_after_state,
                observed_at = now()
            FROM repointed_candidates repair
            WHERE event.normalized_event_id = repair.normalized_event_id
              AND event.event_identity = repair.old_event_identity
              AND event.resource_id = repair.old_resource_id
              AND event.before_state IS NOT DISTINCT FROM repair.before_state
              AND event.after_state IS NOT DISTINCT FROM repair.after_state
              AND NOT EXISTS (
                  SELECT 1
                  FROM release_identity_collisions collision
                  WHERE collision.normalized_event_id = repair.normalized_event_id
              )
            RETURNING
                repair.old_event_identity,
                event.normalized_event_id,
                event.canonicality_state,
                repair.event_kind,
                repair.old_resource_id,
                repair.new_resource_id
        ),
        synthetic_orphaned_candidates AS (
            SELECT DISTINCT ON (event.normalized_event_id)
                event.normalized_event_id,
                event.canonicality_state
            FROM repair_map repair
            JOIN normalized_events event
              ON event.resource_id = repair.old_resource_id
            WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
              AND event.chain_id = 'ethereum-mainnet'
              AND event.logical_name_id = repair.logical_name_id
              AND event.block_number >= repair.min_block_number
              AND event.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND (
                  event.event_kind IN (
                      'RegistrationGranted',
                      'SurfaceBound',
                      'SurfaceUnbound',
                      'AuthorityEpochChanged'
                  )
                  OR (
                      event.event_kind = 'ResolverChanged'
                      AND event.after_state->>'source_event' = 'AuthorityEpochChanged'
                  )
              )
            ORDER BY event.normalized_event_id, repair.min_block_number
        ),
        orphaned_candidates AS (
            SELECT
                normalized_event_id,
                canonicality_state
            FROM synthetic_orphaned_candidates

            UNION

            SELECT
                normalized_event_id,
                canonicality_state
            FROM release_identity_collisions
        ),
        orphaned_events AS (
            UPDATE normalized_events event
            SET
                canonicality_state = 'orphaned'::canonicality_state,
                observed_at = now()
            FROM orphaned_candidates repair
            WHERE event.normalized_event_id = repair.normalized_event_id
            RETURNING
                event.normalized_event_id,
                event.canonicality_state
        ),
        queued_changes AS (
            INSERT INTO projection_normalized_event_changes (
                normalized_event_id,
                changed_at,
                change_kind,
                canonicality_state
            )
            SELECT
                normalized_event_id,
                now(),
                'canonicality_update',
                canonicality_state
            FROM updated
            UNION ALL
            SELECT
                normalized_event_id,
                now(),
                'canonicality_update',
                canonicality_state
            FROM orphaned_events
            RETURNING
                change_id,
                normalized_event_id,
                changed_at
        ),
        orphaned_surface_bindings AS (
            UPDATE surface_bindings binding
            SET
                canonicality_state = 'orphaned'::canonicality_state,
                observed_at = now()
            FROM repair_map repair
            WHERE binding.resource_id = repair.old_resource_id
              AND NOT EXISTS (
                  SELECT 1
                  FROM normalized_events backing
                  WHERE backing.resource_id = repair.old_resource_id
                    AND backing.canonicality_state IN (
                        'canonical'::canonicality_state,
                        'safe'::canonicality_state,
                        'finalized'::canonicality_state
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM repointed_candidates repointed
                        WHERE repointed.normalized_event_id = backing.normalized_event_id
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM orphaned_candidates orphaned
                        WHERE orphaned.normalized_event_id = backing.normalized_event_id
                    )
              )
              AND binding.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            RETURNING binding.surface_binding_id
        ),
        orphaned_resources AS (
            UPDATE resources resource
            SET
                canonicality_state = 'orphaned'::canonicality_state,
                observed_at = now()
            FROM repair_map repair
            WHERE resource.resource_id = repair.old_resource_id
              AND NOT EXISTS (
                  SELECT 1
                  FROM normalized_events backing
                  WHERE backing.resource_id = repair.old_resource_id
                    AND backing.canonicality_state IN (
                        'canonical'::canonicality_state,
                        'safe'::canonicality_state,
                        'finalized'::canonicality_state
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM repointed_candidates repointed
                        WHERE repointed.normalized_event_id = backing.normalized_event_id
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM orphaned_candidates orphaned
                        WHERE orphaned.normalized_event_id = backing.normalized_event_id
                    )
              )
              AND resource.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            RETURNING resource.resource_id
        ),
        affected_resource_keys AS (
            SELECT
                'permissions_current'::TEXT AS projection,
                resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
            FROM updated
            CROSS JOIN LATERAL (
                VALUES (old_resource_id), (new_resource_id)
            ) AS resource(resource_id)
            WHERE event_kind = 'PermissionChanged'

            UNION ALL

            SELECT
                'record_inventory_current'::TEXT AS projection,
                resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
            FROM updated
            CROSS JOIN LATERAL (
                VALUES (old_resource_id), (new_resource_id)
            ) AS resource(resource_id)
            WHERE event_kind IN (
                'ResolverChanged',
                'RecordChanged',
                'RecordVersionChanged'
            )
        ),
        queued_resource_invalidations AS (
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload,
                last_changed_at,
                invalidated_at
            )
            SELECT
                projection,
                projection_key,
                key_payload,
                now(),
                now()
            FROM affected_resource_keys
            WHERE projection_key IS NOT NULL
              AND btrim(projection_key) <> ''
            GROUP BY projection, projection_key, key_payload
            ON CONFLICT (projection, projection_key)
            DO UPDATE SET
                key_payload = EXCLUDED.key_payload,
                generation = projection_invalidations.generation + 1,
                last_changed_at = GREATEST(
                    projection_invalidations.last_changed_at,
                    EXCLUDED.last_changed_at
                ),
                invalidated_at = EXCLUDED.invalidated_at,
                claim_token = NULL,
                claimed_at = NULL,
                last_failure_reason = NULL,
                last_failure_at = NULL
            RETURNING projection_key
        )
        SELECT input.event_identity
        FROM input
        JOIN updated
          ON updated.old_event_identity = input.event_identity
        "#,
    )
    .bind(&event_identities)
    .bind(&old_resource_ids)
    .bind(&new_resource_ids)
    .bind(&logical_name_ids)
    .bind(&min_block_numbers)
    .bind(&labelhashes)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 unwrapped-authority renewal normalized-event resource_id")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .into_iter()
        .filter(|event_identity| !repaired.contains(event_identity))
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 renewal resource_id repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v1_unwrapped_authority_renewal_resource_id_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields.len() != 1 || differing_fields[0] != "resource_id" {
        return false;
    }
    if existing.resource_id.is_none()
        || incoming.resource_id.is_none()
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.block_number.is_none()
        || existing
            .after_state
            .get("labelhash")
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .is_none()
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.source_family != "ens_v1_registrar_l1"
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !matches!(
            existing.event_kind.as_str(),
            "ExpiryChanged" | "RegistrationRenewed"
        )
    {
        return false;
    }

    existing.logical_name_id == incoming.logical_name_id
        && existing.before_state == incoming.before_state
        && existing.after_state == incoming.after_state
}
