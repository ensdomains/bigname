use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

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
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut after_states = Vec::new();

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
        let labelhash = existing
            .after_state
            .get("labelhash")
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_default();
        let old_before_state = serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 renewal before_state",
        )?;
        let new_before_state = serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 renewal before_state",
        )?;
        let after_state = serialize_jsonb_value(
            &event.after_state,
            "failed to serialize ENSv1 renewal after_state",
        )?;

        event_identities.push(event.event_identity.clone());
        old_resource_ids.push(old_resource_id);
        new_resource_ids.push(new_resource_id);
        logical_name_ids.push(logical_name_id.clone());
        min_block_numbers.push(min_block_number);
        labelhashes.push(labelhash.to_ascii_lowercase());
        old_before_states.push(old_before_state);
        new_before_states.push(new_before_state);
        after_states.push(after_state);
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
                $6::TEXT[],
                $7::TEXT[],
                $8::TEXT[],
                $9::TEXT[]
            ) AS input(
                event_identity,
                old_resource_id,
                new_resource_id,
                logical_name_id,
                min_block_number,
                labelhash,
                old_before_state,
                new_before_state,
                after_state
            )
        ),
        resource_candidates AS (
            SELECT
                input.*,
                old_resource.provenance->>'authority_key' AS old_authority_key,
                new_resource.provenance->>'authority_key' AS new_authority_key,
                CASE
                    WHEN input.new_before_state::JSONB ->> 'expiry' ~ '^-?[0-9]+$'
                    THEN (input.new_before_state::JSONB ->> 'expiry')::BIGINT
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
             AND NULLIF(old_resource.provenance->>'labelhash', '') IS NOT NULL
             AND (
                 NULLIF(input.labelhash, '') IS NULL
                 OR lower(old_resource.provenance->>'labelhash') = input.labelhash
             )
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
             AND lower(new_resource.provenance->>'labelhash') =
                 lower(old_resource.provenance->>'labelhash')
             AND (
                 NULLIF(input.labelhash, '') IS NULL
                 OR lower(new_resource.provenance->>'labelhash') = input.labelhash
             )
             AND NULLIF(new_resource.provenance->>'authority_key', '') IS NOT NULL
             AND new_resource.block_number <= input.min_block_number
            WHERE input.old_before_state::JSONB IS NOT DISTINCT FROM
                  input.new_before_state::JSONB
               OR (
                  input.old_before_state::JSONB - 'expiry' =
                      input.new_before_state::JSONB - 'expiry'
                  AND input.after_state::JSONB ->> 'expiry' =
                      input.old_before_state::JSONB ->> 'expiry'
                  AND input.old_before_state::JSONB ->> 'expiry' <>
                      input.new_before_state::JSONB ->> 'expiry'
                  AND input.after_state::JSONB ->> 'expiry' ~ '^-?[0-9]+$'
                  AND input.new_before_state::JSONB ->> 'expiry' ~ '^-?[0-9]+$'
                  AND (
                      (
                          input.new_before_state::JSONB ->> 'expiry' =
                              new_resource.provenance->>'expiry'
                          AND new_resource.provenance->>'expiry' ~ '^-?[0-9]+$'
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM normalized_events prior
                          WHERE prior.resource_id = input.new_resource_id
                            AND prior.logical_name_id = input.logical_name_id
                            AND prior.chain_id = 'ethereum-mainnet'
                            AND prior.source_family = 'ens_v1_registrar_l1'
                            AND prior.derivation_kind = 'ens_v1_unwrapped_authority'
                            AND prior.canonicality_state IN (
                                'canonical'::canonicality_state,
                                'safe'::canonicality_state,
                                'finalized'::canonicality_state
                            )
                            AND prior.event_kind IN (
                                'RegistrationGranted',
                                'RegistrationRenewed',
                                'ExpiryChanged'
                            )
                            AND prior.after_state->>'expiry' =
                                input.new_before_state::JSONB ->> 'expiry'
                            AND prior.block_number < input.min_block_number
                      )
                  )
               )
        ),
        repair_map AS (
            SELECT DISTINCT ON (old_resource_id, new_resource_id)
                *
            FROM resource_candidates
            ORDER BY old_resource_id, new_resource_id, min_block_number
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
            LEFT JOIN input input_event
              ON input_event.event_identity = event.event_identity
             AND input_event.new_resource_id = repair.new_resource_id
            CROSS JOIN LATERAL (
                SELECT
                    CASE
                        WHEN event.event_kind IN ('RegistrationRenewed', 'ExpiryChanged')
                         AND input_event.event_identity IS NOT NULL
                        THEN input_event.new_before_state::JSONB
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
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 unwrapped-authority renewal normalized-event resource_id")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .zip(old_resource_ids.iter())
        .zip(new_resource_ids.iter())
        .zip(logical_name_ids.iter())
        .zip(labelhashes.iter())
        .zip(old_before_states.iter())
        .zip(new_before_states.iter())
        .filter(|((((((event_identity, _), _), _), _), _), _)| {
            !repaired.contains(event_identity.as_str())
        })
        .map(
            |((((((event_identity, old_resource_id), new_resource_id), logical_name_id), labelhash), old_before_state), new_before_state)| {
                format!(
                    "{event_identity} (old_resource_id={old_resource_id}, new_resource_id={new_resource_id}, logical_name_id={logical_name_id}, labelhash={labelhash}, old_before_state={old_before_state}, new_before_state={new_before_state})"
                )
            },
        )
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 renewal resource_id repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) async fn repair_ens_v1_unwrapped_authority_registration_release_before_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_registration_release_before_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let (Some(resource_id), Some(logical_name_id)) =
            (existing.resource_id, existing.logical_name_id.as_ref())
        else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        resource_ids.push(resource_id);
        logical_name_ids.push(logical_name_id.clone());
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 registration release before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 registration release before_state",
        )?);
        after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize ENSv1 registration release after_state",
        )?);
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
                $3::TEXT[],
                $4::TEXT[],
                $5::TEXT[],
                $6::TEXT[]
            ) AS input(
                event_identity,
                resource_id,
                logical_name_id,
                old_before_state,
                new_before_state,
                after_state
            )
        ),
        repair_map AS (
            SELECT input.*
            FROM input
            JOIN resources resource
              ON resource.resource_id = input.resource_id
             AND resource.chain_id = 'ethereum-mainnet'
             AND resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND resource.provenance->>'authority_kind' = 'registrar'
             AND resource.provenance->>'logical_name_id' = input.logical_name_id
            WHERE input.old_before_state::JSONB - 'registrant' =
                  input.new_before_state::JSONB - 'registrant'
              AND input.old_before_state::JSONB ->> 'expiry' =
                  input.new_before_state::JSONB ->> 'expiry'
              AND COALESCE(input.old_before_state::JSONB ->> 'expiry', '') ~
                  '^-?[0-9]+$'
              AND COALESCE(input.old_before_state::JSONB ->> 'registrant', '') <> ''
              AND COALESCE(input.new_before_state::JSONB ->> 'registrant', '') <> ''
              AND input.after_state::JSONB ? 'released_at'
              AND input.after_state::JSONB ? 'labelhash'
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                before_state = repair.new_before_state::JSONB,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.resource_id = repair.resource_id
              AND event.logical_name_id = repair.logical_name_id
              AND event.event_kind = 'RegistrationReleased'
              AND event.source_family = 'ens_v1_registrar_l1'
              AND event.derivation_kind = 'ens_v1_unwrapped_authority'
              AND event.chain_id = 'ethereum-mainnet'
              AND event.before_state IS NOT DISTINCT FROM repair.old_before_state::JSONB
              AND event.after_state IS NOT DISTINCT FROM repair.after_state::JSONB
            RETURNING
                event.event_identity,
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
            RETURNING
                change_id,
                normalized_event_id,
                changed_at
        )
        SELECT input.event_identity
        FROM input
        JOIN updated
          ON updated.event_identity = input.event_identity
        "#,
    )
    .bind(&event_identities)
    .bind(&resource_ids)
    .bind(&logical_name_ids)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 unwrapped-authority registration release before_state")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .zip(resource_ids.iter())
        .filter(|(event_identity, _)| !repaired.contains(event_identity.as_str()))
        .map(|(event_identity, resource_id)| {
            format!("{event_identity} (resource_id={resource_id})")
        })
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 registration release before_state repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) async fn repair_ens_v1_unwrapped_authority_renewal_before_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut block_numbers = Vec::new();
    let mut log_indexes = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_renewal_before_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let (Some(resource_id), Some(logical_name_id), Some(block_number), Some(log_index)) = (
            existing.resource_id,
            existing.logical_name_id.as_ref(),
            existing.block_number,
            existing.log_index,
        ) else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        resource_ids.push(resource_id);
        logical_name_ids.push(logical_name_id.clone());
        block_numbers.push(block_number);
        log_indexes.push(log_index);
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 renewal before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 renewal before_state",
        )?);
        after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize ENSv1 renewal after_state",
        )?);
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
                $3::TEXT[],
                $4::BIGINT[],
                $5::BIGINT[],
                $6::TEXT[],
                $7::TEXT[],
                $8::TEXT[]
            ) AS input(
                event_identity,
                resource_id,
                logical_name_id,
                block_number,
                log_index,
                old_before_state,
                new_before_state,
                after_state
            )
        ),
        repair_map AS (
            SELECT input.*
            FROM input
            JOIN resources resource
              ON resource.resource_id = input.resource_id
             AND resource.chain_id = 'ethereum-mainnet'
             AND resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND resource.provenance->>'authority_kind' = 'registrar'
             AND resource.provenance->>'logical_name_id' = input.logical_name_id
             AND NULLIF(resource.provenance->>'labelhash', '') IS NOT NULL
             AND resource.block_number <= input.block_number
             AND (
                 NULLIF(input.after_state::JSONB ->> 'labelhash', '') IS NULL
                 OR lower(resource.provenance->>'labelhash') =
                    lower(input.after_state::JSONB ->> 'labelhash')
             )
            WHERE input.old_before_state::JSONB - 'expiry' =
                  input.new_before_state::JSONB - 'expiry'
              AND input.old_before_state::JSONB ->> 'expiry' <>
                  input.new_before_state::JSONB ->> 'expiry'
              AND input.after_state::JSONB ->> 'expiry' ~ '^-?[0-9]+$'
              AND input.new_before_state::JSONB ->> 'expiry' ~ '^-?[0-9]+$'
              AND input.after_state::JSONB ->> 'expiry' <>
                  input.new_before_state::JSONB ->> 'expiry'
              AND (
                  EXISTS (
                      SELECT 1
                      FROM normalized_events prior
                      WHERE prior.resource_id = input.resource_id
                        AND prior.logical_name_id = input.logical_name_id
                        AND prior.chain_id = 'ethereum-mainnet'
                        AND prior.source_family = 'ens_v1_registrar_l1'
                        AND prior.derivation_kind = 'ens_v1_unwrapped_authority'
                        AND prior.canonicality_state IN (
                            'canonical'::canonicality_state,
                            'safe'::canonicality_state,
                            'finalized'::canonicality_state
                        )
                        AND prior.event_kind IN (
                            'RegistrationGranted',
                            'RegistrationRenewed',
                            'ExpiryChanged'
                        )
                        AND prior.after_state->>'expiry' =
                            input.new_before_state::JSONB ->> 'expiry'
                        AND (
                            prior.block_number < input.block_number
                            OR (
                                prior.block_number = input.block_number
                                AND COALESCE(prior.log_index, -1) < input.log_index
                            )
                        )
                  )
                  OR (
                      input.old_before_state::JSONB ->> 'expiry' ~ '^-?[0-9]+$'
                      AND (input.old_before_state::JSONB ->> 'expiry')::BIGINT <
                          (input.after_state::JSONB ->> 'expiry')::BIGINT
                      AND (input.new_before_state::JSONB ->> 'expiry')::BIGINT <
                          (input.after_state::JSONB ->> 'expiry')::BIGINT
                  )
              )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                before_state = repair.new_before_state::JSONB,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.resource_id = repair.resource_id
              AND event.logical_name_id = repair.logical_name_id
              AND event.event_kind IN ('ExpiryChanged', 'RegistrationRenewed')
              AND event.source_family = 'ens_v1_registrar_l1'
              AND event.derivation_kind = 'ens_v1_unwrapped_authority'
              AND event.chain_id = 'ethereum-mainnet'
              AND event.before_state IS NOT DISTINCT FROM repair.old_before_state::JSONB
              AND event.after_state IS NOT DISTINCT FROM repair.after_state::JSONB
            RETURNING
                event.event_identity,
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
            RETURNING
                change_id,
                normalized_event_id,
                changed_at
        )
        SELECT input.event_identity
        FROM input
        JOIN updated
          ON updated.event_identity = input.event_identity
        "#,
    )
    .bind(&event_identities)
    .bind(&resource_ids)
    .bind(&logical_name_ids)
    .bind(&block_numbers)
    .bind(&log_indexes)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 unwrapped-authority renewal before_state")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .zip(resource_ids.iter())
        .filter(|(event_identity, _)| !repaired.contains(event_identity.as_str()))
        .map(|(event_identity, resource_id)| {
            format!("{event_identity} (resource_id={resource_id})")
        })
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 renewal before_state repair rejected invalid resource anchors for events: {}",
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
    if !renewal_resource_id_repair_differences_allowed(differing_fields) {
        return false;
    }
    if existing.resource_id.is_none()
        || incoming.resource_id.is_none()
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.block_number.is_none()
        || existing
            .after_state
            .get("expiry")
            .and_then(Value::as_i64)
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
        && existing.after_state == incoming.after_state
        && (existing.before_state == incoming.before_state
            || renewal_before_state_expiry_repair_allowed(
                &existing.before_state,
                &incoming.before_state,
                &incoming.after_state,
            ))
}

pub(crate) fn ens_v1_unwrapped_authority_renewal_before_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !matches!(differing_fields, ["before_state"]) {
        return false;
    }
    if existing.resource_id.is_none()
        || existing.resource_id != incoming.resource_id
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.logical_name_id != incoming.logical_name_id
        || existing.block_number.is_none()
        || existing.log_index.is_none()
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.source_family != "ens_v1_registrar_l1"
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !matches!(
            existing.event_kind.as_str(),
            "ExpiryChanged" | "RegistrationRenewed"
        )
        || existing.after_state != incoming.after_state
    {
        return false;
    }

    renewal_same_resource_before_state_expiry_repair_allowed(
        &existing.before_state,
        &incoming.before_state,
        &incoming.after_state,
    )
}

fn renewal_resource_id_repair_differences_allowed(differing_fields: &[&'static str]) -> bool {
    matches!(
        differing_fields,
        ["resource_id"] | ["resource_id", "before_state"]
    )
}

fn renewal_before_state_expiry_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    after_state: &Value,
) -> bool {
    let Some(existing_expiry) = existing_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(incoming_expiry) = incoming_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(after_expiry) = after_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    if existing_expiry != after_expiry || incoming_expiry == existing_expiry {
        return false;
    }
    let mut existing_without_expiry = existing_before_state.clone();
    let Some(existing_object) = existing_without_expiry.as_object_mut() else {
        return false;
    };
    existing_object.remove("expiry");

    let mut incoming_without_expiry = incoming_before_state.clone();
    let Some(incoming_object) = incoming_without_expiry.as_object_mut() else {
        return false;
    };
    incoming_object.remove("expiry");

    existing_without_expiry == incoming_without_expiry
}

fn renewal_same_resource_before_state_expiry_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    after_state: &Value,
) -> bool {
    let Some(existing_expiry) = existing_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(incoming_expiry) = incoming_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(after_expiry) = after_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    if incoming_expiry == existing_expiry || incoming_expiry == after_expiry {
        return false;
    }
    let mut existing_without_expiry = existing_before_state.clone();
    let Some(existing_object) = existing_without_expiry.as_object_mut() else {
        return false;
    };
    existing_object.remove("expiry");

    let mut incoming_without_expiry = incoming_before_state.clone();
    let Some(incoming_object) = incoming_without_expiry.as_object_mut() else {
        return false;
    };
    incoming_object.remove("expiry");

    existing_without_expiry == incoming_without_expiry
}

pub(crate) fn ens_v1_unwrapped_authority_registration_release_before_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !matches!(differing_fields, ["before_state"]) {
        return false;
    }
    if existing.resource_id.is_none()
        || existing.resource_id != incoming.resource_id
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.logical_name_id != incoming.logical_name_id
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.source_family != "ens_v1_registrar_l1"
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || existing.event_kind != "RegistrationReleased"
        || existing.after_state != incoming.after_state
    {
        return false;
    }

    registration_release_before_state_repair_allowed(
        &existing.before_state,
        &incoming.before_state,
        &incoming.after_state,
    )
}

fn registration_release_before_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    after_state: &Value,
) -> bool {
    if !after_state.get("released_at").is_some_and(Value::is_number)
        || !after_state
            .get("labelhash")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
    {
        return false;
    }

    let Some(existing_registrant) = existing_before_state
        .get("registrant")
        .and_then(Value::as_str)
    else {
        return false;
    };
    let Some(incoming_registrant) = incoming_before_state
        .get("registrant")
        .and_then(Value::as_str)
    else {
        return false;
    };
    if existing_registrant.trim().is_empty()
        || incoming_registrant.trim().is_empty()
        || existing_registrant.eq_ignore_ascii_case(incoming_registrant)
    {
        return false;
    }

    let Some(existing_expiry) = existing_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(incoming_expiry) = incoming_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    if existing_expiry != incoming_expiry {
        return false;
    }

    let mut existing_without_registrant = existing_before_state.clone();
    if let Some(object) = existing_without_registrant.as_object_mut() {
        object.remove("registrant");
    }
    let mut incoming_without_registrant = incoming_before_state.clone();
    if let Some(object) = incoming_without_registrant.as_object_mut() {
        object.remove("registrant");
    }

    existing_without_registrant == incoming_without_registrant
}
