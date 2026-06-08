use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_unwrapped_authority_registry_event_time_resource_ids(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_resource_ids = Vec::new();
    let mut new_resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut block_numbers = Vec::new();
    let mut block_hashes = Vec::new();
    let mut transaction_hashes = Vec::new();
    let mut log_indexes = Vec::new();
    let mut event_kinds = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut old_after_states = Vec::new();
    let mut new_after_states = Vec::new();
    let mut registration_resource_ids = Vec::new();
    let mut registration_block_hashes = Vec::new();
    let mut registration_transaction_hashes = Vec::new();
    let mut registration_log_indexes = Vec::new();

    for event in events {
        if event.event_kind != "RegistrationGranted" {
            continue;
        }
        let (Some(resource_id), Some(block_hash), Some(transaction_hash), Some(log_index)) = (
            event.resource_id,
            event.block_hash.as_ref(),
            event.transaction_hash.as_ref(),
            event.log_index,
        ) else {
            continue;
        };
        registration_resource_ids.push(resource_id);
        registration_block_hashes.push(block_hash.clone());
        registration_transaction_hashes.push(transaction_hash.clone());
        registration_log_indexes.push(log_index);
    }

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_registry_event_time_resource_id_repair_allowed(
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
        let (Some(logical_name_id), Some(block_number)) =
            (existing.logical_name_id.as_ref(), existing.block_number)
        else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        old_resource_ids.push(old_resource_id);
        new_resource_ids.push(new_resource_id);
        logical_name_ids.push(logical_name_id.clone());
        block_numbers.push(block_number);
        block_hashes.push(
            existing
                .block_hash
                .clone()
                .or_else(|| event.block_hash.clone())
                .unwrap_or_default(),
        );
        transaction_hashes.push(
            existing
                .transaction_hash
                .clone()
                .or_else(|| event.transaction_hash.clone())
                .unwrap_or_default(),
        );
        log_indexes.push(existing.log_index.or(event.log_index).unwrap_or(-1));
        event_kinds.push(event.event_kind.clone());
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 registry event-time before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 registry event-time before_state",
        )?);
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv1 registry event-time after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired ENSv1 registry event-time after_state",
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
                $3::UUID[],
                $4::TEXT[],
                $5::BIGINT[],
                $6::TEXT[],
                $7::TEXT[],
                $8::BIGINT[],
                $9::TEXT[],
                $10::TEXT[],
                $11::TEXT[],
                $12::TEXT[],
                $13::TEXT[]
            ) AS input(
                event_identity,
                old_resource_id,
                new_resource_id,
                logical_name_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                event_kind,
                old_before_state,
                new_before_state,
                old_after_state,
                new_after_state
            )
        ),
        registration_input AS (
            SELECT *
            FROM unnest(
                $14::UUID[],
                $15::TEXT[],
                $16::TEXT[],
                $17::BIGINT[]
            ) AS registration(
                resource_id,
                block_hash,
                transaction_hash,
                log_index
            )
        ),
        repair_map AS (
            SELECT input.*
            FROM input
            JOIN resources old_resource
              ON old_resource.resource_id = input.old_resource_id
             AND old_resource.chain_id = 'ethereum-mainnet'
             AND old_resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND old_resource.provenance->>'authority_kind' IN (
                 'registrar',
                 'wrapper',
                 'registry_only'
             )
             AND (
                 old_resource.provenance->>'logical_name_id' = input.logical_name_id
                 OR (
                     old_resource.provenance->>'authority_kind' = 'registry_only'
                     AND old_resource.provenance->>'logical_name_id' IS DISTINCT FROM
                         input.logical_name_id
                 )
             )
            LEFT JOIN resources new_resource
              ON new_resource.resource_id = input.new_resource_id
             AND new_resource.resource_id <> old_resource.resource_id
             AND (
                 (
                     new_resource.resource_id IS NOT NULL
                     AND new_resource.chain_id = 'ethereum-mainnet'
                     AND new_resource.canonicality_state IN (
                         'canonical'::canonicality_state,
                         'safe'::canonicality_state,
                         'finalized'::canonicality_state
                     )
                     AND new_resource.provenance->>'logical_name_id' = input.logical_name_id
                     AND (
                         (
                             new_resource.provenance->>'authority_kind' = 'registry_only'
                             AND new_resource.block_number <= input.block_number
                             AND (
                                 (
                                     old_resource.provenance->>'authority_kind' IN ('registrar', 'wrapper')
                                     AND old_resource.block_number > input.block_number
                                 )
                                 OR (
                                     old_resource.provenance->>'authority_kind' = 'registry_only'
                                     AND old_resource.provenance->>'authority_key' IS DISTINCT FROM
                                         new_resource.provenance->>'authority_key'
                                 )
                             )
                         )
                         OR (
                             new_resource.provenance->>'authority_kind' = 'registrar'
                             AND old_resource.provenance->>'authority_kind' = 'registry_only'
                             AND input.block_hash <> ''
                             AND input.transaction_hash <> ''
                             AND input.log_index >= 0
                             AND new_resource.block_number = input.block_number
                             AND new_resource.block_hash = input.block_hash
                             AND split_part(new_resource.provenance->>'authority_key', ':', 1) =
                                 'registrar'
                             AND split_part(new_resource.provenance->>'authority_key', ':', 2) =
                                 'ethereum-mainnet'
                             AND split_part(new_resource.provenance->>'authority_key', ':', 5) =
                                 input.block_hash
                             AND split_part(new_resource.provenance->>'authority_key', ':', 6) ~
                                 '^[0-9]+$'
                             AND (
                                 split_part(
                                     new_resource.provenance->>'authority_key',
                                     ':',
                                     6
                                 )::BIGINT
                             ) > input.log_index
                             AND EXISTS (
                                 SELECT 1
                                 FROM (
                                     SELECT
                                         event.resource_id,
                                         event.block_hash,
                                         event.transaction_hash,
                                         COALESCE(event.log_index, -1) AS log_index
                                     FROM normalized_events event
                                     WHERE event.resource_id = input.new_resource_id
                                       AND event.event_kind = 'RegistrationGranted'
                                       AND event.canonicality_state IN (
                                           'canonical'::canonicality_state,
                                           'safe'::canonicality_state,
                                           'finalized'::canonicality_state
                                       )

                                     UNION ALL

                                     SELECT
                                         registration.resource_id,
                                         registration.block_hash,
                                         registration.transaction_hash,
                                         registration.log_index
                                     FROM registration_input registration
                                     WHERE registration.resource_id = input.new_resource_id
                                 ) registration
                                 WHERE registration.resource_id = input.new_resource_id
                                   AND registration.block_hash = input.block_hash
                                   AND registration.transaction_hash = input.transaction_hash
                                   AND registration.log_index > input.log_index
                             )
                         )
                     )
                 )
                 OR (
                     new_resource.resource_id IS NULL
                     AND input.new_resource_id <> old_resource.resource_id
                     AND old_resource.provenance->>'authority_kind' IN ('registrar', 'wrapper')
                     AND old_resource.block_number > input.block_number
                 )
             )
             AND (
                 new_resource.resource_id IS NULL
                 OR (
                     lower(COALESCE(new_resource.provenance->>'labelhash', '')) =
                         lower(COALESCE(old_resource.provenance->>'labelhash', ''))
                     OR (
                         old_resource.provenance->>'authority_kind' = 'wrapper'
                         AND COALESCE(old_resource.provenance->>'labelhash', '') = ''
                         AND COALESCE(new_resource.provenance->>'labelhash', '') <> ''
                     )
                 )
             )
             AND (
                 (
                     input.old_before_state::JSONB IS NOT DISTINCT FROM
                         input.new_before_state::JSONB
                     AND input.old_after_state::JSONB IS NOT DISTINCT FROM
                         input.new_after_state::JSONB
                 )
                 OR (
                     input.event_kind = 'AuthorityTransferred'
                     AND input.old_after_state::JSONB IS NOT DISTINCT FROM
                         input.new_after_state::JSONB
                     AND input.old_before_state::JSONB - 'owner' =
                         input.new_before_state::JSONB - 'owner'
                     AND COALESCE(input.old_before_state::JSONB ->> 'owner', '') <> ''
                     AND COALESCE(input.new_before_state::JSONB ->> 'owner', '') <> ''
                 )
                 OR (
                     input.event_kind = 'PermissionChanged'
                     AND (
                         input.old_before_state::JSONB IS NOT DISTINCT FROM
                             input.new_before_state::JSONB
                         OR (
                             input.old_before_state::JSONB - 'grant_source' - 'revocation_source' =
                                 input.new_before_state::JSONB - 'grant_source' - 'revocation_source'
                             AND (
                                 input.old_before_state::JSONB -> 'grant_source' IS NOT DISTINCT FROM
                                     input.new_before_state::JSONB -> 'grant_source'
                                 OR (
                                     input.old_before_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_before_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_before_state::JSONB #>> '{grant_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_before_state::JSONB #>> '{grant_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_before_state::JSONB #>> '{grant_source,authority_kind}' =
                                         new_resource.provenance->>'authority_kind'
                                     AND input.new_before_state::JSONB #>> '{grant_source,authority_key}' =
                                         new_resource.provenance->>'authority_key'
                                     AND COALESCE(
                                         input.old_before_state::JSONB #>> '{grant_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_before_state::JSONB #>> '{grant_source,source_event_kind}' =
                                         input.new_before_state::JSONB #>> '{grant_source,source_event_kind}'
                                 )
                             )
                             AND (
                                 input.old_before_state::JSONB -> 'revocation_source' IS NOT DISTINCT FROM
                                     input.new_before_state::JSONB -> 'revocation_source'
                                 OR (
                                     input.old_before_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_before_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_before_state::JSONB #>> '{revocation_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         new_resource.provenance->>'authority_kind'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,authority_key}' =
                                         new_resource.provenance->>'authority_key'
                                     AND COALESCE(
                                         input.old_before_state::JSONB #>> '{revocation_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_before_state::JSONB #>> '{revocation_source,source_event_kind}' =
                                         input.new_before_state::JSONB #>> '{revocation_source,source_event_kind}'
                                 )
                             )
                         )
                     )
                     AND (
                         input.old_after_state::JSONB IS NOT DISTINCT FROM
                             input.new_after_state::JSONB
                         OR (
                             input.old_after_state::JSONB - 'grant_source' - 'revocation_source' =
                                 input.new_after_state::JSONB - 'grant_source' - 'revocation_source'
                             AND (
                                 input.old_after_state::JSONB -> 'grant_source' IS NOT DISTINCT FROM
                                     input.new_after_state::JSONB -> 'grant_source'
                                 OR (
                                     input.old_after_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_after_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_after_state::JSONB #>> '{grant_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_after_state::JSONB #>> '{grant_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_after_state::JSONB #>> '{grant_source,authority_kind}' =
                                         new_resource.provenance->>'authority_kind'
                                     AND input.new_after_state::JSONB #>> '{grant_source,authority_key}' =
                                         new_resource.provenance->>'authority_key'
                                     AND COALESCE(
                                         input.old_after_state::JSONB #>> '{grant_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_after_state::JSONB #>> '{grant_source,source_event_kind}' =
                                         input.new_after_state::JSONB #>> '{grant_source,source_event_kind}'
                                 )
                             )
                             AND (
                                 input.old_after_state::JSONB -> 'revocation_source' IS NOT DISTINCT FROM
                                     input.new_after_state::JSONB -> 'revocation_source'
                                 OR (
                                     input.old_after_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_after_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_after_state::JSONB #>> '{revocation_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         new_resource.provenance->>'authority_kind'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,authority_key}' =
                                         new_resource.provenance->>'authority_key'
                                     AND COALESCE(
                                         input.old_after_state::JSONB #>> '{revocation_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_after_state::JSONB #>> '{revocation_source,source_event_kind}' =
                                         input.new_after_state::JSONB #>> '{revocation_source,source_event_kind}'
                                 )
                             )
                         )
                     )
                 )
                 OR (
                     new_resource.resource_id IS NULL
                     AND input.event_kind = 'PermissionChanged'
                     AND (
                         input.old_before_state::JSONB IS NOT DISTINCT FROM
                             input.new_before_state::JSONB
                         OR (
                             input.old_before_state::JSONB - 'grant_source' - 'revocation_source' =
                                 input.new_before_state::JSONB - 'grant_source' - 'revocation_source'
                             AND (
                                 input.old_before_state::JSONB -> 'grant_source' IS NOT DISTINCT FROM
                                     input.new_before_state::JSONB -> 'grant_source'
                                 OR (
                                     input.old_before_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_before_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_before_state::JSONB #>> '{grant_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_before_state::JSONB #>> '{grant_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_before_state::JSONB #>> '{grant_source,authority_kind}' =
                                         'registry_only'
                                     AND input.new_before_state::JSONB #>> '{grant_source,authority_key}' LIKE
                                         'registry-only:ethereum-mainnet:%'
                                     AND COALESCE(
                                         input.old_before_state::JSONB #>> '{grant_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_before_state::JSONB #>> '{grant_source,source_event_kind}' =
                                         input.new_before_state::JSONB #>> '{grant_source,source_event_kind}'
                                 )
                             )
                             AND (
                                 input.old_before_state::JSONB -> 'revocation_source' IS NOT DISTINCT FROM
                                     input.new_before_state::JSONB -> 'revocation_source'
                                 OR (
                                     input.old_before_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_before_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_before_state::JSONB #>> '{revocation_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         'registry_only'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,authority_key}' LIKE
                                         'registry-only:ethereum-mainnet:%'
                                     AND COALESCE(
                                         input.old_before_state::JSONB #>> '{revocation_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_before_state::JSONB #>> '{revocation_source,source_event_kind}' =
                                         input.new_before_state::JSONB #>> '{revocation_source,source_event_kind}'
                                 )
                             )
                         )
                     )
                     AND (
                         input.old_after_state::JSONB IS NOT DISTINCT FROM
                             input.new_after_state::JSONB
                         OR (
                             input.old_after_state::JSONB - 'grant_source' - 'revocation_source' =
                                 input.new_after_state::JSONB - 'grant_source' - 'revocation_source'
                             AND (
                                 input.old_after_state::JSONB -> 'grant_source' IS NOT DISTINCT FROM
                                     input.new_after_state::JSONB -> 'grant_source'
                                 OR (
                                     input.old_after_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_after_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_after_state::JSONB #>> '{grant_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_after_state::JSONB #>> '{grant_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_after_state::JSONB #>> '{grant_source,authority_kind}' =
                                         'registry_only'
                                     AND input.new_after_state::JSONB #>> '{grant_source,authority_key}' LIKE
                                         'registry-only:ethereum-mainnet:%'
                                     AND COALESCE(
                                         input.old_after_state::JSONB #>> '{grant_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_after_state::JSONB #>> '{grant_source,source_event_kind}' =
                                         input.new_after_state::JSONB #>> '{grant_source,source_event_kind}'
                                 )
                             )
                             AND (
                                 input.old_after_state::JSONB -> 'revocation_source' IS NOT DISTINCT FROM
                                     input.new_after_state::JSONB -> 'revocation_source'
                                 OR (
                                     input.old_after_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_after_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_after_state::JSONB #>> '{revocation_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         'registry_only'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,authority_key}' LIKE
                                         'registry-only:ethereum-mainnet:%'
                                     AND COALESCE(
                                         input.old_after_state::JSONB #>> '{revocation_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_after_state::JSONB #>> '{revocation_source,source_event_kind}' =
                                         input.new_after_state::JSONB #>> '{revocation_source,source_event_kind}'
                                 )
                             )
                         )
                     )
                 )
             )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                resource_id = repair.new_resource_id,
                before_state = repair.new_before_state::JSONB,
                after_state = repair.new_after_state::JSONB,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.resource_id = repair.old_resource_id
              AND event.before_state IS NOT DISTINCT FROM repair.old_before_state::JSONB
              AND event.after_state IS NOT DISTINCT FROM repair.old_after_state::JSONB
            RETURNING
                event.event_identity,
                event.normalized_event_id,
                event.canonicality_state,
                event.event_kind,
                repair.old_resource_id,
                repair.new_resource_id
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
            WHERE event_kind IN ('AuthorityTransferred', 'PermissionChanged')

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
          ON updated.event_identity = input.event_identity
        "#,
    )
    .bind(&event_identities)
    .bind(&old_resource_ids)
    .bind(&new_resource_ids)
    .bind(&logical_name_ids)
    .bind(&block_numbers)
    .bind(&block_hashes)
    .bind(&transaction_hashes)
    .bind(&log_indexes)
    .bind(&event_kinds)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .bind(&registration_resource_ids)
    .bind(&registration_block_hashes)
    .bind(&registration_transaction_hashes)
    .bind(&registration_log_indexes)
    .fetch_all(&mut **executor)
    .await
    .context(
        "failed to repair ENSv1 unwrapped-authority event-time registry normalized-event resource_id",
    )?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .zip(old_resource_ids.iter())
        .zip(new_resource_ids.iter())
        .filter(|((event_identity, _), _)| !repaired.contains(event_identity.as_str()))
        .map(|((event_identity, old_resource_id), new_resource_id)| {
            format!(
                "{event_identity} (old_resource_id={old_resource_id}, new_resource_id={new_resource_id})"
            )
        })
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 registry event-time resource_id repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v1_unwrapped_authority_registry_event_time_resource_id_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !registry_event_time_repair_differences_allowed(differing_fields) {
        return false;
    }
    if existing.resource_id.is_none()
        || incoming.resource_id.is_none()
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.logical_name_id != incoming.logical_name_id
        || existing.block_number.is_none()
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !matches!(
            existing.source_family.as_str(),
            "ens_v1_registry_l1" | "ens_v1_resolver_l1"
        )
        || !matches!(
            existing.event_kind.as_str(),
            "ResolverChanged"
                | "RecordChanged"
                | "RecordVersionChanged"
                | "PermissionChanged"
                | "AuthorityTransferred"
        )
    {
        return false;
    }

    if differing_fields.len() == 1 {
        return true;
    }

    if existing.event_kind == "AuthorityTransferred" {
        return authority_transfer_state_repair_allowed(
            &existing.before_state,
            &incoming.before_state,
            &existing.after_state,
            &incoming.after_state,
        );
    }

    existing.event_kind == "PermissionChanged"
        && permission_state_authority_repair_allowed(&existing.before_state, &incoming.before_state)
        && permission_state_authority_repair_allowed(&existing.after_state, &incoming.after_state)
}

fn registry_event_time_repair_differences_allowed(differing_fields: &[&'static str]) -> bool {
    matches!(
        differing_fields,
        ["resource_id"]
            | ["resource_id", "before_state"]
            | ["resource_id", "after_state"]
            | ["resource_id", "before_state", "after_state"]
    )
}

fn authority_transfer_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    existing_after_state: &Value,
    incoming_after_state: &Value,
) -> bool {
    if existing_after_state != incoming_after_state {
        return false;
    }

    let Some(existing_owner) = existing_before_state.get("owner").and_then(Value::as_str) else {
        return false;
    };
    let Some(incoming_owner) = incoming_before_state.get("owner").and_then(Value::as_str) else {
        return false;
    };
    if existing_owner.is_empty() || incoming_owner.is_empty() {
        return false;
    }

    let mut existing_without_owner = existing_before_state.clone();
    if let Some(object) = existing_without_owner.as_object_mut() {
        object.remove("owner");
    }
    let mut incoming_without_owner = incoming_before_state.clone();
    if let Some(object) = incoming_without_owner.as_object_mut() {
        object.remove("owner");
    }

    existing_without_owner == incoming_without_owner
}

fn permission_state_authority_repair_allowed(
    existing_state: &Value,
    incoming_state: &Value,
) -> bool {
    if existing_state == incoming_state {
        return true;
    }

    if permission_state_without_authority_sources(existing_state)
        != permission_state_without_authority_sources(incoming_state)
    {
        return false;
    }

    let grant_source_repair_allowed = authority_source_unchanged_or_repaired(
        existing_state.get("grant_source"),
        incoming_state.get("grant_source"),
    );
    let revocation_source_repair_allowed = authority_source_unchanged_or_repaired(
        existing_state.get("revocation_source"),
        incoming_state.get("revocation_source"),
    );

    grant_source_repair_allowed && revocation_source_repair_allowed
}

fn permission_state_without_authority_sources(state: &Value) -> Value {
    let mut value = state.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("grant_source");
        object.remove("revocation_source");
    }
    value
}

fn authority_source_unchanged_or_repaired(
    existing_source: Option<&Value>,
    incoming_source: Option<&Value>,
) -> bool {
    existing_source == incoming_source
        || authority_source_transition_allowed(existing_source, incoming_source)
}

fn authority_source_transition_allowed(
    existing_source: Option<&Value>,
    incoming_source: Option<&Value>,
) -> bool {
    let (Some(existing_source), Some(incoming_source)) = (existing_source, incoming_source) else {
        return false;
    };
    existing_source.get("kind").and_then(Value::as_str) == Some("ens_v1_authority")
        && incoming_source.get("kind").and_then(Value::as_str) == Some("ens_v1_authority")
        && existing_source
            .get("authority_kind")
            .and_then(Value::as_str)
            .is_some_and(|authority_kind| {
                matches!(authority_kind, "registrar" | "wrapper" | "registry_only")
            })
        && incoming_source
            .get("authority_kind")
            .and_then(Value::as_str)
            .is_some_and(|incoming_authority_kind| {
                let existing_authority_kind = existing_source
                    .get("authority_kind")
                    .and_then(Value::as_str);
                incoming_authority_kind == "registry_only"
                    || (existing_authority_kind == Some("registry_only")
                        && incoming_authority_kind == "registrar")
            })
        && existing_source
            .get("authority_key")
            .and_then(Value::as_str)
            .is_some_and(|authority_key| !authority_key.is_empty())
        && incoming_source
            .get("authority_key")
            .and_then(Value::as_str)
            .is_some_and(|authority_key| !authority_key.is_empty())
        && existing_source
            .get("source_event_kind")
            .and_then(Value::as_str)
            .is_some_and(|source_event_kind| !source_event_kind.is_empty())
        && existing_source
            .get("source_event_kind")
            .and_then(Value::as_str)
            == incoming_source
                .get("source_event_kind")
                .and_then(Value::as_str)
}
