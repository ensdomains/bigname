use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_authority_epoch_resolver_boundary_after_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut old_after_states = Vec::new();
    let mut new_after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_authority_epoch_resolver_boundary_after_state_repair_allowed(
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
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv1 resolver-boundary after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired ENSv1 resolver-boundary after_state",
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
                $5::TEXT[]
            ) AS input(
                event_identity,
                resource_id,
                logical_name_id,
                old_after_state,
                new_after_state
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
             AND resource.provenance->>'logical_name_id' = input.logical_name_id
             AND resource.provenance->>'authority_kind' IN (
                 'registrar',
                 'wrapper',
                 'registry_only'
             )
            WHERE input.old_after_state::JSONB - 'resolver' =
                  input.new_after_state::JSONB - 'resolver'
              AND input.old_after_state::JSONB ->> 'source_event' =
                  'AuthorityEpochChanged'
              AND input.new_after_state::JSONB ->> 'source_event' =
                  'AuthorityEpochChanged'
              AND COALESCE(input.old_after_state::JSONB ->> 'resolver', '') <> ''
              AND COALESCE(input.new_after_state::JSONB ->> 'resolver', '') <> ''
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                after_state = repair.new_after_state::JSONB,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.resource_id = repair.resource_id
              AND event.after_state IS NOT DISTINCT FROM repair.old_after_state::JSONB
            RETURNING
                event.event_identity,
                event.normalized_event_id,
                event.canonicality_state,
                event.resource_id
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
                'content_update',
                canonicality_state
            FROM updated
            RETURNING
                change_id,
                normalized_event_id,
                changed_at
        ),
        affected_resource_keys AS (
            SELECT
                'record_inventory_current'::TEXT AS projection,
                resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
            FROM updated
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
    .bind(&resource_ids)
    .bind(&logical_name_ids)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 authority-epoch resolver-boundary after_state")?;

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
            "ENSv1 authority-epoch resolver-boundary after_state repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v1_authority_epoch_resolver_boundary_after_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields.len() != 1 || differing_fields[0] != "after_state" {
        return false;
    }
    if existing.resource_id.is_none()
        || existing.resource_id != incoming.resource_id
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.logical_name_id != incoming.logical_name_id
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !matches!(
            existing.source_family.as_str(),
            "ens_v1_registrar_l1" | "ens_v1_registry_l1" | "ens_v1_wrapper_l1"
        )
        || existing.event_kind != "ResolverChanged"
        || existing.transaction_hash.is_some()
        || existing.log_index.is_some()
        || existing.raw_fact_ref.get("kind").and_then(Value::as_str) != Some("raw_block")
    {
        return false;
    }
    if existing.before_state != incoming.before_state {
        return false;
    }
    if existing.before_state.get("resolver") != Some(&Value::Null) {
        return false;
    }
    if after_state_without_resolver(&existing.after_state)
        != after_state_without_resolver(&incoming.after_state)
    {
        return false;
    }
    if required_json_text(&incoming.after_state, "source_event") != Some("AuthorityEpochChanged") {
        return false;
    }
    let Some(existing_resolver) = required_json_text(&existing.after_state, "resolver") else {
        return false;
    };
    let Some(incoming_resolver) = required_json_text(&incoming.after_state, "resolver") else {
        return false;
    };

    is_lower_hex_address(existing_resolver) && is_lower_hex_address(incoming_resolver)
}

fn after_state_without_resolver(value: &Value) -> Option<Value> {
    let mut object = value.as_object()?.clone();
    object.remove("resolver");
    Some(Value::Object(object))
}

fn required_json_text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str().filter(|value| !value.is_empty())
}

fn is_lower_hex_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
