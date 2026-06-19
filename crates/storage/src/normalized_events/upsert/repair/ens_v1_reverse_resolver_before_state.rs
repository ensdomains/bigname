use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_reverse_resolver_before_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_reverse_resolver_before_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 reverse resolver before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 reverse resolver before_state",
        )?);
        after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize ENSv1 reverse resolver after_state",
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
                $2::TEXT[],
                $3::TEXT[],
                $4::TEXT[]
            ) AS input(
                event_identity,
                old_before_state,
                new_before_state,
                after_state
            )
        ),
        repair_map AS (
            SELECT input.*
            FROM input
            WHERE input.old_before_state::JSONB - 'resolver' =
                  input.new_before_state::JSONB - 'resolver'
              AND input.old_before_state::JSONB ? 'resolver'
              AND input.new_before_state::JSONB ? 'resolver'
              AND (
                  (
                      input.old_before_state::JSONB -> 'resolver' = 'null'::JSONB
                      AND input.new_before_state::JSONB ->> 'resolver' ~ '^0x[0-9a-f]{40}$'
                  )
                  OR (
                      input.new_before_state::JSONB -> 'resolver' = 'null'::JSONB
                      AND input.old_before_state::JSONB ->> 'resolver' ~ '^0x[0-9a-f]{40}$'
                  )
              )
              AND input.after_state::JSONB ->> 'namehash' ~ '^0x[0-9a-f]{64}$'
              AND input.after_state::JSONB ->> 'resolver' ~ '^0x[0-9a-f]{40}$'
              AND input.after_state::JSONB #>> '{primary_claim_source,address}' ~
                  '^0x[0-9a-f]{40}$'
              AND input.after_state::JSONB #>> '{primary_claim_source,namespace}' = 'ens'
              AND input.after_state::JSONB #>> '{primary_claim_source,reverse_node}' =
                  input.after_state::JSONB ->> 'namehash'
              AND input.after_state::JSONB #>> '{primary_claim_source,claim_provenance,source_family}' =
                  'ens_v1_reverse_l1'
              AND input.after_state::JSONB #>> '{primary_claim_source,claim_provenance,contract_role}' =
                  'reverse_registrar'
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                before_state = repair.new_before_state::JSONB,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.namespace = 'ens'
              AND event.logical_name_id IS NULL
              AND event.resource_id IS NULL
              AND event.event_kind = 'ResolverChanged'
              AND event.source_family = 'ens_v1_registry_l1'
              AND event.chain_id = 'ethereum-mainnet'
              AND event.derivation_kind = 'ens_v1_unwrapped_authority'
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
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 reverse resolver before_state")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .filter(|event_identity| !repaired.contains(event_identity.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 reverse resolver before_state repair rejected invalid unanchored events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v1_reverse_resolver_before_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !matches!(differing_fields, ["before_state"]) {
        return false;
    }
    if existing.logical_name_id.is_some()
        || incoming.logical_name_id.is_some()
        || existing.resource_id.is_some()
        || incoming.resource_id.is_some()
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.source_family != "ens_v1_registry_l1"
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || existing.event_kind != "ResolverChanged"
        || existing.raw_fact_ref.get("kind").and_then(Value::as_str) != Some("raw_log")
        || existing.after_state != incoming.after_state
    {
        return false;
    }

    reverse_primary_claim_after_state_allowed(&incoming.after_state)
        && before_state_resolver_transition_allowed(&existing.before_state, &incoming.before_state)
}

fn reverse_primary_claim_after_state_allowed(value: &Value) -> bool {
    let Some(namehash) = required_json_text(value, "namehash") else {
        return false;
    };
    let Some(resolver) = required_json_text(value, "resolver") else {
        return false;
    };
    let Some(primary_claim_source) = value.get("primary_claim_source") else {
        return false;
    };
    let Some(claim_provenance) = primary_claim_source.get("claim_provenance") else {
        return false;
    };

    is_lower_hex_hash(namehash)
        && is_lower_hex_address(resolver)
        && required_json_text(primary_claim_source, "namespace") == Some("ens")
        && required_json_text(primary_claim_source, "reverse_node") == Some(namehash)
        && required_json_text(primary_claim_source, "address").is_some_and(is_lower_hex_address)
        && required_json_text(claim_provenance, "source_family") == Some("ens_v1_reverse_l1")
        && required_json_text(claim_provenance, "contract_role") == Some("reverse_registrar")
}

fn before_state_resolver_transition_allowed(existing: &Value, incoming: &Value) -> bool {
    if before_state_without_resolver(existing) != before_state_without_resolver(incoming) {
        return false;
    }

    match (existing.get("resolver"), incoming.get("resolver")) {
        (Some(Value::Null), Some(Value::String(incoming_resolver))) => {
            is_lower_hex_address(incoming_resolver)
        }
        (Some(Value::String(existing_resolver)), Some(Value::Null)) => {
            is_lower_hex_address(existing_resolver)
        }
        _ => false,
    }
}

fn before_state_without_resolver(value: &Value) -> Option<Value> {
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

fn is_lower_hex_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
