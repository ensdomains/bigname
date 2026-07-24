use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Postgres;
use uuid::Uuid;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v2_registration_expiry_after_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_after_states = Vec::new();
    let mut new_after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v2_registration_expiry_after_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv2 registration after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired ENSv2 registration after_state",
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
                $3::TEXT[]
            ) AS input(
                event_identity,
                old_after_state,
                new_after_state
            )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                after_state = input.new_after_state::JSONB,
                canonicality_state = event.canonicality_state,
                observed_at = now()
            FROM input
            WHERE event.event_identity = input.event_identity
              AND event.namespace = 'ens'
              AND event.logical_name_id IS NOT NULL
              AND event.resource_id IS NOT NULL
              AND event.event_kind = 'RegistrationGranted'
              AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND event.derivation_kind = 'ens_v2_registry_resource_surface'
              AND event.after_state IS NOT DISTINCT FROM input.old_after_state::JSONB
              AND input.old_after_state::JSONB - 'expiry' =
                  input.new_after_state::JSONB - 'expiry'
              AND jsonb_typeof(input.old_after_state::JSONB -> 'expiry') = 'number'
              AND jsonb_typeof(input.new_after_state::JSONB -> 'expiry') = 'number'
              AND input.old_after_state::JSONB -> 'expiry'
                  IS DISTINCT FROM input.new_after_state::JSONB -> 'expiry'
            RETURNING event.event_identity
        )
        SELECT event_identity
        FROM updated
        "#,
    )
    .bind(&event_identities)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv2 registration link-time expiry")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .filter(|event_identity| !repaired.contains(event_identity.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv2 registration link-time expiry repair rejected invalid events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v2_registration_expiry_after_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !matches!(differing_fields, ["after_state"])
        || existing.namespace != "ens"
        || existing.logical_name_id.is_none()
        || existing.resource_id.is_none()
        || existing.event_kind != "RegistrationGranted"
        || !matches!(
            existing.source_family.as_str(),
            "ens_v2_root_l1" | "ens_v2_registry_l1"
        )
        || existing.derivation_kind != "ens_v2_registry_resource_surface"
        || existing.raw_fact_ref.get("kind").and_then(Value::as_str) != Some("raw_log")
        || !existing
            .event_identity
            .starts_with("ens_v2_registry_resource_surface:")
    {
        return false;
    }

    if after_state_without_expiry(&existing.after_state)
        != after_state_without_expiry(&incoming.after_state)
    {
        return false;
    }

    let Some(existing_expiry) = registration_expiry(&existing.after_state) else {
        return false;
    };
    let Some(incoming_expiry) = registration_expiry(&incoming.after_state) else {
        return false;
    };

    existing_expiry != incoming_expiry && registration_after_state_is_anchored(incoming)
}

fn after_state_without_expiry(value: &Value) -> Option<Value> {
    let mut object = value.as_object()?.clone();
    object.remove("expiry")?;
    Some(Value::Object(object))
}

fn registration_expiry(value: &Value) -> Option<u64> {
    value.get("expiry")?.as_u64()
}

fn registration_after_state_is_anchored(event: &NormalizedEvent) -> bool {
    let state = &event.after_state;
    let Some(chain_id) = event.chain_id.as_deref() else {
        return false;
    };
    let Some(registry_contract_instance_id) =
        required_json_text(state, "registry_contract_instance_id")
    else {
        return false;
    };
    if Uuid::parse_str(registry_contract_instance_id).is_err() {
        return false;
    }
    let Some(authority_key) = required_json_text(state, "authority_key") else {
        return false;
    };
    let Some(upstream_resource) = required_json_text(state, "upstream_resource") else {
        return false;
    };
    let authority_prefix = format!("ens-v2-registry:{chain_id}:{registry_contract_instance_id}:");

    required_json_text(state, "authority_kind") == Some("ens_v2_registry")
        && required_json_text(state, "status") == Some("registered")
        && authority_key == format!("{authority_prefix}{upstream_resource}")
        && required_json_text(state, "registrant").is_some_and(is_lower_hex_address)
        && required_json_text(state, "labelhash").is_some_and(is_lower_hex_hash)
        && required_json_text(state, "token_id").is_some()
        && required_json_text(state, "current_token_id").is_some()
}

fn required_json_text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str().filter(|value| !value.is_empty())
}

fn is_lower_hex_address(value: &str) -> bool {
    is_lower_hex(value, 40)
}

fn is_lower_hex_hash(value: &str) -> bool {
    is_lower_hex(value, 64)
}

fn is_lower_hex(value: &str, digits: usize) -> bool {
    value.len() == digits + 2
        && value.starts_with("0x")
        && value
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
