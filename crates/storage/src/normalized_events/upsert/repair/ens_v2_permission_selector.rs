use std::collections::{HashMap, HashSet};

use alloy_primitives::hex;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::Postgres;
use uuid::Uuid;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v2_permission_selector_links(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut new_logical_name_ids = Vec::new();
    let mut old_after_states = Vec::new();
    let mut new_after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v2_permission_selector_link_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        new_logical_name_ids.push(
            event
                .logical_name_id
                .clone()
                .expect("validated ENSv2 permission selector repair has a logical name"),
        );
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv2 permission after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired ENSv2 permission after_state",
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
                new_logical_name_id,
                old_after_state,
                new_after_state
            )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                logical_name_id = input.new_logical_name_id,
                after_state = input.new_after_state::JSONB,
                canonicality_state = event.canonicality_state,
                observed_at = now()
            FROM input
            WHERE event.event_identity = input.event_identity
              AND event.namespace = 'ens'
              AND event.logical_name_id IS NULL
              AND event.resource_id IS NOT NULL
              AND event.event_kind = 'PermissionChanged'
              AND event.source_family = 'ens_v2_resolver_l1'
              AND event.derivation_kind = 'ens_v2_permissions'
              AND event.raw_fact_ref ->> 'kind' = 'raw_log'
              AND event.after_state IS NOT DISTINCT FROM input.old_after_state::JSONB
              AND input.old_after_state::JSONB - 'selector' =
                  input.new_after_state::JSONB - 'selector'
              AND input.old_after_state::JSONB -> 'selector' = jsonb_build_object(
                  'kind', 'unknown',
                  'key', NULL,
                  'hash', NULL,
                  'normalized_name', NULL,
                  'dns_encoded_name', NULL
              )
              AND input.new_after_state::JSONB -> 'selector' ->> 'kind'
                  IN ('name', 'text', 'addr')
              AND input.new_after_state::JSONB -> 'selector' ->> 'normalized_name' <> ''
              AND input.new_logical_name_id =
                  'ens:' || (
                      input.new_after_state::JSONB
                      -> 'selector'
                      ->> 'normalized_name'
                  )
            RETURNING event.event_identity
        )
        SELECT event_identity
        FROM updated
        "#,
    )
    .bind(&event_identities)
    .bind(&new_logical_name_ids)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv2 permission selector links")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .filter(|event_identity| !repaired.contains(event_identity.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv2 permission selector link repair rejected invalid events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v2_permission_selector_link_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields != ["logical_name_id", "after_state"]
        || existing.namespace != "ens"
        || existing.logical_name_id.is_some()
        || existing.resource_id.is_none()
        || existing.event_kind != "PermissionChanged"
        || existing.source_family != "ens_v2_resolver_l1"
        || existing.derivation_kind != "ens_v2_permissions"
        || existing.raw_fact_ref.get("kind").and_then(Value::as_str) != Some("raw_log")
        || !existing.event_identity.starts_with("ens_v2_permissions:")
        || after_state_without_selector(&existing.after_state)
            != after_state_without_selector(&incoming.after_state)
        || existing.after_state.get("selector") != Some(&unknown_selector())
        || !permission_event_is_anchored(incoming)
    {
        return false;
    }

    let Some(logical_name_id) = incoming.logical_name_id.as_deref() else {
        return false;
    };
    let Some(selector) = incoming.after_state.get("selector") else {
        return false;
    };
    let Some(normalized_name) = required_text(selector, "normalized_name") else {
        return false;
    };
    if logical_name_id != format!("{}:{normalized_name}", incoming.namespace) {
        return false;
    }
    let Some(dns_encoded_name) = required_text(selector, "dns_encoded_name")
        .and_then(|value| value.strip_prefix("0x"))
        .and_then(|value| hex::decode(value).ok())
    else {
        return false;
    };
    if bigname_domain::normalization::normalize_dns_encoded_name(&dns_encoded_name)
        .ok()
        .map(|name| name.normalized_name)
        .as_deref()
        != Some(normalized_name)
    {
        return false;
    }

    match required_text(selector, "kind") {
        Some("name") => {
            selector.get("key") == Some(&Value::Null) && selector.get("hash") == Some(&Value::Null)
        }
        Some("text") => {
            required_text(selector, "key").is_some()
                && required_text(selector, "hash").is_some_and(is_lower_hex_hash)
        }
        Some("addr") => {
            required_text(selector, "key")
                .is_some_and(|value| value.bytes().all(|byte| byte.is_ascii_digit()))
                && selector.get("hash") == Some(&Value::Null)
        }
        _ => false,
    }
}

fn after_state_without_selector(value: &Value) -> Option<Value> {
    let mut object = value.as_object()?.clone();
    object.remove("selector")?;
    Some(Value::Object(object))
}

fn unknown_selector() -> Value {
    json!({
        "kind": "unknown",
        "key": null,
        "hash": null,
        "normalized_name": null,
        "dns_encoded_name": null,
    })
}

fn permission_event_is_anchored(event: &NormalizedEvent) -> bool {
    let state = &event.after_state;
    let Some(chain_id) = event.chain_id.as_deref() else {
        return false;
    };
    let Some(raw_emitter) = required_text(&event.raw_fact_ref, "emitting_address") else {
        return false;
    };
    let Some(resolver_contract_instance_id) = required_text(state, "resolver_contract_instance_id")
    else {
        return false;
    };
    if Uuid::parse_str(resolver_contract_instance_id).is_err() {
        return false;
    }
    let Some(scope) = state.get("scope") else {
        return false;
    };
    required_text(state, "source_event") == Some("EACRolesChanged")
        && required_text(state, "upstream_resource").is_some_and(is_lower_hex_hash)
        && state.get("root_resource") == Some(&Value::Bool(false))
        && required_text(scope, "kind") == Some("resolver")
        && required_text(scope, "chain_id") == Some(chain_id)
        && required_text(scope, "resolver_address")
            .is_some_and(|address| address.eq_ignore_ascii_case(raw_emitter))
        && is_lower_hex_address(raw_emitter)
}

fn required_text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
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
