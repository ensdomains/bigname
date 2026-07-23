use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_wrapper_token_before_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_wrapper_token_before_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 wrapper-token before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 wrapper-token before_state",
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
                old_before_state,
                new_before_state
            )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                before_state = input.new_before_state::JSONB,
                observed_at = now()
            FROM input
            WHERE event.event_identity = input.event_identity
              AND event.before_state IS NOT DISTINCT FROM input.old_before_state::JSONB
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
                'content_update',
                canonicality_state
            FROM updated
            RETURNING normalized_event_id
        )
        SELECT event_identity
        FROM updated
        "#,
    )
    .bind(&event_identities)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 wrapper-token before_state")?;

    Ok(repaired.into_iter().collect())
}

pub(crate) fn ens_v1_wrapper_token_before_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields.len() != 1 || differing_fields[0] != "before_state" {
        return false;
    }
    if existing.namespace != "ens"
        || existing.source_family != "ens_v1_wrapper_l1"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || existing.event_kind != "TokenControlTransferred"
    {
        return false;
    }
    if existing.after_state != incoming.after_state {
        return false;
    }
    if required_json_text(&incoming.after_state, "authority_kind") != Some("wrapper") {
        return false;
    }
    let Some(authority_key) = required_json_text(&incoming.after_state, "authority_key") else {
        return false;
    };
    let Some(namehash) = required_json_text(&incoming.after_state, "namehash") else {
        return false;
    };
    let Some(to) = required_json_text(&incoming.after_state, "to") else {
        return false;
    };

    authority_key.starts_with("wrapper:ethereum-mainnet:")
        && authority_key.contains(namehash)
        && is_lower_hex_hash(namehash)
        && is_lower_hex_address(to)
        && (wrapper_token_authority_kind_repair_allowed(
            &existing.before_state,
            &incoming.before_state,
        ) || wrapper_token_from_owner_repair_allowed(
            &existing.before_state,
            &incoming.before_state,
        ))
}

fn wrapper_token_authority_kind_repair_allowed(
    existing_before_state: &serde_json::Value,
    incoming_before_state: &serde_json::Value,
) -> bool {
    if before_state_without_key(existing_before_state, "authority_kind")
        != before_state_without_key(incoming_before_state, "authority_kind")
    {
        return false;
    }

    repairable_existing_authority_kind(existing_before_state)
        && repairable_incoming_authority_kind(incoming_before_state)
}

fn wrapper_token_from_owner_repair_allowed(
    existing_before_state: &serde_json::Value,
    incoming_before_state: &serde_json::Value,
) -> bool {
    if before_state_without_key(existing_before_state, "from")
        != before_state_without_key(incoming_before_state, "from")
    {
        return false;
    }
    let Some(existing_from) = required_json_text(existing_before_state, "from") else {
        return false;
    };
    let Some(incoming_from) = required_json_text(incoming_before_state, "from") else {
        return false;
    };

    existing_from != incoming_from
        && is_lower_hex_address(existing_from)
        && is_lower_hex_address(incoming_from)
}

fn before_state_without_key(value: &serde_json::Value, key: &str) -> Option<serde_json::Value> {
    let mut object = value.as_object()?.clone();
    object.remove(key);
    Some(serde_json::Value::Object(object))
}

fn required_json_text<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str().filter(|value| !value.is_empty())
}

fn repairable_existing_authority_kind(value: &serde_json::Value) -> bool {
    match value.get("authority_kind") {
        Some(serde_json::Value::String(authority_kind)) => {
            matches!(authority_kind.as_str(), "registrar" | "registry_only")
        }
        Some(serde_json::Value::Null) => true,
        _ => false,
    }
}

fn repairable_incoming_authority_kind(value: &serde_json::Value) -> bool {
    match value.get("authority_kind") {
        Some(serde_json::Value::String(authority_kind)) => {
            matches!(authority_kind.as_str(), "registrar" | "registry_only")
        }
        Some(serde_json::Value::Null) => true,
        _ => false,
    }
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

fn is_lower_hex_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
