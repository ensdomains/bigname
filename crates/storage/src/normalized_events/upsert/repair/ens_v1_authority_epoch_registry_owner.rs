use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_authority_epoch_registry_owner_after_states(
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
        if !ens_v1_authority_epoch_registry_owner_after_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv1 authority-epoch after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired ENSv1 authority-epoch after_state",
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
                observed_at = now()
            FROM input
            WHERE event.event_identity = input.event_identity
              AND event.after_state IS NOT DISTINCT FROM input.old_after_state::JSONB
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
    .bind(&old_after_states)
    .bind(&new_after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 authority-epoch registry-owner after_state")?;

    Ok(repaired.into_iter().collect())
}

pub(crate) fn ens_v1_authority_epoch_registry_owner_after_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields.len() != 1 || differing_fields[0] != "after_state" {
        return false;
    }
    if existing.namespace != "ens"
        || existing.source_family != "ens_v1_registry_l1"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || existing.event_kind != "AuthorityEpochChanged"
    {
        return false;
    }
    if after_state_without_registry_owner(&existing.after_state)
        != after_state_without_registry_owner(&incoming.after_state)
    {
        return false;
    }
    if existing.after_state.get("registry_owner").is_some() {
        return false;
    }
    if required_json_text(&incoming.after_state, "authority_kind") != Some("registry_only") {
        return false;
    }
    let Some(authority_key) = required_json_text(&incoming.after_state, "authority_key") else {
        return false;
    };
    if !authority_key.starts_with("registry-only:ethereum-mainnet:0x") {
        return false;
    }
    let Some(registry_owner) = required_json_text(&incoming.after_state, "registry_owner") else {
        return false;
    };

    is_lower_hex_address(registry_owner)
}

fn after_state_without_registry_owner(value: &serde_json::Value) -> Option<serde_json::Value> {
    let mut object = value.as_object()?.clone();
    object.remove("registry_owner");
    Some(serde_json::Value::Object(object))
}

fn required_json_text<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
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
