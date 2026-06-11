use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(crate) async fn repair_ens_v1_registry_resolver_observation_key_after_states(
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
        if !ens_v1_registry_resolver_observation_key_after_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv1 registry resolver after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired ENSv1 registry resolver after_state",
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
                'canonicality_update',
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
    .context("failed to repair ENSv1 registry resolver observation-key after_state")?;

    Ok(repaired.into_iter().collect())
}

pub(crate) fn ens_v1_registry_resolver_observation_key_after_state_repair_allowed(
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
        || existing.derivation_kind != "ens_v1_registry_resolver_changed"
        || existing.event_kind != "ResolverChanged"
        || existing.logical_name_id.is_some()
        || existing.resource_id.is_some()
    {
        return false;
    }
    if after_state_without_observation_key(&existing.after_state)
        != after_state_without_observation_key(&incoming.after_state)
    {
        return false;
    }

    let Some(node) = required_after_text(&incoming.after_state, "node") else {
        return false;
    };
    let Some(emitting_address) = required_after_text(&incoming.after_state, "emitting_address")
    else {
        return false;
    };
    let expected_key = format!("resolver:{emitting_address}:{node}");
    let Some(incoming_key) = required_after_text(&incoming.after_state, "observation_key") else {
        return false;
    };
    let Some(existing_key) = required_after_text(&existing.after_state, "observation_key") else {
        return false;
    };

    required_after_text(&incoming.after_state, "raw_resolver") == Some(ZERO_ADDRESS)
        && incoming.after_state.get("resolver") == Some(&Value::Null)
        && incoming
            .after_state
            .get("tombstone")
            .and_then(Value::as_bool)
            == Some(true)
        && incoming_key.eq_ignore_ascii_case(&expected_key)
        && resolver_observation_key_targets_node(existing_key, node)
        && !existing_key.eq_ignore_ascii_case(incoming_key)
}

fn after_state_without_observation_key(after_state: &Value) -> Value {
    let mut value = after_state.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("observation_key");
    }
    value
}

fn required_after_text<'a>(after_state: &'a Value, key: &str) -> Option<&'a str> {
    after_state
        .get(key)?
        .as_str()
        .filter(|value| !value.is_empty())
}

fn resolver_observation_key_targets_node(observation_key: &str, node: &str) -> bool {
    observation_key
        .strip_prefix("resolver:")
        .and_then(|remaining| remaining.rsplit_once(':'))
        .map(|(_, observed_node)| observed_node.eq_ignore_ascii_case(node))
        .unwrap_or(false)
}
