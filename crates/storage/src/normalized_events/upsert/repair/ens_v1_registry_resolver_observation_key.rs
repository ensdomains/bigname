use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, ensure};
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
    let mut requires_edge_proofs = Vec::new();

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
        requires_edge_proofs.push(ens_v1_registry_resolver_context_reattribution_allowed(
            existing, event,
        ));
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
                $4::BOOLEAN[]
            ) AS input(
                event_identity,
                old_after_state,
                new_after_state,
                requires_edge_proof
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
              AND event.after_state IS NOT DISTINCT FROM input.old_after_state::JSONB
              AND (
                  NOT input.requires_edge_proof
                  OR EXISTS (
                      SELECT 1
                      FROM discovery_edges edge
                      WHERE edge.chain_id = event.chain_id
                        AND edge.discovery_source =
                                input.new_after_state::JSONB ->> 'discovery_source'
                        AND edge.provenance ->> 'observation_key' =
                                input.new_after_state::JSONB ->> 'observation_key'
                        AND edge.edge_kind = 'resolver'
                        AND edge.active_from_block_number = event.block_number
                        AND edge.active_from_block_hash = event.block_hash
                        AND edge.from_contract_instance_id =
                                (input.new_after_state::JSONB ->> 'from_contract_instance_id')::UUID
                        AND edge.to_contract_instance_id =
                                (input.new_after_state::JSONB ->> 'to_contract_instance_id')::UUID
                  )
              )
            RETURNING event.event_identity
        )
        SELECT event_identity
        FROM updated
        "#,
    )
    .bind(&event_identities)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .bind(&requires_edge_proofs)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 registry resolver observation-key after_state")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let missing_edge_proofs = event_identities
        .iter()
        .zip(&requires_edge_proofs)
        .filter_map(|(event_identity, requires_edge_proof)| {
            (*requires_edge_proof && !repaired.contains(event_identity))
                .then_some(event_identity.as_str())
        })
        .collect::<Vec<_>>();
    ensure!(
        missing_edge_proofs.is_empty(),
        "ENSv1 registry resolver context reattribution lacks a matching reconciled discovery edge for event identities {}",
        missing_edge_proofs.join(",")
    );

    Ok(repaired)
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
    ens_v1_registry_resolver_tombstone_observation_key_repair_allowed(existing, incoming)
        || ens_v1_registry_resolver_context_reattribution_allowed(existing, incoming)
}

fn ens_v1_registry_resolver_tombstone_observation_key_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> bool {
    if after_state_without_fields(&existing.after_state, &["observation_key"])
        != after_state_without_fields(&incoming.after_state, &["observation_key"])
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

fn ens_v1_registry_resolver_context_reattribution_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> bool {
    if after_state_without_fields(
        &existing.after_state,
        &["observation_key", "from_contract_instance_id"],
    ) != after_state_without_fields(
        &incoming.after_state,
        &["observation_key", "from_contract_instance_id"],
    ) {
        return false;
    }

    let Some(node) = required_after_text(&incoming.after_state, "node") else {
        return false;
    };
    let Some(incoming_key) = required_after_text(&incoming.after_state, "observation_key") else {
        return false;
    };
    let Some(existing_key) = required_after_text(&existing.after_state, "observation_key") else {
        return false;
    };
    let Some(incoming_from) =
        required_after_uuid(&incoming.after_state, "from_contract_instance_id")
    else {
        return false;
    };
    let Some(existing_from) =
        required_after_uuid(&existing.after_state, "from_contract_instance_id")
    else {
        return false;
    };
    let Some(_) = required_after_uuid(&incoming.after_state, "to_contract_instance_id") else {
        return false;
    };
    let Some(resolver) = required_after_text(&incoming.after_state, "resolver") else {
        return false;
    };
    let Some(raw_resolver) = required_after_text(&incoming.after_state, "raw_resolver") else {
        return false;
    };

    incoming
        .after_state
        .get("source_event")
        .and_then(Value::as_str)
        == Some("NewResolver")
        && incoming
            .after_state
            .get("edge_kind")
            .and_then(Value::as_str)
            == Some("resolver")
        && incoming
            .after_state
            .get("tombstone")
            .and_then(Value::as_bool)
            == Some(false)
        && incoming
            .after_state
            .get("active_edge")
            .and_then(Value::as_bool)
            == Some(true)
        && !raw_resolver.eq_ignore_ascii_case(ZERO_ADDRESS)
        && resolver.eq_ignore_ascii_case(raw_resolver)
        && incoming_from != existing_from
        && resolver_observation_key_targets_node(existing_key, node)
        && resolver_observation_key_targets_node(incoming_key, node)
        && !existing_key.eq_ignore_ascii_case(incoming_key)
}

fn after_state_without_fields(after_state: &Value, fields: &[&str]) -> Value {
    let mut value = after_state.clone();
    if let Some(object) = value.as_object_mut() {
        for field in fields {
            object.remove(*field);
        }
    }
    value
}

fn required_after_text<'a>(after_state: &'a Value, key: &str) -> Option<&'a str> {
    after_state
        .get(key)?
        .as_str()
        .filter(|value| !value.is_empty())
}

fn required_after_uuid(after_state: &Value, key: &str) -> Option<sqlx::types::Uuid> {
    required_after_text(after_state, key)?.parse().ok()
}

fn resolver_observation_key_targets_node(observation_key: &str, node: &str) -> bool {
    observation_key
        .strip_prefix("resolver:")
        .and_then(|remaining| remaining.rsplit_once(':'))
        .map(|(_, observed_node)| observed_node.eq_ignore_ascii_case(node))
        .unwrap_or(false)
}
