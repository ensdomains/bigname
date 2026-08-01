use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

const RETIRED_RAW_FACT_FIELDS: &[&str] = &["topic0", "topic1", "topic2", "data_hex"];
const RETIRED_AFTER_STATE_FIELDS: &[&str] = &[
    "discovery_source",
    "edge_kind",
    "observation_key",
    "from_contract_instance_id",
    "to_contract_instance_id",
];

pub(crate) async fn repair_ens_v1_subregistry_owner_discovery_payloads(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_raw_fact_refs = Vec::new();
    let mut new_raw_fact_refs = Vec::new();
    let mut old_after_states = Vec::new();
    let mut new_after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_subregistry_owner_discovery_payload_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_raw_fact_refs.push(serialize_jsonb_value(
            &existing.raw_fact_ref,
            "failed to serialize existing ENSv1 subregistry raw_fact_ref",
        )?);
        new_raw_fact_refs.push(serialize_jsonb_value(
            &event.raw_fact_ref,
            "failed to serialize rehomed ENSv1 subregistry raw_fact_ref",
        )?);
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv1 subregistry after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize rehomed ENSv1 subregistry after_state",
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
                $4::TEXT[],
                $5::TEXT[]
            ) AS input(
                event_identity,
                old_raw_fact_ref,
                new_raw_fact_ref,
                old_after_state,
                new_after_state
            )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                raw_fact_ref = input.new_raw_fact_ref::JSONB,
                after_state = input.new_after_state::JSONB,
                observed_at = now()
            FROM input
            WHERE event.event_identity = input.event_identity
              AND event.raw_fact_ref IS NOT DISTINCT FROM input.old_raw_fact_ref::JSONB
              AND event.after_state IS NOT DISTINCT FROM input.old_after_state::JSONB
            RETURNING event.event_identity
        )
        SELECT event_identity
        FROM updated
        "#,
    )
    .bind(&event_identities)
    .bind(&old_raw_fact_refs)
    .bind(&new_raw_fact_refs)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair rehomed ENSv1 subregistry normalized-event payload")?
    .into_iter()
    .collect::<HashSet<_>>();

    ensure!(
        repaired.len() == event_identities.len(),
        "rehomed ENSv1 subregistry payload repair updated {} of {} eligible events",
        repaired.len(),
        event_identities.len()
    );
    Ok(repaired)
}

pub(crate) fn ens_v1_subregistry_owner_discovery_payload_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields != ["raw_fact_ref", "after_state"]
        || !is_subregistry_assignment_history(existing)
        || !is_subregistry_assignment_history(incoming)
    {
        return false;
    }

    let Some(existing_raw_fact) = existing.raw_fact_ref.as_object() else {
        return false;
    };
    if RETIRED_RAW_FACT_FIELDS.iter().any(|field| {
        existing_raw_fact
            .get(*field)
            .and_then(Value::as_str)
            .is_none()
    }) || value_without_fields(&existing.raw_fact_ref, RETIRED_RAW_FACT_FIELDS)
        != incoming.raw_fact_ref
        || existing.raw_fact_ref.get("topic1") != incoming.after_state.get("parent_node")
        || existing.raw_fact_ref.get("topic2") != incoming.after_state.get("labelhash")
    {
        return false;
    }

    let Some(existing_after_state) = existing.after_state.as_object() else {
        return false;
    };
    if value_without_fields(&existing.after_state, RETIRED_AFTER_STATE_FIELDS)
        != incoming.after_state
        || existing_after_state
            .get("edge_kind")
            .and_then(Value::as_str)
            != Some("subregistry")
        || existing_after_state.get("observation_key") != incoming.after_state.get("child_node")
        || existing_after_state
            .get("discovery_source")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        || existing_after_state
            .get("from_contract_instance_id")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<sqlx::types::Uuid>().ok())
            .is_none()
    {
        return false;
    }

    let tombstone = incoming
        .after_state
        .get("tombstone")
        .and_then(Value::as_bool);
    let active_edge = incoming
        .after_state
        .get("active_edge")
        .and_then(Value::as_bool);
    let to_contract_instance_id = existing_after_state.get("to_contract_instance_id");
    incoming
        .after_state
        .get("source_event")
        .and_then(Value::as_str)
        == Some("NewOwner")
        && incoming.raw_fact_ref.get("emitting_address")
            == incoming.after_state.get("emitting_address")
        && matches!(
            (tombstone, active_edge),
            (Some(false), Some(true)) | (Some(true), Some(false))
        )
        && match tombstone {
            Some(false) => to_contract_instance_id
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<sqlx::types::Uuid>().ok())
                .is_some(),
            Some(true) => to_contract_instance_id == Some(&Value::Null),
            None => false,
        }
}

fn is_subregistry_assignment_history(event: &NormalizedEvent) -> bool {
    event.derivation_kind == "ens_v1_subregistry_changed"
        && event.event_kind == "SubregistryChanged"
        && event.logical_name_id.is_none()
        && event.resource_id.is_none()
        && matches!(
            (
                event.namespace.as_str(),
                event.source_family.as_str(),
                event.chain_id.as_deref(),
            ),
            ("ens", "ens_v1_registry_l1", Some("ethereum-mainnet"))
                | ("basenames", "basenames_base_registry", Some("base-mainnet"))
        )
}

fn value_without_fields(value: &Value, fields: &[&str]) -> Value {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        for field in fields {
            object.remove(*field);
        }
    }
    value
}
