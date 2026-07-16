use std::collections::{HashMap, HashSet};

use alloy_primitives::{Address, B256};
use anyhow::{Context, Result};
use sqlx::Postgres;
use uuid::Uuid;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};
use crate::evm_primitives::ens_namehash_label_bytes;

pub(crate) async fn repair_primary_claim_source_after_states(
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
        if !primary_claim_source_after_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing normalized-event after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired normalized-event after_state",
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
                event.canonicality_state,
                input.old_after_state,
                input.new_after_state,
                event.namespace
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
        candidate_primary_keys AS (
            SELECT
                'primary_names_current'::TEXT AS projection,
                lower(claim_source ->> 'address')
                    || ':' || COALESCE(claim_source ->> 'namespace', updated.namespace)
                    || ':' || (claim_source ->> 'coin_type') AS projection_key,
                jsonb_build_object(
                    'address', lower(claim_source ->> 'address'),
                    'namespace', COALESCE(claim_source ->> 'namespace', updated.namespace),
                    'coin_type', claim_source ->> 'coin_type'
                ) AS key_payload,
                updated.normalized_event_id,
                queued_changes.change_id,
                queued_changes.changed_at
            FROM updated
            JOIN queued_changes
              ON queued_changes.normalized_event_id = updated.normalized_event_id
            CROSS JOIN LATERAL (
                VALUES
                    (updated.old_after_state::JSONB -> 'primary_claim_source'),
                    (updated.new_after_state::JSONB -> 'primary_claim_source')
            ) AS tuple(claim_source)
            WHERE claim_source ->> 'address' IS NOT NULL
              AND claim_source ->> 'address' <> ''
              AND COALESCE(claim_source ->> 'namespace', updated.namespace) IS NOT NULL
              AND COALESCE(claim_source ->> 'namespace', updated.namespace) <> ''
              AND claim_source ->> 'coin_type' IS NOT NULL
              AND claim_source ->> 'coin_type' <> ''
        ),
        queued_primary_invalidations AS (
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload,
                first_change_id,
                last_change_id,
                first_normalized_event_id,
                last_normalized_event_id,
                last_changed_at,
                invalidated_at
            )
            SELECT
                projection,
                projection_key,
                key_payload,
                MIN(change_id),
                MAX(change_id),
                MIN(normalized_event_id),
                MAX(normalized_event_id),
                MAX(changed_at),
                now()
            FROM candidate_primary_keys
            WHERE projection_key IS NOT NULL
              AND btrim(projection_key) <> ''
            GROUP BY projection, projection_key, key_payload
            ON CONFLICT (projection, projection_key)
            DO UPDATE SET
                key_payload = EXCLUDED.key_payload,
                generation = projection_invalidations.generation + 1,
                first_change_id = LEAST(
                    projection_invalidations.first_change_id,
                    EXCLUDED.first_change_id
                ),
                last_change_id = GREATEST(
                    projection_invalidations.last_change_id,
                    EXCLUDED.last_change_id
                ),
                first_normalized_event_id = LEAST(
                    projection_invalidations.first_normalized_event_id,
                    EXCLUDED.first_normalized_event_id
                ),
                last_normalized_event_id = GREATEST(
                    projection_invalidations.last_normalized_event_id,
                    EXCLUDED.last_normalized_event_id
                ),
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
        SELECT event_identity
        FROM updated
        "#,
    )
    .bind(&event_identities)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair primary-claim normalized-event after_state")?;

    Ok(repaired.into_iter().collect())
}

pub(crate) fn primary_claim_source_after_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    basenames_primary_claim_source_after_state_repair_allowed(existing, incoming, differing_fields)
        || ens_v1_reverse_name_primary_claim_source_after_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
}

fn basenames_primary_claim_source_after_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields.len() != 1 || differing_fields[0] != "after_state" {
        return false;
    }
    if existing.namespace != "basenames"
        || existing.source_family != "basenames_base_primary"
        || existing.chain_id.as_deref() != Some("base-mainnet")
        || existing.derivation_kind != "ens_v1_reverse_claim"
        || existing.event_kind != "RecordChanged"
        || existing.logical_name_id.is_some()
        || existing.resource_id.is_some()
        || existing
            .after_state
            .get("record_key")
            .and_then(|value| value.as_str())
            != Some("name")
    {
        return false;
    }

    if after_state_without_primary_claim_source(&existing.after_state)
        != after_state_without_primary_claim_source(&incoming.after_state)
    {
        return false;
    }

    let Some(existing_source) = existing.after_state.get("primary_claim_source") else {
        return false;
    };
    let Some(incoming_source) = incoming.after_state.get("primary_claim_source") else {
        return false;
    };

    primary_claim_source_required_text(existing_source, &["coin_type"]) == Some("60")
        && primary_claim_source_required_text(
            existing_source,
            &["claim_provenance", "emitting_address"],
        ) == Some("0x79ea96012eea67a83431f1701b3dff7e37f9e282")
        && primary_claim_source_uuid(
            existing_source,
            &["claim_provenance", "contract_instance_id"],
        )
        .is_some()
        && primary_claim_source_required_text(incoming_source, &["coin_type"]) == Some("2147492101")
        && primary_claim_source_required_text(
            incoming_source,
            &["claim_provenance", "emitting_address"],
        ) == Some("0x0000000000d8e504002cc26e3ec46d81971c1664")
        && primary_claim_source_uuid(
            incoming_source,
            &["claim_provenance", "contract_instance_id"],
        )
        .is_some()
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["address"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["namespace"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["reverse_node"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["reverse_name"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["claim_provenance", "source_family"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["claim_provenance", "contract_role"],
        )
}

fn ens_v1_reverse_name_primary_claim_source_after_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields != ["after_state"]
        || existing.namespace != "ens"
        || existing.source_family != "ens_v1_resolver_l1"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || existing.event_kind != "RecordChanged"
        || existing.logical_name_id.is_some()
        || existing.resource_id.is_some()
        || existing.transaction_hash.is_none()
        || existing.log_index.is_none()
        || existing
            .before_state
            .as_object()
            .is_none_or(|state| !state.is_empty())
        || existing
            .raw_fact_ref
            .get("kind")
            .and_then(|value| value.as_str())
            != Some("raw_log")
    {
        return false;
    }

    let Some(base_after_state) = after_state_without_primary_claim_source(&existing.after_state)
    else {
        return false;
    };
    if Some(base_after_state.clone())
        != after_state_without_primary_claim_source(&incoming.after_state)
        || !ens_v1_reverse_name_observation_after_state(&base_after_state)
    {
        return false;
    }

    match (
        existing.after_state.get("primary_claim_source"),
        incoming.after_state.get("primary_claim_source"),
    ) {
        (None, Some(source)) | (Some(source), None) => ens_v1_primary_claim_source_valid(source),
        _ => false,
    }
}

fn ens_v1_reverse_name_observation_after_state(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    json_object_has_exact_keys(
        object,
        &["raw_name", "record_family", "record_key", "selector_key"],
    ) && object
        .get("raw_name")
        .is_some_and(serde_json::Value::is_string)
        && object.get("record_family").and_then(|value| value.as_str()) == Some("name")
        && object.get("record_key").and_then(|value| value.as_str()) == Some("name")
        && object
            .get("selector_key")
            .is_some_and(serde_json::Value::is_null)
}

fn ens_v1_primary_claim_source_valid(value: &serde_json::Value) -> bool {
    let Some(source) = value.as_object() else {
        return false;
    };
    if !json_object_has_exact_keys(
        source,
        &[
            "address",
            "claim_provenance",
            "coin_type",
            "namespace",
            "reverse_name",
            "reverse_node",
        ],
    ) {
        return false;
    }

    let Some(address) = source.get("address").and_then(|value| value.as_str()) else {
        return false;
    };
    let Ok(parsed_address) = address.parse::<Address>() else {
        return false;
    };
    let expected_reverse_name = format!("{}.addr.reverse", address.trim_start_matches("0x"));
    if address != format!("{parsed_address:#x}")
        || source.get("namespace").and_then(|value| value.as_str()) != Some("ens")
        || source.get("coin_type").and_then(|value| value.as_str()) != Some("60")
        || source.get("reverse_name").and_then(|value| value.as_str())
            != Some(expected_reverse_name.as_str())
    {
        return false;
    }

    let Some(reverse_node) = source.get("reverse_node").and_then(|value| value.as_str()) else {
        return false;
    };
    let Ok(parsed_reverse_node) = reverse_node.parse::<B256>() else {
        return false;
    };
    if reverse_node != format!("{parsed_reverse_node:#x}")
        || parsed_reverse_node
            != ens_namehash_label_bytes(&[
                address.trim_start_matches("0x").as_bytes(),
                b"addr",
                b"reverse",
            ])
    {
        return false;
    }

    let Some(provenance) = source
        .get("claim_provenance")
        .and_then(|value| value.as_object())
    else {
        return false;
    };
    json_object_has_exact_keys(
        provenance,
        &[
            "contract_instance_id",
            "contract_role",
            "emitting_address",
            "source_family",
        ],
    ) && provenance
        .get("source_family")
        .and_then(|value| value.as_str())
        == Some("ens_v1_reverse_l1")
        && provenance
            .get("contract_role")
            .and_then(|value| value.as_str())
            == Some("reverse_registrar")
        && optional_primary_claim_source_uuid_valid(provenance.get("contract_instance_id"))
        && optional_primary_claim_source_address_valid(provenance.get("emitting_address"))
}

fn json_object_has_exact_keys(
    object: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> bool {
    object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
}

fn optional_primary_claim_source_uuid_valid(value: Option<&serde_json::Value>) -> bool {
    match value {
        Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(value)) => {
            value.parse::<Uuid>().is_ok_and(|value| !value.is_nil())
        }
        _ => false,
    }
}

fn optional_primary_claim_source_address_valid(value: Option<&serde_json::Value>) -> bool {
    match value {
        Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(value)) => value
            .parse::<Address>()
            .is_ok_and(|address| value == &format!("{address:#x}")),
        _ => false,
    }
}

fn after_state_without_primary_claim_source(
    value: &serde_json::Value,
) -> Option<serde_json::Value> {
    let mut object = value.as_object()?.clone();
    object.remove("primary_claim_source");
    Some(serde_json::Value::Object(object))
}

fn primary_claim_source_text<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str()
}

fn primary_claim_source_required_text<'a>(
    value: &'a serde_json::Value,
    path: &[&str],
) -> Option<&'a str> {
    primary_claim_source_text(value, path).filter(|value| !value.trim().is_empty())
}

fn primary_claim_source_required_text_matches(
    existing: &serde_json::Value,
    incoming: &serde_json::Value,
    path: &[&str],
) -> bool {
    match (
        primary_claim_source_required_text(existing, path),
        primary_claim_source_required_text(incoming, path),
    ) {
        (Some(existing), Some(incoming)) => existing == incoming,
        _ => false,
    }
}

fn primary_claim_source_uuid(value: &serde_json::Value, path: &[&str]) -> Option<Uuid> {
    let uuid: Uuid = primary_claim_source_required_text(value, path)?
        .parse()
        .ok()?;
    if uuid.is_nil() { None } else { Some(uuid) }
}
