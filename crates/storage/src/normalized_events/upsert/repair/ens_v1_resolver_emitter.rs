use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_resolver_emitter_raw_fact_refs(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_raw_fact_refs = Vec::new();
    let mut new_raw_fact_refs = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_resolver_emitter_raw_fact_ref_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_raw_fact_refs.push(serialize_jsonb_value(
            &existing.raw_fact_ref,
            "failed to serialize existing ENSv1 resolver raw_fact_ref",
        )?);
        new_raw_fact_refs.push(serialize_jsonb_value(
            &event.raw_fact_ref,
            "failed to serialize ENSv1 resolver raw_fact_ref with emitter provenance",
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
                old_raw_fact_ref,
                new_raw_fact_ref
            )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                raw_fact_ref = input.new_raw_fact_ref::JSONB,
                canonicality_state = event.canonicality_state,
                observed_at = now()
            FROM input
            WHERE event.event_identity = input.event_identity
              AND event.raw_fact_ref IS NOT DISTINCT FROM input.old_raw_fact_ref::JSONB
            RETURNING event.event_identity
        )
        SELECT event_identity
        FROM updated
        "#,
    )
    .bind(&event_identities)
    .bind(&old_raw_fact_refs)
    .bind(&new_raw_fact_refs)
    .fetch_all(&mut **executor)
    .await
    .context("failed to retain ENSv1 resolver emitter normalized-event provenance")?
    .into_iter()
    .collect::<HashSet<_>>();

    ensure!(
        repaired.len() == event_identities.len(),
        "ENSv1 resolver emitter provenance repair updated {} of {} eligible events",
        repaired.len(),
        event_identities.len()
    );
    Ok(repaired)
}

pub(crate) fn ens_v1_resolver_emitter_raw_fact_ref_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields != ["raw_fact_ref"]
        || !is_resolver_local_event(existing)
        || !is_resolver_local_event(incoming)
    {
        return false;
    }

    let Some(emitting_address) = incoming
        .raw_fact_ref
        .get("emitting_address")
        .and_then(Value::as_str)
    else {
        return false;
    };
    if !is_lowercase_evm_address(emitting_address)
        || existing.raw_fact_ref.get("emitting_address").is_some()
    {
        return false;
    }

    let mut without_emitter = incoming.raw_fact_ref.clone();
    let Some(raw_fact) = without_emitter.as_object_mut() else {
        return false;
    };
    raw_fact.remove("emitting_address");
    without_emitter == existing.raw_fact_ref
}

fn is_resolver_local_event(event: &NormalizedEvent) -> bool {
    event.derivation_kind == "ens_v1_unwrapped_authority"
        && matches!(
            event.event_kind.as_str(),
            "RecordChanged" | "RecordVersionChanged"
        )
        && matches!(
            (
                event.namespace.as_str(),
                event.chain_id.as_deref(),
                event.source_family.as_str(),
            ),
            ("ens", Some("ethereum-mainnet"), "ens_v1_resolver_l1")
                | ("basenames", Some("base-mainnet"), "basenames_base_resolver")
        )
}

fn is_lowercase_evm_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value == value.to_ascii_lowercase()
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}
