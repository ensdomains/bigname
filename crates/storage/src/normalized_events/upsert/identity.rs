use std::collections::HashMap;

use anyhow::{Result, bail};
use sqlx::Postgres;

use super::{
    batch::load_normalized_events_by_identities, repair::normalized_event_identity_repair_allowed,
};
use crate::normalized_events::types::NormalizedEvent;

pub(super) async fn validate_existing_normalized_events(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<HashMap<String, NormalizedEvent>> {
    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    let existing_events = load_normalized_events_by_identities(executor, &event_identities).await?;
    let existing_by_identity = existing_events
        .into_iter()
        .map(|event| (event.event_identity.clone(), event))
        .collect::<HashMap<_, _>>();

    for event in events {
        if let Some(existing) = existing_by_identity.get(&event.event_identity) {
            ensure_normalized_event_identity_matches(existing, event)?;
        }
    }

    Ok(existing_by_identity)
}

pub(super) fn normalized_event_snapshots_after_upsert(
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Vec<NormalizedEvent> {
    events
        .iter()
        .map(|event| {
            let mut snapshot = event.clone();
            if let Some(existing) = existing_by_identity.get(&event.event_identity) {
                snapshot.canonicality_state = existing
                    .canonicality_state
                    .merge_observation(event.canonicality_state);
            }
            snapshot
        })
        .collect()
}

fn ensure_normalized_event_identity_matches(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> Result<()> {
    let differing_fields = normalized_event_identity_differences(existing, incoming);
    if !differing_fields.is_empty() {
        if normalized_event_identity_repair_allowed(existing, incoming, &differing_fields) {
            return Ok(());
        }

        bail!(
            "normalized event identity mismatch for event {} (differing_fields={}, existing={}, incoming={})",
            existing.event_identity,
            differing_fields.join(","),
            normalized_event_identity_summary(existing),
            normalized_event_identity_summary(incoming)
        );
    }

    Ok(())
}

pub(in crate::normalized_events) fn normalized_event_identity_differences(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if existing.namespace != incoming.namespace {
        fields.push("namespace");
    }
    if existing.logical_name_id != incoming.logical_name_id {
        fields.push("logical_name_id");
    }
    if existing.resource_id != incoming.resource_id {
        fields.push("resource_id");
    }
    if existing.event_kind != incoming.event_kind {
        fields.push("event_kind");
    }
    if existing.source_family != incoming.source_family {
        fields.push("source_family");
    }
    if existing.manifest_version != incoming.manifest_version {
        fields.push("manifest_version");
    }
    if existing.source_manifest_id != incoming.source_manifest_id {
        fields.push("source_manifest_id");
    }
    if existing.chain_id != incoming.chain_id {
        fields.push("chain_id");
    }
    if existing.block_number != incoming.block_number {
        fields.push("block_number");
    }
    if existing.block_hash != incoming.block_hash {
        fields.push("block_hash");
    }
    if existing.transaction_hash != incoming.transaction_hash {
        fields.push("transaction_hash");
    }
    if existing.log_index != incoming.log_index {
        fields.push("log_index");
    }
    if existing.raw_fact_ref != incoming.raw_fact_ref {
        fields.push("raw_fact_ref");
    }
    if existing.derivation_kind != incoming.derivation_kind {
        fields.push("derivation_kind");
    }
    if existing.before_state != incoming.before_state {
        fields.push("before_state");
    }
    if existing.after_state != incoming.after_state {
        fields.push("after_state");
    }
    fields
}

pub(in crate::normalized_events) fn normalized_event_identity_summary(
    event: &NormalizedEvent,
) -> String {
    format!(
        "namespace={:?} logical_name_id={:?} resource_id={:?} event_kind={:?} source_family={:?} manifest_version={:?} source_manifest_id={:?} chain_id={:?} block_number={:?} block_hash={:?} transaction_hash={:?} log_index={:?} raw_fact_ref={} derivation_kind={:?} before_state={} after_state={}",
        event.namespace,
        event.logical_name_id,
        event.resource_id,
        event.event_kind,
        event.source_family,
        event.manifest_version,
        event.source_manifest_id,
        event.chain_id,
        event.block_number,
        event.block_hash,
        event.transaction_hash,
        event.log_index,
        event.raw_fact_ref,
        event.derivation_kind,
        event.before_state,
        event.after_state
    )
}
