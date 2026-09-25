//! Field-wise comparison of what today's readers serve with what the family readers return for the
//! same key. Excluded by rule: the publication target (`target_block_number`, `target_block_hash`)
//! that the served rows stamp and the family rows do not carry, `last_recomputed_at`, and
//! `manifest_version`, which the family rows do not keep. Today's hydrated values are replaced by
//! the baseline they overlay, because the families do not hold hydration results yet.
use serde_json::{Map, Value};

use super::inventory::FamilyRecordInventory;
use crate::{AddressRecordCurrentEntry, PrimaryNameCurrentSnapshot, RecordInventoryCurrentRow};

/// One field whose values differ.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Difference {
    pub field: String,
    pub today: Value,
    pub family: Value,
}

fn push(differences: &mut Vec<Difference>, field: &str, today: Value, family: Value) {
    if today != family {
        differences.push(Difference {
            field: field.to_owned(),
            today,
            family,
        });
    }
}

fn without(value: &Value, keys: &[&str]) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(key, _)| !keys.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Map<_, _>>(),
        ),
        other => other.clone(),
    }
}

const TARGET: [&str; 2] = ["target_block_number", "target_block_hash"];
const HYDRATION: &str = "canonical_head_multicall_hydration";

/// Today's entries with each hydrated entry read as the baseline it overlays.
fn baseline_entries(entries: &Value) -> Value {
    match entries {
        Value::Array(entries) => Value::Array(
            entries
                .iter()
                .map(|entry| {
                    entry
                        .get(HYDRATION)
                        .and_then(|hydration| hydration.get("baseline"))
                        .cloned()
                        .unwrap_or_else(|| entry.clone())
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

/// The record inventory rows of one resource.
pub fn compare_record_inventory(
    today: Option<&RecordInventoryCurrentRow>,
    family: Option<&RecordInventoryCurrentRow>,
) -> Vec<Difference> {
    let mut differences = Vec::new();
    let (today, family) = match (today, family) {
        (None, None) => return differences,
        (Some(today), Some(family)) => (today, family),
        (today, family) => {
            push(
                &mut differences,
                "row",
                Value::Bool(today.is_some()),
                Value::Bool(family.is_some()),
            );
            return differences;
        }
    };
    let fields: [(&str, Value, Value); 11] = [
        (
            "record_version_boundary",
            today.record_version_boundary.clone(),
            family.record_version_boundary.clone(),
        ),
        (
            "enumeration_basis",
            today.enumeration_basis.clone(),
            family.enumeration_basis.clone(),
        ),
        (
            "selectors",
            today.selectors.clone(),
            family.selectors.clone(),
        ),
        (
            "explicit_gaps",
            today.explicit_gaps.clone(),
            family.explicit_gaps.clone(),
        ),
        (
            "unsupported_families",
            today.unsupported_families.clone(),
            family.unsupported_families.clone(),
        ),
        (
            "last_change",
            today.last_change.clone().unwrap_or(Value::Null),
            family.last_change.clone().unwrap_or(Value::Null),
        ),
        (
            "entries",
            baseline_entries(&today.entries),
            family.entries.clone(),
        ),
        (
            "provenance",
            without(&today.provenance, &["attributed_event_ids"]),
            without(&family.provenance, &["attributed_event_ids"]),
        ),
        (
            "provenance.attributed_event_ids",
            today.provenance["attributed_event_ids"].clone(),
            family.provenance["attributed_event_ids"].clone(),
        ),
        ("coverage", today.coverage.clone(), family.coverage.clone()),
        (
            "chain_positions",
            without(&today.chain_positions, &TARGET),
            without(&family.chain_positions, &TARGET),
        ),
    ];
    for (field, today, family) in fields {
        push(&mut differences, field, today, family);
    }
    push(
        &mut differences,
        "canonicality_summary",
        without(&today.canonicality_summary, &TARGET),
        without(&family.canonicality_summary, &TARGET),
    );
    differences
}

/// Shape checks for every coin-60 pair a family row serves: the value event is the
/// `AddressChanged` half at log n and its sibling the `AddrChanged` half at log n + 1 of the same
/// transaction, and the row's `record_event_ids` reproduce today's provenance, which lists the
/// value event and not the sibling. The design's provenance names both events (item 6); that is
/// not served yet, so the row keeps today's shape and [`super::CompatibilityPair`] carries the
/// sibling for step 7. Returns what does not hold.
pub fn check_compatibility_pairs(inventory: &FamilyRecordInventory) -> Vec<Difference> {
    let mut differences = Vec::new();
    let ids = &inventory.row.provenance["record_event_ids"];
    let listed = |id: Option<i64>| {
        id.is_some_and(|id| {
            ids.as_array()
                .is_some_and(|ids| ids.contains(&Value::from(id)))
        })
    };
    for pair in &inventory.compatibility_pairs {
        let (value, sibling) = (&pair.value_position, &pair.sibling_position);
        let adjacent = value.block_number == sibling.block_number
            && value.transaction_index == sibling.transaction_index
            && value
                .log_index
                .zip(sibling.log_index)
                .is_some_and(|(n, m)| n + 1 == m);
        push(
            &mut differences,
            &format!("pair {} adjacency", pair.record_key),
            Value::Bool(true),
            Value::Bool(adjacent),
        );
        push(
            &mut differences,
            &format!("pair {} provenance as today", pair.record_key),
            Value::Bool(true),
            Value::Bool(listed(pair.value_event_id) && !listed(pair.sibling_event_id)),
        );
    }
    differences
}

/// One primary-name claim tuple. Today's hydrated claim is read as its baseline.
pub fn compare_primary_name(
    today: Option<&PrimaryNameCurrentSnapshot>,
    family: Option<&PrimaryNameCurrentSnapshot>,
) -> Vec<Difference> {
    let mut differences = Vec::new();
    let view = |snapshot: Option<&PrimaryNameCurrentSnapshot>, today: bool| {
        snapshot.map_or(Value::Null, |snapshot| {
            let row = &snapshot.row;
            let hydration = row
                .claim_provenance
                .get(HYDRATION)
                .and_then(|hydration| hydration.get("baseline"))
                .filter(|_| today);
            let (status, raw, normalized) = match hydration {
                Some(baseline) => (
                    baseline["claim_status"].clone(),
                    baseline["raw_claim_name"].clone(),
                    baseline["claim_name_is_normalized"].clone(),
                ),
                None => (
                    Value::from(row.claim_status.as_str()),
                    Value::from(row.raw_claim_name.clone()),
                    Value::from(snapshot.claim_name_is_normalized),
                ),
            };
            serde_json::json!({
                "address": row.address,
                "namespace": row.namespace,
                "coin_type": row.coin_type,
                "claim_status": status,
                "raw_claim_name": raw,
                "claim_name_is_normalized": normalized,
                "claim_provenance": without(&row.claim_provenance,
                    &[TARGET[0], TARGET[1], HYDRATION]),
            })
        })
    };
    let (today, family) = (view(today, true), view(family, false));
    for field in [
        "address",
        "namespace",
        "coin_type",
        "claim_status",
        "raw_claim_name",
        "claim_name_is_normalized",
        "claim_provenance",
    ] {
        push(
            &mut differences,
            field,
            today.get(field).cloned().unwrap_or(Value::Null),
            family.get(field).cloned().unwrap_or(Value::Null),
        );
    }
    differences
}

fn address_entry(entry: &AddressRecordCurrentEntry) -> Value {
    serde_json::json!({
        "address": entry.address,
        "logical_name_id": entry.logical_name_id,
        "namespace": entry.namespace,
        "canonical_display_name": entry.canonical_display_name,
        "normalized_name": entry.normalized_name,
        "namehash": entry.namehash,
        "surface_binding_id": entry.surface_binding_id.map(|id| id.to_string()),
        "resource_id": entry.resource_id.map(|id| id.to_string()),
        "record_resource_id": entry.record_resource_id.to_string(),
        "binding_kind": entry.binding_kind.map(|kind| format!("{kind:?}")),
        "coin_type": entry.coin_type,
        "record_key": entry.record_key,
        "provenance": entry.provenance,
        "coverage": entry.coverage,
        "chain_positions": without(&entry.chain_positions, &TARGET),
        "canonicality_summary": without(&entry.canonicality_summary, &TARGET),
    })
}

/// One page of names resolving to an address, entry by entry, and its continuation.
pub fn compare_address_records(
    today: &[AddressRecordCurrentEntry],
    family: &[AddressRecordCurrentEntry],
) -> Vec<Difference> {
    let mut differences = Vec::new();
    push(
        &mut differences,
        "entries",
        Value::Array(today.iter().map(address_entry).collect()),
        Value::Array(family.iter().map(address_entry).collect()),
    );
    differences
}
