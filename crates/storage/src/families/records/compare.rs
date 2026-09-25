//! Field-wise comparison of what today's readers serve with what the family readers return for the
//! same key. Excluded by rule: the publication target (`target_block_number`, `target_block_hash`)
//! that the served rows stamp and the family rows do not carry, `last_recomputed_at`, and
//! `manifest_version`, which the family rows do not keep. Today's hydrated values are replaced by
//! the baseline they overlay, because the families do not hold hydration results yet.
//!
//! A difference names the exact field that differs: objects are compared key by key, and the
//! record lists (entries, selectors, unsupported families, the entries of an address's names)
//! element by element under the element's own key, with a separate `.order` difference when the
//! same elements come in another order. So an accepted difference can be checked field by field,
//! and a second difference on the same result shows up on its own. A missing field is `None`,
//! apart from any stored value, so no stored value compares equal to a missing one.
use std::collections::BTreeSet;

use serde_json::{Map, Value, json};

use super::{
    FamilyPosition,
    inventory::{CompatibilityPair, FamilyRecordInventory},
};
use crate::{AddressRecordCurrentEntry, PrimaryNameCurrentSnapshot, RecordInventoryCurrentRow};

/// One field whose values differ; `None` is a side where the field or element is missing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Difference {
    pub field: String,
    pub today: Option<Value>,
    pub family: Option<Value>,
}

fn push_either(
    differences: &mut Vec<Difference>,
    field: &str,
    today: Option<Value>,
    family: Option<Value>,
) {
    if today != family {
        differences.push(Difference {
            field: field.to_owned(),
            today,
            family,
        });
    }
}

fn push(differences: &mut Vec<Difference>, field: &str, today: Value, family: Value) {
    push_either(differences, field, Some(today), Some(family));
}

fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_owned()
    } else {
        format!("{path}.{key}")
    }
}

/// A list of objects that each carry a text `key_field`, as a map by that key and the key order.
fn keyed(value: &Value, key_field: &str) -> Option<(Map<String, Value>, Vec<String>)> {
    let items = value.as_array()?;
    let mut map = Map::new();
    let mut order = Vec::new();
    for item in items {
        let key = item.get(key_field)?.as_str()?.to_owned();
        if map.insert(key.clone(), item.clone()).is_some() {
            return None;
        }
        order.push(key);
    }
    Some((map, order))
}

const LIST_KEYS: [&str; 2] = ["record_key", "record_family"];

/// Push one difference per differing field under `path`.
fn diff(differences: &mut Vec<Difference>, path: &str, today: &Value, family: &Value) {
    if today == family {
        return;
    }
    match (today, family) {
        (Value::Object(today), Value::Object(family)) => {
            let keys: BTreeSet<&String> = today.keys().chain(family.keys()).collect();
            for key in keys {
                let path = join(path, key);
                match (today.get(key), family.get(key)) {
                    (Some(today), Some(family)) => diff(differences, &path, today, family),
                    (today, family) => {
                        push_either(differences, &path, today.cloned(), family.cloned());
                    }
                }
            }
        }
        (Value::Array(_), Value::Array(_)) => {
            let lists = LIST_KEYS.iter().find_map(|field| {
                Some((keyed(today, field)?, keyed(family, field)?))
                    .filter(|((today, _), (family, _))| !today.is_empty() || !family.is_empty())
            });
            match lists {
                Some(((today, today_order), (family, family_order))) => {
                    diff_keyed(
                        differences,
                        path,
                        &today,
                        &today_order,
                        &family,
                        &family_order,
                    );
                }
                None => push(differences, path, today.clone(), family.clone()),
            }
        }
        _ => push(differences, path, today.clone(), family.clone()),
    }
}

/// Element by element under each element's key, then the order of the common elements.
fn diff_keyed(
    differences: &mut Vec<Difference>,
    path: &str,
    today: &Map<String, Value>,
    today_order: &[String],
    family: &Map<String, Value>,
    family_order: &[String],
) {
    let keys: BTreeSet<&String> = today.keys().chain(family.keys()).collect();
    for key in keys {
        let element = format!("{path}[{key}]");
        match (today.get(key), family.get(key)) {
            (Some(today), Some(family)) => diff(differences, &element, today, family),
            (today, family) => push_either(differences, &element, today.cloned(), family.cloned()),
        }
    }
    let common = |order: &[String], other: &Map<String, Value>| -> Vec<Value> {
        order
            .iter()
            .filter(|key| other.contains_key(*key))
            .map(|key| json!(key))
            .collect()
    };
    push(
        differences,
        &format!("{path}.order"),
        Value::Array(common(today_order, family)),
        Value::Array(common(family_order, today)),
    );
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

fn inventory_view(row: &RecordInventoryCurrentRow, today: bool) -> Value {
    json!({
        "record_version_boundary": row.record_version_boundary,
        "enumeration_basis": row.enumeration_basis,
        "selectors": row.selectors,
        "explicit_gaps": row.explicit_gaps,
        "unsupported_families": row.unsupported_families,
        "last_change": row.last_change,
        "entries": if today { baseline_entries(&row.entries) } else { row.entries.clone() },
        "provenance": row.provenance,
        "coverage": row.coverage,
        "chain_positions": without(&row.chain_positions, &TARGET),
        "canonicality_summary": without(&row.canonicality_summary, &TARGET),
    })
}

/// The record inventory rows of one resource.
pub fn compare_record_inventory(
    today: Option<&RecordInventoryCurrentRow>,
    family: Option<&RecordInventoryCurrentRow>,
) -> Vec<Difference> {
    let mut differences = Vec::new();
    match (today, family) {
        (None, None) => {}
        (Some(today), Some(family)) => diff(
            &mut differences,
            "",
            &inventory_view(today, true),
            &inventory_view(family, false),
        ),
        (today, family) => push(
            &mut differences,
            "row",
            Value::Bool(today.is_some()),
            Value::Bool(family.is_some()),
        ),
    }
    differences
}

fn pair_view(pair: &CompatibilityPair) -> Value {
    let position = |position: &FamilyPosition| {
        json!({
            "block_number": position.block_number,
            "transaction_index": position.transaction_index,
            "log_index": position.log_index,
            "event_identity": position.event_identity,
        })
    };
    json!({
        "record_key": pair.record_key,
        "value_event_id": pair.value_event_id,
        "value_position": position(&pair.value_position),
        "sibling_event_id": pair.sibling_event_id,
        "sibling_position": position(&pair.sibling_position),
    })
}

/// The coin-60 pairs of a family row against the pairs today's row serves, found independently
/// of the family reader (`expected`): record key, both event ids and both positions, pair by pair
/// under the record key. A duplicate record key on either side is its own difference. The row's
/// provenance is compared with today's separately and lists only the value event, as today's
/// does; the design's provenance names both events, which step 7 serves from the sibling the pair
/// carries.
pub fn check_compatibility_pairs(
    inventory: &FamilyRecordInventory,
    expected: &[CompatibilityPair],
) -> Vec<Difference> {
    let mut differences = Vec::new();
    let duplicates = |pairs: &[CompatibilityPair]| {
        let mut seen = BTreeSet::new();
        pairs
            .iter()
            .filter(|pair| !seen.insert(pair.record_key.clone()))
            .map(|pair| json!(pair.record_key))
            .collect::<Vec<_>>()
    };
    push(
        &mut differences,
        "compatibility_pairs.duplicates",
        Value::Array(duplicates(expected)),
        Value::Array(duplicates(&inventory.compatibility_pairs)),
    );
    let view = |pairs: &[CompatibilityPair]| Value::Array(pairs.iter().map(pair_view).collect());
    diff(
        &mut differences,
        "compatibility_pairs",
        &view(expected),
        &view(&inventory.compatibility_pairs),
    );
    differences
}

/// One primary-name claim tuple. Today's hydrated claim is read as its baseline.
pub fn compare_primary_name(
    today: Option<&PrimaryNameCurrentSnapshot>,
    family: Option<&PrimaryNameCurrentSnapshot>,
) -> Vec<Difference> {
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
            json!({
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
    let mut differences = Vec::new();
    match (view(today, true), view(family, false)) {
        (Value::Null, Value::Null) => {}
        (today @ Value::Object(_), family @ Value::Object(_)) => {
            diff(&mut differences, "", &today, &family);
        }
        (today, family) => push(
            &mut differences,
            "row",
            Value::Bool(!today.is_null()),
            Value::Bool(!family.is_null()),
        ),
    }
    differences
}

fn address_entry(entry: &AddressRecordCurrentEntry) -> Value {
    json!({
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

/// The complete sequence of names resolving to an address, every page of both readers, entry by
/// entry under `<logical name id>|<record resource id>|<surface binding id or ->`, then the order
/// of the common entries. The surface binding keeps the entries a surface dedupe lists for one
/// name and record resource apart, so they are never paired by position. A missing entry is one
/// difference and every later entry is still compared.
pub fn compare_address_records(
    today: &[AddressRecordCurrentEntry],
    family: &[AddressRecordCurrentEntry],
) -> Vec<Difference> {
    // A repeated key is compared as its own occurrence (`key#2` and on), never overwritten.
    let sequence = |entries: &[AddressRecordCurrentEntry]| {
        let mut map = Map::new();
        let mut order = Vec::new();
        for entry in entries {
            let binding = entry
                .surface_binding_id
                .map_or_else(|| "-".to_owned(), |id| id.to_string());
            let base = format!(
                "{}|{}|{binding}",
                entry.logical_name_id, entry.record_resource_id
            );
            let mut key = base.clone();
            let mut occurrence = 1;
            while map.contains_key(&key) {
                occurrence += 1;
                key = format!("{base}#{occurrence}");
            }
            order.push(key.clone());
            map.insert(key, address_entry(entry));
        }
        (map, order)
    };
    let ((today, today_order), (family, family_order)) = (sequence(today), sequence(family));
    let mut differences = Vec::new();
    push(
        &mut differences,
        "entries.count",
        json!(today_order.len()),
        json!(family_order.len()),
    );
    diff_keyed(
        &mut differences,
        "entries",
        &today,
        &today_order,
        &family,
        &family_order,
    );
    differences
}

/// [`compare_address_records`] and, whatever the entries show, the continuation of every page.
pub fn compare_address_results(
    today: &[AddressRecordCurrentEntry],
    today_pages: &[String],
    family: &[AddressRecordCurrentEntry],
    family_pages: &[String],
) -> Vec<Difference> {
    let mut differences = compare_address_records(today, family);
    push(
        &mut differences,
        "pages",
        json!(today_pages),
        json!(family_pages),
    );
    differences
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyed_lists_name_the_element_and_field_that_differ() {
        let mut differences = Vec::new();
        diff(
            &mut differences,
            "",
            &json!({"entries": [{"record_key": "a", "value": 1}, {"record_key": "b", "value": 2}],
                    "provenance": {"ids": [1, 2]}}),
            &json!({"entries": [{"record_key": "b", "value": 3}],
                    "provenance": {"ids": [1, 2], "extra": null}}),
        );
        assert_eq!(
            differences,
            [
                Difference {
                    field: "entries[a]".into(),
                    today: Some(json!({"record_key": "a", "value": 1})),
                    family: None,
                },
                Difference {
                    field: "entries[b].value".into(),
                    today: Some(json!(2)),
                    family: Some(json!(3)),
                },
                Difference {
                    field: "provenance.extra".into(),
                    today: None,
                    family: Some(Value::Null),
                },
            ]
        );
    }

    #[test]
    fn a_stored_absent_marker_is_not_a_missing_field() {
        let mut differences = Vec::new();
        diff(
            &mut differences,
            "",
            &json!({"chain_positions": {"block_number": 1}}),
            &json!({"chain_positions": {"block_number": 1, "mutated": "<absent>"}}),
        );
        assert_eq!(
            differences,
            [Difference {
                field: "chain_positions.mutated".into(),
                today: None,
                family: Some(json!("<absent>")),
            }]
        );
    }

    fn address(name: &str, value: &str) -> AddressRecordCurrentEntry {
        AddressRecordCurrentEntry {
            address: "0x01".into(),
            logical_name_id: name.into(),
            namespace: "ens".into(),
            canonical_display_name: name.into(),
            normalized_name: name.into(),
            namehash: name.into(),
            surface_binding_id: None,
            resource_id: None,
            record_resource_id: uuid::Uuid::nil(),
            binding_kind: None,
            coin_type: "60".into(),
            record_key: "addr:60".into(),
            provenance: json!({"value": value}),
            coverage: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn a_page_difference_shows_beside_an_entry_difference() {
        let differences = compare_address_results(
            &[address("a", "1")],
            &["cursor-a".into()],
            &[address("a", "2")],
            &["cursor-b".into()],
        );
        let fields: Vec<&str> = differences.iter().map(|d| d.field.as_str()).collect();
        let key = format!("a|{}|-", uuid::Uuid::nil());
        assert_eq!(
            fields,
            [format!("entries[{key}].provenance.value").as_str(), "pages"]
        );
    }

    #[test]
    fn a_repeated_address_key_is_compared_per_occurrence() {
        let differences = compare_address_records(
            &[address("a", "1"), address("a", "2")],
            &[address("a", "1"), address("a", "3")],
        );
        let key = format!("a|{}|-", uuid::Uuid::nil());
        assert_eq!(
            differences,
            [Difference {
                field: format!("entries[{key}#2].provenance.value"),
                today: Some(json!("2")),
                family: Some(json!("3")),
            }]
        );
    }

    #[test]
    fn entries_of_one_name_under_two_bindings_are_not_paired_by_position() {
        let bound = |binding: u128, value: &str| AddressRecordCurrentEntry {
            surface_binding_id: Some(uuid::Uuid::from_u128(binding)),
            ..address("a", value)
        };
        let differences = compare_address_records(
            &[bound(1, "1"), bound(2, "2")],
            &[bound(2, "2"), bound(1, "1")],
        );
        let fields: Vec<&str> = differences.iter().map(|d| d.field.as_str()).collect();
        assert_eq!(fields, ["entries.order"]);
    }

    #[test]
    fn a_reordered_list_reports_the_order() {
        let mut differences = Vec::new();
        diff(
            &mut differences,
            "selectors",
            &json!([{"record_key": "a"}, {"record_key": "b"}]),
            &json!([{"record_key": "b"}, {"record_key": "a"}]),
        );
        assert_eq!(
            differences,
            [Difference {
                field: "selectors.order".into(),
                today: Some(json!(["a", "b"])),
                family: Some(json!(["b", "a"])),
            }]
        );
    }
}
