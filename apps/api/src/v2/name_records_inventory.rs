use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::RecordInventoryCurrentRow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ResolutionRecordKey;

use super::name_record::string_field;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RecordInventory {
    pub(crate) known_keys: Vec<String>,
    pub(crate) unset_keys: Vec<String>,
    pub(crate) unsupported_keys: Vec<String>,
}

pub(crate) fn default_requested_records(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> Vec<ResolutionRecordKey> {
    let mut records = BTreeMap::new();
    let Some(record_inventory) = record_inventory else {
        return Vec::new();
    };

    for section in [
        record_inventory.selectors.as_array(),
        record_inventory.entries.as_array(),
        record_inventory.explicit_gaps.as_array(),
    ]
    .into_iter()
    .flatten()
    {
        for item in section {
            if let Some(record) = product_record_from_item(item) {
                records.entry(record.record_key.clone()).or_insert(record);
            }
        }
    }

    records.into_values().collect()
}

pub(crate) fn validate_product_record(record: ResolutionRecordKey) -> Option<ResolutionRecordKey> {
    match (
        record.record_family.as_str(),
        record.selector_key.as_deref(),
    ) {
        ("addr", Some(_)) | ("text", Some(_)) => Some(record),
        ("avatar", None) | ("contenthash", None) => Some(record),
        _ => None,
    }
}

pub(super) fn inventory_summary(
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: Option<&[ResolutionRecordKey]>,
) -> RecordInventory {
    let Some(record_inventory) = record_inventory else {
        return RecordInventory::default();
    };

    let unset_keys = keys_from_sections(&[&record_inventory.explicit_gaps]);
    let mut unsupported_keys = record_inventory
        .entries
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| string_field(entry.get("status")).as_deref() == Some("unsupported"))
        .filter_map(product_record_from_item)
        .map(|record| record.record_key)
        .collect::<BTreeSet<_>>();

    if let Some(records) = requested_records {
        for record in records {
            if unsupported_family_reason(record_inventory, &record.record_family).is_some() {
                unsupported_keys.insert(record.record_key.clone());
            }
        }
    }
    let known_keys = keys_from_sections(&[&record_inventory.selectors, &record_inventory.entries])
        .into_iter()
        // Route-local inventory partitions unsupported-status entries into unsupported_keys only.
        .filter(|key| !unsupported_keys.contains(key))
        .collect();

    RecordInventory {
        known_keys,
        unset_keys,
        unsupported_keys: unsupported_keys.into_iter().collect(),
    }
}

pub(super) fn inventory_item_for_record<'a>(
    section: &'a Value,
    record: &ResolutionRecordKey,
) -> Option<&'a Value> {
    section.as_array().into_iter().flatten().find(|item| {
        product_record_from_item(item)
            .is_some_and(|candidate| candidate.record_key == record.record_key)
    })
}

pub(super) fn product_record_from_item(item: &Value) -> Option<ResolutionRecordKey> {
    let record_key = string_field(item.get("record_key"))?;
    validate_product_record(crate::parse_resolution_record_key(&record_key)?)
}

pub(super) fn unsupported_family_reason(
    record_inventory: &RecordInventoryCurrentRow,
    record_family: &str,
) -> Option<String> {
    record_inventory
        .unsupported_families
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|family| {
            (string_field(family.get("record_family")).as_deref() == Some(record_family))
                .then(|| string_field(family.get("unsupported_reason")))
                .flatten()
        })
}

fn keys_from_sections(sections: &[&Value]) -> Vec<String> {
    sections
        .iter()
        .flat_map(|section| section.as_array().into_iter().flatten())
        .filter_map(product_record_from_item)
        .map(|record| record.record_key)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
