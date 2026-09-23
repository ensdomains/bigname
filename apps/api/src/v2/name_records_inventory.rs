use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::RecordInventoryCurrentRow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::name_record::string_field;
use super::support::{ResolutionRecordKey, parse_resolution_record_key, serving_record_inventory};

mod abi;
#[cfg(test)]
pub(crate) use abi::abi_content_types_test_hooks;
pub(crate) use abi::{
    abi_input_for_identity_row, fill_records_route_abi_content_types, load_abi_content_types,
};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RecordInventory {
    pub(crate) known_keys: Vec<String>,
    pub(crate) unset_keys: Vec<String>,
    pub(crate) unsupported_keys: Vec<String>,
    /// Observed ABI content types, or `null` with `abi_unsupported_reason`; filled by
    /// [`RecordInventory::set_abi_content_types`] once per served container.
    pub(crate) abi_content_types: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) abi_unsupported_reason: Option<String>,
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

/// The sections of an inventory row the summary reads, so the lookup route's identity-facade
/// row and the records route's full row produce one container.
pub(crate) struct InventorySections<'a> {
    pub(crate) authoritative: bool,
    pub(crate) selectors: &'a Value,
    pub(crate) entries: &'a Value,
    pub(crate) explicit_gaps: &'a Value,
    pub(crate) unsupported_families: &'a Value,
}

impl<'a> From<&'a RecordInventoryCurrentRow> for InventorySections<'a> {
    fn from(row: &'a RecordInventoryCurrentRow) -> Self {
        Self {
            authoritative: serving_record_inventory(Some(row)).is_some(),
            selectors: &row.selectors,
            entries: &row.entries,
            explicit_gaps: &row.explicit_gaps,
            unsupported_families: &row.unsupported_families,
        }
    }
}

impl<'a> From<&'a bigname_storage::IdentityRecordInventoryRow> for InventorySections<'a> {
    fn from(row: &'a bigname_storage::IdentityRecordInventoryRow) -> Self {
        // The records route derives coverage from support_status the same way
        // (crates/storage/src/record_inventory/snapshot_reads.rs) and serves no explicit gaps.
        Self {
            authoritative: row.support_status == "supported",
            selectors: &row.selectors,
            entries: &row.entries,
            explicit_gaps: &NO_GAPS,
            unsupported_families: &row.unsupported_families,
        }
    }
}

static NO_GAPS: Value = Value::Array(Vec::new());

pub(super) fn inventory_summary(
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: Option<&[ResolutionRecordKey]>,
) -> RecordInventory {
    inventory_summary_of(
        record_inventory.map(InventorySections::from),
        requested_records,
    )
}

pub(crate) fn inventory_summary_of(
    record_inventory: Option<InventorySections<'_>>,
    requested_records: Option<&[ResolutionRecordKey]>,
) -> RecordInventory {
    let Some(record_inventory) = record_inventory else {
        return RecordInventory::default();
    };

    if !record_inventory.authoritative {
        // An unsupported row can assert neither presence nor absence, so every product key it
        // knows about, and every requested key, is unsupported (docs/api-v2-routes.md).
        let mut unsupported_keys = keys_from_sections(&[
            record_inventory.selectors,
            record_inventory.entries,
            record_inventory.explicit_gaps,
        ])
        .into_iter()
        .collect::<BTreeSet<_>>();
        unsupported_keys.extend(
            requested_records
                .into_iter()
                .flatten()
                .map(|record| record.record_key.clone()),
        );
        return RecordInventory {
            unsupported_keys: unsupported_keys.into_iter().collect(),
            ..RecordInventory::default()
        };
    }

    let unset_keys = keys_from_sections(&[record_inventory.explicit_gaps]);
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
            if family_unsupported_reason(
                record_inventory.unsupported_families,
                &record.record_family,
            )
            .is_some()
            {
                unsupported_keys.insert(record.record_key.clone());
            }
        }
    }
    let known_keys = keys_from_sections(&[record_inventory.selectors, record_inventory.entries])
        .into_iter()
        // Route-local inventory partitions unsupported-status entries into unsupported_keys only.
        .filter(|key| !unsupported_keys.contains(key))
        .collect();

    RecordInventory {
        known_keys,
        unset_keys,
        unsupported_keys: unsupported_keys.into_iter().collect(),
        ..RecordInventory::default()
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
    validate_product_record(parse_resolution_record_key(&record_key)?)
}

pub(super) fn unsupported_family_reason(
    record_inventory: &RecordInventoryCurrentRow,
    record_family: &str,
) -> Option<String> {
    family_unsupported_reason(&record_inventory.unsupported_families, record_family)
}

fn family_unsupported_reason(unsupported_families: &Value, record_family: &str) -> Option<String> {
    unsupported_families
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
