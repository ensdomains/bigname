use anyhow::{Context, Result, bail};
use bigname_storage::{
    RecordInventoryCurrentRow, SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey,
};
use serde_json::Value;

use crate::json_helpers::{json_field, json_string_field};
use crate::validation::VerifiedQuerySummary;

pub(super) fn ensure_storage_selector_families_supported(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    queries: &[VerifiedQuerySummary],
    request_key: &str,
    context: &str,
) -> Result<()> {
    let record_inventory_row = record_inventory_row.with_context(|| {
        format!(
            "{context} requires record_inventory_current to revalidate supported selectors for request_key {request_key}"
        )
    })?;
    let unsupported_families = record_inventory_row
        .unsupported_families
        .as_array()
        .with_context(|| {
            format!("{context} record_inventory_current.unsupported_families must be a JSON array")
        })?;
    let entries = record_inventory_row.entries.as_array().with_context(|| {
        format!("{context} record_inventory_current.entries must be a JSON array")
    })?;

    for query in queries {
        let (record_family, selector_key) =
            super::selector_family_and_key(&query.record_key, &query.selector);

        if unsupported_families.iter().any(|entry| {
            json_string_field(json_field(entry, "record_family"))
                .is_some_and(|value| value == record_family)
        }) {
            bail!(
                "{context} record family {record_family} is still unsupported in record_inventory_current for request_key {request_key}"
            );
        }

        if entries.iter().any(|entry| {
            json_string_field(json_field(entry, "record_key"))
                .is_some_and(|value| value == query.record_key)
                && json_string_field(json_field(entry, "status"))
                    .is_some_and(|value| value == "unsupported")
                && selector_key_matches_inventory(entry, selector_key.as_deref())
        }) {
            bail!(
                "{context} selector {} is still unsupported in record_inventory_current for request_key {request_key}",
                query.record_key
            );
        }
    }

    Ok(())
}

pub(crate) fn selector_family_and_key(
    record_key: &str,
    selector: &SupportedVerifiedRecordKey,
) -> (String, Option<String>) {
    match selector {
        SupportedVerifiedRecordKey::Addr { coin_type } => {
            ("addr".to_owned(), Some(coin_type.clone()))
        }
        SupportedVerifiedRecordKey::Avatar => ("avatar".to_owned(), None),
        SupportedVerifiedRecordKey::Contenthash => ("contenthash".to_owned(), None),
        SupportedVerifiedRecordKey::Text => (
            "text".to_owned(),
            record_key.strip_prefix("text:").map(str::to_owned),
        ),
    }
}

fn selector_key_matches_inventory(entry: &Value, selector_key: Option<&str>) -> bool {
    match (json_field(entry, "selector_key"), selector_key) {
        (None | Some(Value::Null), None) => true,
        (Some(Value::String(left)), Some(right)) => left == right,
        _ => false,
    }
}
