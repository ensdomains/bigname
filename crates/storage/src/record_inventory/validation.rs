use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::projection_helpers::{require_resource_json_array, require_resource_json_object};

use super::{
    boundary_key::{decode_chain_position, decode_record_version_boundary},
    row_decode::RecordInventoryCurrentRow,
};

pub(super) fn validate_record_inventory_current_row(row: &RecordInventoryCurrentRow) -> Result<()> {
    decode_record_version_boundary(&row.record_version_boundary, Some(row.resource_id))
        .context("record_inventory_current row has invalid record_version_boundary")?;

    if row.manifest_version <= 0 {
        bail!(
            "record_inventory_current row for resource_id {} has non-positive manifest_version {}",
            row.resource_id,
            row.manifest_version
        );
    }

    validate_enumeration_basis(&row.enumeration_basis, row.resource_id)?;
    let cacheable_selector_keys = validate_selector_array(&row.selectors, row.resource_id)?;
    validate_explicit_gap_array(&row.explicit_gaps, row.resource_id)?;
    validate_unsupported_families(&row.unsupported_families, row.resource_id)?;
    validate_last_change(&row.last_change, row.resource_id)?;
    validate_entries(&row.entries, row.resource_id, &cacheable_selector_keys)?;
    require_resource_json_object(
        &row.provenance,
        "provenance",
        "record_inventory_current",
        row.resource_id,
    )?;
    require_resource_json_object(
        &row.coverage,
        "coverage",
        "record_inventory_current",
        row.resource_id,
    )?;
    require_resource_json_object(
        &row.chain_positions,
        "chain_positions",
        "record_inventory_current",
        row.resource_id,
    )?;
    require_resource_json_object(
        &row.canonicality_summary,
        "canonicality_summary",
        "record_inventory_current",
        row.resource_id,
    )?;

    Ok(())
}

fn validate_enumeration_basis(value: &Value, resource_id: Uuid) -> Result<()> {
    let object = require_resource_json_object(
        value,
        "enumeration_basis",
        "record_inventory_current",
        resource_id,
    )?;
    required_bool_field(object, "observed_selectors", "enumeration_basis")?;
    required_bool_field(object, "capability_declared_families", "enumeration_basis")?;
    required_bool_field(object, "globally_enumerable", "enumeration_basis")?;
    Ok(())
}

fn validate_selector_array(value: &Value, resource_id: Uuid) -> Result<BTreeSet<String>> {
    let items =
        require_resource_json_array(value, "selectors", "record_inventory_current", resource_id)?;
    let mut previous_record_key: Option<&str> = None;
    let mut cacheable_record_keys = BTreeSet::new();

    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "record_inventory_current row for resource_id {} selectors[{index}] must be a JSON object",
                resource_id
            )
        })?;
        let record_key = validate_selector_identity(
            object,
            "selectors",
            index,
            resource_id,
            SelectorFieldExpectation::CacheableOnly,
        )?;
        if let Some(previous_record_key) = previous_record_key
            && record_key <= previous_record_key
        {
            bail!(
                "record_inventory_current row for resource_id {} selectors must be sorted by record_key ascending",
                resource_id
            );
        }
        if required_bool_field(object, "cacheable", "selector entry")? {
            cacheable_record_keys.insert(record_key.to_owned());
        }
        previous_record_key = Some(record_key);
    }

    Ok(cacheable_record_keys)
}

fn validate_explicit_gap_array(value: &Value, resource_id: Uuid) -> Result<()> {
    let items = require_resource_json_array(
        value,
        "explicit_gaps",
        "record_inventory_current",
        resource_id,
    )?;
    let mut previous_record_key: Option<&str> = None;

    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "record_inventory_current row for resource_id {} explicit_gaps[{index}] must be a JSON object",
                resource_id
            )
        })?;
        let record_key = validate_selector_identity(
            object,
            "explicit_gaps",
            index,
            resource_id,
            SelectorFieldExpectation::GapReasonOnly,
        )?;
        if let Some(previous_record_key) = previous_record_key
            && record_key <= previous_record_key
        {
            bail!(
                "record_inventory_current row for resource_id {} explicit_gaps must be sorted by record_key ascending",
                resource_id
            );
        }
        previous_record_key = Some(record_key);
    }

    Ok(())
}

fn validate_unsupported_families(value: &Value, resource_id: Uuid) -> Result<()> {
    let items = require_resource_json_array(
        value,
        "unsupported_families",
        "record_inventory_current",
        resource_id,
    )?;
    let mut previous_record_family: Option<&str> = None;

    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "record_inventory_current row for resource_id {} unsupported_families[{index}] must be a JSON object",
                resource_id
            )
        })?;
        let record_family = required_string_field(
            object,
            "record_family",
            "record_inventory_current unsupported_families entry",
        )?;
        required_string_field(
            object,
            "unsupported_reason",
            "record_inventory_current unsupported_families entry",
        )?;
        if let Some(previous_record_family) = previous_record_family
            && record_family <= previous_record_family
        {
            bail!(
                "record_inventory_current row for resource_id {} unsupported_families must be sorted by record_family ascending",
                resource_id
            );
        }
        previous_record_family = Some(record_family);
    }

    Ok(())
}

fn validate_last_change(value: &Option<Value>, resource_id: Uuid) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };

    let object = require_resource_json_object(
        value,
        "last_change",
        "record_inventory_current",
        resource_id,
    )?;
    required_positive_i64_field(object, "normalized_event_id", "last_change")?;
    required_string_field(object, "event_kind", "last_change")?;
    decode_chain_position(
        object
            .get("chain_position")
            .with_context(|| "last_change must include chain_position".to_owned())?,
        "last_change.chain_position",
    )?;
    Ok(())
}

fn validate_entries(
    value: &Value,
    resource_id: Uuid,
    expected_record_keys: &BTreeSet<String>,
) -> Result<()> {
    let items =
        require_resource_json_array(value, "entries", "record_inventory_current", resource_id)?;
    let mut seen_record_keys = BTreeSet::new();

    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "record_inventory_current row for resource_id {} entries[{index}] must be a JSON object",
                resource_id
            )
        })?;
        let record_key = validate_selector_identity(
            object,
            "entries",
            index,
            resource_id,
            SelectorFieldExpectation::StatusDriven,
        )?;
        if !seen_record_keys.insert(record_key.to_owned()) {
            bail!(
                "record_inventory_current row for resource_id {} entries must not duplicate record_key {}",
                resource_id,
                record_key
            );
        }
    }

    let missing_record_keys = expected_record_keys
        .difference(&seen_record_keys)
        .cloned()
        .collect::<Vec<_>>();
    let extra_record_keys = seen_record_keys
        .difference(expected_record_keys)
        .cloned()
        .collect::<Vec<_>>();
    if !missing_record_keys.is_empty() || !extra_record_keys.is_empty() {
        let mut drift = Vec::new();
        if !missing_record_keys.is_empty() {
            drift.push(format!(
                "missing cacheable selectors [{}]",
                missing_record_keys.join(", ")
            ));
        }
        if !extra_record_keys.is_empty() {
            drift.push(format!(
                "extra selectors outside cacheable selector space [{}]",
                extra_record_keys.join(", ")
            ));
        }
        bail!(
            "record_inventory_current row for resource_id {} entries must match the cacheable selectors surfaced by selectors ({})",
            resource_id,
            drift.join("; ")
        );
    }

    Ok(())
}

fn validate_selector_identity<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    index: usize,
    resource_id: Uuid,
    expectation: SelectorFieldExpectation,
) -> Result<&'a str> {
    let record_key = required_string_field(
        object,
        "record_key",
        "record_inventory_current selector entry",
    )?;
    let record_family = required_string_field(
        object,
        "record_family",
        "record_inventory_current selector entry",
    )?;
    let selector_key = optional_string_field(
        object,
        "selector_key",
        "record_inventory_current selector entry",
    )?;
    let expected_record_key = match selector_key {
        Some(selector_key) => format!("{record_family}:{selector_key}"),
        None => record_family.to_owned(),
    };
    if record_key != expected_record_key {
        bail!(
            "record_inventory_current row for resource_id {} {}[{index}] record_key {} must match selector identity {}",
            resource_id,
            field_name,
            record_key,
            expected_record_key
        );
    }

    match expectation {
        SelectorFieldExpectation::CacheableOnly => {
            required_bool_field(object, "cacheable", "selector entry")?;
        }
        SelectorFieldExpectation::GapReasonOnly => {
            required_string_field(object, "gap_reason", "explicit_gap entry")?;
        }
        SelectorFieldExpectation::StatusDriven => {
            let status = required_string_field(object, "status", "record_cache entry")?.to_owned();
            match status.as_str() {
                "success" => {
                    if !object.contains_key("value") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status success must include value",
                            resource_id
                        );
                    }
                    if object.contains_key("unsupported_reason") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status success must not include unsupported_reason",
                            resource_id
                        );
                    }
                }
                "not_found" => {
                    if object.contains_key("value") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status not_found must not include value",
                            resource_id
                        );
                    }
                    if object.contains_key("unsupported_reason") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status not_found must not include unsupported_reason",
                            resource_id
                        );
                    }
                }
                "unsupported" => {
                    if object.contains_key("value") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status unsupported must not include value",
                            resource_id
                        );
                    }
                    required_string_field(
                        object,
                        "unsupported_reason",
                        "record_cache entry unsupported_reason",
                    )?;
                }
                _ => {
                    bail!(
                        "record_inventory_current row for resource_id {} entries[{index}] has unsupported status {}",
                        resource_id,
                        status
                    );
                }
            }
        }
    }

    Ok(record_key)
}

fn required_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{context} must include non-empty string field {field_name}"))
}

fn optional_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<Option<&'a str>> {
    match object.get(field_name) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value)),
        Some(_) => bail!("{context} field {field_name} must be null or non-empty string"),
    }
}

fn required_bool_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<bool> {
    object
        .get(field_name)
        .and_then(Value::as_bool)
        .with_context(|| format!("{context} must include boolean field {field_name}"))
}

fn required_positive_i64_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<i64> {
    object
        .get(field_name)
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .with_context(|| format!("{context} must include positive integer field {field_name}"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectorFieldExpectation {
    CacheableOnly,
    GapReasonOnly,
    StatusDriven,
}
