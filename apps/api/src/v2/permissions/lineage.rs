use bigname_storage::PermissionsCurrentRow;
use serde_json::{Map, Value, json};

use super::super::{permission_powers_value, slug_to_numeric};
use super::PermissionLineage;
use crate::v2::{V2Error, V2Result};

pub(super) fn permission_lineage(row: &PermissionsCurrentRow) -> V2Result<PermissionLineage> {
    Ok(PermissionLineage {
        grant: map_permission_lineage_value(&row.grant_source)?,
        revocation: row
            .revocation_source
            .as_ref()
            .map(map_permission_lineage_value)
            .transpose()?,
        inheritance_path: non_empty_array(&row.inheritance_path)
            .map(map_permission_lineage_value)
            .transpose()?,
        transfer_behavior: map_optional_permission_lineage_value(&row.transfer_behavior)?,
    })
}

fn map_permission_lineage_value(value: &Value) -> V2Result<Value> {
    match value {
        Value::Object(object) => map_permission_lineage_object(object),
        Value::Array(items) => Ok(Value::Array(
            items
                .iter()
                .map(map_permission_lineage_value)
                .collect::<V2Result<Vec<_>>>()?,
        )),
        _ => Ok(value.clone()),
    }
}

fn map_optional_permission_lineage_value(value: &Value) -> V2Result<Option<Value>> {
    if value.is_null() {
        return Ok(None);
    }

    let mapped = map_permission_lineage_value(value)?;
    if mapped_permission_lineage_value_is_empty(&mapped) {
        Ok(None)
    } else {
        Ok(Some(mapped))
    }
}

fn mapped_permission_lineage_value_is_empty(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.is_empty(),
        Value::Array(items) => {
            items.is_empty() || items.iter().all(mapped_permission_lineage_value_is_empty)
        }
        _ => false,
    }
}

fn map_permission_lineage_object(object: &Map<String, Value>) -> V2Result<Value> {
    let mut mapped = Map::new();
    if let Some(kind) = object.get("kind") {
        mapped.insert(
            "kind".to_owned(),
            Value::String(product_lineage_kind(kind)?),
        );
    }
    if let Some(registration_id) = object.get("resource_id") {
        mapped.insert(
            "registration_id".to_owned(),
            product_lineage_string_value(registration_id, "resource_id")?,
        );
    }
    if object.contains_key("resolver_address") {
        mapped.insert("resolver".to_owned(), product_lineage_resolver(object)?);
    }
    if let Some(powers) = object.get("powers") {
        mapped.insert("powers".to_owned(), permission_powers_value(powers)?);
    }
    if let Some(relation) = object.get("relation_kind") {
        mapped.insert(
            "relation".to_owned(),
            Value::String(product_lineage_relation(relation)?),
        );
    }
    Ok(Value::Object(mapped))
}

/// The NameWrapper relation behind an `ens_v1_authority` grant or revocation.
fn product_lineage_relation(value: &Value) -> V2Result<String> {
    match value.as_str() {
        Some(relation @ ("holder" | "operator" | "token_approval")) => Ok(relation.to_owned()),
        _ => Err(lineage_mapping_error()),
    }
}

fn product_lineage_kind(value: &Value) -> V2Result<String> {
    let Some(kind) = value.as_str() else {
        return Err(lineage_mapping_error());
    };
    let mapped = match kind {
        "raw_log" | "normalized_event" => "event",
        "permission_row" => "permission",
        "resource_authority" => "registration_authority",
        "resource_rebound" => "registration_rebound",
        "ens_v1_authority" => "ens_v1_authority",
        "registry_root_fallback" => "registry_root_fallback",
        "resolver_root_fallback" => "resolver_root_fallback",
        _ => return Err(lineage_mapping_error()),
    };
    Ok(mapped.to_owned())
}

fn product_lineage_resolver(object: &Map<String, Value>) -> V2Result<Value> {
    let chain_id = object
        .get("chain_id")
        .and_then(Value::as_str)
        .and_then(slug_to_numeric)
        .ok_or_else(lineage_mapping_error)?;
    let address = object
        .get("resolver_address")
        .and_then(Value::as_str)
        .ok_or_else(lineage_mapping_error)?
        .to_ascii_lowercase();

    Ok(json!({
        "chain_id": chain_id,
        "address": address,
    }))
}

fn product_lineage_string_value(value: &Value, _field: &str) -> V2Result<Value> {
    value
        .as_str()
        .map(|value| Value::String(value.to_owned()))
        .ok_or_else(lineage_mapping_error)
}

fn lineage_mapping_error() -> V2Error {
    V2Error::internal_error("failed to map permission lineage")
}

fn non_empty_array(value: &Value) -> Option<&Value> {
    value
        .as_array()
        .filter(|values| !values.is_empty())
        .map(|_| value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::ErrorCode;

    #[test]
    fn lineage_mapping_rejects_unknown_storage_kind() {
        let error = map_permission_lineage_value(&json!({
            "kind": "contract_internal",
            "source_event": "EACRolesChanged"
        }))
        .expect_err("unknown lineage kinds must fail loudly");

        assert_eq!(error.code(), ErrorCode::InternalError);
    }

    #[test]
    fn lineage_mapping_serves_the_wrapper_relation_and_rejects_unknown_ones() {
        let mapped = map_permission_lineage_value(&json!({
            "kind": "ens_v1_authority",
            "authority_kind": "wrapper",
            "authority_key": "wrapper:ethereum-mainnet:1:0xnode:0xblock:0",
            "authority_contract": "0x00000000000000000000000000000000000026aa",
            "relation_kind": "operator",
            "node": "0xnode",
            "source_event_kind": "ApprovalForAll",
            "owner": "0x0000000000000000000000000000000000002611"
        }))
        .expect("wrapper relation must map");
        assert_eq!(
            mapped,
            json!({"kind": "ens_v1_authority", "relation": "operator"})
        );

        let error = map_permission_lineage_value(&json!({
            "kind": "ens_v1_authority",
            "relation_kind": "parent"
        }))
        .expect_err("unknown relations must fail loudly");
        assert_eq!(error.code(), ErrorCode::InternalError);
    }
}
