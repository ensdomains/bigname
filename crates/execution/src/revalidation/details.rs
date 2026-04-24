use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

use crate::primary_name::validate_verified_primary_name_ref;

pub(crate) fn normalize_alias_detail(value: Option<&Value>, namespace: &str) -> Result<Value> {
    let Some(alias) = value else {
        return Ok(Value::Object(default_alias_detail()));
    };
    let alias = alias
        .as_object()
        .with_context(|| "alias detail must be a JSON object".to_owned())?;
    let mut normalized = default_alias_detail();

    let final_target = match alias.get("final_target") {
        None | Some(Value::Null) => Value::Null,
        Some(value) => {
            validate_verified_primary_name_ref(Some(value), "alias.final_target", namespace)?;
            value.clone()
        }
    };
    let hops = alias
        .get("hops")
        .and_then(Value::as_array)
        .with_context(|| "alias.hops must be a JSON array".to_owned())?;
    for (index, hop) in hops.iter().enumerate() {
        validate_verified_primary_name_ref(Some(hop), &format!("alias.hops[{index}]"), namespace)?;
    }
    if final_target.is_null() != hops.is_empty() {
        bail!("alias detail must set final_target and non-empty hops together");
    }
    normalized.insert("final_target".to_owned(), final_target);
    normalized.insert("hops".to_owned(), Value::Array(hops.clone()));
    Ok(Value::Object(normalized))
}

pub(crate) fn normalize_wildcard_detail(value: Option<&Value>, namespace: &str) -> Result<Value> {
    let Some(wildcard) = value else {
        return Ok(Value::Object(default_wildcard_detail()));
    };
    let wildcard = wildcard
        .as_object()
        .with_context(|| "wildcard detail must be a JSON object".to_owned())?;
    let mut normalized = default_wildcard_detail();

    let source = match wildcard.get("source") {
        None | Some(Value::Null) => Value::Null,
        Some(value) => {
            validate_verified_primary_name_ref(Some(value), "wildcard.source", namespace)?;
            value.clone()
        }
    };
    let matched_labels = wildcard
        .get("matched_labels")
        .and_then(Value::as_array)
        .with_context(|| "wildcard.matched_labels must be a JSON array".to_owned())?;
    if source.is_null() && !matched_labels.is_empty() {
        bail!("wildcard detail must keep matched_labels empty when source is null");
    }
    if !source.is_null() && matched_labels.is_empty() {
        bail!("wildcard detail must keep matched_labels non-empty when source is present");
    }
    normalized.insert("source".to_owned(), source);
    normalized.insert(
        "matched_labels".to_owned(),
        Value::Array(matched_labels.clone()),
    );
    Ok(Value::Object(normalized))
}

pub(crate) fn normalize_transport_detail(value: Option<&Value>) -> Result<Value> {
    let Some(transport) = value else {
        return Ok(Value::Object(default_transport_detail()));
    };
    let transport = transport
        .as_object()
        .with_context(|| "transport detail must be a JSON object".to_owned())?;
    let mut normalized = default_transport_detail();
    for field_name in [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ] {
        let value = match transport.get(field_name) {
            None | Some(Value::Null) => Value::Null,
            Some(Value::String(value)) if !value.trim().is_empty() => Value::String(value.clone()),
            Some(_) => {
                bail!("transport detail field {field_name} must be null or a non-empty string")
            }
        };
        normalized.insert(field_name.to_owned(), value);
    }
    Ok(Value::Object(normalized))
}

pub(super) fn default_alias_detail() -> Map<String, Value> {
    let mut alias = Map::new();
    alias.insert("final_target".to_owned(), Value::Null);
    alias.insert("hops".to_owned(), Value::Array(Vec::new()));
    alias
}

pub(super) fn default_wildcard_detail() -> Map<String, Value> {
    let mut wildcard = Map::new();
    wildcard.insert("source".to_owned(), Value::Null);
    wildcard.insert("matched_labels".to_owned(), Value::Array(Vec::new()));
    wildcard
}

pub(super) fn default_transport_detail() -> Map<String, Value> {
    let mut transport = Map::new();
    transport.insert("source_chain_id".to_owned(), Value::Null);
    transport.insert("target_chain_id".to_owned(), Value::Null);
    transport.insert("contract_address".to_owned(), Value::Null);
    transport.insert("latest_event_kind".to_owned(), Value::Null);
    transport
}
