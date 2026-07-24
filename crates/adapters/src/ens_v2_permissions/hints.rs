use alloy_primitives::hex;
use anyhow::Result;
use bigname_storage::load_resource;
use serde_json::Value;

use super::normalized::{is_registry_permission_source, permission_resource_id};
use super::types::{PermissionsRawLogRow, ResolverResourceHint};
use super::util::dns_decode;

pub(super) fn resolver_resource_hint(
    raw_log: &PermissionsRawLogRow,
    upstream_resource: String,
    dns_encoded_name: Vec<u8>,
    selector_kind: &str,
    selector_key: Option<String>,
    selector_hash: Option<String>,
) -> Result<ResolverResourceHint> {
    let normalized_name = dns_decode(&dns_encoded_name).ok();
    Ok(ResolverResourceHint {
        upstream_resource,
        logical_name_id: normalized_name
            .as_deref()
            .filter(|name| !name.is_empty())
            .map(|name| format!("{}:{name}", raw_log.namespace)),
        normalized_name,
        dns_encoded_name: Some(dns_encoded_name),
        selector_kind: selector_kind.to_owned(),
        selector_key,
        selector_hash,
        first_ref: raw_log.reference(),
    })
}

pub(super) fn fallback_resource_hint(
    raw_log: &PermissionsRawLogRow,
    upstream_resource: String,
    is_root: bool,
) -> ResolverResourceHint {
    ResolverResourceHint {
        upstream_resource,
        logical_name_id: None,
        normalized_name: None,
        dns_encoded_name: None,
        selector_kind: if is_root { "root" } else { "unknown" }.to_owned(),
        selector_key: None,
        selector_hash: None,
        first_ref: raw_log.reference(),
    }
}

pub(super) async fn load_persisted_resolver_resource_hint(
    pool: &sqlx::PgPool,
    raw_log: &PermissionsRawLogRow,
    upstream_resource: &str,
) -> Result<Option<ResolverResourceHint>> {
    if is_registry_permission_source(&raw_log.source_family) {
        return Ok(None);
    }
    let resource_id = permission_resource_id(
        &raw_log.chain_id,
        raw_log.emitting_contract_instance_id,
        upstream_resource,
        false,
    );
    let Some(resource) = load_resource(pool, resource_id).await? else {
        return Ok(None);
    };
    let provenance = &resource.provenance;
    let contract_instance_id = raw_log.emitting_contract_instance_id.to_string();
    if resource.chain_id != raw_log.chain_id
        || required_text(provenance, "adapter") != Some("ens_v2_permissions")
        || required_text(provenance, "chain_id") != Some(raw_log.chain_id.as_str())
        || required_text(provenance, "upstream_resource") != Some(upstream_resource)
        || required_text(provenance, "source_family") != Some(raw_log.source_family.as_str())
        || required_text(provenance, "resolver_contract_instance_id")
            != Some(contract_instance_id.as_str())
        || !required_text(provenance, "resolver_address")
            .is_some_and(|address| address.eq_ignore_ascii_case(&raw_log.emitting_address))
    {
        return Ok(None);
    }

    let Some(normalized_name) = required_text(provenance, "normalized_name") else {
        return Ok(None);
    };
    let Some(logical_name_id) = required_text(provenance, "logical_name_id") else {
        return Ok(None);
    };
    if logical_name_id != format!("{}:{normalized_name}", raw_log.namespace) {
        return Ok(None);
    }
    let Some(dns_encoded_name) = required_text(provenance, "dns_encoded_name")
        .and_then(|value| value.strip_prefix("0x"))
        .and_then(|value| hex::decode(value).ok())
    else {
        return Ok(None);
    };
    if dns_decode(&dns_encoded_name).ok().as_deref() != Some(normalized_name) {
        return Ok(None);
    }
    let Some(selector_kind) = required_text(provenance, "selector_kind") else {
        return Ok(None);
    };
    if !matches!(selector_kind, "name" | "text" | "addr") {
        return Ok(None);
    }
    let selector_key = optional_text(provenance, "selector_key");
    let selector_hash = optional_text(provenance, "selector_hash");
    let selector_is_valid = match selector_kind {
        "name" => selector_key.is_none() && selector_hash.is_none(),
        "text" => {
            selector_key.as_deref().is_some_and(|key| !key.is_empty())
                && selector_hash.as_deref().is_some_and(is_lower_hex_hash)
        }
        "addr" => {
            selector_key.as_deref().is_some_and(|coin_type| {
                !coin_type.is_empty() && coin_type.bytes().all(|byte| byte.is_ascii_digit())
            }) && selector_hash.is_none()
        }
        _ => false,
    };
    if !selector_is_valid {
        return Ok(None);
    }

    Ok(Some(ResolverResourceHint {
        upstream_resource: upstream_resource.to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        normalized_name: Some(normalized_name.to_owned()),
        dns_encoded_name: Some(dns_encoded_name),
        selector_kind: selector_kind.to_owned(),
        selector_key,
        selector_hash,
        first_ref: raw_log.reference(),
    }))
}

fn required_text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str().filter(|value| !value.is_empty())
}

fn optional_text(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn is_lower_hex_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
