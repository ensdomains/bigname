use anyhow::Result;
use bigname_storage::load_resource;
use serde_json::Value;

mod durable;
mod same_batch;

use durable::load_durable_resolver_resource_hint;
#[cfg(test)]
pub(super) use durable::resolver_hint_from_durable_observation;
pub(super) use same_batch::load_same_batch_resolver_resource_hint;

use crate::ens_v2_common::ActiveEmitter;

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
    active_emitters: &[ActiveEmitter],
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

    // The selector and name fields in persisted provenance have no source-log anchor. They are
    // metadata only: recover the whole hint from the newest admitted Named* observation before the
    // role log on its exact stored ancestry.
    load_durable_resolver_resource_hint(pool, raw_log, upstream_resource, active_emitters).await
}

fn required_text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str().filter(|value| !value.is_empty())
}
