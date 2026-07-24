use std::collections::HashMap;

use alloy_primitives::hex;
use anyhow::{Context, Result};
use bigname_domain::normalization::normalize_name;
use bigname_storage::{chain_lineage_contains_ancestor_at_block, load_resource};
use serde_json::Value;

use crate::ens_v2_common::{
    ActiveEmitter, active_emitter_for_log, emitters_by_address, normalize_address,
};
use crate::{
    adapter_manifest::ActiveManifestEventTopic0sBySignature, evm_abi::keccak_signature_hex,
};

use super::constants::{
    ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE, ABI_EVENT_NAMED_RESOURCE_SIGNATURE,
    ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE,
};
use super::decode::build_permissions_observation;
use super::normalized::{is_registry_permission_source, permission_resource_id};
use super::types::{PermissionsObservation, PermissionsRawLogRow, ResolverResourceHint};
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

    let Some(normalized_name) = required_text(provenance, "normalized_name") else {
        return Ok(None);
    };
    let Some(logical_name_id) = required_text(provenance, "logical_name_id") else {
        return Ok(None);
    };
    if logical_name_id != format!("{}:{normalized_name}", raw_log.namespace) {
        return Ok(None);
    }
    let Some(selector_kind) = required_text(provenance, "selector_kind") else {
        return Ok(None);
    };
    if !matches!(selector_kind, "name" | "text" | "addr") {
        return Ok(None);
    }
    let Some((selector_key, selector_hash)) =
        validated_persisted_selector_fields(provenance, selector_kind)
    else {
        return Ok(None);
    };
    let Some(dns_encoded_name) = validated_dns_encoded_name(
        pool,
        raw_log,
        upstream_resource,
        normalized_name,
        selector_kind,
        selector_key.as_deref(),
        selector_hash.as_deref(),
        provenance.get("dns_encoded_name"),
        active_emitters,
    )
    .await?
    else {
        return Ok(None);
    };

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

fn exact_optional_text(value: &Value, key: &str) -> Option<Option<String>> {
    match value.get(key)? {
        Value::Null => Some(None),
        Value::String(value) if !value.is_empty() => Some(Some(value.clone())),
        _ => None,
    }
}

pub(super) fn validated_persisted_selector_fields(
    provenance: &Value,
    selector_kind: &str,
) -> Option<(Option<String>, Option<String>)> {
    let selector_key = exact_optional_text(provenance, "selector_key")?;
    let selector_hash = exact_optional_text(provenance, "selector_hash")?;
    let valid = match selector_kind {
        "name" => selector_key.is_none() && selector_hash.is_none(),
        "text" => selector_key.is_some() && selector_hash.as_deref().is_some_and(is_lower_hex_hash),
        "addr" => {
            selector_key
                .as_deref()
                .is_some_and(|coin_type| coin_type.bytes().all(|byte| byte.is_ascii_digit()))
                && selector_hash.is_none()
        }
        _ => false,
    };
    valid.then_some((selector_key, selector_hash))
}

async fn validated_dns_encoded_name(
    pool: &sqlx::PgPool,
    raw_log: &PermissionsRawLogRow,
    upstream_resource: &str,
    normalized_name: &str,
    selector_kind: &str,
    selector_key: Option<&str>,
    selector_hash: Option<&str>,
    persisted_value: Option<&Value>,
    active_emitters: &[ActiveEmitter],
) -> Result<Option<Vec<u8>>> {
    let dns_encoded_name = match persisted_value {
        None | Some(Value::Null) => {
            let Some(_reencoded) = reencoded_dns_encoded_name(normalized_name) else {
                return Ok(None);
            };
            // Re-encoding validates legacy provenance. Keep the exact bytes from the durable
            // preimage observation so raw-log staging retention cannot change derived output.
            match load_durable_dns_encoded_name(
                pool,
                raw_log,
                upstream_resource,
                normalized_name,
                selector_kind,
                selector_key,
                selector_hash,
                active_emitters,
            )
            .await?
            {
                DurableDnsEncodedName::Matched(value) => value,
                DurableDnsEncodedName::Missing | DurableDnsEncodedName::Mismatch => {
                    return Ok(None);
                }
            }
        }
        Some(Value::String(value)) if !value.is_empty() => match value
            .strip_prefix("0x")
            .and_then(|value| hex::decode(value).ok())
        {
            Some(value) => value,
            None => return Ok(None),
        },
        Some(_) => return Ok(None),
    };
    Ok(
        (dns_decode(&dns_encoded_name).ok().as_deref() == Some(normalized_name))
            .then_some(dns_encoded_name),
    )
}

fn reencoded_dns_encoded_name(normalized_name: &str) -> Option<Vec<u8>> {
    let normalized = normalize_name(normalized_name).ok()?;
    if normalized.normalized_name != normalized_name {
        return None;
    }
    let dns_encoded_name = normalized.dns_encoded_name;
    (dns_decode(&dns_encoded_name).ok().as_deref() == Some(normalized_name))
        .then_some(dns_encoded_name)
}

enum DurableDnsEncodedName {
    Missing,
    Matched(Vec<u8>),
    Mismatch,
}

async fn load_durable_dns_encoded_name(
    pool: &sqlx::PgPool,
    raw_log: &PermissionsRawLogRow,
    upstream_resource: &str,
    normalized_name: &str,
    selector_kind: &str,
    selector_key: Option<&str>,
    selector_hash: Option<&str>,
    active_emitters: &[ActiveEmitter],
) -> Result<DurableDnsEncodedName> {
    let event_topics = ActiveManifestEventTopic0sBySignature::new(HashMap::from([
        (
            ABI_EVENT_NAMED_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE),
        ),
    ]));
    let Some(source_event) = selector_source_event(selector_kind) else {
        return Ok(DurableDnsEncodedName::Mismatch);
    };
    let rows = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            i64,
            String,
            String,
            i64,
            i64,
            Value,
            Value,
        ),
    >(
        r#"
        SELECT
            event.block_number,
            event.block_hash,
            event.transaction_hash,
            event.log_index,
            event.namespace,
            event.source_family,
            event.manifest_version,
            event.source_manifest_id,
            event.raw_fact_ref,
            event.after_state
        FROM normalized_events event
        WHERE event.chain_id = $1
          AND event.derivation_kind = 'raw_log_preimage_observation'
          AND event.event_kind = 'PreimageObserved'
          AND event.namespace = $2
          AND event.source_family = $3
          AND LOWER(event.raw_fact_ref ->> 'emitting_address') = LOWER($4)
          AND LOWER(event.raw_fact_ref ->> 'topic1') = LOWER($5)
          AND event.after_state ->> 'source_event' = $6
          AND event.after_state ->> 'decoded_name' = $7
          AND (
              event.block_number,
              (event.raw_fact_ref ->> 'transaction_index')::BIGINT,
              event.log_index
          ) < ($8::BIGINT, $9::BIGINT, $10::BIGINT)
          AND event.block_number IS NOT NULL
          AND event.block_hash IS NOT NULL
          AND event.transaction_hash IS NOT NULL
          AND event.log_index IS NOT NULL
          AND event.source_manifest_id IS NOT NULL
          AND event.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
            event.block_number DESC,
            (event.raw_fact_ref ->> 'transaction_index')::BIGINT DESC,
            event.log_index DESC,
            event.event_identity DESC
        "#,
    )
    .bind(&raw_log.chain_id)
    .bind(&raw_log.namespace)
    .bind(&raw_log.source_family)
    .bind(&raw_log.emitting_address)
    .bind(upstream_resource)
    .bind(source_event)
    .bind(normalized_name)
    .bind(raw_log.block_number)
    .bind(raw_log.transaction_index)
    .bind(raw_log.log_index)
    .fetch_all(pool)
    .await
    .context("failed to load durable ENSv2 named-resource observations")?;

    if rows.is_empty() {
        return Ok(DurableDnsEncodedName::Missing);
    }

    let active_emitters_by_address = emitters_by_address(active_emitters);
    for (
        block_number,
        block_hash,
        transaction_hash,
        log_index,
        namespace,
        source_family,
        manifest_version,
        source_manifest_id,
        raw_fact_ref,
        after_state,
    ) in rows
    {
        // The durable event supplies exact DNS bytes, so canonicality alone is insufficient:
        // a still-canonical sibling can normalize to the same name with different bytes.
        if !chain_lineage_contains_ancestor_at_block(
            pool,
            &raw_log.chain_id,
            &raw_log.block_hash,
            &block_hash,
            block_number,
        )
        .await?
        {
            continue;
        }
        let Some(transaction_index) = required_i64(&raw_fact_ref, "transaction_index") else {
            continue;
        };
        let Some(emitting_address) = required_text(&raw_fact_ref, "emitting_address") else {
            continue;
        };
        let emitting_address = normalize_address(emitting_address);
        let Some(emitter) =
            active_emitters_by_address
                .get(&emitting_address)
                .and_then(|emitters| {
                    active_emitter_for_log(emitters, block_number, transaction_index, log_index)
                })
        else {
            continue;
        };
        if emitter.contract_instance_id != raw_log.emitting_contract_instance_id
            || emitter.source_manifest_id != source_manifest_id
            || emitter.namespace != namespace
            || emitter.source_family != source_family
            || emitter.manifest_version != manifest_version
            || !durable_raw_fact_matches(
                &raw_fact_ref,
                raw_log,
                &block_hash,
                block_number,
                &transaction_hash,
                transaction_index,
                log_index,
                &emitting_address,
            )
        {
            continue;
        }
        let Some(topics) = durable_topics(&raw_fact_ref, source_event) else {
            continue;
        };
        let Some(data) =
            required_text(&raw_fact_ref, "data_hex").and_then(|value| hex::decode(value).ok())
        else {
            continue;
        };
        let candidate = PermissionsRawLogRow {
            chain_id: raw_log.chain_id.clone(),
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            emitting_contract_instance_id: emitter.contract_instance_id,
            topics,
            data,
            canonicality_state: raw_log.canonicality_state,
            source_manifest_id,
            namespace,
            source_family,
            manifest_version,
        };
        let Ok(Some(observation)) = build_permissions_observation(&candidate, &event_topics) else {
            continue;
        };
        let Some(dns_encoded_name) = observed_dns_encoded_name(
            &observation,
            upstream_resource,
            selector_kind,
            selector_key,
            selector_hash,
        ) else {
            continue;
        };
        let durable_dns_encoded_name = format!("0x{}", hex::encode(dns_encoded_name.as_slice()));
        if required_text(&after_state, "dns_encoded_name")
            != Some(durable_dns_encoded_name.as_str())
            || required_text(&after_state, "decoded_name") != Some(normalized_name)
        {
            continue;
        }
        if dns_decode(&dns_encoded_name).ok().as_deref() == Some(normalized_name) {
            return Ok(DurableDnsEncodedName::Matched(dns_encoded_name));
        }
    }

    Ok(DurableDnsEncodedName::Mismatch)
}

fn selector_source_event(selector_kind: &str) -> Option<&'static str> {
    match selector_kind {
        "name" => Some("NamedResource"),
        "text" => Some("NamedTextResource"),
        "addr" => Some("NamedAddrResource"),
        _ => None,
    }
}

fn required_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key)?.as_i64()
}

#[allow(clippy::too_many_arguments)]
fn durable_raw_fact_matches(
    raw_fact_ref: &Value,
    current: &PermissionsRawLogRow,
    block_hash: &str,
    block_number: i64,
    transaction_hash: &str,
    transaction_index: i64,
    log_index: i64,
    emitting_address: &str,
) -> bool {
    required_text(raw_fact_ref, "kind") == Some("raw_log")
        && required_text(raw_fact_ref, "chain_id") == Some(current.chain_id.as_str())
        && required_text(raw_fact_ref, "block_hash") == Some(block_hash)
        && required_i64(raw_fact_ref, "block_number") == Some(block_number)
        && required_text(raw_fact_ref, "transaction_hash") == Some(transaction_hash)
        && required_i64(raw_fact_ref, "transaction_index") == Some(transaction_index)
        && required_i64(raw_fact_ref, "log_index") == Some(log_index)
        && required_text(raw_fact_ref, "emitting_address")
            .is_some_and(|address| address.eq_ignore_ascii_case(emitting_address))
}

fn durable_topics(raw_fact_ref: &Value, source_event: &str) -> Option<Vec<String>> {
    let mut topics = vec![
        required_text(raw_fact_ref, "topic0")?.to_owned(),
        required_text(raw_fact_ref, "topic1")?.to_owned(),
    ];
    if matches!(source_event, "NamedTextResource" | "NamedAddrResource") {
        topics.push(required_text(raw_fact_ref, "topic2")?.to_owned());
    }
    Some(topics)
}

pub(super) fn observed_dns_encoded_name(
    observation: &PermissionsObservation,
    upstream_resource: &str,
    selector_kind: &str,
    selector_key: Option<&str>,
    selector_hash: Option<&str>,
) -> Option<Vec<u8>> {
    match observation {
        PermissionsObservation::NamedResource { resource, name }
            if selector_kind == "name"
                && resource == upstream_resource
                && selector_key.is_none()
                && selector_hash.is_none() =>
        {
            Some(name.clone())
        }
        PermissionsObservation::NamedTextResource {
            resource,
            name,
            key_hash,
            key,
        } if selector_kind == "text"
            && resource == upstream_resource
            && selector_key == Some(key.as_str())
            && selector_hash == Some(key_hash.as_str()) =>
        {
            Some(name.clone())
        }
        PermissionsObservation::NamedAddrResource {
            resource,
            name,
            coin_type,
        } if selector_kind == "addr"
            && resource == upstream_resource
            && selector_key == Some(coin_type.as_str())
            && selector_hash.is_none() =>
        {
            Some(name.clone())
        }
        _ => None,
    }
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
