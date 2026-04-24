use std::collections::BTreeSet;

use anyhow::{Result, bail};
use serde_json::Value;

use super::{
    decode::{
        append_version_boundary_key_parts, decode_manifest_versions,
        decode_requested_chain_positions, decode_version_boundary,
        manifest_version_identity_key as decode_manifest_version_identity_key,
    },
    types::ExecutionCacheKey,
};

pub(super) fn normalize_execution_cache_key(
    cache_key: &ExecutionCacheKey,
) -> Result<ExecutionCacheKey> {
    let request_key = cache_key.request_key.trim();
    if request_key.is_empty() {
        bail!("execution cache key has empty request_key");
    }

    let requested_chain_positions =
        normalize_requested_chain_positions(&cache_key.requested_chain_positions, request_key)?;
    let manifest_versions = normalize_manifest_versions(&cache_key.manifest_versions, request_key)?;
    validate_version_boundary(
        &cache_key.topology_version_boundary,
        "topology_version_boundary",
        request_key,
    )?;
    validate_version_boundary(
        &cache_key.record_version_boundary,
        "record_version_boundary",
        request_key,
    )?;

    Ok(ExecutionCacheKey {
        request_key: request_key.to_owned(),
        requested_chain_positions,
        manifest_versions,
        topology_version_boundary: cache_key.topology_version_boundary.clone(),
        record_version_boundary: cache_key.record_version_boundary.clone(),
    })
}

pub(super) fn execution_cache_key_storage_key(cache_key: &ExecutionCacheKey) -> Result<String> {
    let normalized = normalize_execution_cache_key(cache_key)?;
    let requested_positions = decode_requested_chain_positions(
        &normalized.requested_chain_positions,
        &normalized.request_key,
    )?;
    let manifest_versions =
        decode_manifest_versions(&normalized.manifest_versions, &normalized.request_key)?;
    let topology_version_boundary = decode_version_boundary(
        &normalized.topology_version_boundary,
        "topology_version_boundary",
        &normalized.request_key,
    )?;
    let record_version_boundary = decode_version_boundary(
        &normalized.record_version_boundary,
        "record_version_boundary",
        &normalized.request_key,
    )?;

    let mut key = String::new();
    super::decode::append_key_part(&mut key, &normalized.request_key);

    for position in requested_positions {
        super::decode::append_key_part(&mut key, &position.chain_id);
        super::decode::append_key_part(&mut key, &position.block_number.to_string());
        super::decode::append_key_part(&mut key, &position.block_hash);
    }

    for manifest_version in manifest_versions {
        super::decode::append_key_part(
            &mut key,
            &manifest_version
                .source_manifest_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
        );
        super::decode::append_key_part(
            &mut key,
            manifest_version
                .source_family
                .as_deref()
                .unwrap_or_default(),
        );
        super::decode::append_key_part(&mut key, &manifest_version.manifest_version.to_string());
    }

    append_version_boundary_key_parts(&mut key, &topology_version_boundary);
    append_version_boundary_key_parts(&mut key, &record_version_boundary);

    Ok(key)
}

pub(super) fn validate_version_boundary(
    value: &Value,
    field_name: &str,
    request_key: &str,
) -> Result<()> {
    decode_version_boundary(value, field_name, request_key).map(|_| ())
}

pub(super) fn version_boundary_storage_key(
    value: &Value,
    field_name: &str,
    request_key: &str,
) -> Result<String> {
    let boundary = decode_version_boundary(value, field_name, request_key)?;
    let mut key = String::new();
    append_version_boundary_key_parts(&mut key, &boundary);
    Ok(key)
}

pub(super) fn manifest_version_identity_key(
    source_manifest_id: Option<i64>,
    source_family: Option<&str>,
    manifest_version: i64,
) -> String {
    decode_manifest_version_identity_key(source_manifest_id, source_family, manifest_version)
}

pub(super) fn manifest_versions_contain_identity(
    value: &Value,
    request_key: &str,
    target_identity: &str,
) -> Result<bool> {
    Ok(decode_manifest_versions(value, request_key)?
        .iter()
        .any(|manifest_version| manifest_version.identity_key() == target_identity))
}

pub(super) fn execution_outcome_block_dependencies(
    request_key: &str,
    requested_chain_positions: &Value,
    topology_version_boundary: &Value,
    record_version_boundary: &Value,
) -> Result<BTreeSet<(String, String)>> {
    let mut dependencies = BTreeSet::new();
    for position in decode_requested_chain_positions(requested_chain_positions, request_key)? {
        dependencies.insert((position.chain_id, position.block_hash));
    }

    let topology_boundary = decode_version_boundary(
        topology_version_boundary,
        "topology_version_boundary",
        request_key,
    )?;
    dependencies.insert((
        topology_boundary.chain_position.chain_id,
        topology_boundary.chain_position.block_hash,
    ));

    let record_boundary = decode_version_boundary(
        record_version_boundary,
        "record_version_boundary",
        request_key,
    )?;
    dependencies.insert((
        record_boundary.chain_position.chain_id,
        record_boundary.chain_position.block_hash,
    ));

    if dependencies.is_empty() {
        bail!(
            "execution outcome for request_key {request_key} has no block-hash-bearing dependencies"
        );
    }

    Ok(dependencies)
}

fn normalize_requested_chain_positions(value: &Value, request_key: &str) -> Result<Value> {
    let positions = decode_requested_chain_positions(value, request_key)?;
    let normalized = positions
        .iter()
        .map(super::decode::RequestedChainPositionParts::to_value)
        .collect::<Vec<_>>();
    Ok(Value::Array(normalized))
}

fn normalize_manifest_versions(value: &Value, request_key: &str) -> Result<Value> {
    let manifest_versions = decode_manifest_versions(value, request_key)?;
    let normalized = manifest_versions
        .iter()
        .map(super::decode::ManifestVersionParts::to_value)
        .collect::<Vec<_>>();
    Ok(Value::Array(normalized))
}
