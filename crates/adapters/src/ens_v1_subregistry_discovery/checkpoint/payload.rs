use alloy_primitives::hex;
use anyhow::{Context, Result, anyhow, bail};
use bigname_storage::CanonicalityState;
use serde_json::{Value, json};
use sqlx::types::Uuid;

use super::super::{
    EnsV1SubregistryDiscoverySyncSummary,
    assignment::{ObservedRegistryAssignment, RegistryDiscoveryKind},
    hex_topic::hex_string,
    loader::RegistryRawLogRow,
};

pub(super) fn assignment_payload(assignment: &ObservedRegistryAssignment) -> Value {
    json!({
        "observation_key": assignment.observation_key,
        "discovery_source": assignment.discovery_source,
        "from_address": assignment.from_address,
        "to_address": assignment.to_address,
        "parent_node": assignment.parent_node,
        "labelhash": assignment.labelhash,
        "node": assignment.node,
        "migration_epoch_input": assignment.migration_epoch_input,
        "old_root_resolver_exception": assignment.old_root_resolver_exception,
        "discovery_kind": discovery_kind_str(assignment.discovery_kind),
        "raw_log": raw_log_payload(&assignment.raw_log),
    })
}

fn raw_log_payload(raw_log: &RegistryRawLogRow) -> Value {
    json!({
        "chain_id": raw_log.chain_id,
        "block_hash": raw_log.block_hash,
        "block_number": raw_log.block_number,
        "transaction_hash": raw_log.transaction_hash,
        "transaction_index": raw_log.transaction_index,
        "log_index": raw_log.log_index,
        "emitting_address": raw_log.emitting_address,
        "topics": raw_log.topics,
        "data_hex": hex_string(&raw_log.data),
        "canonicality_state": raw_log.canonicality_state.as_str(),
        "emitting_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
        "source_manifest_id": raw_log.source_manifest_id,
        "namespace": raw_log.namespace,
        "source_family": raw_log.source_family,
        "manifest_version": raw_log.manifest_version,
        "contract_role": raw_log.contract_role,
    })
}

pub(super) fn assignment_from_payload(payload: &Value) -> Result<ObservedRegistryAssignment> {
    let raw_log = payload
        .get("raw_log")
        .context("checkpointed assignment is missing raw_log")?;
    Ok(ObservedRegistryAssignment {
        observation_key: string_field(payload, "observation_key")?,
        discovery_source: string_field(payload, "discovery_source")?,
        from_address: string_field(payload, "from_address")?,
        to_address: string_field(payload, "to_address")?,
        parent_node: optional_string_field(payload, "parent_node")?,
        labelhash: optional_string_field(payload, "labelhash")?,
        node: optional_string_field(payload, "node")?,
        migration_epoch_input: bool_field(payload, "migration_epoch_input")?,
        old_root_resolver_exception: bool_field(payload, "old_root_resolver_exception")?,
        raw_log: raw_log_from_payload(raw_log)?,
        discovery_kind: discovery_kind_from_str(&string_field(payload, "discovery_kind")?)?,
    })
}

fn raw_log_from_payload(payload: &Value) -> Result<RegistryRawLogRow> {
    let data_hex = string_field(payload, "data_hex")?;
    let data_hex = data_hex.strip_prefix("0x").unwrap_or(&data_hex);
    let data = hex::decode(data_hex).context("checkpointed raw log data_hex is invalid")?;
    let canonicality_state = string_field(payload, "canonicality_state")?;
    let canonicality_state = CanonicalityState::parse(&canonicality_state)
        .map_err(|_| anyhow!("unknown checkpointed canonicality_state {canonicality_state}"))?;
    Ok(RegistryRawLogRow {
        chain_id: string_field(payload, "chain_id")?,
        block_hash: string_field(payload, "block_hash")?,
        block_number: i64_field(payload, "block_number")?,
        transaction_hash: string_field(payload, "transaction_hash")?,
        transaction_index: i64_field(payload, "transaction_index")?,
        log_index: i64_field(payload, "log_index")?,
        emitting_address: string_field(payload, "emitting_address")?,
        topics: string_vec_field(payload, "topics")?,
        data,
        canonicality_state,
        emitting_contract_instance_id: Uuid::parse_str(&string_field(
            payload,
            "emitting_contract_instance_id",
        )?)
        .context("checkpointed emitting_contract_instance_id is invalid")?,
        source_manifest_id: i64_field(payload, "source_manifest_id")?,
        namespace: string_field(payload, "namespace")?,
        source_family: string_field(payload, "source_family")?,
        manifest_version: i64_field(payload, "manifest_version")?,
        contract_role: optional_string_field(payload, "contract_role")?,
    })
}

pub(super) fn summary_payload(summary: &EnsV1SubregistryDiscoverySyncSummary) -> Value {
    json!({
        "scanned_log_count": summary.scanned_log_count,
        "matched_log_count": summary.matched_log_count,
        "active_observation_count": summary.active_observation_count,
        "active_edge_count": summary.active_edge_count,
        "admitted_edge_count": summary.admitted_edge_count,
        "inserted_edge_count": summary.inserted_edge_count,
        "deactivated_edge_count": summary.deactivated_edge_count,
        "total_normalized_event_count": summary.total_normalized_event_count,
        "total_normalized_event_inserted_count": summary.total_normalized_event_inserted_count,
    })
}

pub(super) fn summary_from_payload(
    payload: &Value,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    Ok(EnsV1SubregistryDiscoverySyncSummary {
        scanned_log_count: usize_field(payload, "scanned_log_count")?,
        matched_log_count: usize_field(payload, "matched_log_count")?,
        active_observation_count: usize_field(payload, "active_observation_count")?,
        active_edge_count: usize_field(payload, "active_edge_count")?,
        admitted_edge_count: usize_field(payload, "admitted_edge_count")?,
        inserted_edge_count: usize_field(payload, "inserted_edge_count")?,
        deactivated_edge_count: usize_field(payload, "deactivated_edge_count")?,
        total_normalized_event_count: usize_field(payload, "total_normalized_event_count")?,
        total_normalized_event_inserted_count: usize_field(
            payload,
            "total_normalized_event_inserted_count",
        )?,
    })
}

const fn discovery_kind_str(kind: RegistryDiscoveryKind) -> &'static str {
    match kind {
        RegistryDiscoveryKind::Subregistry => "subregistry",
        RegistryDiscoveryKind::Resolver => "resolver",
    }
}

fn discovery_kind_from_str(value: &str) -> Result<RegistryDiscoveryKind> {
    match value {
        "subregistry" => Ok(RegistryDiscoveryKind::Subregistry),
        "resolver" => Ok(RegistryDiscoveryKind::Resolver),
        _ => bail!("unknown checkpointed registry discovery kind {value}"),
    }
}

fn string_field(payload: &Value, field: &str) -> Result<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("checkpoint payload is missing string field {field}"))
}

fn optional_string_field(payload: &Value, field: &str) -> Result<Option<String>> {
    let Some(value) = payload.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_owned()))
        .with_context(|| format!("checkpoint payload field {field} must be a string or null"))
}

fn bool_field(payload: &Value, field: &str) -> Result<bool> {
    payload
        .get(field)
        .and_then(Value::as_bool)
        .with_context(|| format!("checkpoint payload is missing bool field {field}"))
}

fn i64_field(payload: &Value, field: &str) -> Result<i64> {
    payload
        .get(field)
        .and_then(Value::as_i64)
        .with_context(|| format!("checkpoint payload is missing i64 field {field}"))
}

fn usize_field(payload: &Value, field: &str) -> Result<usize> {
    usize::try_from(i64_field(payload, field)?)
        .with_context(|| format!("checkpoint payload field {field} overflows usize"))
}

fn string_vec_field(payload: &Value, field: &str) -> Result<Vec<String>> {
    let values = payload
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("checkpoint payload is missing array field {field}"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .with_context(|| format!("checkpoint payload field {field} contains non-string"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_log_canonicality_encoding_remains_backward_compatible() -> Result<()> {
        for (state, persisted) in [
            (CanonicalityState::Observed, "observed"),
            (CanonicalityState::Canonical, "canonical"),
            (CanonicalityState::Safe, "safe"),
            (CanonicalityState::Finalized, "finalized"),
            (CanonicalityState::Orphaned, "orphaned"),
        ] {
            let raw_log = RegistryRawLogRow {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xblock".to_owned(),
                block_number: 1,
                transaction_hash: "0xtx".to_owned(),
                transaction_index: 2,
                log_index: 3,
                emitting_address: "0x0000000000000000000000000000000000000001".to_owned(),
                topics: vec!["0xtopic".to_owned()],
                data: vec![0, 1, 255],
                canonicality_state: state,
                emitting_contract_instance_id: Uuid::from_u128(1),
                source_manifest_id: 4,
                namespace: "ens_v1".to_owned(),
                source_family: "ens_v1_registry".to_owned(),
                manifest_version: 5,
                contract_role: Some("registry".to_owned()),
            };

            let payload = raw_log_payload(&raw_log);
            assert_eq!(
                payload.get("canonicality_state").and_then(Value::as_str),
                Some(persisted)
            );
            assert_eq!(raw_log_from_payload(&payload)?.canonicality_state, state);
        }

        let mut invalid = raw_log_payload(&RegistryRawLogRow {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 1,
            transaction_hash: "0xtx".to_owned(),
            transaction_index: 2,
            log_index: 3,
            emitting_address: "0x0000000000000000000000000000000000000001".to_owned(),
            topics: vec![],
            data: vec![],
            canonicality_state: CanonicalityState::Observed,
            emitting_contract_instance_id: Uuid::from_u128(1),
            source_manifest_id: 4,
            namespace: "ens_v1".to_owned(),
            source_family: "ens_v1_registry".to_owned(),
            manifest_version: 5,
            contract_role: None,
        });
        invalid["canonicality_state"] = Value::String("invalid".to_owned());
        assert_eq!(
            raw_log_from_payload(&invalid)
                .expect_err("unknown canonicality must fail")
                .to_string(),
            "unknown checkpointed canonicality_state invalid"
        );
        Ok(())
    }
}
