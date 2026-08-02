use alloy_primitives::{B256, keccak256};
use anyhow::{Context, bail};
use serde_json::{Value, json};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use super::model::RawLogInput;

pub(super) fn stable_uuid(seed: &str) -> Uuid {
    let hash = keccak256(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

pub(super) fn ens_v2_registry_resource_id(
    chain_id: &str,
    contract_instance_id: Uuid,
    upstream_resource: &str,
) -> Uuid {
    stable_uuid(&format!(
        "ens-v2-resource:{chain_id}:{contract_instance_id}:{upstream_resource}"
    ))
}

pub(super) fn ens_v2_resolver_resource_id(
    chain_id: &str,
    contract_instance_id: Uuid,
    upstream_resource: &str,
) -> Uuid {
    stable_uuid(&format!(
        "ens-v2-resolver-resource:{chain_id}:{contract_instance_id}:{upstream_resource}"
    ))
}

pub(super) fn contract_id(chain_id: &str, address: &str) -> Uuid {
    stable_uuid(&format!(
        "contract:{chain_id}:{}",
        address.to_ascii_lowercase()
    ))
}

pub(super) fn hash_hex(bytes: &[u8]) -> String {
    format!("{:#x}", keccak256(bytes))
}

pub(super) fn namehash(labels: &[String]) -> String {
    let node = labels.iter().rev().fold(B256::ZERO, |node, label| {
        let mut input = [0u8; 64];
        input[..32].copy_from_slice(node.as_slice());
        input[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
        keccak256(input)
    });
    format!("{node:#x}")
}

pub(super) fn dns_encode(labels: &[String]) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for label in labels {
        require_label(label)?;
        bytes.push(u8::try_from(label.len()).context("raw DNS label exceeds 255 bytes")?);
        bytes.extend_from_slice(label.as_bytes());
    }
    bytes.push(0);
    Ok(bytes)
}

pub(super) fn decode_dns_name(bytes: &[u8]) -> anyhow::Result<Vec<String>> {
    let mut labels = Vec::new();
    let mut cursor = 0usize;
    loop {
        let length = usize::from(
            *bytes
                .get(cursor)
                .context("DNS name is missing its root terminator")?,
        );
        cursor += 1;
        if length == 0 {
            if cursor != bytes.len() {
                bail!("DNS name has trailing bytes after its root terminator");
            }
            break;
        }
        let end = cursor
            .checked_add(length)
            .context("DNS label length overflow")?;
        let raw = bytes.get(cursor..end).context("DNS label is truncated")?;
        let label = std::str::from_utf8(raw).context("DNS label is not UTF-8")?;
        require_label(label)?;
        labels.push(label.to_owned());
        cursor = end;
    }
    if labels.is_empty() {
        bail!("root-only DNS names do not identify a name surface");
    }
    Ok(labels)
}

pub(super) fn require_label(label: &str) -> anyhow::Result<()> {
    if label.is_empty() || label.contains('.') || label.contains('\0') {
        bail!("raw name contains an invalid DNS label");
    }
    if label.len() > usize::from(u8::MAX) {
        bail!("raw DNS label exceeds 255 bytes");
    }
    Ok(())
}

pub(super) fn normalize_address(address: &str) -> anyhow::Result<String> {
    let address = address.to_ascii_lowercase();
    if address.len() != 42 || !address.starts_with("0x") {
        bail!("invalid EVM address {address}");
    }
    Ok(address)
}

pub(super) fn raw_fact_ref(raw: &RawLogInput) -> Value {
    json!({
        "kind": "raw_log",
        "chain_id": raw.chain_id,
        "block_hash": raw.block_hash,
        "block_number": raw.block_number,
        "transaction_hash": raw.transaction_hash,
        "transaction_index": raw.transaction_index,
        "log_index": raw.log_index,
        "emitting_address": raw.emitting_address,
    })
}

pub(super) fn provenance(raw: &RawLogInput, source_event: &str, manifest_id: i64) -> Value {
    json!({
        "source": "raw_log",
        "source_event": source_event,
        "source_manifest_id": manifest_id,
        "chain_id": raw.chain_id,
        "block_hash": raw.block_hash,
        "block_number": raw.block_number,
        "transaction_hash": raw.transaction_hash,
        "transaction_index": raw.transaction_index,
        "log_index": raw.log_index,
        "emitting_address": raw.emitting_address,
    })
}

pub(super) fn event_time(raw: &RawLogInput) -> OffsetDateTime {
    raw.block_timestamp + Duration::microseconds(raw.log_index.max(0))
}

pub(super) fn derivation_kind(source_family: &str, event_kind: &str) -> &'static str {
    if event_kind == "Upgraded" {
        "proxy_upgrade"
    } else if event_kind == "PreimageObserved" {
        "raw_log_preimage_observation"
    } else if source_family == "ens_v1_reverse_l1" || source_family == "basenames_base_primary" {
        "ens_v1_reverse_claim"
    } else if source_family == "ens_v2_resolver_l1" {
        if event_kind == "PermissionChanged" {
            "ens_v2_permissions"
        } else {
            "ens_v2_resolver"
        }
    } else if source_family == "ens_v2_registrar_l1" {
        "ens_v2_registrar"
    } else if matches!(source_family, "ens_v2_registry_l1" | "ens_v2_root_l1") {
        if matches!(event_kind, "PermissionChanged" | "RootPermissionChanged") {
            "ens_v2_permissions"
        } else {
            "ens_v2_registry_resource_surface"
        }
    } else {
        "ens_v1_unwrapped_authority"
    }
}
