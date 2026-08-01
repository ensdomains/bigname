use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{hex, keccak256};
use anyhow::{Context, Result};

pub(crate) const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
pub(crate) const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
pub(crate) const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
pub(crate) const GENERIC_SOURCE_SCOPE_ADDRESS: &str = "*";
pub(crate) const REGISTRY_CREATED_SIGNATURE: &str = "RegistryCreated()";

const GENERIC_RESOLVER_RECORD_EVENT_SIGNATURES: &[&str] = &[
    "ABIChanged(bytes32,uint256)",
    "AddrChanged(bytes32,address)",
    "AddressChanged(bytes32,uint256,bytes)",
    "ContentChanged(bytes32,bytes32)",
    "ContenthashChanged(bytes32,bytes)",
    "DNSRecordChanged(bytes32,bytes,uint16,bytes)",
    "DNSRecordDeleted(bytes32,bytes,uint16)",
    "DNSZonehashChanged(bytes32,bytes,bytes)",
    "DataChanged(bytes32,string,string,bytes)",
    "InterfaceChanged(bytes32,bytes4,address)",
    "NameChanged(bytes32,string)",
    "TextChanged(bytes32,string,string)",
    "TextChanged(bytes32,string,string,string)",
    "VersionChanged(bytes32,uint64)",
];

pub(crate) fn generic_resolver_record_topic0s() -> Vec<String> {
    GENERIC_RESOLVER_RECORD_EVENT_SIGNATURES
        .iter()
        .map(|signature| format!("0x{}", hex::encode(keccak256(signature.as_bytes()))))
        .collect()
}

pub(crate) async fn load_match_all_topic0s_by_source_family(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let source_families = vec![
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
    ];
    let events = bigname_manifests::load_active_manifest_abi_events_by_chain_and_source_families(
        pool,
        chain,
        &source_families,
    )
    .await
    .with_context(|| format!("failed to load live match-all event topics for {chain}"))?;

    let mut topic0s_by_source_family = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        let selected = match event.source_family.as_str() {
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1 | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => true,
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1 => {
                event.canonical_signature == REGISTRY_CREATED_SIGNATURE
            }
            _ => false,
        };
        if selected && let Some(topic0) = event.topic0 {
            topic0s_by_source_family
                .entry(event.source_family)
                .or_default()
                .insert(topic0.to_ascii_lowercase());
        }
    }
    Ok(topic0s_by_source_family)
}
