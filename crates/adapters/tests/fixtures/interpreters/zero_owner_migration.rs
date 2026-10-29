//! Shared raw input for the adapter and Interpret identity-writer tests of an unwrapped
//! ENSv1→ENSv2 migration of a `.eth` lease with no known holder. The migration receipts are
//! captured from Ethereum mainnet. Each name's earlier history is one constructed
//! legacy-controller renewal, as `zero-owner-migration.json` records under `provenance`.
//! [`registered`] gives the same names a known holder instead, for comparison.

use std::collections::BTreeMap;

use alloy_primitives::{U256, hex, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Context;
use serde_json::Value;
use time::OffsetDateTime;

use super::adapter_api::{
    AddressAdmissionInput, BatchInput, DiscoveryRuleInput, ManifestInput, RawBlockInput,
    RawLogInput,
};

const CHAIN: &str = "ethereum-mainnet";
/// The families an unwrapped migration reads, from the checked-in mainnet manifests.
const FAMILIES: &[&str] = &[
    "ens_v1_registrar_l1",
    "ens_v1_registry_l1",
    "ens_v1_wrapper_l1",
    "ens_v2_migration_l1",
    "ens_v2_registry_l1",
];
/// The `legacy_registrar_controller` of the mainnet `ens_v1_registrar_l1` manifest.
const LEGACY_CONTROLLER: &str = "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5";

sol! {
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
    event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
}

pub fn fixture() -> anyhow::Result<Value> {
    Ok(serde_json::from_str(include_str!(
        "zero-owner-migration.json"
    ))?)
}

fn quantity(value: &Value) -> anyhow::Result<i64> {
    let text = value.as_str().context("fixture quantity")?;
    Ok(i64::from_str_radix(
        text.strip_prefix("0x").unwrap_or(text),
        16,
    )?)
}

fn block(number: i64, hash: String, timestamp: i64) -> anyhow::Result<RawBlockInput> {
    Ok(RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: hash,
        block_number: number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(timestamp)?,
        canonicality_state: "canonical".to_owned(),
    })
}

/// The captured receipts named in `receipts`, under the checked-in mainnet manifests at their own
/// addresses and start blocks. The block before each receipt's block is constructed: it holds one
/// legacy-controller `NameRenewed` per name the receipt migrates, in the fixture's name order.
pub fn input(receipts: &[&str]) -> anyhow::Result<BatchInput> {
    build(receipts, false)
}

/// As [`input`], but each name's constructed history is a legacy-controller `NameRegistered` to
/// the holder that migrates it, at the same position, so its lease has a known holder.
pub fn registered(receipts: &[&str]) -> anyhow::Result<BatchInput> {
    build(receipts, true)
}

fn build(receipts: &[&str], registered: bool) -> anyhow::Result<BatchInput> {
    let fixture = fixture()?;
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
    )?;
    let mut manifests = Vec::new();
    let mut admissions = Vec::new();
    let mut discovery_rules = Vec::new();
    for (index, family) in FAMILIES.iter().enumerate() {
        let source = &repository
            .manifests()
            .iter()
            .find(|loaded| {
                loaded.manifest.chain == CHAIN
                    && loaded.manifest.source_family == *family
                    && loaded.manifest.rollout_status.is_active()
            })
            .with_context(|| format!("active mainnet manifest {family}"))?
            .manifest;
        let manifest_id = 900 + i64::try_from(index)?;
        for (contract_index, contract) in source.contracts.iter().enumerate() {
            admissions.push(AddressAdmissionInput {
                address: contract.address.to_ascii_lowercase(),
                contract_instance_id: format!(
                    "00000000-0000-0000-0000-{:012x}",
                    manifest_id * 100 + i64::try_from(contract_index)?,
                )
                .parse()?,
                source_manifest_id: Some(manifest_id),
                role: Some(contract.role.clone()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(i64::try_from(contract.start_block.unwrap_or(0))?),
                active_to_block: None,
            });
        }
        discovery_rules.extend(
            source
                .discovery_rules
                .iter()
                .map(|rule| DiscoveryRuleInput {
                    manifest_id,
                    edge_kind: rule.edge_kind.clone(),
                    from_role: Some(rule.from_role.clone()),
                    admission: rule.admission.clone(),
                }),
        );
        manifests.push(ManifestInput {
            manifest_id,
            manifest_version: i64::try_from(source.manifest_version)?,
            namespace: source.namespace.clone(),
            source_family: source.source_family.clone(),
            chain_id: CHAIN.to_owned(),
            deployment_label: source.deployment_epoch.clone(),
            normalizer_version: source.normalizer_version.clone(),
            payload_json: serde_json::to_string(source)?,
        });
    }
    let mut blocks = BTreeMap::new();
    let mut raw_logs = Vec::new();
    for key in receipts {
        let receipt = &fixture["receipts"][*key];
        let number = quantity(&receipt["blockNumber"])?;
        let timestamp = quantity(&receipt["blockTimestamp"])?;
        blocks.insert(
            number,
            block(
                number,
                receipt["blockHash"]
                    .as_str()
                    .context("block hash")?
                    .to_owned(),
                timestamp,
            )?,
        );
        for log in receipt["logs"].as_array().context("receipt logs")? {
            raw_logs.push(RawLogInput {
                chain_id: CHAIN.to_owned(),
                block_hash: log["blockHash"]
                    .as_str()
                    .context("log block hash")?
                    .to_owned(),
                block_number: quantity(&log["blockNumber"])?,
                block_timestamp: OffsetDateTime::from_unix_timestamp(quantity(
                    &log["blockTimestamp"],
                )?)?,
                canonicality_state: "canonical".to_owned(),
                transaction_hash: log["transactionHash"]
                    .as_str()
                    .context("transaction hash")?
                    .to_owned(),
                transaction_index: quantity(&log["transactionIndex"])?,
                log_index: quantity(&log["logIndex"])?,
                emitting_address: log["address"].as_str().context("emitter")?.to_owned(),
                topics: log["topics"]
                    .as_array()
                    .context("topics")?
                    .iter()
                    .map(|topic| topic.as_str().context("topic").map(str::to_owned))
                    .collect::<anyhow::Result<_>>()?,
                data: hex::decode(log["data"].as_str().context("log data")?)?,
            });
        }
        // Constructed, not captured: see the fixture's provenance.
        let history = number - 1;
        let history_block = block(history, format!("0x{history:064x}"), timestamp - 12)?;
        let names = fixture["names"].as_array().context("fixture names")?;
        for (log_index, name) in names
            .iter()
            .filter(|name| name["receipt"] == *key)
            .enumerate()
        {
            let label = name["label"].as_str().context("label")?;
            let expires = U256::from(name["v2_expiry"].as_u64().context("expiry")?);
            let history_log = if registered {
                NameRegistered {
                    name: label.to_owned(),
                    label: keccak256(label.as_bytes()),
                    owner: name["holder"].as_str().context("holder")?.parse()?,
                    cost: U256::ZERO,
                    expires,
                }
                .encode_log_data()
            } else {
                NameRenewed {
                    name: label.to_owned(),
                    label: keccak256(label.as_bytes()),
                    cost: U256::ZERO,
                    expires,
                }
                .encode_log_data()
            };
            raw_logs.push(RawLogInput {
                chain_id: CHAIN.to_owned(),
                block_hash: history_block.block_hash.clone(),
                block_number: history,
                block_timestamp: history_block.block_timestamp,
                canonicality_state: "canonical".to_owned(),
                transaction_hash: format!("0x{:064x}", history << 8),
                transaction_index: 0,
                log_index: i64::try_from(log_index)?,
                emitting_address: LEGACY_CONTROLLER.to_owned(),
                topics: history_log
                    .topics()
                    .iter()
                    .map(|topic| format!("{topic:#x}"))
                    .collect(),
                data: history_log.data.to_vec(),
            });
        }
        blocks.insert(history, history_block);
    }
    raw_logs.sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    Ok(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        admissions,
        discovery_rules,
        prior_events: Vec::new(),
        blocks: blocks.into_values().collect(),
        raw_logs,
    })
}
