//! Shared raw fixture input for adapter and strict Interpret identity-writer tests.
//! Captured registration403 precedes reservation407; source JSON records exact receipt provenance.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::hex;
use anyhow::Context;
use serde_json::Value;
use time::OffsetDateTime;

use super::adapter_api::{
    AddressAdmissionInput, BatchInput, BatchOutput, DiscoveryRuleInput, ManifestInput,
    RawBlockInput, RawLogInput,
};

pub const CHAIN: &str = "ethereum-sepolia";
pub const REGISTRATION_BLOCK: i64 = 403;
pub const RESERVATION_BLOCK: i64 = 407;
pub const PRE_BLOCK: i64 = 414;
pub const MIGRATION_BLOCK: i64 = 415;
pub const AFTER_EXPIRY_BLOCK: i64 = 416;

pub fn fixture() -> anyhow::Result<Value> {
    Ok(serde_json::from_str(include_str!(
        "numeric-short-lease-migration.json"
    ))?)
}

fn integer(value: &Value) -> anyhow::Result<i64> {
    value.as_i64().map(Ok).unwrap_or_else(|| {
        value
            .as_str()
            .context("fixture integer")?
            .parse()
            .map_err(Into::into)
    })
}

/// Uses shipped Sepolia ABI/event admission, with disposable addresses and zero start blocks
/// from the retained fixture. It introduces no controller logs or registrar predecessor state.
pub fn input() -> anyhow::Result<BatchInput> {
    let fixture = fixture()?;
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let local = fixture["local_contracts"]
        .as_object()
        .context("local contracts")?;
    let mut manifests = Vec::new();
    let mut admissions = Vec::new();
    let mut discovery_rules = Vec::new();
    for (index, (family, roles)) in local.iter().enumerate() {
        let mut source = repository
            .manifests()
            .iter()
            .find(|loaded| loaded.manifest.source_family == *family)
            .with_context(|| format!("shipped fixture source {family}"))?
            .manifest
            .clone();
        let manifest_id = 800 + index as i64;
        let roles = roles.as_object().context("fixture contract roles")?;
        let mut aliases = BTreeMap::new();
        source.contracts.retain_mut(|contract| {
            let Some(address) = roles.get(&contract.role).and_then(Value::as_str) else {
                return false;
            };
            aliases.insert(contract.address.clone(), address.to_owned());
            contract.address = address.to_owned();
            contract.start_block = Some(0);
            true
        });
        for root in &mut source.roots {
            if let Some(address) = aliases.get(&root.address) {
                root.address = address.clone();
            }
            root.start_block = Some(0);
        }
        if family == "ens_v2_migration_l1" {
            source.correlation_addresses.insert(
                "ens_v1_base_registrar".to_owned(),
                local["ens_v1_registrar_l1"]["registrar"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
            source.correlation_addresses.insert(
                "ens_v1_name_wrapper".to_owned(),
                local["ens_v1_wrapper_l1"]["name_wrapper"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
        }
        for (contract_index, contract) in source.contracts.iter().enumerate() {
            admissions.push(AddressAdmissionInput {
                address: contract.address.clone(),
                contract_instance_id: format!(
                    "00000000-0000-0000-0000-{:012x}",
                    manifest_id * 100 + contract_index as i64,
                )
                .parse()?,
                source_manifest_id: Some(manifest_id),
                role: Some(contract.role.clone()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
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
            manifest_version: source.manifest_version as i64,
            namespace: source.namespace.clone(),
            source_family: source.source_family.clone(),
            chain_id: CHAIN.to_owned(),
            deployment_label: source.deployment_epoch.clone(),
            normalizer_version: source.normalizer_version.clone(),
            payload_json: serde_json::to_string(&source)?,
        });
    }
    let mut raw_logs = Vec::new();
    let mut blocks = BTreeMap::new();
    for group in fixture["receipts"]
        .as_array()
        .context("captured receipts")?
    {
        let receipt = &group["receipt"];
        let number = integer(&receipt["blockNumber"])?;
        blocks.insert(
            number,
            RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: receipt["blockHash"]
                    .as_str()
                    .context("block hash")?
                    .to_owned(),
                block_number: number,
                block_timestamp: OffsetDateTime::from_unix_timestamp(integer(
                    &receipt["blockTimestamp"],
                )?)?,
                canonicality_state: "canonical".to_owned(),
            },
        );
        for log in receipt["logs"].as_array().context("captured logs")? {
            raw_logs.push(RawLogInput {
                chain_id: CHAIN.to_owned(),
                block_hash: log["blockHash"]
                    .as_str()
                    .context("log block hash")?
                    .to_owned(),
                block_number: integer(&log["blockNumber"])?,
                block_timestamp: OffsetDateTime::from_unix_timestamp(integer(
                    &log["blockTimestamp"],
                )?)?,
                canonicality_state: "canonical".to_owned(),
                transaction_hash: log["transactionHash"]
                    .as_str()
                    .context("transaction hash")?
                    .to_owned(),
                transaction_index: integer(&log["transactionIndex"])?,
                log_index: integer(&log["logIndex"])?,
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
    }
    let checkpoint = &fixture["pre_block"];
    blocks.insert(
        PRE_BLOCK,
        RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: checkpoint["hash"].as_str().unwrap().to_owned(),
            block_number: PRE_BLOCK,
            block_timestamp: OffsetDateTime::from_unix_timestamp(integer(
                &checkpoint["timestamp"],
            )?)?,
            canonicality_state: "canonical".to_owned(),
        },
    );
    // The failed chain run never reached this action. Only the empty time boundary is constructed.
    blocks.insert(
        AFTER_EXPIRY_BLOCK,
        RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: format!("0x{:064x}", AFTER_EXPIRY_BLOCK),
            block_number: AFTER_EXPIRY_BLOCK,
            block_timestamp: OffsetDateTime::from_unix_timestamp(
                integer(&fixture["v1_expiry"])? + 1,
            )?,
            canonicality_state: "canonical".to_owned(),
        },
    );
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

pub fn range(input: &BatchInput, from: i64, to: i64) -> BatchInput {
    let mut range = input.clone();
    range
        .blocks
        .retain(|block| (from..=to).contains(&block.block_number));
    range
        .raw_logs
        .retain(|raw| (from..=to).contains(&raw.block_number));
    range
}

pub fn assert_receipt_order(input: &BatchInput) {
    assert!(
        input.prior_events.is_empty(),
        "fixture must not seed registrar state"
    );
    let counts = |block| {
        input
            .raw_logs
            .iter()
            .filter(|raw| raw.block_number == block)
            .count()
    };
    assert_eq!(counts(REGISTRATION_BLOCK), 3);
    assert_eq!(counts(RESERVATION_BLOCK), 2);
    assert_eq!(counts(MIGRATION_BLOCK), 10);
    assert_eq!(
        input
            .raw_logs
            .iter()
            .filter(|raw| raw.block_number == MIGRATION_BLOCK)
            .map(|raw| raw.log_index)
            .collect::<Vec<_>>(),
        (0..10).collect::<Vec<_>>()
    );
    let registrar = input
        .manifests
        .iter()
        .find(|manifest| manifest.source_family == "ens_v1_registrar_l1")
        .unwrap();
    let registrar_roles = input
        .admissions
        .iter()
        .filter(|admission| admission.source_manifest_id == Some(registrar.manifest_id))
        .filter_map(|admission| admission.role.as_deref())
        .collect::<BTreeSet<_>>();
    assert_eq!(registrar_roles, BTreeSet::from(["registrar"]));
}

pub fn registrar_grant(output: &BatchOutput) -> &super::adapter_api::NormalizedEvent {
    output
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "RegistrationGranted"
                && event.block_number == Some(REGISTRATION_BLOCK)
        })
        .expect("direct numeric receipt must produce its real registrar lease")
}
