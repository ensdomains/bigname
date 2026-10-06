//! Local fixture declarations retain the checked-in Sepolia admission surface.
//! The generated addresses and block floors are synthetic, not deployments.

use std::{collections::BTreeMap, fs, path::Path};

use alloy_primitives::{Address, keccak256};
use anyhow::{Context, Result, ensure};
use bigname_manifests::{ManifestRepository, load_repository};
use serde::Serialize;

pub(super) const CHAIN: &str = "ethereum-sepolia";
const SOURCES: &[(&str, &str)] = &[
    (
        "ens_v1_registry_l1",
        include_str!("../../../../manifests/sepolia/ethereum/ens/ens_v1_registry_l1/v1.toml"),
    ),
    (
        "ens_v1_registrar_l1",
        include_str!("../../../../manifests/sepolia/ethereum/ens/ens_v1_registrar_l1/v1.toml"),
    ),
    (
        "ens_v1_wrapper_l1",
        include_str!("../../../../manifests/sepolia/ethereum/ens/ens_v1_wrapper_l1/v1.toml"),
    ),
    (
        "ens_v1_resolver_l1",
        include_str!("../../../../manifests/sepolia/ethereum/ens/ens_v1_resolver_l1/v1.toml"),
    ),
];

#[derive(Serialize)]
pub(super) struct ManifestReceipt {
    pub(super) source_family: String,
    pub(super) source_keccak256: String,
    pub(super) fixture_keccak256: String,
}

pub(super) fn address(role: &str) -> Address {
    let digest = keccak256(format!("registry-node-scale-v1:role:{role}"));
    Address::from_slice(&digest.as_slice()[12..])
}

pub(super) fn write(root: &Path) -> Result<(ManifestRepository, Vec<ManifestReceipt>)> {
    let mut receipts = Vec::new();
    for (family, source) in SOURCES {
        let mut value: toml::Value = toml::from_str(source)?;
        let mut replacements = BTreeMap::new();
        let contracts = value["contracts"]
            .as_array_mut()
            .context("contracts missing")?;
        for contract in contracts {
            let role = contract["role"].as_str().context("role missing")?;
            let old_address = contract["address"]
                .as_str()
                .context("address missing")?
                .to_lowercase();
            let fixture = format!("{:#x}", address(role));
            replacements.insert(old_address, fixture.clone());
            contract["address"] = fixture.into();
            contract["start_block"] = 0_i64.into();
        }
        for root in value["roots"].as_array_mut().context("roots missing")? {
            let old = root["address"]
                .as_str()
                .context("root address missing")?
                .to_lowercase();
            root["address"] = replacements
                .get(&old)
                .context("unmatched root")?
                .clone()
                .into();
            root["start_block"] = 0_i64.into();
        }
        // Retain all event fragments, roles, capabilities, normalizer and family
        // metadata. Only deployment addresses and lower bounds are retargeted.
        let generated = toml::to_string_pretty(&value)?;
        let path = root.join(format!("ethereum/ens/{family}/v1.toml"));
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, &generated)?;
        receipts.push(ManifestReceipt {
            source_family: (*family).to_owned(),
            source_keccak256: format!("{:#x}", keccak256(source.as_bytes())),
            fixture_keccak256: format!("{:#x}", keccak256(generated.as_bytes())),
        });
    }
    let repository = load_repository(root)?;
    ensure!(
        repository.manifests().len() == 4,
        "fixture manifest count changed"
    );
    Ok((repository, receipts))
}

pub(super) fn require_event(
    repository: &ManifestRepository,
    emitter: Address,
    topic: &str,
    topic_count: usize,
) -> Result<()> {
    for loaded in repository.manifests() {
        let manifest = &loaded.manifest;
        for contract in &manifest.contracts {
            if !contract
                .address
                .eq_ignore_ascii_case(&format!("{emitter:#x}"))
            {
                continue;
            }
            for event in &manifest.abi.events {
                if event.topic0()?.as_deref() == Some(topic)
                    && (event.emitter_roles.is_empty()
                        || event.emitter_roles.contains(&contract.role))
                {
                    ensure!(
                        event
                            .parsed_event()?
                            .inputs
                            .iter()
                            .filter(|input| input.indexed)
                            .count()
                            + 1
                            == topic_count,
                        "fixture indexed argument layout differs from the admitted ABI"
                    );
                    return Ok(());
                }
            }
        }
    }
    anyhow::bail!("fixture event {topic} is not admitted for {emitter:#x}")
}
