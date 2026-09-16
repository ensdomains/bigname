//! Deployment provenance checks against the pinned official artifacts.
use std::{collections::BTreeSet, path::PathBuf};

use alloy_json_abi::{Event, JsonAbi};
use anyhow::{Context, Result};
use serde_json::Value;

use crate::{ResolverReadFeature, load_repository, normalize_address};

const ARTIFACTS: &str = ".refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia";
const EPOCH: &str = "ens_v2_sepolia_20260915";

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn artifact(name: &str) -> Result<Value> {
    let path = workspace().join(ARTIFACTS).join(format!("{name}.json"));
    serde_json::from_slice(&std::fs::read(&path).with_context(|| path.display().to_string())?)
        .context("deployment artifact JSON")
}

fn block(value: &Value) -> Result<u64> {
    let number = value["receipt"]["blockNumber"]
        .as_str()
        .context("receipt block")?;
    Ok(u64::from_str_radix(number.trim_start_matches("0x"), 16)?)
}

#[test]
fn official_sepolia_addresses_receipts_and_abi_match_the_pinned_deployment() -> Result<()> {
    let repository = load_repository(workspace().join("manifests/sepolia"))?;
    let roles = [
        ("root_registry", "RootRegistry"),
        ("registry", "ETHRegistry"),
        ("registrar", "ETHRegistrar"),
        ("ensv1_mirror_resolver", "ENSV1Resolver"),
        ("public_resolver_v2", "PublicResolverV2"),
        (
            "unlocked_migration_controller",
            "UnlockedMigrationController",
        ),
        ("locked_migration_controller", "LockedMigrationController"),
        ("graveyard", "Graveyard"),
        ("batch_registrar", "BatchRegistrar"),
        ("ens_v1_renewal_bridge", "ETHRenewerV1"),
        ("verifiable_factory", "VerifiableFactory"),
        ("migration_helper", "MigrationHelper"),
        ("wrapper_registry_implementation", "WrapperRegistryImpl"),
    ];
    let mut seen = BTreeSet::new();
    for loaded in repository
        .manifests()
        .iter()
        .filter(|m| m.manifest.source_family.starts_with("ens_v2_"))
    {
        let manifest = &loaded.manifest;
        assert_eq!(manifest.deployment_epoch, EPOCH);
        let mut abi_events = Vec::new();
        for contract in &manifest.contracts {
            let name = roles
                .iter()
                .find(|(role, _)| *role == contract.role)
                .context("unaccounted deployment role")?
                .1;
            assert!(
                seen.insert(contract.role.clone()),
                "duplicate role {}",
                contract.role
            );
            let deployed = artifact(name)?;
            assert_eq!(
                normalize_address(&contract.address),
                normalize_address(deployed["address"].as_str().context("artifact address")?)
            );
            assert_eq!(contract.start_block, Some(block(&deployed)?), "{name}");
            let abi: JsonAbi = serde_json::from_value(deployed["abi"].clone())?;
            abi_events.extend(abi.events.into_values().flatten());
        }
        for root in &manifest.roots {
            let deployed = artifact(&root.name)?;
            assert_eq!(
                normalize_address(&root.address),
                normalize_address(deployed["address"].as_str().context("root address")?)
            );
            assert_eq!(root.start_block, Some(block(&deployed)?));
        }
        // Registry instances and resolver proxies emit their implementation ABI.
        for name in match manifest.source_family.as_str() {
            "ens_v2_registry_l1" => &["UserRegistryImpl"][..],
            "ens_v2_resolver_l1" => &["PermissionedResolverImpl"][..],
            _ => &[],
        } {
            let abi: JsonAbi = serde_json::from_value(artifact(name)?["abi"].clone())?;
            abi_events.extend(abi.events.into_values().flatten());
        }
        for declared in &manifest.abi.events {
            let event = Event::parse(&declared.fragment)?;
            assert!(
                abi_events.iter().any(|upstream| {
                    upstream.signature() == event.signature()
                        && upstream.anonymous == event.anonymous
                        && upstream
                            .inputs
                            .iter()
                            .map(|input| input.indexed)
                            .eq(event.inputs.iter().map(|input| input.indexed))
                }),
                "{} has an unsupported ABI layout: {}",
                manifest.source_family,
                declared.fragment
            );
        }
    }
    assert_eq!(
        seen,
        roles.iter().map(|(role, _)| (*role).to_owned()).collect()
    );
    Ok(())
}

#[test]
fn official_sepolia_keeps_canonical_v1_dependencies_and_both_execution_arms() -> Result<()> {
    let repository = load_repository(workspace().join("manifests/sepolia"))?;
    let family = |name: &str| {
        &repository
            .manifests()
            .iter()
            .find(|m| m.manifest.source_family == name)
            .unwrap()
            .manifest
    };
    let resolver = family("ens_v2_resolver_l1");
    let implementation = &resolver.resolver_implementations[0];
    assert_eq!(resolver.resolver_implementations.len(), 1);
    assert_eq!(
        normalize_address(&implementation.address),
        normalize_address(
            artifact("PermissionedResolverImpl")?["address"]
                .as_str()
                .unwrap()
        )
    );
    assert_eq!(
        implementation.read_features,
        [ResolverReadFeature::Ensip19DefaultAddress]
    );
    let migration = family("ens_v2_migration_l1");
    for (key, source, role) in [
        ("ens_v1_name_wrapper", "ens_v1_wrapper_l1", "name_wrapper"),
        ("ens_v1_base_registrar", "ens_v1_registrar_l1", "registrar"),
    ] {
        let contract = family(source)
            .contracts
            .iter()
            .find(|c| c.role == role)
            .unwrap();
        assert_eq!(
            normalize_address(&migration.correlation_addresses[key]),
            normalize_address(&contract.address)
        );
    }
    assert_eq!(
        normalize_address(&resolver.correlation_addresses["ens_v1_registry"]),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    let reverse = &family("ens_v1_reverse_l1").contracts[0];
    assert_eq!(
        normalize_address(&reverse.address),
        "0xa0a1abcdae1a2a4a2ef8e9113ff0e02dd81dc0c6"
    );
    assert_eq!(reverse.start_block, Some(3_789_411));
    let execution = family("ens_execution");
    assert_eq!(execution.verified_authority_arms(), ["ens_v1", "ens_v2"]);
    assert_eq!(
        normalize_address(&execution.contracts[0].address),
        normalize_address(
            artifact("UpgradableUniversalResolverProxy")?["address"]
                .as_str()
                .unwrap()
        )
    );
    assert_eq!(
        execution.contracts[0].start_block, None,
        "no invented creation receipt for the long-lived proxy"
    );
    assert!(!workspace().join("manifests/sepolia-hackathon").exists());
    Ok(())
}
