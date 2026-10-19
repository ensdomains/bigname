//! Mainnet ENSv2 deployment provenance checks against the pinned official artifacts.
use std::{collections::BTreeSet, path::PathBuf};

use alloy_json_abi::{Event, JsonAbi};
use anyhow::{Context, Result};
use serde_json::Value;

use crate::{ResolverReadFeature, SourceManifest, load_repository, normalize_address};

const DEPLOYMENTS: &str = ".refs/ens_v2_mainnet_20261002/contracts/deployments";
const EPOCH: &str = "ens_v2_mainnet_20261002";
/// The folders holding the contracts that the 2026-10-13 and 2026-10-18 redeploys replaced.
const REPLACED: [&str; 2] = ["mainnet-root-20261013", "mainnet-redeploy-20261018"];

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_artifact(path: PathBuf) -> Result<Value> {
    serde_json::from_slice(&std::fs::read(&path).with_context(|| path.display().to_string())?)
        .context("deployment artifact JSON")
}

fn artifact(name: &str) -> Result<Value> {
    read_artifact(
        workspace()
            .join(DEPLOYMENTS)
            .join("mainnet")
            .join(format!("{name}.json")),
    )
}

fn address(value: &Value) -> Result<String> {
    Ok(normalize_address(
        value["address"].as_str().context("artifact address")?,
    ))
}

fn block(value: &Value) -> Result<u64> {
    let number = value["receipt"]["blockNumber"]
        .as_str()
        .context("receipt block")?;
    Ok(u64::from_str_radix(number.trim_start_matches("0x"), 16)?)
}

/// Constructor argument `index` (from 0) of an artifact whose arguments are all addresses or
/// integers.
fn argument(value: &Value, index: usize) -> Result<String> {
    let data = value["argsData"]
        .as_str()
        .context("argsData")?
        .trim_start_matches("0x");
    let word = data
        .get(index * 64..(index + 1) * 64)
        .context("constructor argument")?;
    Ok(format!("0x{}", &word[24..]))
}

fn active(family: &str) -> Result<SourceManifest> {
    let repository = load_repository(workspace().join("manifests/mainnet"))?;
    let mut active = repository
        .manifests()
        .iter()
        .filter(|m| m.manifest.source_family == family && m.manifest.rollout_status.is_active());
    let manifest = active.next().context("active family")?.manifest.clone();
    anyhow::ensure!(active.next().is_none(), "one active {family}");
    Ok(manifest)
}

fn declared(manifest: &SourceManifest, role: &str) -> Result<String> {
    Ok(normalize_address(
        &manifest
            .contracts
            .iter()
            .find(|contract| contract.role == role)
            .with_context(|| format!("{} declares {role}", manifest.source_family))?
            .address,
    ))
}

#[test]
fn official_mainnet_addresses_receipts_and_abi_match_the_pinned_deployment() -> Result<()> {
    let repository = load_repository(workspace().join("manifests/mainnet"))?;
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
        ("user_registry_implementation", "UserRegistryImpl"),
    ];
    let mut seen = BTreeSet::new();
    let mut families = BTreeSet::new();
    for loaded in repository
        .manifests()
        .iter()
        .filter(|m| m.manifest.source_family.starts_with("ens_v2_"))
    {
        let manifest = &loaded.manifest;
        assert_eq!(manifest.deployment_epoch, EPOCH);
        assert_eq!(manifest.chain, "ethereum-mainnet");
        assert!(manifest.rollout_status.is_active());
        families.insert(manifest.source_family.as_str());
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
            assert_eq!(normalize_address(&contract.address), address(&deployed)?);
            assert_eq!(contract.start_block, Some(block(&deployed)?), "{name}");
            let abi: JsonAbi = serde_json::from_value(deployed["abi"].clone())?;
            abi_events.extend(abi.events.into_values().flatten());
        }
        for root in &manifest.roots {
            let deployed = artifact(&root.name)?;
            assert_eq!(normalize_address(&root.address), address(&deployed)?);
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
        families,
        BTreeSet::from([
            "ens_v2_migration_l1",
            "ens_v2_registrar_l1",
            "ens_v2_registry_l1",
            "ens_v2_resolver_l1",
            "ens_v2_root_l1",
        ])
    );
    assert_eq!(
        seen,
        roles.iter().map(|(role, _)| (*role).to_owned()).collect()
    );
    Ok(())
}

#[test]
fn official_mainnet_keeps_canonical_v1_dependencies_and_both_execution_arms() -> Result<()> {
    let resolver = active("ens_v2_resolver_l1")?;
    let implementation = &resolver.resolver_implementations[0];
    assert_eq!(resolver.resolver_implementations.len(), 1);
    let permissioned = artifact("PermissionedResolverImpl")?;
    assert_eq!(
        normalize_address(&implementation.address),
        address(&permissioned)?
    );
    assert_eq!(implementation.start_block, Some(block(&permissioned)?));
    assert_eq!(
        implementation.read_features,
        [ResolverReadFeature::Ensip19DefaultAddress]
    );

    // Each correlation is the ENSv1 contract an active ENSv1 manifest declares, and the one the
    // ENSv2 contracts were constructed with.
    let migration = active("ens_v2_migration_l1")?;
    let wrapper = declared(&active("ens_v1_wrapper_l1")?, "name_wrapper")?;
    assert_eq!(
        normalize_address(&migration.correlation_addresses["ens_v1_name_wrapper"]),
        wrapper
    );
    for (name, index) in [
        ("UnlockedMigrationController", 0),
        ("LockedMigrationController", 0),
        ("ETHRenewerV1", 6),
    ] {
        assert_eq!(argument(&artifact(name)?, index)?, wrapper, "{name}");
    }
    assert_eq!(
        normalize_address(&migration.correlation_addresses["ens_v1_base_registrar"]),
        declared(&active("ens_v1_registrar_l1")?, "registrar")?
    );
    let registry = declared(&active("ens_v1_registry_l1")?, "registry")?;
    assert_eq!(
        normalize_address(&resolver.correlation_addresses["ens_v1_registry"]),
        registry
    );
    assert_eq!(argument(&artifact("ENSV1Resolver")?, 2)?, registry);
    let reverse = &active("ens_v1_reverse_l1")?.contracts[0];
    assert_eq!(
        normalize_address(&reverse.address),
        "0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb"
    );
    assert_eq!(reverse.start_block, Some(16_925_606));

    // The registry family's approvals are watched from the declared ETHRegistry: no registry
    // this profile admits starts earlier.
    let registry_family = active("ens_v2_registry_l1")?;
    let approvals = registry_family
        .abi
        .events
        .iter()
        .find(|event| event.name == "ApprovalForAll")
        .context("ApprovalForAll")?;
    assert_eq!(
        approvals.start_block,
        Some(block(&artifact("ETHRegistry")?)?)
    );

    let execution = active("ens_execution")?;
    assert_eq!(execution.deployment_epoch, EPOCH);
    assert_eq!(execution.verified_authority_arms(), ["ens_v1", "ens_v2"]);
    let client = execution
        .contracts
        .iter()
        .find(|contract| contract.role == "universal_resolver")
        .context("the client-facing proxy")?;
    assert_eq!(
        normalize_address(&client.address),
        address(&artifact("UpgradableUniversalResolverProxy")?)?
    );
    // The pinned artifact carries no receipt. The proxy is read from the first block of the
    // deployment, the earliest receipt any ENSv2 declaration of this profile carries.
    let first = [
        "RootRegistry",
        "ETHRegistry",
        "ETHRegistrar",
        "ENSV1Resolver",
        "PublicResolverV2",
        "PermissionedResolverImpl",
        "UnlockedMigrationController",
        "LockedMigrationController",
        "Graveyard",
        "BatchRegistrar",
        "ETHRenewerV1",
        "VerifiableFactory",
        "MigrationHelper",
        "WrapperRegistryImpl",
        "UserRegistryImpl",
        "ManagedUniversalResolverProxy",
    ]
    .into_iter()
    .map(|name| block(&artifact(name)?))
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .min();
    assert_eq!(client.start_block, first);
    assert_eq!(client.start_block, Some(26_105_292));
    let managed_artifact = artifact("ManagedUniversalResolverProxy")?;
    let managed = execution
        .contracts
        .iter()
        .find(|contract| contract.role == "universal_resolver_managed")
        .context("the managed proxy")?;
    assert_eq!(
        normalize_address(&managed.address),
        address(&managed_artifact)?
    );
    assert_eq!(managed.start_block, Some(block(&managed_artifact)?));
    assert_eq!(
        execution.universal_resolver_implementations,
        [address(&artifact("UniversalResolverV2")?)?]
    );
    assert_eq!(
        argument(&artifact("UniversalResolverV2")?, 0)?,
        declared(&active("ens_v2_root_l1")?, "root_registry")?
    );
    assert!(
        !workspace()
            .join("manifests/mainnet/ethereum/ens/ens_execution/v1.toml")
            .exists()
    );
    Ok(())
}

/// Contracts the later redeploys replaced are treated as never deployed: no mainnet manifest
/// names one of their addresses.
#[test]
fn official_mainnet_declares_no_replaced_contract() -> Result<()> {
    let mut replaced = BTreeSet::new();
    for folder in REPLACED {
        for entry in std::fs::read_dir(workspace().join(DEPLOYMENTS).join(folder))? {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
                && let Some(value) = read_artifact(path)?["address"].as_str()
            {
                replaced.insert(normalize_address(value));
            }
        }
    }
    assert_eq!(replaced.len(), 25);
    let repository = load_repository(workspace().join("manifests/mainnet"))?;
    for loaded in repository.manifests() {
        let manifest = &loaded.manifest;
        let named = manifest
            .contracts
            .iter()
            .map(|contract| contract.address.as_str())
            .chain(manifest.roots.iter().map(|root| root.address.as_str()))
            .chain(
                manifest
                    .resolver_implementations
                    .iter()
                    .map(|implementation| implementation.address.as_str()),
            )
            .chain(
                manifest
                    .universal_resolver_implementations
                    .iter()
                    .map(String::as_str),
            )
            .chain(manifest.correlation_addresses.values().map(String::as_str));
        for address in named {
            assert!(
                !replaced.contains(&normalize_address(address)),
                "{} names replaced contract {address}",
                loaded.relative_path.display()
            );
        }
    }
    Ok(())
}
