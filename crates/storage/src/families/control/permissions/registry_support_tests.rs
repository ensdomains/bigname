//! A declaration is read only under its own chain's reviewed epoch, and a factory origin is
//! supported exactly for the factory and implementations the active migration manifest
//! declares.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgConnection, raw_sql, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{Declaration, Declarations, Model, USER_ROLE, WRAPPER_ROLE, load};
use crate::families::{id_index_plan_tests::with_database, name::FamilyPublication};

const SEPOLIA: &str = "ethereum-sepolia";
const MAINNET: &str = "ethereum-mainnet";
const SEPOLIA_EPOCH: &str = "ens_v2_sepolia_20261001";
const MAINNET_EPOCH: &str = "ens_v2_mainnet_20261002";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/VerifiableFactory.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_FACTORY: &str = "0x77510ba162dfea3d8989e3f0928a691b345730d2";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const MAINNET_WRAPPER: &str = "0xe86a4ef4d7a210aaa1d6a9594fc7add7cd2cf888";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const MAINNET_USER: &str = "0xfaa23592aca10a6d8875b18ea86954e8eb6d8d72";
// The 2026-10-02 pair, as a manifest would declare a further pair.
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-redeploy-20261023/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const OLD_WRAPPER: &str = "0xcf3edb3f5501de63b57254d910b6782ee370039b";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-redeploy-20261026/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const OLD_USER: &str = "0x75ff9adbcfce2469b097ac68f6ae650eae1351bb";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_ETH: &str = "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/RootRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_ROOT: &str = "0x2553c09be6447beffdb86fa622ad41fb8f77a91e";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ENSV1Resolver.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_MIRROR: &str = "0x394d8eb4a4fdf599384deab25f31d111d755c398";
const SEPOLIA_FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
const SEPOLIA_WRAPPER: &str = "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880";
const SEPOLIA_ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const SEPOLIA_ROOT: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
const SEPOLIA_MIRROR: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";
/// An archived mainnet WrapperRegistry implementation that no manifest declares.
const ARCHIVED_WRAPPER: &str = "0x93b83f7861306dffa2176fd534f9729aa38e89e1";
const OTHER_REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const BLOCK: i64 = 100;

fn contract(role: &str, address: &str) -> Value {
    json!({"role": role, "address": address, "proxy_kind": "none", "start_block": 0})
}

/// The retained declaration of one active manifest, as `Declarations::load` reads it.
fn declaration(id: i64, family: &str, chain: &str, epoch: &str, contracts: Value) -> Declaration {
    Declaration {
        source_manifest_id: id,
        manifest_version: 1,
        namespace: "ens".into(),
        source_family: family.into(),
        after_state: json!({
            "rollout_status": "active",
            "manifest_payload": {
                "manifest_version": 1, "rollout_status": "active", "chain": chain,
                "namespace": "ens", "source_family": family, "deployment_epoch": epoch,
                "contracts": contracts,
            },
        }),
    }
}

fn publication(chain: &str) -> Result<FamilyPublication> {
    Ok(FamilyPublication {
        chain_id: chain.into(),
        block_number: BLOCK,
        block_hash: format!("block-{BLOCK}"),
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
        block_timestamp_json: json!("2027-01-15T08:00:00Z"),
        admission: None,
        admission_manifests: None,
    })
}

#[test]
fn a_registry_or_root_declaration_is_read_under_its_own_chains_epoch() -> Result<()> {
    let instance = Uuid::from_u128(7);
    // (case, publication chain, manifest chain, epoch, family, role, declared, read, supported)
    let cases = [
        (
            "mainnet ETHRegistry",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
            MAINNET_ETH,
            true,
        ),
        (
            "mainnet RootRegistry",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_root_l1",
            "root_registry",
            MAINNET_ROOT,
            MAINNET_ROOT,
            true,
        ),
        (
            "Sepolia ETHRegistry",
            SEPOLIA,
            SEPOLIA,
            SEPOLIA_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            SEPOLIA_ETH,
            SEPOLIA_ETH,
            true,
        ),
        (
            "Sepolia RootRegistry",
            SEPOLIA,
            SEPOLIA,
            SEPOLIA_EPOCH,
            "ens_v2_root_l1",
            "root_registry",
            SEPOLIA_ROOT,
            SEPOLIA_ROOT,
            true,
        ),
        (
            "a registry manifest's own registry",
            SEPOLIA,
            SEPOLIA,
            SEPOLIA_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            OTHER_REGISTRY,
            OTHER_REGISTRY,
            true,
        ),
        (
            "a mainnet manifest under the Sepolia epoch",
            MAINNET,
            MAINNET,
            SEPOLIA_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
            MAINNET_ETH,
            false,
        ),
        (
            "a Sepolia manifest under the mainnet epoch",
            SEPOLIA,
            SEPOLIA,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            SEPOLIA_ETH,
            SEPOLIA_ETH,
            false,
        ),
        (
            "a manifest of another chain",
            SEPOLIA,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
            MAINNET_ETH,
            false,
        ),
        (
            "the root role in the registry family",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "root_registry",
            MAINNET_ETH,
            MAINNET_ETH,
            false,
        ),
        (
            "the registry role in the root family",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_root_l1",
            "registry",
            MAINNET_ROOT,
            MAINNET_ROOT,
            false,
        ),
        (
            "an undeclared registry",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
            OTHER_REGISTRY,
            false,
        ),
        (
            "a declared factory",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_migration_l1",
            "verifiable_factory",
            MAINNET_FACTORY,
            MAINNET_FACTORY,
            false,
        ),
    ];
    let mut failures = Vec::new();
    for (case, chain, manifest_chain, epoch, family, role, declared, read, supported) in cases {
        let declarations = Declarations(vec![declaration(
            1,
            family,
            manifest_chain,
            epoch,
            json!([contract(role, declared)]),
        )]);
        let publication = publication(chain)?;
        let model = declarations
            .declared(&publication, read, instance)
            .map(|registry| registry.model);
        if model != supported.then_some(Model::Declared) {
            failures.push(format!("{case}: {model:?}"));
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn the_mirror_is_the_one_its_deployments_resolver_manifest_declares() -> Result<()> {
    let mirror = |address: &str| contract("ensv1_mirror_resolver", address);
    let mut proxied = mirror(MAINNET_MIRROR);
    proxied["proxy_kind"] = json!("transparent");
    let mut later = mirror(MAINNET_MIRROR);
    later["start_block"] = json!(BLOCK + 1);
    let resolver = |chain: &str, epoch: &str, contracts: Value| {
        declaration(1, "ens_v2_resolver_l1", chain, epoch, contracts)
    };
    // (case, publication chain, declarations, mirror)
    let cases = [
        (
            "the mainnet mirror",
            MAINNET,
            vec![resolver(
                MAINNET,
                MAINNET_EPOCH,
                json!([mirror(MAINNET_MIRROR)]),
            )],
            Some(MAINNET_MIRROR),
        ),
        (
            "the Sepolia mirror",
            SEPOLIA,
            vec![resolver(
                SEPOLIA,
                SEPOLIA_EPOCH,
                json!([mirror(SEPOLIA_MIRROR)]),
            )],
            Some(SEPOLIA_MIRROR),
        ),
        ("no resolver manifest", MAINNET, Vec::new(), None),
        (
            "a mainnet mirror under the Sepolia epoch",
            MAINNET,
            vec![resolver(
                MAINNET,
                SEPOLIA_EPOCH,
                json!([mirror(MAINNET_MIRROR)]),
            )],
            None,
        ),
        (
            "another chain's mirror",
            MAINNET,
            vec![resolver(
                SEPOLIA,
                SEPOLIA_EPOCH,
                json!([mirror(SEPOLIA_MIRROR)]),
            )],
            None,
        ),
        (
            "a mirror role outside the resolver family",
            MAINNET,
            vec![declaration(
                1,
                "ens_v2_registry_l1",
                MAINNET,
                MAINNET_EPOCH,
                json!([mirror(MAINNET_MIRROR)]),
            )],
            None,
        ),
        (
            "two mirrors",
            MAINNET,
            vec![resolver(
                MAINNET,
                MAINNET_EPOCH,
                json!([mirror(MAINNET_MIRROR), mirror(SEPOLIA_MIRROR)]),
            )],
            None,
        ),
        (
            "a proxied mirror",
            MAINNET,
            vec![resolver(MAINNET, MAINNET_EPOCH, json!([proxied]))],
            None,
        ),
        (
            "a mirror started after the publication",
            MAINNET,
            vec![resolver(MAINNET, MAINNET_EPOCH, json!([later]))],
            None,
        ),
    ];
    let mut failures = Vec::new();
    for (case, chain, declarations, expected) in cases {
        let declared = Declarations(declarations).mirror_resolver(&publication(chain)?);
        if declared.as_deref() != expected {
            failures.push(format!("{case}: {declared:?}"));
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// The same declaration under another namespace than `ens`.
fn other_namespace(mut declaration: Declaration) -> Declaration {
    declaration.namespace = "basenames".into();
    declaration.after_state["manifest_payload"]["namespace"] = json!("basenames");
    declaration
}

#[test]
fn a_declaration_under_another_namespace_is_not_read() -> Result<()> {
    let instance = Uuid::from_u128(7);
    let mut failures = Vec::new();
    for (case, family, role, address) in [
        (
            "an ETHRegistry",
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
        ),
        (
            "a RootRegistry",
            "ens_v2_root_l1",
            "root_registry",
            MAINNET_ROOT,
        ),
    ] {
        let declarations = Declarations(vec![other_namespace(declaration(
            1,
            family,
            MAINNET,
            MAINNET_EPOCH,
            json!([contract(role, address)]),
        ))]);
        let model = declarations
            .declared(&publication(MAINNET)?, address, instance)
            .map(|registry| registry.model);
        if model.is_some() {
            failures.push(format!("{case}: {model:?}"));
        }
    }
    let mirror = Declarations(vec![other_namespace(declaration(
        1,
        "ens_v2_resolver_l1",
        MAINNET,
        MAINNET_EPOCH,
        json!([contract("ensv1_mirror_resolver", MAINNET_MIRROR)]),
    ))])
    .mirror_resolver(&publication(MAINNET)?);
    if mirror.is_some() {
        failures.push(format!("a mirror: {mirror:?}"));
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[path = "registry_support_origin_tests.rs"]
mod origin;

#[path = "registry_support_departure_tests.rs"]
mod departure;
