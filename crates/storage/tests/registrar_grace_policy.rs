//! Registrar grace of each reviewed ETHRegistry deployment, read through the phase schema's own
//! manifest and contract-address tables.
#[path = "../../project/tests/families_support/mod.rs"]
mod families_support;

use anyhow::Result;
use bigname_storage::families::control::lifecycle::{
    AuthoritySelection, NameInput, NamePlace, load_name_facts,
};
use families_support::{CHAIN, Fixture, hash, uuid};
use serde_json::json;

const NAME: &str = "ens:0x01";

struct Deployment {
    chain: &'static str,
    label: &'static str,
    registry: &'static str,
}

// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const SEPOLIA: Deployment = Deployment {
    chain: CHAIN,
    label: "ens_v2_sepolia_20261001",
    registry: "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4",
};

// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET: Deployment = Deployment {
    chain: "ethereum-mainnet",
    label: "ens_v2_mainnet_20261002",
    registry: "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2",
};

/// A manifest version declaring the registry, its contract instance with one address range,
/// and a lifecycle triple that places `name` on that instance.
async fn declared(
    fixture: &Fixture,
    deployment: &Deployment,
    name: &str,
    version: i64,
    rollout_status: &str,
    instance: &str,
    range: (i64, Option<i64>),
) -> Result<()> {
    let payload = json!({"contracts": [{"role": "registry", "address": deployment.registry,
        "proxy_kind": "none", "start_block": 0}]});
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES ($1, 'ens', 'ens_v2_registry_l1', $2, $3, $4, 'fixture', $5, $6)
         RETURNING manifest_id",
    )
    .bind(version)
    .bind(deployment.chain)
    .bind(deployment.label)
    .bind(rollout_status)
    .bind(format!(
        "fixture/{}/{}/ens_v2_registry_l1/v{version}.toml",
        deployment.chain, deployment.label
    ))
    .bind(&payload)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1::uuid, $2, 'contract')",
    )
    .bind(instance)
    .bind(deployment.chain)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
             active_from_block_number, active_from_block_hash, active_to_block_number,
             active_to_block_hash, source_manifest_id, deactivated_at)
         VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $6 IS NULL THEN NULL ELSE now() END)",
    )
    .bind(instance)
    .bind(deployment.chain)
    .bind(deployment.registry)
    .bind(range.0)
    .bind(hash(range.0))
    .bind(range.1)
    .bind(range.1.map(hash))
    .bind(manifest)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO project_lifecycle_triple_summary (chain_id, logical_name_id,
             registry_identifier, token_id, block_number, event_identity)
         VALUES ($1, $2, $3, '1', $4, $5)",
    )
    .bind(deployment.chain)
    .bind(name)
    .bind(instance)
    .bind(range.0)
    .bind(format!("triple-{instance}"))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// The grace registries the lifecycle loader gives `name` on `chain`, which decide whether the
/// name's ENSv2 expiry carries registrar grace.
async fn grace_registries(fixture: &Fixture, chain: &str, name: &str) -> Result<Vec<String>> {
    let facts = load_name_facts(
        &fixture.pool,
        chain,
        &[NameInput {
            logical_name_id: name.into(),
            namehash: name.trim_start_matches("ens:").into(),
            selection: AuthoritySelection::default(),
            place: NamePlace::EthSecondLevel,
        }],
    )
    .await?;
    let mut registries: Vec<String> = facts[0]
        .grace_registries
        .iter()
        .map(|registry| format!("{registry:?}"))
        .collect();
    registries.sort();
    Ok(registries)
}

fn open_from(instance: &str, start: i64) -> String {
    format!("GraceRegistry {{ identifier: {instance:?}, start: {start}, end: None }}")
}

// Deprecating the declaring manifest and closing the address range is bigname bookkeeping.
// The registrar's grace is an immutable, so the closed range still carries it.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/ETHRegistrar.sol:L43 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/ETHRegistrar.sol:L97 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn a_retired_registry_range_keeps_its_registrar_grace() -> Result<()> {
    let fixture = Fixture::new("registrar_grace_policy", 8).await?;
    let (retired, current) = (uuid(0x7001), uuid(0x7002));
    declared(
        &fixture,
        &SEPOLIA,
        NAME,
        1,
        "deprecated",
        &retired,
        (1, Some(4)),
    )
    .await?;
    declared(&fixture, &SEPOLIA, NAME, 2, "active", &current, (5, None)).await?;
    assert_eq!(
        grace_registries(&fixture, CHAIN, NAME).await?,
        [
            format!("GraceRegistry {{ identifier: {retired:?}, start: 1, end: Some(4) }}"),
            open_from(&current, 5),
        ]
    );
    fixture.cleanup().await
}

// Each reviewed deployment's ETHRegistry carries grace on its own chain under its own deployment
// label. The same address declared under any other label carries none.
#[tokio::test]
async fn each_reviewed_deployment_carries_grace_under_its_own_label_only() -> Result<()> {
    let fixture = Fixture::new("registrar_grace_deployments", 8).await?;
    fixture.lineage(MAINNET.chain, 8).await?;
    let mainnet = uuid(0x7101);
    declared(
        &fixture,
        &MAINNET,
        "ens:0x11",
        1,
        "active",
        &mainnet,
        (2, None),
    )
    .await?;
    assert_eq!(
        grace_registries(&fixture, MAINNET.chain, "ens:0x11").await?,
        [open_from(&mainnet, 2)]
    );

    let sepolia = uuid(0x7102);
    declared(
        &fixture,
        &SEPOLIA,
        "ens:0x12",
        1,
        "active",
        &sepolia,
        (2, None),
    )
    .await?;
    assert_eq!(
        grace_registries(&fixture, SEPOLIA.chain, "ens:0x12").await?,
        [open_from(&sepolia, 2)]
    );

    // Deprecated with a closed range, since a chain has one active manifest per family and one
    // open range per address. The retired-range test shows that neither decides grace. The
    // deployment label and chain do.
    for (n, other) in [
        (
            0x13_u32,
            Deployment {
                label: SEPOLIA.label,
                ..MAINNET
            },
        ),
        (
            0x14,
            Deployment {
                label: "ens_v2_mainnet_custom",
                ..MAINNET
            },
        ),
        (
            0x15,
            Deployment {
                chain: SEPOLIA.chain,
                ..MAINNET
            },
        ),
    ] {
        let instance = uuid(0x7100 + n);
        let name = format!("ens:0x{n:x}");
        declared(
            &fixture,
            &other,
            &name,
            1,
            "deprecated",
            &instance,
            (2, Some(6)),
        )
        .await?;
        let loaded = grace_registries(&fixture, other.chain, &name).await?;
        assert!(
            loaded.is_empty(),
            "{} {}: {loaded:?}",
            other.chain,
            other.label
        );
    }
    fixture.cleanup().await
}
