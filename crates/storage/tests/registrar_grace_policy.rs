//! Registrar grace of the admitted Sepolia ETHRegistry, read through the phase schema's own
//! manifest and contract-address tables.
#[path = "../../project/tests/families_support/mod.rs"]
mod families_support;

use anyhow::Result;
use bigname_storage::families::control::lifecycle::{
    AuthoritySelection, NameInput, NamePlace, load_name_facts,
};
use families_support::{CHAIN, Fixture, hash, uuid};
use serde_json::json;

// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const ETH_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const NAME: &str = "ens:0x01";

/// A manifest version declaring the registry, its contract instance with one address range,
/// and a lifecycle triple that places the name on that instance.
async fn declared(
    fixture: &Fixture,
    version: i64,
    rollout_status: &str,
    instance: &str,
    range: (i64, Option<i64>),
) -> Result<()> {
    let payload = json!({"contracts": [{"role": "registry", "address": ETH_REGISTRY,
        "proxy_kind": "none", "start_block": 0}]});
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES ($1, 'ens', 'ens_v2_registry_l1', $2, 'ens_v2_sepolia_20261001', $3, 'fixture',
             $4, $5) RETURNING manifest_id",
    )
    .bind(version)
    .bind(CHAIN)
    .bind(rollout_status)
    .bind(format!("fixture/ens_v2_registry_l1/v{version}.toml"))
    .bind(&payload)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1::uuid, $2, 'contract')",
    )
    .bind(instance)
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
             active_from_block_number, active_from_block_hash, active_to_block_number,
             active_to_block_hash, source_manifest_id, deactivated_at)
         VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $6 IS NULL THEN NULL ELSE now() END)",
    )
    .bind(instance)
    .bind(CHAIN)
    .bind(ETH_REGISTRY)
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
    .bind(CHAIN)
    .bind(NAME)
    .bind(instance)
    .bind(range.0)
    .bind(format!("triple-{instance}"))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// Deprecating the declaring manifest and closing the address range is bigname bookkeeping.
// The registrar's grace is an immutable, so the closed range still carries it.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/ETHRegistrar.sol:L43 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/ETHRegistrar.sol:L97 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn a_retired_registry_range_keeps_its_registrar_grace() -> Result<()> {
    let fixture = Fixture::new("registrar_grace_policy", 8).await?;
    let (retired, current) = (uuid(0x7001), uuid(0x7002));
    declared(&fixture, 1, "deprecated", &retired, (1, Some(4))).await?;
    declared(&fixture, 2, "active", &current, (5, None)).await?;
    let facts = load_name_facts(
        &fixture.pool,
        CHAIN,
        &[NameInput {
            logical_name_id: NAME.into(),
            namehash: "0x01".into(),
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
    assert_eq!(
        registries,
        [
            format!("GraceRegistry {{ identifier: {retired:?}, start: 1, end: Some(4) }}"),
            format!("GraceRegistry {{ identifier: {current:?}, start: 5, end: None }}"),
        ]
    );
    fixture.cleanup().await
}
