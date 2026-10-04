use super::*;
use alloy_primitives::U256;
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, DiscoveryRuleInput, ManifestInput, RawBlockInput, RawLogInput,
    StateCacheCapacity, prepare_schema_v2_batch_incremental,
};

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x00000000000000000000000000000000000000d4";
const HOLDER: &str = "0x00000000000000000000000000000000000000e1";
const REVOKED: &str = "0x00000000000000000000000000000000000000e2";
const LABEL_HOLDER: &str = "0x00000000000000000000000000000000000000e3";
const INSTANCE: u128 = 0x941;

sol! {
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
}

fn roles_changed(resource: u64, account: &str, old: U256, new: U256, block: i64) -> Result<RawLogInput> {
    let data = EACRolesChanged {
        resource: U256::from(resource),
        account: account.parse()?,
        oldRoleBitmap: old,
        newRoleBitmap: new,
    }
    .encode_log_data();
    Ok(RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: 0,
        emitting_address: REGISTRY.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    })
}

/// The admitted Sepolia ENSv2 registry family declaration, moved onto this test's chain.
fn registry_family() -> (ManifestInput, AddressAdmissionInput) {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )
    .unwrap();
    let loaded = repository
        .manifests()
        .iter()
        .find(|m| {
            m.manifest.source_family == "ens_v2_registry_l1"
                && m.manifest.rollout_status == bigname_manifests::RolloutStatus::Active
        })
        .unwrap();
    let mut manifest = loaded.manifest.clone();
    manifest.chain = CHAIN.into();
    (
        ManifestInput {
            manifest_id: INSTANCE as i64,
            manifest_version: manifest.manifest_version as i64,
            namespace: manifest.namespace.clone(),
            source_family: "ens_v2_registry_l1".into(),
            chain_id: CHAIN.into(),
            deployment_label: manifest.deployment_epoch.clone(),
            normalizer_version: manifest.normalizer_version.clone(),
            payload_json: serde_json::to_string(&manifest).unwrap(),
        },
        AddressAdmissionInput {
            address: REGISTRY.into(),
            contract_instance_id: Uuid::from_u128(INSTANCE),
            source_manifest_id: Some(INSTANCE as i64),
            role: Some("registry".into()),
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        },
    )
}

// Root role changes are EACRolesChanged on resource 0.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L277-L284 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn registry_selector_reads_the_root_resource_the_adapter_derives() -> Result<()> {
    let bit = |index: usize| U256::from(1) << index;
    let logs = vec![
        roles_changed(0, HOLDER, U256::ZERO, bit(0) | bit(128), 120)?,
        roles_changed(0, REVOKED, U256::ZERO, bit(16), 121)?,
        roles_changed(0, REVOKED, bit(16), U256::ZERO, 122)?,
        roles_changed(5, LABEL_HOLDER, U256::ZERO, bit(16), 123)?,
    ];
    let (manifest, admission) = registry_family();
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests: vec![manifest],
            discovery_rules: ["subregistry", "resolver", "registry_announcement"]
                .map(|edge_kind| DiscoveryRuleInput {
                    manifest_id: INSTANCE as i64,
                    edge_kind: edge_kind.into(),
                    from_role: Some("registry".into()),
                    admission: "reachable_from_root".into(),
                })
                .into(),
            admissions: vec![admission],
            prior_events: vec![],
            blocks: (120..=123)
                .map(|block| RawBlockInput {
                    chain_id: CHAIN.into(),
                    block_hash: format!("0xhistory{block}"),
                    block_number: block,
                    block_timestamp: timestamp(1_700_000_000 + block),
                    canonicality_state: "canonical".into(),
                })
                .collect(),
            raw_logs: logs,
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    let root_events = output
        .normalized_events
        .iter()
        .filter(|e| e.event_kind == "RootPermissionChanged")
        .collect::<Vec<_>>();
    assert_eq!(root_events.len(), 3, "{:?}", output.normalized_events);
    let adapter_root = root_events[0].resource_id.unwrap();
    assert!(root_events.iter().all(|e| e.resource_id == Some(adapter_root)));

    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=123).await?;
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1, $2, 'contract')")
        .bind(Uuid::from_u128(INSTANCE)).bind(CHAIN).execute(&database.pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number)
        VALUES ($1, $2, $3, 0)").bind(Uuid::from_u128(INSTANCE)).bind(CHAIN).bind(REGISTRY).execute(&database.pool).await?;
    let computed = bigname_storage::load_registry_root_resource(&database.pool, CHAIN, REGISTRY, 123)
        .await?
        .expect("the active instance has a root resource");
    assert_eq!(computed.resource_id, adapter_root);
    assert!(!computed.migration_registry);

    upsert_test_token_lineages(&database.pool, &output.token_lineages.iter()
        .map(|l| address_name_token_lineage(l.token_lineage_id, &l.block_hash, l.block_number))
        .collect::<Vec<_>>()).await?;
    upsert_test_resources(&database.pool, &output.resources.iter()
        .map(|r| address_name_resource(r.resource_id, r.token_lineage_id, &r.block_hash, r.block_number))
        .collect::<Vec<_>>()).await?;
    let events = output
        .normalized_events
        .iter()
        .map(|e| {
            let mut event = v2_history_event(&e.event_identity, e.logical_name_id.as_deref(),
                e.resource_id, &e.event_kind, e.block_number.unwrap());
            event.log_index = e.log_index;
            event.transaction_hash = e.transaction_hash.clone();
            event.source_family = e.source_family.clone();
            event.manifest_version = e.manifest_version;
            event.derivation_kind = e.derivation_kind.clone();
            event.raw_fact_ref = e.raw_fact_ref.clone();
            event.before_state = e.before_state.clone();
            event.after_state = e.after_state.clone();
            event
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families_on(&database.pool, CHAIN, 123).await?;
    database.seed_snapshot_selector_chain_positions(&json!({CHAIN: {
        "chain_id":CHAIN,"block_number":123,"block_hash":"0xhistory123","timestamp":"2023-11-14T22:15:23Z"
    }})).await?;

    let page = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registry=1:{REGISTRY}")).await?;
    assert_eq!(page["data"], json!([{
        "address": HOLDER,
        "grant_scope": {"kind":"root", "detail":{"registry":{"chain_id":1, "address":REGISTRY}}},
        "powers": ["registrar", "admin_registrar"],
        "registration_id": adapter_root.to_string(),
        "authority_context": "resource_audit",
    }]), "{page}");
    assert!(page["meta"].get("completeness").is_none(), "{page}");
    database.cleanup().await
}
