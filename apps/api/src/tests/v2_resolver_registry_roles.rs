use super::*;
use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, ManifestInput, RawBlockInput, RawLogInput,
    StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_project::{BatchRequest, Engine, RunMode};

const REGISTRY: &str = "0x00000000000000000000000000000000000000a1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000a2";
const HOLDER: &str = "0x00000000000000000000000000000000000000a3";
const REGISTRY_OWNER: &str = "0x00000000000000000000000000000000000000a5";
const RESOLVER: &str = "0x00000000000000000000000000000000000000a4";
const NAME: &str = "roles-presurface.eth";
const CHAIN: &str = "ethereum-mainnet";
sol! {
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event TokenTransfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event Transfer(bytes32 indexed node, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
}
fn raw(data: alloy_primitives::LogData, block: i64, index: i64, address: &str) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: index,
        emitting_address: address.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    }
}
fn token_transfer(from: &str, to: &str, id: U256, block: i64) -> Result<RawLogInput> {
    let mut data = TokenTransfer {
        from: from.parse()?,
        to: to.parse()?,
        tokenId: id,
    }
    .encode_log_data();
    // Solidity's ERC-721 event is named Transfer; disambiguate its Rust type from ENSRegistry.Transfer.
    data.topics_mut()[0] = keccak256("Transfer(address,address,uint256)");
    Ok(raw(data, block, 0, REGISTRAR))
}

// The numeric BaseRegistrar ABI is actively admitted on Sepolia. Reuse those exact family
// declarations on this test's standard history chain; this does not change Mainnet admission.
fn family_inputs() -> (Vec<ManifestInput>, Vec<AddressAdmissionInput>) {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )
    .unwrap();
    let mut manifests = Vec::new();
    let mut admissions = Vec::new();
    for (id, family, address, role) in [
        (931, "ens_v1_registry_l1", REGISTRY, "registry"),
        (932, "ens_v1_registrar_l1", REGISTRAR, "registrar"),
    ] {
        let loaded = repository
            .manifests()
            .iter()
            .find(|m| {
                m.manifest.source_family == family
                    && m.manifest.rollout_status == bigname_manifests::RolloutStatus::Active
            })
            .unwrap();
        let mut manifest = loaded.manifest.clone();
        manifest.chain = CHAIN.into();
        manifests.push(ManifestInput {
            manifest_id: id,
            manifest_version: manifest.manifest_version as i64,
            namespace: manifest.namespace.clone(),
            source_family: family.into(),
            chain_id: CHAIN.into(),
            deployment_label: manifest.deployment_epoch.clone(),
            normalizer_version: manifest.normalizer_version.clone(),
            payload_json: serde_json::to_string(&manifest).unwrap(),
        });
        admissions.push(AddressAdmissionInput {
            address: address.into(),
            contract_instance_id: Uuid::from_u128(id as u128),
            source_manifest_id: Some(id),
            role: Some(role.into()),
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        });
    }
    (manifests, admissions)
}

// Registry ownership and resolver selection need no label preimage. A numeric registerOnly
// grant independently retains the same node's lease without changing that registry owner.
// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L88-L93 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
#[tokio::test]
async fn resolver_roles_follow_real_presurface_registry_grants_at_the_publication() -> Result<()> {
    let node: B256 = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let other: B256 = bigname_lookup::ens_namehash_hex("unknown.child.eth")?.parse()?;
    let id = U256::from_be_bytes(keccak256("roles-presurface").0);
    let logs = vec![
        token_transfer(
            "0x0000000000000000000000000000000000000000",
            HOLDER,
            id,
            120,
        )?,
        raw(
            NameRegistered {
                id,
                owner: HOLDER.parse()?,
                expires: U256::from(1_900_000_000_u64),
            }
            .encode_log_data(),
            120,
            1,
            REGISTRAR,
        ),
        raw(
            Transfer {
                node,
                owner: REGISTRY_OWNER.parse()?,
            }
            .encode_log_data(),
            121,
            0,
            REGISTRY,
        ),
        raw(
            NewResolver {
                node,
                resolver: RESOLVER.parse()?,
            }
            .encode_log_data(),
            122,
            0,
            REGISTRY,
        ),
        raw(
            Transfer {
                node: other,
                owner: "0x00000000000000000000000000000000000000b6".parse()?,
            }
            .encode_log_data(),
            123,
            0,
            REGISTRY,
        ),
        raw(
            NewResolver {
                node: other,
                resolver: RESOLVER.parse()?,
            }
            .encode_log_data(),
            124,
            0,
            REGISTRY,
        ),
    ];
    let (manifests, admissions) = family_inputs();
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests,
            discovery_rules: vec![],
            admissions,
            prior_events: vec![],
            blocks: (120..=124)
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
    assert!(output.name_surfaces.is_empty());
    assert!(output.surface_bindings.is_empty());
    let grant = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "RegistrationGranted")
        .unwrap();
    let lease = grant.resource_id.unwrap();
    let permissions = output
        .normalized_events
        .iter()
        .filter(|e| {
            e.event_kind == "PermissionChanged"
                && e.after_state["scope"]["kind"] == "resolver"
                && e.after_state["effective_powers"]
                    .as_array()
                    .is_some_and(|p| !p.is_empty())
        })
        .collect::<Vec<_>>();
    assert_eq!(permissions.len(), 2, "{:?}", output.normalized_events);
    let registry = permissions
        .iter()
        .find(|e| e.after_state["subject"] == REGISTRY_OWNER)
        .unwrap()
        .resource_id
        .unwrap();
    assert_ne!(registry, lease);
    assert!(permissions.iter().all(|e| e.logical_name_id.is_none()));
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=124).await?;
    upsert_test_token_lineages(
        &database.pool,
        &output
            .token_lineages
            .iter()
            .map(|l| address_name_token_lineage(l.token_lineage_id, &l.block_hash, l.block_number))
            .collect::<Vec<_>>(),
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &output
            .resources
            .iter()
            .map(|r| {
                address_name_resource(
                    r.resource_id,
                    r.token_lineage_id,
                    &r.block_hash,
                    r.block_number,
                )
            })
            .collect::<Vec<_>>(),
    )
    .await?;
    let events = output
        .normalized_events
        .iter()
        .map(|e| {
            assert_eq!(e.consumer_visibility, "activated");
            let mut event = v2_history_event(
                &e.event_identity,
                e.logical_name_id.as_deref(),
                e.resource_id,
                &e.event_kind,
                e.block_number.unwrap(),
            );
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
    Engine::new(database.pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 124,
            affected_from_block: 120,
            affected_to_block: 124,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let names: i64 = sqlx::query_scalar("SELECT count(*) FROM bigname_phase.name_current")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(
        names, 0,
        "Project must not invent a name from numeric identities"
    );
    // Resolver discovery/overview support is a separate capability. Seed that declared metadata;
    // the permission resources, subject, scope, powers and provenance came from adapter + Project.
    let mut resolver = resolver_current_row(CHAIN, RESOLVER);
    resolver.chain_positions = json!({CHAIN: {"chain_id":CHAIN,"block_number":124,"block_hash":"0xhistory124","timestamp":"2023-11-14T22:15:24Z"}});
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(&database, &[resolver]).await?;
    let route = format!("/v1/resolvers/1/{RESOLVER}/roles?page_size=1");
    let first = v2_resolver_payload_for_database(&database, &route).await?;
    assert_eq!(first["page"]["total_count"], 2, "{first}");
    assert_eq!(first["data"][0]["address"], REGISTRY_OWNER, "{first}");
    assert_role_followthrough(&database, &first["data"][0], lease).await?;
    let raw = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={registry}"),
    )
    .await?;
    assert_eq!(raw["data"], json!([]), "{raw}");
    let cursor = first["page"]["next_cursor"].as_str().expect("second role");
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(CHAIN, "0xhistory140", None, 140, 1_700_000_140)],
    )
    .await?;
    let mut release = v2_history_event(
        "future-role-release",
        None,
        Some(lease),
        "RegistrationReleased",
        140,
    );
    release.source_family = "ens_v2_migration_l1".into();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[release]).await?;
    let stable = v2_resolver_payload_for_database(&database, &route).await?;
    assert_eq!(
        stable["data"], first["data"],
        "unpublished release changed role handle"
    );
    let second =
        v2_resolver_payload_for_database(&database, &format!("{route}&cursor={cursor}")).await?;
    assert_eq!(second["page"]["total_count"], 2, "{second}");
    assert_eq!(second["page"]["has_more"], false);
    let no_lease = permissions
        .iter()
        .find(|e| e.after_state["subject"] != REGISTRY_OWNER)
        .unwrap()
        .resource_id
        .unwrap();
    assert_role_followthrough(&database, &second["data"][0], no_lease).await?;
    // Move only the retained grant above Project: it must not relabel published R rows.
    sqlx::query("UPDATE bigname_phase.normalized_events SET block_number=140,block_hash='0xhistory140' WHERE event_identity=$1")
        .bind(&grant.event_identity).execute(&database.pool).await?;
    let unproven = v2_resolver_payload_for_database(&database, &route).await?;
    assert_role_followthrough(&database, &unproven["data"][0], registry).await?;
    database.cleanup().await
}

async fn assert_role_followthrough(
    database: &TestDatabase,
    role: &Value,
    expected: Uuid,
) -> Result<()> {
    assert_eq!(role["registration_id"], expected.to_string(), "{role}");
    assert!(role.get("name").is_none(), "{role}");
    assert!(role.get("grant_event").is_some(), "{role}");
    let address = role["address"].as_str().unwrap();
    let followed = v2_permissions_payload_for_database(
        database,
        &format!("/v1/permissions?registration_id={expected}&address={address}"),
    )
    .await?;
    assert!(
        followed["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["grant_scope"]["kind"] == "resolver"
                && r["grant_scope"]["detail"]["resolver"]["address"] == RESOLVER
                && r["address"] == role["address"]
                && r["powers"] == role["powers"]),
        "role {role} did not select its grant: {followed}"
    );
    Ok(())
}
