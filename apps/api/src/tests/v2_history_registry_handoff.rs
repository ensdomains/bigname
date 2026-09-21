use super::*;
use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, ManifestInput, RawBlockInput, RawLogInput,
    StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use sha2::Digest;

const REGISTRY: &str = "0x00000000000000000000000000000000000000a1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000a2";
const WRAPPER: &str = "0x00000000000000000000000000000000000000a6";
const HOLDER: &str = "0x00000000000000000000000000000000000000a3";
const REGISTRY_OWNER: &str = "0x00000000000000000000000000000000000000a5";
const RESOLVER: &str = "0x00000000000000000000000000000000000000a4";
const NAME: &str = "dormant.eth";
const CHAIN: &str = "ethereum-mainnet";
sol! {
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event TokenTransfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event Transfer(bytes32 indexed node, address owner);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
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
        (933, "ens_v1_wrapper_l1", WRAPPER, "name_wrapper"),
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

#[tokio::test]
async fn wrapper_first_materialization_keeps_dormant_registry_resolver_out_of_lease_history()
-> Result<()> {
    let node: B256 = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let parent: B256 = bigname_lookup::ens_namehash_hex("eth")?.parse()?;
    let label = keccak256("dormant");
    let id = U256::from_be_bytes(label.0);
    // registerOnly emits the mint and numeric grant without changing registry ownership.
    // wrapETH2LD then transfers the token, reclaims registry ownership, and wraps the name.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L278 @ ens_v1@91c966f)
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
        token_transfer(HOLDER, WRAPPER, id, 123)?,
        raw(
            NewOwner {
                node: parent,
                label,
                owner: WRAPPER.parse()?,
            }
            .encode_log_data(),
            123,
            1,
            REGISTRY,
        ),
        raw(
            NameWrapped {
                node,
                name: b"\x07dormant\x03eth\0".to_vec().into(),
                owner: HOLDER.parse()?,
                fuses: 1,
                expiry: 1_900_000_000,
            }
            .encode_log_data(),
            123,
            2,
            WRAPPER,
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
    let grant = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "RegistrationGranted")
        .unwrap();
    assert!(
        grant.logical_name_id.is_none(),
        "numeric grant unexpectedly had a surface"
    );
    let lease = grant.resource_id.unwrap();
    let controlling = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "ResolverChanged" && e.block_number == Some(122))
        .unwrap();
    let registry = controlling.resource_id.unwrap();
    assert_ne!(registry, lease);
    assert!(controlling.logical_name_id.is_none());
    assert!(
        output
            .normalized_events
            .iter()
            .any(|e| e.block_number == Some(123)
                && e.log_index == Some(1)
                && e.resource_id == Some(lease)
                && e.event_kind == "AuthorityEpochChanged")
    );
    assert!(
        output
            .normalized_events
            .iter()
            .all(|e| e.resource_id != Some(registry)
                || !matches!(e.event_kind.as_str(), "SurfaceBound" | "SurfaceUnbound")),
        "R must remain pre-surface"
    );
    let dormant = output
        .normalized_events
        .iter()
        .find(|e| {
            e.resource_id == Some(registry)
                && e.event_kind == "ResolverChanged"
                && e.after_state["surface_materialization"] == true
        })
        .unwrap();
    assert_eq!(dormant.after_state["authority_kind"], "registry_only");
    assert!(dormant.after_state["authority_key"].is_null());
    assert!(
        dormant
            .event_identity
            .contains(":ResolverChanged:surface-materialization:")
    );
    let binding = output
        .surface_bindings
        .iter()
        .find(|b| b.resource_id != registry)
        .unwrap();
    let wrapper = binding.resource_id;
    let wrapper_lineage = output
        .resources
        .iter()
        .find(|r| r.resource_id == wrapper)
        .unwrap()
        .token_lineage_id
        .unwrap();
    assert_eq!(
        dormant.logical_name_id.as_deref(),
        Some(format!("ens:{node:#x}").as_str())
    );

    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:dormant.eth",
        NAME,
        NAME,
        &format!("{node:#x}"),
        wrapper,
        wrapper_lineage,
        binding.surface_binding_id,
        HOLDER,
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=123).await?;
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
    sqlx::query("UPDATE bigname_phase.surface_bindings SET block_number = 123, block_hash = '0xhistory123', active_from = to_timestamp(1700000123) WHERE resource_id = $1")
        .bind(wrapper).execute(&database.pool).await?;
    // Persist the adapter's actual identity, ordering and state payloads. The fixture helper
    // supplies activated visibility; source manifest FKs are irrelevant to this read-only slice.
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
    let global = v2_history_payload_for_database(&database, "/v1/events?page_size=100").await?;
    let active_id = hex::encode(sha2::Sha256::digest(controlling.event_identity.as_bytes()));
    let dormant_id = hex::encode(sha2::Sha256::digest(dormant.event_identity.as_bytes()));
    let active_row = global["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == active_id)
        .unwrap();
    assert_eq!(active_row["registration_id"], json!(lease), "{global}");
    let all = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?name={NAME}&page_size=100"),
    )
    .await?;
    let rows = all["data"].as_array().unwrap();
    let dormant_row = rows.iter().find(|r| r["id"] == dormant_id).unwrap();
    assert_eq!(
        dormant_row["registration_id"],
        Value::Null,
        "dormant R borrowed a live lease: {all}"
    );
    let expected = rows
        .iter()
        .filter(|r| r["registration_id"] == json!(lease))
        .count();
    let filtered = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={lease}&page_size=100"),
    )
    .await?;
    assert_eq!(
        filtered["data"].as_array().unwrap().len(),
        expected,
        "{filtered}"
    );
    assert_eq!(
        filtered["page"]["total_count"],
        json!(expected),
        "{filtered}"
    );
    assert!(
        filtered["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["id"] != dormant_id),
        "{filtered}"
    );
    database.cleanup().await
}
