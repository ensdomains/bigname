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
const NAME: &str = "released-audit.eth";
const CONTROLLER: &str = "0x00000000000000000000000000000000000000a6";
const RESOLVER: &str = "0x00000000000000000000000000000000000000a4";
const EXPIRY: i64 = 1_700_000_130;
const RELEASE_TIME: i64 = EXPIRY + 90 * 86400 + 1;
const CHAIN: &str = "ethereum-mainnet";
sol! {
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event TokenTransfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event Transfer(bytes32 indexed node, address owner);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event ControllerRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
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

// Reuse the checked-in mainnet registry v3 and registrar v1 declarations, including
// the admitted legacy controller that supplies the name preimage.
fn family_inputs() -> (Vec<ManifestInput>, Vec<AddressAdmissionInput>) {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
    )
    .unwrap();
    let mut manifests = Vec::new();
    let mut admissions = Vec::new();
    for (id, family, address, role) in [
        (931, "ens_v1_registry_l1", REGISTRY, "registry"),
        (932, "ens_v1_registrar_l1", REGISTRAR, "registrar"),
        (
            932,
            "ens_v1_registrar_l1",
            CONTROLLER,
            "legacy_registrar_controller",
        ),
    ] {
        let loaded = repository
            .manifests()
            .iter()
            .find(|m| {
                m.manifest.source_family == family
                    && m.version_tag
                        == if family == "ens_v1_registry_l1" {
                            "v3"
                        } else {
                            "v1"
                        }
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
            contract_instance_id: Uuid::from_u128(
                id as u128 + if address == CONTROLLER { 1000 } else { 0 },
            ),
            source_manifest_id: Some(id),
            role: Some(role.into()),
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        });
    }
    manifests.dedup_by_key(|manifest| manifest.manifest_id);
    (manifests, admissions)
}

// A released lease's tombstone retains its historical identity while the unchanged registry
// authority keeps distinct permission evidence. The release revokes the lease, not that registry.
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L104 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
#[tokio::test]
async fn released_materialized_registry_audits_keep_followable_resource_handles() -> Result<()> {
    let node: B256 = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let eth: B256 = bigname_lookup::ens_namehash_hex("eth")?.parse()?;
    let label = keccak256("released-audit");
    let id = U256::from_be_bytes(label.0);
    let mut controller = ControllerRegistered {
        name: "released-audit".into(),
        label,
        owner: HOLDER.parse()?,
        cost: U256::from(7),
        expires: U256::from(EXPIRY),
    }
    .encode_log_data();
    controller.topics_mut()[0] =
        keccak256("NameRegistered(string,bytes32,address,uint256,uint256)");
    let logs = vec![
        token_transfer(
            "0x0000000000000000000000000000000000000000",
            HOLDER,
            id,
            120,
        )?,
        raw(
            NewOwner {
                node: eth,
                label,
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            120,
            1,
            REGISTRY,
        ),
        raw(
            NameRegistered {
                id,
                owner: HOLDER.parse()?,
                expires: U256::from(EXPIRY),
            }
            .encode_log_data(),
            120,
            2,
            REGISTRAR,
        ),
        raw(controller, 120, 3, CONTROLLER),
        token_transfer(HOLDER, REGISTRY_OWNER, id, 121)?,
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
                    block_timestamp: timestamp(if block == 123 {
                        RELEASE_TIME
                    } else {
                        1_700_000_000 + block
                    }),
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
    let release = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "RegistrationReleased")
        .expect("actual scheduled expiry release");
    let lease = release.resource_id.unwrap();
    assert_eq!(release.block_number, Some(123));
    let registry = output
        .normalized_events
        .iter()
        .find(|e| e.block_number == Some(121) && e.event_kind == "AuthorityEpochChanged")
        .unwrap()
        .resource_id
        .unwrap();
    assert_ne!(registry, lease);
    assert!(!output.name_surfaces.is_empty());
    assert!(
        output
            .surface_bindings
            .iter()
            .any(|b| b.resource_id == registry)
    );
    assert!(
        !output
            .normalized_events
            .iter()
            .any(|e| e.block_number == Some(123) && e.resource_id == Some(registry)),
        "unchanged registry authority must retain its grants"
    );
    println!("actual release={release:?}; registry={registry}; lease={lease}");
    let database = TestDatabase::new_migrated().await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &(120..=123)
            .map(|block| {
                raw_block(
                    CHAIN,
                    &format!("0xhistory{block}"),
                    None,
                    block,
                    if block == 123 {
                        RELEASE_TIME
                    } else {
                        1_700_000_000 + block
                    },
                )
            })
            .collect::<Vec<_>>(),
    )
    .await?;
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
                let mut row = address_name_resource(
                    r.resource_id,
                    r.token_lineage_id,
                    &r.block_hash,
                    r.block_number,
                );
                row.provenance = r.provenance.clone();
                row
            })
            .collect::<Vec<_>>(),
    )
    .await?;
    upsert_test_name_surfaces(
        &database.pool,
        &output
            .name_surfaces
            .iter()
            .map(|n| {
                let mut row = name_surface(&format!("ens:{}", n.raw_name));
                row.input_name = n.raw_name.clone();
                row.canonical_display_name = n.raw_name.clone();
                row.normalized_name = n.raw_name.clone();
                row.namehash = n.namehash.clone();
                row.block_hash = n.block_hash.clone();
                row.block_number = n.block_number;
                row.provenance = n.provenance.clone();
                row
            })
            .collect::<Vec<_>>(),
    )
    .await?;
    upsert_test_surface_bindings(
        &database.pool,
        &output
            .surface_bindings
            .iter()
            .map(|b| {
                assert_eq!(b.logical_name_id, format!("ens:{node:#x}"));
                let mut row = address_name_surface_binding(
                    b.surface_binding_id,
                    &format!("ens:{NAME}"),
                    b.resource_id,
                    &b.block_hash,
                    b.block_number,
                    b.active_from.unix_timestamp(),
                );
                row.provenance = b.provenance.clone();
                row.active_to = output
                    .binding_closures
                    .iter()
                    .filter(|c| {
                        c.logical_name_id == b.logical_name_id
                            && c.except_surface_binding_id != Some(b.surface_binding_id)
                            && (c.block_number > b.block_number
                                || (c.block_number == b.block_number
                                    && (
                                        b.provenance["transaction_index"].as_i64().unwrap_or(-1),
                                        b.provenance["log_index"].as_i64().unwrap_or(-1),
                                    ) < (c.transaction_index, c.log_index)))
                    })
                    .map(|c| c.active_to)
                    .min();
                row
            })
            .collect::<Vec<_>>(),
    )
    .await?;
    let events = output
        .normalized_events
        .iter()
        .map(|e| {
            let mut row = v2_history_event(
                &e.event_identity,
                e.logical_name_id.as_deref(),
                e.resource_id,
                &e.event_kind,
                e.block_number.unwrap(),
            );
            row.log_index = e.log_index;
            row.transaction_hash = e.transaction_hash.clone();
            row.source_family = e.source_family.clone();
            row.manifest_version = e.manifest_version;
            row.derivation_kind = e.derivation_kind.clone();
            row.raw_fact_ref = e.raw_fact_ref.clone();
            row.before_state = e.before_state.clone();
            row.after_state = e.after_state.clone();
            row
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    for (block, handle, status) in [(122, lease, "active"), (123, registry, "released")] {
        Engine::new(database.pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.into(),
                target_block: block,
                affected_from_block: 120,
                affected_to_block: block,
                resume_current: None,
                mode: RunMode::Normal,
            })
            .await?;
        let mut resolver = resolver_current_row(CHAIN, RESOLVER);
        resolver.chain_positions = json!({CHAIN:{"chain_id":CHAIN,"block_number":block,"block_hash":format!("0xhistory{block}"),"timestamp":timestamp(if block==123 { RELEASE_TIME } else { 1_700_000_000+block }).format(&time::format_description::well_known::Rfc3339)?}});
        database
            .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
            .await?;
        upsert_test_resolver_current_rows(&database, &[resolver]).await?;
        let current = bigname_storage::load_name_current(&database.pool, &format!("ens:{node:#x}"))
            .await?
            .expect("actual materialized Project row");
        assert_eq!(current.resource_id, Some(registry));
        assert_eq!(current.declared_summary["registration"]["status"], status);
        assert_eq!(
            current.declared_summary["registration"]["resource_id"],
            lease.to_string(),
            "historical lease identity must remain intact"
        );
        println!("Project bound {block}: {}", current.declared_summary);
        let permissions = v2_permissions_payload_for_database(
            &database,
            &format!("/v1/permissions?address={HOLDER}"),
        )
        .await?;
        let roles = v2_resolver_payload_for_database(
            &database,
            &format!("/v1/resolvers/1/{RESOLVER}/roles"),
        )
        .await?;
        let permission = permissions["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["powers"] == json!(["registration_control"]))
            .expect("retained registry resource grant");
        let role = roles["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["address"] == HOLDER)
            .expect("retained registry resolver grant");
        assert_eq!(
            (
                permission["registration_id"].as_str(),
                role["registration_id"].as_str()
            ),
            (
                Some(handle.to_string().as_str()),
                Some(handle.to_string().as_str())
            ),
            "permissions={permissions}; roles={roles}"
        );
        let direct = v2_permissions_payload_for_database(
            &database,
            &format!("/v1/permissions?registration_id={handle}&address={HOLDER}"),
        )
        .await?;
        assert!(
            direct["data"].as_array().unwrap().contains(permission),
            "{direct}"
        );
        assert!(
            direct["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["registration_id"] == role["registration_id"]
                    && r["powers"]
                        .as_array()
                        .is_some_and(|p| p.contains(&json!("resolver_control")))),
            "{direct}"
        );
        if block == 122 {
            // Unsupported name coverage must not erase an otherwise current registration's audit handle.
            sqlx::query("UPDATE bigname_phase.name_current SET support_status='unsupported',unsupported_reason='resolver_abi_unknown' WHERE resource_id=$1").bind(registry).execute(&database.pool).await?;
            let unsupported = v2_permissions_payload_for_database(
                &database,
                &format!("/v1/permissions?registration_id={lease}&address={HOLDER}"),
            )
            .await?;
            assert!(
                unsupported["data"].as_array().unwrap().contains(permission),
                "{unsupported}"
            );
            let unsupported_roles = v2_resolver_payload_for_database(
                &database,
                &format!("/v1/resolvers/1/{RESOLVER}/roles"),
            )
            .await?;
            assert!(
                unsupported_roles["data"].as_array().unwrap().contains(role),
                "{unsupported_roles}"
            );
        } else {
            assert_eq!(permission["authority_context"], "resource_audit");
            let old = v2_permissions_payload_for_database(
                &database,
                &format!("/v1/permissions?registration_id={lease}&address={HOLDER}"),
            )
            .await?;
            assert_eq!(
                old["data"],
                json!([]),
                "released lease selected registry evidence: {old}"
            );
            let name = v2_permissions_payload_for_database(
                &database,
                &format!("/v1/permissions?name={NAME}"),
            )
            .await?;
            assert_eq!(name["data"], json!([]), "{name}");
        }
    }
    database.cleanup().await
}
