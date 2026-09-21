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
const NAME: &str = "token-presurface.eth";
const CHAIN: &str = "ethereum-mainnet";
sol! {
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event TokenTransfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event Transfer(bytes32 indexed node, address owner);
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

// Numeric registration retains the token identity without a name surface; token transfer
// itself does not reclaim registry ownership.
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
#[tokio::test]
async fn token_only_handoff_follows_actual_nodeless_authority_epoch() -> Result<()> {
    let node: B256 = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let id = U256::from_be_bytes(keccak256("token-presurface").0);
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
        token_transfer(HOLDER, REGISTRY_OWNER, id, 122)?,
        token_transfer(REGISTRY_OWNER, HOLDER, id, 123)?,
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
    assert!(output.name_surfaces.is_empty() && output.surface_bindings.is_empty());
    let lease = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "RegistrationGranted")
        .unwrap()
        .resource_id
        .unwrap();
    let registry = output
        .normalized_events
        .iter()
        .find(|e| e.block_number == Some(121) && e.event_kind == "AuthorityEpochChanged")
        .unwrap()
        .resource_id
        .unwrap();
    assert_ne!(lease, registry);
    for (block, resource, kind) in [(122, lease, "registrar"), (123, registry, "registry_only")] {
        let epoch = output
            .normalized_events
            .iter()
            .find(|e| e.block_number == Some(block) && e.event_kind == "AuthorityEpochChanged")
            .unwrap();
        assert_eq!(epoch.resource_id, Some(resource));
        assert_eq!(epoch.after_state["authority_kind"], kind);
        assert!(epoch.logical_name_id.is_none());
        for field in ["node", "child_node", "namehash"] {
            assert!(
                epoch.after_state.get(field).is_none(),
                "producer unexpectedly supplied {field}: {epoch:?}"
            );
        }
        let companion = output
            .normalized_events
            .iter()
            .find(|e| e.block_number == Some(block) && e.event_kind == "TokenControlTransferred")
            .unwrap();
        assert_eq!(companion.after_state["namehash"], format!("{node:#x}"));
        assert_eq!(companion.transaction_hash, epoch.transaction_hash);
        assert_eq!(companion.log_index, epoch.log_index);
        assert_eq!(
            companion.raw_fact_ref["emitting_address"],
            epoch.raw_fact_ref["emitting_address"]
        );
        println!("actual producer block {block}: epoch={epoch:?} companion={companion:?}");
    }
    let database = TestDatabase::new_migrated().await?;
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
            assert_eq!(e.transaction_index, Some(0));
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
    // All observations, including both token handoffs, are activated before publication.
    // Only the published bound may affect the mapping or the permission projection.
    for (block, expected_control) in [(121, registry), (122, lease), (123, registry)] {
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
        database.seed_snapshot_selector_chain_positions(&json!({CHAIN:{"chain_id":CHAIN,"block_number":block,"block_hash":format!("0xhistory{block}"),"timestamp":"2023-11-14T22:15:23Z"}})).await?;
        let names: i64 = sqlx::query_scalar("SELECT count(*) FROM bigname_phase.name_current")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(names, 0);
        let projected:Vec<Uuid>=sqlx::query_scalar("SELECT resource_id FROM bigname_phase.permissions_current WHERE subject=$1 AND scope='resource' AND effective_powers ? 'resource_control'").bind(REGISTRY_OWNER).fetch_all(&database.pool).await?;
        assert_eq!(
            projected,
            vec![expected_control],
            "actual Project grant at block {block}"
        );
        assert_followthrough(&database, lease, block).await?;
        let map = bigname_storage::load_registry_permission_registration_map(
            &database.pool,
            &[registry],
            None,
            &std::collections::BTreeMap::from([(CHAIN.into(), block)]),
        )
        .await?;
        assert_eq!(
            map.get(&registry).copied(),
            (expected_control == registry).then_some(lease),
            "block {block}"
        );
    }
    assert_exact_companion_exclusions(&database, registry, lease).await?;
    database.cleanup().await
}

async fn assert_followthrough(database: &TestDatabase, lease: Uuid, block: i64) -> Result<()> {
    let address = v2_permissions_payload_for_database(
        database,
        &format!("/v1/permissions?address={REGISTRY_OWNER}"),
    )
    .await?;
    let row = address["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["powers"]
                .as_array()
                .is_some_and(|p| p.contains(&json!("registration_control")))
        })
        .unwrap_or_else(|| panic!("no produced grant at {block}: {address}"));
    assert_eq!(
        row["registration_id"],
        lease.to_string(),
        "block {block}: {row}"
    );
    let explicit = v2_permissions_payload_for_database(
        database,
        &format!("/v1/permissions?registration_id={lease}"),
    )
    .await?;
    assert!(
        explicit["data"].as_array().unwrap().contains(row),
        "block {block}: address advertises {row}, following L returns {explicit}"
    );
    let both = v2_permissions_payload_for_database(
        database,
        &format!("/v1/permissions?registration_id={lease}&address={REGISTRY_OWNER}"),
    )
    .await?;
    assert_eq!(both["data"], address["data"], "block {block}: {both}");
    Ok(())
}

async fn assert_exact_companion_exclusions(
    database: &TestDatabase,
    registry: Uuid,
    lease: Uuid,
) -> Result<()> {
    let identity:String=sqlx::query_scalar("SELECT event_identity FROM normalized_events WHERE block_number=122 AND event_kind='TokenControlTransferred'").fetch_one(&database.pool).await?;
    let original: Value =
        sqlx::query_scalar("SELECT to_jsonb(ne) FROM normalized_events ne WHERE event_identity=$1")
            .bind(&identity)
            .fetch_one(&database.pool)
            .await?;
    for assignment in [
        "namespace='basenames'",
        "source_family='ens_v1_registry_l1'",
        "manifest_version=2",
        "transaction_hash='0xother'",
        "transaction_index=1",
        "log_index=1",
        "consumer_visibility='candidate',migration_correlation_ids=ARRAY['r7-companion-exclusion']",
        "canonicality_state='orphaned'",
        "raw_fact_ref=raw_fact_ref || '{\"emitting_address\":\"0x00000000000000000000000000000000000000ff\"}'::jsonb",
        "raw_fact_ref=raw_fact_ref || '{\"kind\":\"raw_block\"}'::jsonb",
        "raw_fact_ref=raw_fact_ref || '{\"chain_id\":\"ethereum-sepolia\"}'::jsonb",
    ] {
        sqlx::query(&format!(
            "UPDATE normalized_events SET {assignment} WHERE event_identity=$1"
        ))
        .bind(&identity)
        .execute(&database.pool)
        .await?;
        assert_relation(database, registry, lease, 122, true).await?;
        sqlx::query("UPDATE normalized_events SET namespace=$2->>'namespace', source_family=$2->>'source_family', manifest_version=($2->>'manifest_version')::bigint, transaction_hash=$2->>'transaction_hash', transaction_index=($2->>'transaction_index')::bigint, log_index=($2->>'log_index')::bigint, consumer_visibility=$2->>'consumer_visibility',migration_correlation_ids=(jsonb_populate_record(NULL::bigname_phase.normalized_events,$2)).migration_correlation_ids,canonicality_state=($2->>'canonicality_state')::bigname_phase.canonicality_state,raw_fact_ref=$2->'raw_fact_ref' WHERE event_identity=$1").bind(&identity).bind(&original).execute(&database.pool).await?;
        assert_relation(database, registry, lease, 122, false).await?;
    }
    // A valid token transfer without an eligible authority epoch does not itself change control.
    sqlx::query("UPDATE normalized_events SET consumer_visibility='candidate',migration_correlation_ids=ARRAY['r7-companion-exclusion'] WHERE block_number=122 AND event_kind='AuthorityEpochChanged'").execute(&database.pool).await?;
    assert_relation(database, registry, lease, 122, true).await?;
    sqlx::query("UPDATE normalized_events SET consumer_visibility='activated',migration_correlation_ids=ARRAY[]::text[] WHERE block_number=122 AND event_kind='AuthorityEpochChanged'").execute(&database.pool).await?;
    sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state) SELECT chain_id,'0xhistory122orphan',block_number,block_timestamp,'orphaned' FROM chain_lineage WHERE chain_id=$1 AND block_hash='0xhistory122'").bind(CHAIN).execute(&database.pool).await?;
    sqlx::query("UPDATE normalized_events SET block_hash='0xhistory122orphan',raw_fact_ref=raw_fact_ref || '{\"block_hash\":\"0xhistory122orphan\"}'::jsonb WHERE block_number=122").execute(&database.pool).await?;
    assert_relation(database, registry, lease, 122, true).await?;
    sqlx::query("UPDATE normalized_events SET block_hash='0xhistory122',raw_fact_ref=raw_fact_ref || '{\"block_hash\":\"0xhistory122\"}'::jsonb WHERE block_number=122").execute(&database.pool).await?;
    // Conflicting same-log companions cannot provide an unambiguous node identity.
    sqlx::query("INSERT INTO normalized_events OVERRIDING SYSTEM VALUE SELECT (jsonb_populate_record(NULL::bigname_phase.normalized_events, $1 || jsonb_build_object('normalized_event_id',900000000001::bigint,'event_identity','ambiguous-token-companion','after_state',($1->'after_state') || jsonb_build_object('namehash',$2::text)))).*")
        .bind(&original).bind(bigname_lookup::ens_namehash_hex("another-token.eth")?).execute(&database.pool).await?;
    assert_relation(database, registry, lease, 122, true).await?;
    sqlx::query("DELETE FROM normalized_events WHERE event_identity='ambiguous-token-companion'")
        .execute(&database.pool)
        .await?;
    assert_relation(database, registry, lease, 122, false).await?;
    // R's token-only epoch must also seed forward reads when no earlier direct-node R
    // observation is eligible; recovering only the reverse lookup would miss this shape.
    sqlx::query("UPDATE normalized_events SET consumer_visibility='candidate',migration_correlation_ids=ARRAY['r7-companion-exclusion'] WHERE resource_id=$1 AND block_number<123").bind(registry).execute(&database.pool).await?;
    assert_relation(database, registry, lease, 123, true).await?;
    Ok(())
}

async fn assert_relation(
    database: &TestDatabase,
    registry: Uuid,
    lease: Uuid,
    bound: i64,
    present: bool,
) -> Result<()> {
    let bounds = std::collections::BTreeMap::from([(CHAIN.into(), bound)]);
    for (resources, requested) in [(vec![registry], None), (vec![], Some(lease))] {
        let map = bigname_storage::load_registry_permission_registration_map(
            &database.pool,
            &resources,
            requested,
            &bounds,
        )
        .await?;
        assert_eq!(
            map.get(&registry).copied(),
            present.then_some(lease),
            "bound={bound}, requested={requested:?}"
        );
    }
    Ok(())
}
