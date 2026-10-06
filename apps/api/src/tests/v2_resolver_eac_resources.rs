//! Raw permission logs are decoded, published by Project, and followed through both HTTP routes.
use super::*;
use alloy_primitives::{B256, LogData, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput, RawBlockInput, RawLogInput,
    StateCacheCapacity, prepare_schema_v2_batch_incremental,
};

const CHAIN: &str = "ethereum-mainnet";
const RESOLVER: &str = "0x0000000000000000000000000000000000000253";
const IMPLEMENTATION: &str = "0x0000000000000000000000000000000000001253";
const HOLDER: &str = "0x0000000000000000000000000000000000000254";
const ADMIN: &str = "0x0000000000000000000000000000000000000255";
const LINKER: &str = "0x0000000000000000000000000000000000000256";
const MANIFEST: i64 = 9253;
sol! {
    event ResourceArgument(uint256 indexed resource, bytes arg);
    event NamedResource(uint256 indexed resource, bytes name);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event Upgraded(address indexed implementation);
}

fn raw(data: LogData, block: i64, index: i64) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: index,
        emitting_address: RESOLVER.into(),
        topics: data
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: data.data.to_vec(),
    }
}
fn roles(
    resource: U256,
    account: &str,
    old: U256,
    new: U256,
    block: i64,
    index: i64,
) -> RawLogInput {
    raw(
        EACRolesChanged {
            resource,
            account: account.parse().unwrap(),
            oldRoleBitmap: old,
            newRoleBitmap: new,
        }
        .encode_log_data(),
        block,
        index,
    )
}
fn bit(index: usize) -> U256 {
    U256::from(1) << index
}
fn argument(arg: &[u8], block: i64) -> (U256, RawLogInput) {
    let resource = U256::from_be_bytes(*keccak256(arg));
    (
        resource,
        raw(
            ResourceArgument {
                resource,
                arg: arg.to_vec().into(),
            }
            .encode_log_data(),
            block,
            0,
        ),
    )
}

fn manifest(historical: bool) -> ManifestInput {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )
    .unwrap();
    let loaded = repository
        .manifests()
        .iter()
        .find(|m| m.manifest.source_family == "ens_v2_resolver_l1")
        .unwrap();
    let mut payload = serde_json::to_value(&loaded.manifest).unwrap();
    payload["chain"] = json!(CHAIN);
    payload["contracts"] = json!([]);
    payload["resolver_implementations"] =
        json!([{"role":"permissioned_resolver", "address":IMPLEMENTATION, "start_block":0}]);
    if historical {
        // This fixture exercises the admitted historical decoder, not current deployment authority.
        // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L266-L293 @ ens_v2_sepolia_20260629@ccaeb58b)
        payload["abi"]["events"] = json!([
            {"name":"NamedResource", "fragment":"event NamedResource(uint256 indexed resource, bytes name)", "normalized_events":["PreimageObserved"]},
            {"name":"EACRolesChanged", "fragment":"event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)", "normalized_events":["PermissionChanged"]},
            {"name":"Upgraded", "fragment":"event Upgraded(address indexed implementation)", "normalized_events":["Upgraded"]}
        ]);
    }
    ManifestInput {
        manifest_id: MANIFEST,
        manifest_version: loaded.manifest.manifest_version as i64,
        namespace: "ens".into(),
        source_family: "ens_v2_resolver_l1".into(),
        chain_id: CHAIN.into(),
        deployment_label: loaded.manifest.deployment_epoch.clone(),
        normalizer_version: loaded.manifest.normalizer_version.clone(),
        payload_json: payload.to_string(),
    }
}

fn interpret(
    manifest: &ManifestInput,
    mut logs: Vec<RawLogInput>,
    last: i64,
) -> Result<BatchOutput> {
    logs.insert(
        0,
        raw(
            Upgraded {
                implementation: IMPLEMENTATION.parse()?,
            }
            .encode_log_data(),
            119,
            0,
        ),
    );
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests: vec![manifest.clone()],
            discovery_rules: vec![],
            admissions: vec![AddressAdmissionInput {
                address: RESOLVER.into(),
                contract_instance_id: Uuid::from_u128(MANIFEST as u128),
                source_manifest_id: Some(MANIFEST),
                role: Some("resolver".into()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            }],
            prior_events: vec![],
            blocks: (119..=last)
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
    Ok(output)
}

async fn seed(
    database: &TestDatabase,
    manifest: &ManifestInput,
    output: &BatchOutput,
    last: i64,
) -> Result<()> {
    seed_v2_history_blocks(database, 119..=last).await?;
    let payload: Value = serde_json::from_str(&manifest.payload_json)?;
    sqlx::query("INSERT INTO manifest_versions (manifest_id,manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload)
        OVERRIDING SYSTEM VALUE VALUES ($1,2,'ens','ens_v2_resolver_l1',$2,'fixture','active','fixture','fixture/eac-resource.toml',$3)")
        .bind(MANIFEST).bind(CHAIN).bind(&payload).execute(&database.pool).await?;
    seed_fixture_manifest_update(
        &database.pool,
        MANIFEST,
        CHAIN,
        "ens",
        "ens_v2_resolver_l1",
        &payload,
    )
    .await?;
    for surface in &output.name_surfaces {
        sqlx::query("INSERT INTO name_surfaces (
            logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
            labelhashes, normalizer_version, visibility_state, normalization_errors,
            deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
            provenance, canonicality_state, preimage_event_identity
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::canonicality_state,$18)")
            .bind(&surface.logical_name_id).bind(&surface.namespace)
            .bind(surface.raw_name()).bind(surface.raw_labels()).bind(surface.dns_encoded_name())
            .bind(&surface.namehash).bind(&surface.labelhashes).bind(&surface.normalizer_version)
            .bind(&surface.visibility_state).bind(&surface.normalization_errors)
            .bind(&surface.deactivation_reason).bind(surface.deactivated_at)
            .bind(&surface.chain_id).bind(&surface.block_hash).bind(surface.block_number)
            .bind(&surface.provenance).bind(&surface.canonicality_state)
            .bind(surface.preimage_event_identity()).execute(&database.pool).await?;
    }
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
            event.source_manifest_id = Some(MANIFEST);
            event.manifest_version = e.manifest_version;
            event.derivation_kind = e.derivation_kind.clone();
            event.raw_fact_ref = e.raw_fact_ref.clone();
            event.before_state = e.before_state.clone();
            event.after_state = e.after_state.clone();
            event
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(())
}

async fn publish(database: &TestDatabase, block: i64) -> Result<()> {
    publish_test_families_on(&database.pool, CHAIN, block).await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({CHAIN: {
            "chain_id":CHAIN,"block_number":block,"block_hash":format!("0xhistory{block}"),
            "timestamp":format!("2023-11-14T22:15:{:02}Z",block-100)
        }}))
        .await
}

async fn walk(database: &TestDatabase, page_size: usize, total: usize) -> Result<Vec<Value>> {
    let base = format!("/v1/resolvers/1/{RESOLVER}/roles?page_size={page_size}");
    let mut route = base.clone();
    let mut rows = Vec::new();
    loop {
        let body = v2_resolver_payload_for_database(database, &route).await?;
        assert_eq!(body["page"]["total_count"], total, "{body}");
        let data = body["data"].as_array().unwrap();
        rows.extend(data.iter().cloned());
        assert!(rows.len() <= total, "duplicate pages: {body}");
        if body["page"]["has_more"] == false {
            assert!(body["page"]["next_cursor"].is_null());
            break;
        }
        assert_eq!(data.len(), page_size);
        route = format!(
            "{base}&cursor={}",
            body["page"]["next_cursor"].as_str().unwrap()
        );
    }
    assert_eq!(rows.len(), total);
    let identities = rows
        .iter()
        .map(|r| {
            (
                r["address"].as_str().unwrap(),
                r["registration_id"].as_str().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert!(identities.windows(2).all(|pair| pair[0] < pair[1]));
    for role in &rows {
        assert!(
            role.get("grant_source").is_none(),
            "private evidence must not leak: {role}"
        );
        assert!(role["eac_resource"].is_string(), "{role}");
        let handle = role["registration_id"].as_str().unwrap();
        let followed = v2_permissions_payload_for_database(
            database,
            &format!("/v1/permissions?registration_id={handle}"),
        )
        .await?;
        let matching = followed["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| {
                r["address"] == role["address"]
                    && r["grant_scope"]["kind"] == "resolver"
                    && r["grant_scope"]["detail"]["resolver"]
                        == json!({"chain_id":1,"address":RESOLVER})
            })
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "{role} -> {followed}");
        for field in ["powers", "eac_resource", "record_resource"] {
            assert_eq!(
                matching[0].get(field),
                role.get(field),
                "{field}: {role} -> {followed}"
            );
        }
    }
    Ok(rows)
}

fn row<'a>(rows: &'a [Value], holder: &str, resource: U256) -> &'a Value {
    rows.iter()
        .find(|row| row["address"] == holder && row["eac_resource"] == resource.to_string())
        .unwrap()
}

// grantSetterRoles emits the argument only on the first grant for that resource; text and
// data calls can share it. Roots use grantRootRoles, including admin-only and link-only roles.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L253-L260 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L307-L337 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L132-L141 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn resolver_eac_resources_survive_paging_and_partial_revokes_through_project() -> Result<()> {
    let zero = U256::ZERO;
    let (address, address_arg) = argument(&U256::from(60).to_be_bytes::<32>(), 121);
    let (shared, shared_arg) = argument(b"url", 122);
    assert!(address > U256::from(u64::MAX) && shared > U256::from(u64::MAX));
    let logs = vec![
        roles(zero, HOLDER, zero, bit(4) | bit(132) | bit(28), 120, 0),
        roles(zero, ADMIN, zero, bit(132), 120, 1),
        roles(zero, LINKER, zero, bit(28), 120, 2),
        address_arg,
        roles(address, HOLDER, zero, bit(0), 121, 1),
        shared_arg,
        roles(shared, HOLDER, zero, bit(4), 122, 1),
        roles(shared, HOLDER, bit(4), bit(4) | bit(24), 123, 0),
        roles(shared, HOLDER, bit(4) | bit(24), bit(24), 124, 0),
        roles(
            zero,
            HOLDER,
            bit(4) | bit(132) | bit(28),
            bit(132) | bit(28),
            124,
            1,
        ),
        roles(address, HOLDER, bit(0), zero, 125, 0),
    ];
    let manifest = manifest(false);
    let output = interpret(&manifest, logs, 125)?;
    let database = TestDatabase::new_migrated().await?;
    seed(&database, &manifest, &output, 125).await?;
    publish(&database, 123).await?;
    let rows = walk(&database, 10, 5).await?;
    assert_eq!(walk(&database, 1, 5).await?, rows);
    for holder in [HOLDER, ADMIN, LINKER] {
        assert!(row(&rows, holder, zero).get("record_resource").is_none());
    }
    assert_eq!(
        row(&rows, HOLDER, address)["record_resource"]["coin_type"],
        60
    );
    let both = &row(&rows, HOLDER, shared)["record_resource"];
    assert_eq!(both["kind"], "argument");
    assert_eq!(
        both["selectors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["text", "data"]
    );
    publish(&database, 124).await?;
    let surviving = walk(&database, 10, 5).await?;
    assert_eq!(
        row(&surviving, HOLDER, shared)["record_resource"],
        json!({"kind":"data","hash":format!("0x{shared:064x}"),"key":"url"})
    );
    assert_eq!(
        row(&surviving, HOLDER, zero)["powers"],
        json!(["link", "admin_set_text"])
    );
    assert!(
        row(&surviving, HOLDER, zero)
            .get("record_resource")
            .is_none()
    );
    assert_eq!(
        rows.iter()
            .map(|r| (&r["address"], &r["registration_id"], &r["eac_resource"]))
            .collect::<Vec<_>>(),
        surviving
            .iter()
            .map(|r| (&r["address"], &r["registration_id"], &r["eac_resource"]))
            .collect::<Vec<_>>()
    );
    publish(&database, 125).await?;
    let remaining = walk(&database, 1, 4).await?;
    assert!(
        !remaining
            .iter()
            .any(|r| r["eac_resource"] == address.to_string())
    );
    database.cleanup().await
}

// authorizeNameRoles hashes the raw DNS name and emits NamedResource before EACRolesChanged.
// A non-UTF8 label has no admitted display name but still has this exact nonzero target.
// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L266-L293 @ ens_v2_sepolia_20260629@ccaeb58b)
// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L66-L79 @ ens_v2_sepolia_20260629@ccaeb58b)
#[tokio::test]
async fn resolver_eac_resources_without_a_historical_display_name_remain_exact() -> Result<()> {
    let node = [b"eth".as_slice(), &[0xff]]
        .into_iter()
        .fold(B256::ZERO, |node, label| {
            keccak256([node.as_slice(), keccak256(label).as_slice()].concat())
        });
    let resource = U256::from_be_bytes(*keccak256(
        [node.as_slice(), B256::ZERO.as_slice()].concat(),
    ));
    let logs = vec![
        raw(
            NamedResource {
                resource,
                name: vec![1, 0xff, 3, b'e', b't', b'h', 0].into(),
            }
            .encode_log_data(),
            120,
            0,
        ),
        roles(resource, HOLDER, U256::ZERO, bit(0), 120, 1),
    ];
    let manifest = manifest(true);
    let output = interpret(&manifest, logs, 120)?;
    let grant = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "PermissionChanged")
        .unwrap();
    assert!(grant.logical_name_id.is_none());
    assert!(output.surface_bindings.is_empty());
    assert_eq!(
        grant.after_state["grant_source"]["upstream_resource"],
        format!("0x{resource:064x}")
    );
    let database = TestDatabase::new_migrated().await?;
    seed(&database, &manifest, &output, 120).await?;
    publish(&database, 120).await?;
    let rows = walk(&database, 1, 1).await?;
    let row = row(&rows, HOLDER, resource);
    assert!(
        row.get("record_resource").is_none() && row.get("name").is_none(),
        "{row}"
    );
    database.cleanup().await
}

#[test]
fn resolver_eac_resource_schema_allows_omission_but_rejects_null_and_numbers() {
    for schema in ["ResolverRole", "PermissionRow"] {
        let document = openapi_contract::document();
        let mut root = document["components"]["schemas"][schema].clone();
        root["components"] = document["components"].clone();
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .offline()
            .build(&root)
            .unwrap();
        let mut row = json!({"address":HOLDER,"registration_id":Uuid::from_u128(1).to_string(),"powers":["set_text"]});
        if schema == "PermissionRow" {
            row["authority_context"] = json!("resource_audit");
            row["grant_scope"] =
                json!({"kind":"resolver","detail":{"resolver":{"chain_id":1,"address":RESOLVER}}});
        }
        assert!(validator.is_valid(&row), "{schema}: {row}");
        for value in [
            json!("0"),
            json!(U256::MAX.to_string()),
            Value::Null,
            json!(0),
        ] {
            row["eac_resource"] = value.clone();
            assert_eq!(
                validator.is_valid(&row),
                value.is_string(),
                "{schema}: {row}"
            );
        }
    }
}
