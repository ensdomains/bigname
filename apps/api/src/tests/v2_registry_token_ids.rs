//! The actual admitted registry decoder and Project feed the public token-ID readers.
use super::*;
use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput, RawBlockInput, RawLogInput,
    StateCacheCapacity, prepare_schema_v2_batch_incremental,
};

#[path = "v2_registry_token_ids/compatibility.rs"]
mod compatibility;
#[path = "v2_registry_token_ids/races.rs"]
mod races;
#[path = "v2_registry_token_ids/routes.rs"]
mod routes;

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x657ea849311d3d5823348dded7c2aaafb3ede09e";
const HOLDER: &str = "0x0000000000000000000000000000000000c0ffee";
const GRANTEE: &str = "0x0000000000000000000000000000000000c0ff0e";
const LABEL: &str = "envoy1084";
const NAME: &str = "envoy1084.eth";
sol! {
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
}

// Exact LibLabel.withVersion arithmetic, shared by _constructTokenId and the independently
// versioned permission resource. A token version must never be inferred from the latter.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L7-L16 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L678-L693 @ ens_v2_sepolia_20261001@07e55a05)
fn token(version: u32) -> U256 {
    let label = U256::from_be_bytes(*keccak256(LABEL));
    label ^ (label & U256::from(u32::MAX)) ^ U256::from(version)
}

fn raw(data: alloy_primitives::LogData, block: i64, index: i64) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: index,
        emitting_address: REGISTRY.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    }
}
fn transfer(version: u32, from: Address, to: Address, block: i64, index: i64) -> RawLogInput {
    raw(
        TransferSingle {
            operator: HOLDER.parse().unwrap(),
            from,
            to,
            id: token(version),
            value: U256::from(1),
        }
        .encode_log_data(),
        block,
        index,
    )
}
fn registration(version: u32, resource_version: u32, block: i64) -> Vec<RawLogInput> {
    registration_with_expiry(version, resource_version, block, 1_900_000_000)
}

fn registration_with_expiry(
    version: u32,
    resource_version: u32,
    block: i64,
    expiry: u64,
) -> Vec<RawLogInput> {
    let holder: Address = HOLDER.parse().unwrap();
    vec![
        raw(
            LabelRegistered {
                tokenId: token(version),
                labelHash: keccak256(LABEL),
                label: LABEL.into(),
                owner: holder,
                expiry,
                sender: holder,
            }
            .encode_log_data(),
            block,
            0,
        ),
        transfer(version, Address::ZERO, holder, block, 1),
        raw(
            TokenResource {
                tokenId: token(version),
                resource: token(resource_version),
            }
            .encode_log_data(),
            block,
            2,
        ),
        raw(
            EACRolesChanged {
                resource: token(resource_version),
                account: holder,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(1),
            }
            .encode_log_data(),
            block,
            3,
        ),
    ]
}

fn fixture(historical: bool) -> Result<(ManifestInput, BatchOutput, Vec<RawLogInput>)> {
    let holder: Address = HOLDER.parse()?;
    let mut logs = vec![raw(
        LabelReserved {
            tokenId: token(0),
            labelHash: keccak256(LABEL),
            label: LABEL.into(),
            expiry: 1_900_000_000,
            sender: holder,
        }
        .encode_log_data(),
        119,
        0,
    )];
    logs.extend(registration(0, 0, 120));
    // The role hook regenerates the token while the EAC resource stays at version 0.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L549-L587 @ ens_v2_sepolia_20261001@07e55a05)
    logs.extend([
        raw(
            EACRolesChanged {
                resource: token(0),
                account: GRANTEE.parse()?,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(1),
            }
            .encode_log_data(),
            121,
            0,
        ),
        transfer(0, holder, Address::ZERO, 121, 1),
        raw(
            TokenRegenerated {
                oldTokenId: token(0),
                newTokenId: token(1),
            }
            .encode_log_data(),
            121,
            2,
        ),
        transfer(1, Address::ZERO, holder, 121, 3),
        // Unregister emits the OLD ID then burns it and increments both counters. Register
        // uses those incremented counters, so token version 2 differs from resource version 1.
        // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L227-L237 @ ens_v2_sepolia_20261001@07e55a05)
        // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L489-L506 @ ens_v2_sepolia_20261001@07e55a05)
        raw(
            LabelUnregistered {
                tokenId: token(1),
                sender: holder,
            }
            .encode_log_data(),
            122,
            0,
        ),
        transfer(1, holder, Address::ZERO, 122, 1),
    ]);
    // Renew must extend an existing expiry, so begin short and extend it at block 127.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L243-L258 @ ens_v2_sepolia_20261001@07e55a05)
    logs.extend(registration_with_expiry(2, 1, 123, 1_700_000_128));
    logs.extend([
        raw(
            EACRolesChanged {
                resource: token(1),
                account: GRANTEE.parse()?,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(1),
            }
            .encode_log_data(),
            124,
            0,
        ),
        transfer(2, holder, Address::ZERO, 124, 1),
        raw(
            TokenRegenerated {
                oldTokenId: token(2),
                newTokenId: token(3),
            }
            .encode_log_data(),
            124,
            2,
        ),
        transfer(3, Address::ZERO, holder, 124, 3),
        transfer(3, holder, GRANTEE.parse()?, 125, 0),
        transfer(3, GRANTEE.parse()?, holder, 126, 0),
        raw(
            ExpiryUpdated {
                tokenId: token(3),
                newExpiry: 1_700_000_130,
                sender: holder,
            }
            .encode_log_data(),
            127,
            0,
        ),
        transfer(3, holder, Address::ZERO, 131, 0),
    ]);
    // Expired-entry replacement increments both versions without TokenRegenerated.
    let mut replacement = registration(4, 2, 131);
    for log in &mut replacement {
        log.log_index += 1;
    }
    logs.extend(replacement);
    logs.extend([
        raw(
            LabelUnregistered {
                tokenId: token(4),
                sender: holder,
            }
            .encode_log_data(),
            132,
            0,
        ),
        transfer(4, holder, Address::ZERO, 132, 1),
        raw(
            LabelReserved {
                tokenId: token(5),
                labelHash: keccak256(LABEL),
                label: LABEL.into(),
                expiry: 1_900_000_000,
                sender: holder,
            }
            .encode_log_data(),
            133,
            0,
        ),
    ]);
    logs.extend(registration(5, 3, 134));
    let (mut manifest, mut rules) = v2_history_bounded_regeneration::manifest_and_rules();
    if historical {
        // Copied without edits from the admitted pre-audit manifest in main before #896;
        // this fixture activates it only in its isolated database, not current admission.
        let repository = bigname_manifests::load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/tests/fixtures/ens-v2-token-historical"),
        )?;
        let mut old = repository.manifests()[0].manifest.clone();
        old.chain = CHAIN.into();
        old.rollout_status = bigname_manifests::RolloutStatus::Active;
        manifest.manifest_version = old.manifest_version as i64;
        manifest.deployment_label = old.deployment_epoch.clone();
        manifest.payload_json = serde_json::to_string(&old)?;
        rules = old
            .discovery_rules
            .iter()
            .map(|r| bigname_adapters::schema_v2::DiscoveryRuleInput {
                manifest_id: manifest.manifest_id,
                edge_kind: r.edge_kind.clone(),
                from_role: Some(r.from_role.clone()),
                admission: r.admission.clone(),
            })
            .collect();
        // The historical pre-audit contract mints before emitting TokenRegenerated.
        // (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registry/PermissionedRegistry.sol:L452 @ ens_v2_sepolia_dev@554c309)
        for log in &mut logs {
            if [121, 124].contains(&log.block_number) && [2, 3].contains(&log.log_index) {
                log.log_index = 5 - log.log_index;
            }
        }
        logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    }

    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests: vec![manifest.clone()],
            discovery_rules: rules,
            admissions: vec![AddressAdmissionInput {
                address: REGISTRY.into(),
                contract_instance_id: Uuid::from_u128(manifest.manifest_id as u128),
                source_manifest_id: Some(manifest.manifest_id),
                role: Some("registry".into()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            }],
            prior_events: vec![],
            blocks: (119..=134)
                .map(|block| RawBlockInput {
                    chain_id: CHAIN.into(),
                    block_hash: format!("0xhistory{block}"),
                    block_number: block,
                    block_timestamp: timestamp(1_700_000_000 + block),
                    canonicality_state: "canonical".into(),
                })
                .collect(),
            raw_logs: logs.clone(),
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    Ok((manifest, output, logs))
}

#[tokio::test]
async fn registry_token_ids_follow_real_registration_regeneration_and_reregistration() -> Result<()>
{
    assert_lifecycle(false).await
}

#[tokio::test]
async fn registry_token_ids_support_historical_preaudit_mint_order() -> Result<()> {
    assert_lifecycle(true).await
}

async fn assert_lifecycle(historical: bool) -> Result<()> {
    let (manifest, output, logs) = fixture(historical)?;
    let registration = |block| {
        output
            .normalized_events
            .iter()
            .find(|e| e.block_number == Some(block) && e.event_kind == "TokenResourceLinked")
            .unwrap()
            .resource_id
            .unwrap()
    };
    let first = registration(120);
    let second = registration(123);
    assert_ne!(first, second);
    let regen = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "TokenRegenerated")
        .unwrap();
    assert_eq!(regen.resource_id, Some(first));
    assert_eq!(
        regen.after_state["old_token_id"],
        format!("{:#066x}", token(0))
    );
    assert_eq!(
        regen.after_state["new_token_id"],
        format!("{:#066x}", token(1))
    );
    assert_ne!(token(0), U256::from_be_bytes(*keccak256(LABEL)));
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 119..=134).await?;
    seed_interpret(&database, &manifest, &logs).await?;
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    let mut old_snapshot = None;
    for (block, version, resource) in [
        (119, None, first),
        (120, Some(0), first),
        (121, Some(1), first),
        (122, None, first),
        (123, Some(2), second),
        (124, Some(3), second),
        (125, Some(3), second),
        (126, Some(3), second),
        (127, Some(3), second),
        (128, Some(3), second),
        (129, Some(3), second),
        (130, None, second),
        (131, Some(4), registration(131)),
        (132, None, registration(131)),
        (133, None, registration(134)),
        (134, Some(5), registration(134)),
    ] {
        engine
            .run_batch(bigname_interpret::BatchRequest {
                chain_id: CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: None,
                mode: bigname_interpret::RunMode::Normal,
            })
            .await?;
        publish_test_families_on(&database.pool, CHAIN, block).await?;
        database.seed_snapshot_selector_chain_positions(&json!({CHAIN:{"chain_id":CHAIN,"block_number":block,"block_hash":format!("0xhistory{block}"),"timestamp":format!("2023-11-14T22:15:{:02}Z",block-100)}})).await?;
        // The retained pre-audit manifest did not admit TransferSingle. Its selected holder
        // therefore remains the registration holder; token-event compatibility is independent
        // of that historical admission limit. Current manifests exercise the transfer branch.
        let current_holder = if block == 125 && !historical {
            GRANTEE
        } else {
            HOLDER
        };
        let detail = v2_names_payload(&database, &format!("/v1/names/{NAME}")).await?;
        let row = &detail["data"];
        if version.is_some() {
            assert_eq!(row["authority"], "ens_v2", "{detail}");
        }
        if version.is_some() {
            assert_eq!(row["registration_id"], resource.to_string(), "{detail}");
        }
        assert_eq!(
            row.get("token_id"),
            version.map(|v| json!(token(v).to_string())).as_ref(),
            "{detail}"
        );
        let snapshot = detail["meta"]["as_of_token"]
            .as_str()
            .context("snapshot token")?;
        let pinned =
            v2_names_payload(&database, &format!("/v1/names/{NAME}?at={snapshot}")).await?;
        assert_eq!(pinned["data"], detail["data"]);
        let lookup:Value=read_json(v2_lookup_response_for_database(&database,"/v1/lookup",json!({"profile":"detail","inputs":[{"name":NAME},{"address":current_holder,"relation":"owner"}]})).await?).await?;
        assert_eq!(
            lookup["data"][0]["record"]["token_id"], row["token_id"],
            "{lookup}"
        );
        assert_eq!(
            lookup["data"][0]["record"]["registration_id"], row["registration_id"],
            "{lookup}"
        );
        if version.is_some() {
            let reverse = lookup["data"][1]["records"]
                .as_array()
                .context("reverse records")?;
            let reverse = reverse
                .iter()
                .find(|r| r["name"] == NAME)
                .with_context(|| {
                    format!("reverse name at {block}, historical={historical}: {lookup}")
                })?;
            assert_eq!(reverse["token_id"], row["token_id"], "{lookup}");
            assert_eq!(reverse["registration_id"], row["registration_id"]);
            let permissions = v2_permissions_payload_for_database(
                &database,
                &format!("/v1/permissions?name={NAME}"),
            )
            .await?;
            assert!(
                !permissions["data"].as_array().unwrap().is_empty(),
                "{permissions}"
            );
            for permission in permissions["data"].as_array().unwrap() {
                assert_eq!(permission["registration_id"], resource.to_string());
                assert!(permission.get("token_id").is_none());
            }
            let feed: Value = read_json(
                v2_lookup_response_for_database(
                    &database,
                    "/v1/lookup",
                    json!({"profile":"feed","inputs":[{"name":NAME}]}),
                )
                .await?,
            )
            .await?;
            assert!(
                feed["data"][0]["record"].get("token_id").is_none(),
                "{feed}"
            );
        }
        if let Some(old) = old_snapshot.as_ref() {
            let (status, payload) =
                read_family_response(&database, &format!("/v1/names/{NAME}?at={old}")).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{payload}");
            assert_eq!(payload["error"]["code"], "stale");
        }
        old_snapshot = Some(snapshot.to_owned());
    }
    // The later current token never rewrites an earlier registration's event identity or words.
    let (status,history)=read_family_response(&database,&format!("/v1/events?registration_id={first}&include=data,raw&from_block=120&to_block=121&page_size=100")).await?;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert!(!history["data"].as_array().unwrap().is_empty(), "{history}");
    for event in history["data"].as_array().unwrap() {
        assert_eq!(event["registration_id"], first.to_string());
        assert!(event.get("token_id").is_none());
    }
    let observed: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events WHERE event_kind='TokenRegenerated' ORDER BY block_number LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(observed["old_token_id"], regen.after_state["old_token_id"]);
    assert_eq!(observed["new_token_id"], regen.after_state["new_token_id"]);
    let ambiguous:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT chain_id,resource_id,block_number,transaction_index,log_index FROM normalized_events WHERE source_family IN ('ens_v2_registry_l1','ens_v2_root_l1') AND event_kind IN ('TokenResourceLinked','TokenRegenerated') AND consumer_visibility='activated' GROUP BY 1,2,3,4,5 HAVING count(*)>1) duplicate")
        .fetch_one(&database.pool).await?;
    assert_eq!(ambiguous, 0);
    let versions:Vec<i64>=sqlx::query_scalar("SELECT DISTINCT manifest_version FROM normalized_events WHERE event_kind IN ('TokenResourceLinked','TokenRegenerated')").fetch_all(&database.pool).await?;
    assert_eq!(versions, vec![if historical { 1 } else { 2 }]);
    database.cleanup().await
}

/// Persist only raw intake and the admitted manifest. Interpret owns all normalized and
/// identity writes, including closing the previous binding before re-registration.
async fn seed_interpret(
    database: &TestDatabase,
    manifest: &ManifestInput,
    logs: &[RawLogInput],
) -> Result<()> {
    v2_history_bounded_rebinding::persist_with_manifests(
        &database.pool,
        std::slice::from_ref(manifest),
        &BatchOutput::default(),
    )
    .await?;
    let payload: Value = serde_json::from_str(&manifest.payload_json)?;
    for rule in payload["discovery_rules"].as_array().unwrap() {
        sqlx::query("INSERT INTO manifest_discovery_rules (manifest_id,edge_kind,from_role,admission,rule_payload) VALUES ($1,$2,$3,$4,$5)")
            .bind(manifest.manifest_id).bind(rule["edge_kind"].as_str().unwrap()).bind(rule["from_role"].as_str()).bind(rule["admission"].as_str().unwrap()).bind(rule).execute(&database.pool).await?;
    }
    let instance = Uuid::from_u128(manifest.manifest_id as u128);
    sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1,$2,'contract')")
        .bind(instance).bind(CHAIN).execute(&database.pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,source_manifest_id) VALUES ($1,$2,$3,0,$4)")
        .bind(instance).bind(CHAIN).bind(REGISTRY).bind(manifest.manifest_id).execute(&database.pool).await?;
    sqlx::query("INSERT INTO manifest_contract_instances (manifest_id,chain_id,declaration_kind,declaration_name,contract_instance_id,declared_address,role,proxy_kind,start_block_number) VALUES ($1,$2,'contract','registry',$3,$4,'registry','none',0)")
        .bind(manifest.manifest_id).bind(CHAIN).bind(instance).bind(REGISTRY).execute(&database.pool).await?;
    for log in logs {
        sqlx::query("INSERT INTO raw_transactions (chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING")
            .bind(CHAIN).bind(&log.block_hash).bind(log.block_number).bind(&log.transaction_hash).bind(log.transaction_index).bind(HOLDER).bind(&log.emitting_address).execute(&database.pool).await?;
        sqlx::query("INSERT INTO raw_logs (chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(CHAIN).bind(&log.block_hash).bind(log.block_number).bind(&log.transaction_hash).bind(log.transaction_index).bind(log.log_index).bind(&log.emitting_address).bind(&log.topics).bind(&log.data).execute(&database.pool).await?;
    }
    Ok(())
}
