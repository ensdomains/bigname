//! Retained ENSv1 child resolution after an admitted, unwrapped parent migration.
//! Logs follow ENSRegistry.setSubnodeRecord, resolver setters and controller cleanup.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L49-L57 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/UnlockedMigrationController.sol:L101-L165 @ ens_v2_sepolia_20261001@07e55a05)
#[path = "migrated_subnames/cost.rs"]
mod cost;
#[path = "migrated_subnames/direct.rs"]
mod direct;
#[path = "migrated_subnames/lookup_hydration.rs"]
mod lookup_hydration;
#[path = "migrated_subnames/lookup_import.rs"]
mod lookup_import;
#[path = "migrated_subnames/lookup_incremental.rs"]
mod lookup_incremental;
#[path = "migrated_subnames/lookup_late_registry.rs"]
mod lookup_late_registry;
#[path = "migrated_subnames/lookup_lifecycle.rs"]
mod lookup_lifecycle;
#[path = "migrated_subnames/lookup_links.rs"]
mod lookup_links;
#[path = "migrated_subnames/lookup_publication.rs"]
mod lookup_publication;
#[path = "migrated_subnames/manifest_sync.rs"]
mod manifest_sync;
#[path = "migrated_subnames/mirror.rs"]
mod mirror;
#[path = "migrated_subnames/missing_parent.rs"]
mod missing_parent;
#[path = "migrated_subnames/nested.rs"]
mod nested;
#[path = "migrated_subnames/replay.rs"]
mod replay;
#[path = "migrated_subnames/wrapper.rs"]
mod wrapper;

use super::compatibility::role_address;
use super::*;
const PATH_CHAIN: &str = "ethereum-sepolia";
const BATCH_REGISTRAR: &str = "0x4a4c8b7cdab6b19dc2cdb417cdb53a2ccbaf5322";
const DEPLOYER: &str = "0x84d3a426d4e12e955d1df95db0b24fe26afe39d3";
const RESERVATION_EXPIRY: u64 = 1_905_356_800;
const BASE: i64 = 11_822_000;
const CHILD: &str = "child.envoy1084.eth";
sol! {
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameChanged(bytes32 indexed node, string name);
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event Upgraded(address indexed implementation);
}
mod registry {
    use super::*;
    sol! { event Transfer(bytes32 indexed node, address owner); }
}
fn emitted(
    data: alloy_primitives::LogData,
    emitter: Address,
    block: i64,
    index: i64,
) -> RawLogInput {
    let mut log = raw(data, BASE + block, index);
    log.chain_id = PATH_CHAIN.into();
    log.emitting_address = format!("{emitter:#x}");
    log
}
async fn publish(database: &TestDatabase, block: i64) -> Result<()> {
    let block = BASE + block;
    publish_test_families_on(&database.pool, PATH_CHAIN, block).await?;
    database.seed_snapshot_selector_chain_positions(&json!({PATH_CHAIN:{"chain_id":PATH_CHAIN,"block_number":block,"block_hash":format!("0xhistory{block}"),"timestamp":bigname_storage::UnixSeconds::from(timestamp(1_700_000_000+block)).internal_string()}})).await
}
async fn setup() -> Result<(TestDatabase, Vec<RawLogInput>, Address)> {
    setup_at(None).await
}

/// `setup`, with the Sepolia profile's root registry declared at `root` instead of its
/// deployed address when one is given. Every root log follows the declaration.
async fn setup_at(root: Option<&str>) -> Result<(TestDatabase, Vec<RawLogInput>, Address)> {
    const DEPLOYED_ROOT: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
    let database = TestDatabase::new_migrated().await?;
    let checked_in =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia");
    let moved = match root {
        Some(root) => {
            let copy = std::env::temp_dir().join(format!(
                "bigname-moved-root-{}-{}",
                std::process::id(),
                NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            let mut pending = vec![(checked_in.clone(), copy.clone())];
            while let Some((from, to)) = pending.pop() {
                std::fs::create_dir_all(&to)?;
                for entry in std::fs::read_dir(&from)? {
                    let entry = entry?;
                    let target = to.join(entry.file_name());
                    if entry.file_type()?.is_dir() {
                        pending.push((entry.path(), target));
                    } else {
                        let text = std::fs::read_to_string(entry.path())?;
                        std::fs::write(target, text.replace(DEPLOYED_ROOT, root))?;
                    }
                }
            }
            Some(copy)
        }
        None => None,
    };
    let repository = bigname_manifests::load_repository(moved.clone().unwrap_or(checked_in));
    if let Some(copy) = moved {
        std::fs::remove_dir_all(copy)?;
    }
    let repository = repository?;
    bigname_manifests::sync_schema_v2_repository(&database.lookup_pool, &repository).await?;
    let declarations: std::collections::BTreeMap<&str, Value> = repository
        .manifests()
        .iter()
        .filter(|m| {
            m.manifest.chain == PATH_CHAIN
                && m.manifest.rollout_status == bigname_manifests::RolloutStatus::Active
        })
        .map(|m| {
            (
                m.manifest.source_family.as_str(),
                serde_json::to_value(&m.manifest).unwrap(),
            )
        })
        .collect();
    database
        .pool
        .set_connect_options(database.pool.connect_options().as_ref().clone().options([(
            "bigname.interpreter_content_hash",
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        )]));
    let mut connections = Vec::new();
    for _ in 0..database.pool.options().get_max_connections() {
        let mut connection = database.pool.acquire().await?;
        sqlx::query("SELECT set_config('bigname.interpreter_content_hash',$1,false)")
            .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .execute(&mut *connection)
            .await?;
        connections.push(connection);
    }
    drop(connections);
    let v1 = role_address(&declarations["ens_v1_registry_l1"], "registry");
    let base = role_address(&declarations["ens_v1_registrar_l1"], "registrar");
    let resolver = role_address(&declarations["ens_v1_resolver_l1"], "public_resolver");
    let v2 = role_address(&declarations["ens_v2_registry_l1"], "registry");
    let root = role_address(&declarations["ens_v2_root_l1"], "root_registry");
    let mirror = role_address(&declarations["ens_v2_resolver_l1"], "ensv1_mirror_resolver");
    let controller = role_address(
        &declarations["ens_v2_migration_l1"],
        "unlocked_migration_controller",
    );
    let graveyard = role_address(&declarations["ens_v2_migration_l1"], "graveyard");
    let proxy = role_address(&declarations["ens_execution"], "universal_resolver");
    let implementation = declarations["ens_execution"]["universal_resolver_implementations"][0]
        .as_str()
        .unwrap()
        .parse()?;
    let parent = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let child = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let eth = bigname_lookup::ens_namehash_hex("eth")?.parse()?;
    let owner = HOLDER.parse()?;
    let child_owner = GRANTEE.parse()?;
    let mut logs = vec![
        emitted(Upgraded { implementation }.encode_log_data(), proxy, 120, 0),
        emitted(
            NewOwner {
                node: eth,
                label: keccak256(LABEL),
                owner,
            }
            .encode_log_data(),
            v1,
            120,
            1,
        ),
        emitted(
            NameRegistered {
                id: U256::from_be_bytes(*keccak256(LABEL)),
                owner,
                expires: U256::from(1_900_000_000u64),
            }
            .encode_log_data(),
            base,
            120,
            2,
        ),
        emitted(
            NameChanged {
                node: parent,
                name: NAME.into(),
            }
            .encode_log_data(),
            resolver,
            120,
            3,
        ),
        emitted(
            LabelReserved {
                tokenId: token(0),
                labelHash: keccak256(LABEL),
                label: LABEL.into(),
                expiry: RESERVATION_EXPIRY,
                sender: BATCH_REGISTRAR.parse()?,
            }
            .encode_log_data(),
            v2,
            120,
            4,
        ),
        emitted(
            NewOwner {
                node: parent,
                label: keccak256("child"),
                owner: child_owner,
            }
            .encode_log_data(),
            v1,
            121,
            0,
        ),
        emitted(
            NewResolver {
                node: child,
                resolver,
            }
            .encode_log_data(),
            v1,
            121,
            1,
        ),
        emitted(
            NameChanged {
                node: child,
                name: CHILD.into(),
            }
            .encode_log_data(),
            resolver,
            121,
            2,
        ),
        emitted(
            NewResolver {
                node: child,
                resolver,
            }
            .encode_log_data(),
            v1,
            121,
            3,
        ),
        emitted(
            AddressChanged {
                node: child,
                coinType: U256::from(60),
                newAddress: owner.to_vec().into(),
            }
            .encode_log_data(),
            resolver,
            121,
            4,
        ),
        emitted(
            Transfer {
                from: owner,
                to: controller,
                tokenId: U256::from_be_bytes(*keccak256(LABEL)),
            }
            .encode_log_data(),
            base,
            122,
            0,
        ),
        emitted(
            NewOwner {
                node: eth,
                label: keccak256(LABEL),
                owner: controller,
            }
            .encode_log_data(),
            v1,
            122,
            1,
        ),
        emitted(
            registry::Transfer {
                node: parent,
                owner: graveyard,
            }
            .encode_log_data(),
            v1,
            122,
            2,
        ),
        emitted(
            NewResolver {
                node: parent,
                resolver: Address::ZERO,
            }
            .encode_log_data(),
            v1,
            122,
            3,
        ),
        emitted(
            Transfer {
                from: controller,
                to: graveyard,
                tokenId: U256::from_be_bytes(*keccak256(LABEL)),
            }
            .encode_log_data(),
            base,
            122,
            4,
        ),
        emitted(
            LabelRegistered {
                tokenId: token(0),
                labelHash: keccak256(LABEL),
                label: LABEL.into(),
                owner,
                expiry: RESERVATION_EXPIRY,
                sender: controller,
            }
            .encode_log_data(),
            v2,
            122,
            5,
        ),
        emitted(
            TransferSingle {
                operator: controller,
                from: Address::ZERO,
                to: owner,
                id: token(0),
                value: U256::from(1),
            }
            .encode_log_data(),
            v2,
            122,
            6,
        ),
        emitted(
            TokenResource {
                tokenId: token(0),
                resource: token(0),
            }
            .encode_log_data(),
            v2,
            122,
            7,
        ),
        emitted(
            EACRolesChanged {
                resource: token(0),
                account: owner,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: [20, 24, 32, 148, 152, 156]
                    .into_iter()
                    .fold(U256::ZERO, |v, b| v | (U256::from(1) << b)),
            }
            .encode_log_data(),
            v2,
            122,
            8,
        ),
        emitted(
            ResolverUpdated {
                tokenId: token(0),
                resolver: Address::ZERO,
                sender: owner,
            }
            .encode_log_data(),
            v2,
            122,
            9,
        ),
        emitted(
            SubregistryUpdated {
                tokenId: token(0),
                subregistry: Address::ZERO,
                sender: owner,
            }
            .encode_log_data(),
            v2,
            122,
            10,
        ),
    ];
    let eth_token = U256::from_be_bytes(*keccak256("eth")) >> 32 << 32;
    logs.extend([
        emitted(
            LabelRegistered {
                tokenId: eth_token,
                labelHash: keccak256("eth"),
                label: "eth".into(),
                owner,
                expiry: u64::MAX,
                sender: DEPLOYER.parse()?,
            }
            .encode_log_data(),
            root,
            120,
            5,
        ),
        emitted(
            TransferSingle {
                operator: DEPLOYER.parse()?,
                from: Address::ZERO,
                to: owner,
                id: eth_token,
                value: U256::from(1),
            }
            .encode_log_data(),
            root,
            120,
            6,
        ),
        emitted(
            TokenResource {
                tokenId: eth_token,
                resource: eth_token,
            }
            .encode_log_data(),
            root,
            120,
            7,
        ),
        emitted(
            SubregistryUpdated {
                tokenId: eth_token,
                subregistry: v2,
                sender: DEPLOYER.parse()?,
            }
            .encode_log_data(),
            root,
            120,
            8,
        ),
        emitted(
            ResolverUpdated {
                tokenId: token(0),
                resolver: mirror,
                sender: BATCH_REGISTRAR.parse()?,
            }
            .encode_log_data(),
            v2,
            120,
            9,
        ),
    ]);
    // Actual reservation call: deployed BatchRegistrar.batchRegister(0, mirror, ...)
    // emits LabelReserved followed by its nonzero ResolverUpdated in one transaction.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/BatchRegistrar.sol:L49-L71 @ ens_v2_sepolia_20261001@07e55a05)
    for log in &mut logs {
        let offset = log.block_number - BASE;
        log.transaction_index = match (offset, log.log_index) {
            (120, 0) => 0,
            (120, 5..=8) => 1,
            (120, 1..=2) => 2,
            (120, 3) => 3,
            (120, 4 | 9) => 4,
            (121, 0..=1) => 0,
            (121, 2) => 1,
            (121, 3) => 2,
            (121, 4) => 3,
            (122, 0..=8) => 0,
            (122, 9) => 1,
            (122, 10) => 2,
            _ => 0,
        };
        log.transaction_hash = format!("0x{:064x}", log.block_number * 100 + log.transaction_index);
    }
    logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    let mut block = None;
    let mut index = 0;
    for log in &mut logs {
        if block != Some(log.block_number) {
            block = Some(log.block_number);
            index = 0;
        }
        log.log_index = index;
        index += 1;
    }
    Ok((database, logs, resolver))
}
async fn seed_and_run(
    database: &TestDatabase,
    logs: &[RawLogInput],
    start: i64,
    end: i64,
) -> Result<()> {
    seed_and_run_with(database, logs, start, end, &[], None).await
}
async fn seed_and_run_with(
    database: &TestDatabase,
    logs: &[RawLogInput],
    start: i64,
    end: i64,
    callers: &[(i64, i64, &str)],
    clock: Option<i64>,
) -> Result<()> {
    seed_and_run_with_targets(database, logs, start, end, callers, &[], clock).await
}

async fn seed_and_run_with_targets(
    database: &TestDatabase,
    logs: &[RawLogInput],
    start: i64,
    end: i64,
    callers: &[(i64, i64, &str)],
    targets: &[(i64, i64, &str)],
    clock: Option<i64>,
) -> Result<()> {
    let blocks = (BASE + start..=BASE + end)
        .map(|n| {
            raw_block(
                PATH_CHAIN,
                &format!("0xhistory{n}"),
                None,
                n,
                clock.unwrap_or(1_700_000_000 + n),
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_schema_v2_lookup_head(
        &database.pool,
        PATH_CHAIN,
        BASE + end,
        &format!("0xhistory{}", BASE + end),
        &bigname_storage::UnixSeconds::from(timestamp(1_700_000_000 + BASE + end))
            .internal_string(),
    )
    .await?;
    for log in logs {
        let caller = if log.block_number == BASE + 121 && log.transaction_index > 0 {
            GRANTEE
        } else if log.block_number == BASE + 120 && matches!(log.transaction_index, 1 | 4) {
            DEPLOYER
        } else {
            HOLDER
        };
        let caller = callers
            .iter()
            .find(|(block, tx, _)| {
                *block + BASE == log.block_number && *tx == log.transaction_index
            })
            .map_or(caller, |(_, _, caller)| *caller);
        let default_target = if log.block_number == BASE + 120 && log.transaction_index == 4 {
            BATCH_REGISTRAR.to_owned()
        } else {
            log.emitting_address.clone()
        };
        let target = targets
            .iter()
            .find(|(block, tx, _)| {
                *block + BASE == log.block_number && *tx == log.transaction_index
            })
            .map_or(default_target.as_str(), |(_, _, target)| *target);
        sqlx::query("INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING").bind(PATH_CHAIN).bind(&log.block_hash).bind(log.block_number).bind(&log.transaction_hash).bind(log.transaction_index).bind(caller).bind(target).execute(&database.pool).await?;
        sqlx::query("INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)").bind(PATH_CHAIN).bind(&log.block_hash).bind(log.block_number).bind(&log.transaction_hash).bind(log.transaction_index).bind(log.log_index).bind(&log.emitting_address).bind(&log.topics).bind(&log.data).execute(&database.pool).await?;
    }
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    for block in BASE + start..=BASE + end {
        engine
            .run_batch(bigname_interpret::BatchRequest {
                chain_id: PATH_CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: None,
                mode: bigname_interpret::RunMode::Normal,
            })
            .await?;
    }
    publish(database, end).await
}
#[tokio::test]
async fn migrated_subname_actual_producer_withholds_disconnected_child() -> Result<()> {
    eprintln!(
        "TYR105 compiled fingerprint {}",
        bigname_content_hash::INTERPRETER_CONTENT_HASH
    );
    let (database, logs, resolver) = setup().await?;
    let initial = logs
        .iter()
        .filter(|log| log.block_number <= BASE + 121)
        .cloned()
        .collect::<Vec<_>>();
    seed_and_run(&database, &initial, 120, 121).await?;
    let before = path_get(&database, &format!("/v1/names/{CHILD}")).await?;
    assert_eq!(before["data"]["authority"], "ens_v1", "{before:#}");
    assert_eq!(
        before["data"]["resolver"]["address"],
        format!("{resolver:#x}"),
        "{before:#}"
    );
    assert_eq!(before["data"]["primary_address"], HOLDER, "{before:#}");
    let later = logs
        .iter()
        .filter(|log| log.block_number == BASE + 122)
        .cloned()
        .collect::<Vec<_>>();
    // The helper's earlier blocks are immutable and conflict-safe; only new ABI logs are added.
    seed_and_run(&database, &later, 122, 122).await?;
    publish(&database, 122).await?;
    let migration: Value = sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE event_kind='MigrationApplied' AND consumer_visibility='activated'").fetch_one(&database.pool).await?;
    assert_eq!(migration["migration_path"], "unwrapped", "{migration}");
    let after = path_get(&database, &format!("/v1/names/{CHILD}")).await?;
    for key in [
        "owner",
        "authority",
        "registration_id",
        "expires_at",
        "registered_at",
    ] {
        assert_eq!(
            after["data"][key], before["data"][key],
            "authority changed: {key}"
        );
    }
    assert!(
        after["data"]["resolver"].is_null(),
        "Disconnected ENSv1 child retained its old resolver: {after:#}"
    );
    assert!(after["data"]["primary_address"].is_null(), "{after:#}");
    database.cleanup().await
}

async fn assert_consumers(
    database: &TestDatabase,
    expected: bool,
    resolver: Address,
    reason: Option<&str>,
) -> Result<()> {
    assert_name_consumers(database, CHILD, HOLDER, expected, resolver, reason).await
}

async fn assert_name_consumers(
    database: &TestDatabase,
    name: &str,
    address: &str,
    expected: bool,
    resolver: Address,
    reason: Option<&str>,
) -> Result<()> {
    let detail_record = lookup_publication::assert_name_prepared_parity(database, name).await?;
    let detail = json!({"data":detail_record});
    let lookup = path_lookup(database, json!({"profile":"detail","inputs":[{"name":name},{"address":address,"relation":"resolves_to"}]})).await?;
    for field in [
        "resolver",
        "records",
        "primary_address",
        "owner",
        "authority",
        "unresolvable_reason",
        "resolution_unsupported_reason",
    ] {
        assert_eq!(
            lookup["data"][0]["record"][field], detail["data"][field],
            "{field}: {lookup:#}"
        );
    }
    let inverse = lookup["data"][1]["records"]
        .as_array()
        .context("inverse lookup records")?;
    assert_eq!(
        inverse.iter().any(|row| row["name"] == name),
        expected,
        "{lookup:#}"
    );
    let bound = path_get(database, &format!("/v1/resolvers/11155111/{resolver:#x}")).await?;
    assert_eq!(
        bound["data"]["bound_names"]["data"]
            .as_array()
            .context("bound names")?
            .iter()
            .any(|row| row["name"] == name),
        expected,
        "{bound:#}"
    );
    let records = path_get(
        database,
        &format!("/v1/names/{name}/records?source=indexed&keys=addr:60"),
    )
    .await?;
    let answer = &records["data"]["records"]["addr:60"];
    assert_eq!(
        answer["status"],
        if expected {
            "ok"
        } else if reason.is_some() {
            "unsupported"
        } else {
            "not_found"
        },
        "{records:#}"
    );
    if let Some(reason) = reason {
        assert_eq!(answer["unsupported_reason"], reason, "{records:#}");
    }
    if expected {
        assert_eq!(answer["value"], address, "{records:#}");
        assert_eq!(detail["data"]["primary_address"], address, "{detail:#}");
        assert_eq!(
            detail["data"]["resolver"]["address"],
            format!("{resolver:#x}")
        );
    } else {
        assert!(answer["value"].is_null(), "{records:#}");
        assert!(detail["data"]["primary_address"].is_null(), "{detail:#}");
    }
    let logical = format!("ens:{}", bigname_lookup::ens_namehash_hex(name)?);
    let row = bigname_storage::families::name::load_family_name(&database.pool, &logical)
        .await?
        .context("child row")?;
    assert_eq!(row.record_serving_resource_id().is_some(), expected);
    if !expected {
        assert!(
            row.declared_summary.get("topology").is_none(),
            "{:?}",
            row.declared_summary
        );
    }
    Ok(())
}

/// The ENSv2 path walk starts at the root registry the profile admits, whatever its address.
/// A retained ENSv1 descendant therefore still resolves when the root is deployed elsewhere.
#[tokio::test]
async fn migrated_subname_descendant_is_retained_under_a_root_registry_at_another_address()
-> Result<()> {
    let (database, logs, resolver) =
        setup_at(Some("0x00000000000000000000000000000000000000b4")).await?;
    let initial: Vec<_> = logs
        .iter()
        .filter(|log| log.block_number <= BASE + 121)
        .cloned()
        .collect();
    seed_and_run(&database, &initial, 120, 121).await?;
    let roots: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT entry.registry FROM project_ens_v2_entry_owner entry
         JOIN normalized_events event ON event.event_identity = entry.event_identity
         WHERE event.source_family = 'ens_v2_root_l1'",
    )
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(roots, ["0x00000000000000000000000000000000000000b4"]);
    assert_consumers(&database, true, resolver, None).await?;
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_parent_only_changes_restore_and_withhold_all_consumers() -> Result<()> {
    let (database, logs, resolver) = setup().await?;
    let initial: Vec<_> = logs
        .iter()
        .filter(|log| log.block_number <= BASE + 121)
        .cloned()
        .collect();
    seed_and_run(&database, &initial, 120, 121).await?;
    assert_consumers(&database, true, resolver, None).await?;
    let logical = format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?);
    let (url, handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(GRANTEE)])
            .await?;
    let engine = bigname_lookup::LookupEngine::new(
        database.pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{PATH_CHAIN}={url}")])?,
    );
    let answer = engine
        .lookup(bigname_lookup::LookupRequest::new(&logical, ["addr:60"])?)
        .await?;
    assert_eq!(
        answer.records[0].ledger_action,
        bigname_lookup::LedgerAction::Written
    );
    assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 1);
    let later: Vec<_> = logs
        .iter()
        .filter(|log| log.block_number == BASE + 122)
        .cloned()
        .collect();
    seed_and_run(&database, &later, 122, 122).await?;
    assert_consumers(&database, false, resolver, None).await?;
    let active: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(
        active, 0,
        "parent-only migration must retire old child comparisons"
    );
    let row = bigname_storage::families::name::load_family_name(&database.pool, &logical)
        .await?
        .context("child row")?;
    assert_eq!(row.unresolvable_reason(), Some("ens_v2_path_no_resolver"));
    let dns = bigname_domain::normalization::normalize_name(CHILD)?.dns_encoded_name;
    let (url, handle) =
        spawn_primary_name_mock_rpc(vec![resolution_resolver_not_found_error(&dns)]).await?;
    let engine = bigname_lookup::LookupEngine::new(
        database.pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{PATH_CHAIN}={url}")])?,
    );
    let answer = engine
        .lookup(bigname_lookup::LookupRequest::new(&logical, ["addr:60"])?)
        .await?;
    assert_eq!(
        answer.records[0].status,
        bigname_lookup::LookupRecordStatus::NotFound,
        "{answer:#?}"
    );
    assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 1);
    let v2 = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4".parse()?;
    let mirror = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0".parse()?;
    let restore = emitted(
        ResolverUpdated {
            tokenId: token(0),
            resolver: mirror,
            sender: HOLDER.parse()?,
        }
        .encode_log_data(),
        v2,
        123,
        0,
    );
    seed_and_run(&database, &[restore], 123, 123).await?;
    assert_consumers(&database, true, resolver, None).await?;
    let custom = emitted(
        SubregistryUpdated {
            tokenId: token(0),
            subregistry: "0x0000000000000000000000000000000000000bad".parse()?,
            sender: HOLDER.parse()?,
        }
        .encode_log_data(),
        v2,
        124,
        0,
    );
    seed_and_run(&database, &[custom], 124, 124).await?;
    assert_consumers(
        &database,
        false,
        resolver,
        Some("ens_v2_path_not_projected"),
    )
    .await?;
    let detail = path_get(&database, &format!("/v1/names/{CHILD}")).await?;
    assert!(
        detail["data"]["unresolvable_reason"].is_null(),
        "unknown custom registry is not no resolver: {detail:#}"
    );
    let before_clear = replay::families(&database).await?;
    let clear = emitted(
        SubregistryUpdated {
            tokenId: token(0),
            subregistry: Address::ZERO,
            sender: HOLDER.parse()?,
        }
        .encode_log_data(),
        v2,
        125,
        0,
    );
    seed_and_run(&database, &[clear], 125, 125).await?;
    assert_consumers(&database, true, resolver, None).await?;
    let undone = bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 124).await?;
    assert_eq!(undone, 1);
    assert_eq!(
        replay::families(&database).await?,
        before_clear,
        "undo restores exact family rows"
    );
    publish(&database, 125).await?;
    assert_consumers(&database, true, resolver, None).await?;
    replay::assert_rebuild(&database, 125).await?;
    assert_consumers(&database, true, resolver, None).await?;
    database.cleanup().await
}

// Real Sepolia namespace selection; older shared test routers force ENS to mainnet.
async fn path_get(database: &TestDatabase, uri: &str) -> Result<Value> {
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {payload:#}");
    Ok(payload)
}
async fn path_lookup(database: &TestDatabase, body: Value) -> Result<Value> {
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lookup")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body)?))?,
        )
        .await?;
    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "lookup: {payload:#}");
    Ok(payload)
}

/// One call's ABI logs in contract order. The block-global log indices are distinct across calls.
fn transaction(
    block: i64,
    tx: i64,
    events: Vec<(Address, alloy_primitives::LogData)>,
) -> Vec<RawLogInput> {
    events
        .into_iter()
        .enumerate()
        .map(|(index, (emitter, data))| {
            let mut log = emitted(data, emitter, block, tx * 100 + index as i64);
            log.transaction_index = tx;
            log.transaction_hash = format!("0x{:064x}", log.block_number * 100 + tx);
            log
        })
        .collect()
}
fn label_token(label: &str) -> U256 {
    U256::from_be_bytes(*keccak256(label)) >> 32 << 32
}
fn register(
    registry: Address,
    label: &str,
    resolver: Address,
    subregistry: Address,
    expiry: u64,
    sender: Address,
) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    let id = label_token(label);
    let owner = HOLDER.parse()?;
    let mut events = vec![
        (
            registry,
            LabelRegistered {
                tokenId: id,
                labelHash: keccak256(label),
                label: label.into(),
                owner,
                expiry,
                sender,
            }
            .encode_log_data(),
        ),
        (
            registry,
            TransferSingle {
                operator: sender,
                from: Address::ZERO,
                to: owner,
                id,
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            registry,
            TokenResource {
                tokenId: id,
                resource: id,
            }
            .encode_log_data(),
        ),
        (
            registry,
            EACRolesChanged {
                resource: id,
                account: owner,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: [20, 24, 148, 152, 156]
                    .into_iter()
                    .fold(U256::ZERO, |v, b| v | (U256::from(1) << b)),
            }
            .encode_log_data(),
        ),
    ];
    if resolver != Address::ZERO {
        events.push((
            registry,
            ResolverUpdated {
                tokenId: id,
                resolver,
                sender,
            }
            .encode_log_data(),
        ));
    }
    if subregistry != Address::ZERO {
        events.push((
            registry,
            SubregistryUpdated {
                tokenId: id,
                subregistry,
                sender,
            }
            .encode_log_data(),
        ));
    }
    Ok(events)
}
