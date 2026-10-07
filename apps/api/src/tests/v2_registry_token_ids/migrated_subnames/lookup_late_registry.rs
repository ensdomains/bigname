//! A delayed UserRegistry initializer changes the supported path without a name/pointer write.
//! Constructed admitted ABI logs exercise Interpret, Project, persisted core and public readers.
use super::*;

const ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
const USER: &str = "0x9bd8a88719068d09ecee662f36c0e3856708366a";
const REGISTRY: &str = "0x0000000000000000000000000000000000025901";
const UNMOUNTED: &str = "0x0000000000000000000000000000000000025903";
const UNRELATED: &str = "unrelated259.eth";
sol! {
    event RegistryCreated();
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
    event URIUpdated(string uri, address renderer, address indexed sender);
}

fn reserve(
    label: &str,
    subregistry: Address,
    resolver: Address,
) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    let registry = ETH.parse()?;
    let sender = BATCH_REGISTRAR.parse()?;
    let id = U256::from_be_bytes(*keccak256(label)) >> 32 << 32;
    // BatchRegistrar can reserve a mount independently of the mounted registry's initialization.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/BatchRegistrar.sol:L48-L71 @ ens_v2_sepolia_20261001@07e55a05)
    Ok(vec![
        (
            registry,
            LabelReserved {
                tokenId: id,
                labelHash: keccak256(label),
                label: label.into(),
                expiry: RESERVATION_EXPIRY,
                sender,
            }
            .encode_log_data(),
        ),
        (
            registry,
            SubregistryUpdated {
                tokenId: id,
                subregistry,
                sender,
            }
            .encode_log_data(),
        ),
        (
            registry,
            ResolverUpdated {
                tokenId: id,
                resolver,
                sender,
            }
            .encode_log_data(),
        ),
    ])
}

fn initializer(registry: Address, block: i64) -> Result<Vec<RawLogInput>> {
    // No ParentUpdated, child registration or pointer event accompanies UserRegistry.initialize.
    // A nonempty root grant is mandatory; URIUpdated is the initializer's final log.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/UserRegistry.sol:L57-L69 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/BoxedENSURIRenderer.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
    Ok(transaction(
        block,
        0,
        vec![
            (registry, RegistryCreated {}.encode_log_data()),
            (
                registry,
                EACRolesChanged {
                    resource: U256::ZERO,
                    account: HOLDER.parse()?,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: U256::from(1) | (U256::from(1) << 128),
                }
                .encode_log_data(),
            ),
            (
                registry,
                URIUpdated {
                    uri: String::new(),
                    renderer: "0x0f5b101b6fc626b9b210bb5e70f60f3dd9ca0d96".parse()?,
                    sender: Address::ZERO,
                }
                .encode_log_data(),
            ),
        ],
    ))
}

async fn core(database: &TestDatabase, name: &str) -> Result<Value> {
    Ok(
        sqlx::query_scalar(
            "SELECT to_jsonb(n) FROM project_lookup_name n WHERE logical_name_id=$1",
        )
        .bind(format!("ens:{}", bigname_lookup::ens_namehash_hex(name)?))
        .fetch_one(&database.pool)
        .await?,
    )
}

async fn summary(database: &TestDatabase) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(n) FROM project_name_summary n WHERE logical_name_id=$1",
    )
    .bind(format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?))
    .fetch_one(&database.pool)
    .await?)
}

async fn assert_retained(database: &TestDatabase, resolver: Address) -> Result<Value> {
    // Read both paths before asserting, so the matched RED retains the actual public mismatch
    // as well as the persisted fields. A work-set-only failure would not prove this bug.
    let detail = path_get(database, &format!("/v1/names/{CHILD}")).await?;
    let lookup = path_lookup(
        database,
        json!({"profile":"detail","inputs":[{"name":CHILD}]}),
    )
    .await?;
    let stored = core(database, CHILD).await?;
    let evidence = json!({"compiled_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH,
        "stored_core":stored,"detail":detail,"lookup":lookup});
    eprintln!("TYR259 late RegistryCreated public and stored evidence: {evidence}");
    assert_eq!(
        detail["data"]["resolver"]["address"],
        format!("{resolver:#x}"),
        "{evidence:#}"
    );
    assert_eq!(detail["data"]["primary_address"], HOLDER, "{evidence:#}");
    assert!(
        detail["data"]["resolution_unsupported_reason"].is_null(),
        "{evidence:#}"
    );
    assert!(
        stored["core"]["declared_summary"]["resolution_unsupported_reason"].is_null()
            && stored["record_serving_resource_id"].is_string()
            && lookup["data"][0]["record"] == detail["data"],
        "delayed RegistryCreated must refresh the persisted selection and actual lookup: {evidence:#}"
    );
    lookup_publication::assert_name_prepared_parity(database, CHILD).await?;
    Ok(evidence)
}

#[tokio::test]
async fn lookup_precomputation_late_registry_initialization_refreshes_stored_unknown() -> Result<()>
{
    let (database, mut logs, resolver) = setup().await?;
    let registry: Address = REGISTRY.parse()?;
    let owner: Address = HOLDER.parse()?;
    let implementation: Address = USER.parse()?;
    // VerifiableFactory.deployProxy with empty initialization bytes emits origin and upgrade,
    // but not RegistryCreated. No entry can be registered before roles are initialized.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1561 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1564 @ ens_v2_sepolia_20261001@07e55a05)
    let mut initial = transaction(
        119,
        0,
        vec![
            (registry, Upgraded { implementation }.encode_log_data()),
            (
                FACTORY.parse()?,
                ProxyDeployed {
                    sender: owner,
                    proxyAddress: registry,
                    salt: U256::from(25901),
                    implementation,
                }
                .encode_log_data(),
            ),
        ],
    );
    let unmounted: Address = UNMOUNTED.parse()?;
    initial.extend(transaction(
        119,
        1,
        vec![
            (unmounted, Upgraded { implementation }.encode_log_data()),
            (
                FACTORY.parse()?,
                ProxyDeployed {
                    sender: owner,
                    proxyAddress: unmounted,
                    salt: U256::from(25903),
                    implementation,
                }
                .encode_log_data(),
            ),
        ],
    ));
    let mirror = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0".parse()?;
    logs.retain(|log| {
        log.block_number <= BASE + 121
            && !(log.block_number == BASE + 120 && log.transaction_index == 4)
    });
    initial.extend(logs);
    initial.extend(transaction(120, 4, reserve(LABEL, registry, mirror)?));
    initial.extend(transaction(
        120,
        5,
        reserve(
            "unrelated259",
            "0x0000000000000000000000000000000000025902".parse()?,
            mirror,
        )?,
    ));
    initial.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    let mut block = None;
    let mut index = 0;
    for log in &mut initial {
        if block != Some(log.block_number) {
            block = Some(log.block_number);
            index = 0;
        }
        log.log_index = index;
        index += 1;
    }
    seed_and_run_with_targets(
        &database,
        &initial,
        119,
        121,
        &[(120, 5, DEPLOYER)],
        &[
            (119, 0, FACTORY),
            (119, 1, FACTORY),
            (120, 5, BATCH_REGISTRAR),
        ],
        None,
    )
    .await?;
    let before = lookup_publication::assert_name_prepared_parity(&database, CHILD).await?;
    assert_eq!(
        before["resolution_unsupported_reason"], "ens_v2_path_not_projected",
        "{before:#}"
    );
    assert!(before["resolver"].is_null() && before["records"].is_null());
    let stored_before = core(&database, CHILD).await?;
    assert_eq!(
        stored_before["core"]["declared_summary"]["resolution_unsupported_reason"],
        "ens_v2_path_not_projected"
    );
    assert!(stored_before["record_serving_resource_id"].is_null());
    let before_components = lookup_publication::components(&database).await?;
    let before_summary = summary(&database).await?;
    let unrelated = core(&database, UNRELATED).await?;
    let registrations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
        WHERE chain_id=$1 AND lower(raw_fact_ref->>'emitting_address')=$2
          AND event_kind IN ('RegistryCreated','RegistrationGranted','NameReserved')",
    )
    .bind(PATH_CHAIN)
    .bind(REGISTRY)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        registrations, 0,
        "origin alone cannot initialize or register a child"
    );
    let due: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_name_summary WHERE chain_id=$1 AND recompose_at<=$2",
    )
    .bind(PATH_CHAIN)
    .bind(1_700_000_000 + BASE + 122)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        due, 0,
        "no deadline may incidentally select the mounted child"
    );

    seed_and_run(&database, &initializer(registry, 122)?, 122, 122).await?;
    let forbidden_signals: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events
        WHERE chain_id=$1 AND block_number=$2 AND (logical_name_id IS NOT NULL OR
        event_kind IN ('ResolverChanged','SubregistryChanged','RegistryNodeChanged','RecordValueChanged','RegistrationGranted','NameReserved','Upgraded','ContractDiscovered'))")
        .bind(PATH_CHAIN).bind(BASE+122).fetch_one(&database.pool).await?;
    assert_eq!(
        forbidden_signals, 0,
        "initializer must be the sole path-support signal"
    );
    let announcement: (i64, Option<String>, Option<Uuid>) = sqlx::query_as("SELECT block_number,logical_name_id,resource_id FROM normalized_events
        WHERE chain_id=$1 AND event_kind='RegistryCreated' AND after_state->>'registry'=$2 AND consumer_visibility='activated'")
        .bind(PATH_CHAIN).bind(REGISTRY).fetch_one(&database.pool).await?;
    assert_eq!(announcement, (BASE + 122, None, None));
    let evidence = assert_retained(&database, resolver).await?;
    assert_eq!(
        summary(&database).await?,
        before_summary,
        "105 summary bytes and clocks do not change for this reason-only support transition"
    );
    assert_eq!(core(&database, UNRELATED).await?, unrelated);
    let after_components = lookup_publication::components(&database).await?;
    bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 121).await?;
    assert_eq!(
        lookup_publication::components(&database).await?,
        before_components
    );
    publish(&database, 122).await?;
    assert_retained(&database, resolver).await?;
    assert_eq!(
        lookup_publication::components(&database).await?,
        after_components
    );
    replay::assert_rebuild(&database, 122).await?;
    assert_retained(&database, resolver).await?;
    // An ordinary resolver value changes only its inventory; the selector plan is captured
    // separately on this exact block, including the zero-loop descendant-scan assertion.
    let record_edit = transaction(
        123,
        0,
        vec![(
            resolver,
            AddressChanged {
                node: bigname_lookup::ens_namehash_hex(CHILD)?.parse()?,
                coinType: U256::from(60),
                newAddress: GRANTEE.parse::<Address>()?.to_vec().into(),
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(
        &database,
        &record_edit,
        123,
        123,
        &[(123, 0, GRANTEE)],
        None,
    )
    .await?;
    let edited = lookup_publication::assert_name_prepared_parity(&database, CHILD).await?;
    assert_eq!(edited["primary_address"], GRANTEE);
    let prepared_before_unmounted = lookup_publication::components(&database).await?;
    seed_and_run(&database, &initializer(unmounted, 124)?, 124, 124).await?;
    assert_eq!(
        lookup_publication::components(&database).await?,
        prepared_before_unmounted,
        "initialization without a mounted surface cannot change prepared components"
    );
    if let Ok(output) = std::env::var("BIGNAME_TYR259_LATE_REGISTRY_DIR") {
        let output = std::path::PathBuf::from(output);
        std::fs::create_dir_all(&output)?;
        std::fs::write(
            output.join("fixture.json"),
            serde_json::to_vec_pretty(&json!({
                "database":database.database_name,"chain":PATH_CHAIN,"initial_end":BASE+121,"initializer":BASE+122,
                "record_only":BASE+123,"unmounted_initializer":BASE+124,
                "child":CHILD,"child_logical_name_id":format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?),
                "unrelated":UNRELATED,"unrelated_logical_name_id":format!("ens:{}", bigname_lookup::ens_namehash_hex(UNRELATED)?),
                "evidence":evidence
            }))?,
        )?;
        database.pool.close().await;
        database.lookup_pool.close().await;
        Ok(())
    } else {
        database.cleanup().await
    }
}
