//! A canonical retirement must not hide a still-used physical resolver from classifier work.
//! These admitted ABI logs exercise Interpret, Project and actual HTTP readers, not an EVM.
use super::*;
const PROXY: &str = "0x0000000000000000000000000000000000025920";
const IMPLEMENTATION: &str = "0x115eb53f0c60696633855f90b138178fb40b2b2c";
const UNKNOWN: &str = "0x0000000000000000000000000000000000025921";
const CLEARED: &str = "cleared.native105.eth";
const DIFFERENT: &str = "ens_v2_path_target_not_projected";
const UNKNOWN_PATH: &str = "ens_v2_path_not_projected";
sol! { event ResolverCreated(); }

fn deployment() -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    let proxy = PROXY.parse()?;
    let owner = HOLDER.parse()?;
    let implementation = IMPLEMENTATION.parse()?;
    // Initializer grants the root upgrade bit124, which _authorizeUpgrade checks without an
    // implementation allowlist. U is a distinct fresh deployment of this same source, with
    // its ERC1967 UUID/canUpgradeFrom checks satisfied; it remains undeclared to the indexer.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L106-L127 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L273-L289 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L344-L349 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L57-L60 @ ens_v2_sepolia_20261001@07e55a05)
    // The compiler input embeds project/lib/verifiable-factory/src/UUPSProxyLogic.sol:84-95.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1560-L1562 @ ens_v2_sepolia_20261001@07e55a05)
    Ok(vec![
        (proxy, Upgraded { implementation }.encode_log_data()),
        (proxy, ResolverCreated {}.encode_log_data()),
        (
            proxy,
            EACRolesChanged {
                resource: U256::ZERO,
                account: owner,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: roles(&[124, 252]),
            }
            .encode_log_data(),
        ),
        (
            FACTORY.parse()?,
            ProxyDeployed {
                sender: owner,
                proxyAddress: proxy,
                salt: U256::from(25920),
                implementation,
            }
            .encode_log_data(),
        ),
    ])
}
fn pointer(label: &str, resolver: Address) -> Result<(Address, alloy_primitives::LogData)> {
    Ok((
        REGISTRY.parse()?,
        ResolverUpdated {
            tokenId: label_token(label),
            resolver,
            sender: HOLDER.parse()?,
        }
        .encode_log_data(),
    ))
}
async fn invariant(database: &TestDatabase) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object(
        'summaries',(SELECT jsonb_agg(to_jsonb(r) ORDER BY logical_name_id) FROM project_name_summary r),
        'relations',(SELECT jsonb_agg(to_jsonb(r) ORDER BY logical_name_id,address,relation) FROM project_lookup_relation r),
        'pointers',(SELECT jsonb_agg(to_jsonb(r) ORDER BY resource_id) FROM project_resource_pointer r))")
        .fetch_one(&database.pool).await?)
}
async fn observe(database: &TestDatabase, expected: &str) -> Result<Value> {
    let detail = path_get(database, &format!("/v1/names/{LEAF}")).await?;
    let lookup = path_lookup(
        database,
        json!({"profile":"detail","inputs":[{"name":LEAF}]}),
    )
    .await?;
    let stored:Value=sqlx::query_scalar("SELECT to_jsonb(row) FROM project_lookup_name row WHERE chain_id=$1 AND logical_name_id=$2")
        .bind(PATH_CHAIN).bind(format!("ens:{}",bigname_lookup::ens_namehash_hex(LEAF)?)).fetch_one(&database.pool).await?;
    let evidence = json!({"compiled_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH,"detail":detail,"lookup":lookup,"stored_core":stored});
    eprintln!("TYR259 physical classifier public and stored evidence: {evidence}");
    assert_eq!(
        detail["data"]["resolution_unsupported_reason"], expected,
        "fresh detail: {evidence:#}"
    );
    assert!(detail["data"]["resolver"].is_null() && detail["data"]["primary_address"].is_null());
    assert!(stored["record_serving_resource_id"].is_null());
    assert!(
        lookup["data"][0]["record"]["resolver"].is_null()
            && lookup["data"][0]["record"]["primary_address"].is_null()
    );
    assert!(
        stored["core"]["declared_summary"]["resolution_unsupported_reason"] == expected
            && lookup["data"][0]["record"] == detail["data"],
        "physical resolver classifier must refresh the persisted reason and actual lookup: {evidence:#}"
    );
    lookup_publication::assert_name_prepared_parity(database, LEAF).await?;
    Ok(evidence)
}
async fn physical(database: &TestDatabase, resource: Uuid) -> Result<Value> {
    let predicate = bigname_storage::families::name::PHYSICAL_POINTER_EVENT_SQL;
    sqlx::query_scalar(&format!("SELECT to_jsonb(event) FROM normalized_events event
        WHERE event.chain_id=$1 AND event.resource_id=$2 AND event.event_kind='ResolverChanged'
          AND event.source_family IN ('ens_v2_root_l1','ens_v2_registry_l1')
          AND event.consumer_visibility='activated' AND event.canonicality_state IN ('canonical','safe','finalized')
          AND {predicate}
        ORDER BY event.block_number DESC,event.transaction_index DESC NULLS LAST,event.log_index DESC NULLS LAST,event.event_identity COLLATE \"C\" DESC LIMIT 1"))
        .bind(PATH_CHAIN).bind(resource).fetch_one(&database.pool).await.map_err(Into::into)
}
async fn upgrade(
    database: &TestDatabase,
    block: i64,
    implementation: &str,
    clock: u64,
) -> Result<()> {
    seed_and_run_with(
        database,
        &transaction(
            block,
            0,
            vec![(
                PROXY.parse()?,
                Upgraded {
                    implementation: implementation.parse()?,
                }
                .encode_log_data(),
            )],
        ),
        block,
        block,
        &[(block, 0, HOLDER)],
        Some(clock as i64),
    )
    .await
}

#[tokio::test]
async fn lookup_precomputation_classifier_upgrade_refreshes_retired_physical_path() -> Result<()> {
    let (database, resolver, resources) = setup_native().await?;
    let owner = HOLDER.parse()?;
    let proxy = PROXY.parse()?;
    let mut logs = transaction(124, 0, deployment()?);
    logs.extend(transaction(124, 1, vec![pointer("leaf", proxy)?]));
    // A genuinely cleared entry keeps historical nonzero P. Canonical expiry skips its null pointer.
    // The candidate filter must not mistake that history for the current physical resolver.
    logs.extend(transaction(
        124,
        2,
        register(
            REGISTRY.parse()?,
            "cleared",
            proxy,
            Address::ZERO,
            T + 40,
            owner,
        )?,
    ));
    logs.extend(transaction(
        124,
        3,
        vec![pointer("cleared", Address::ZERO)?],
    ));
    let node = bigname_lookup::ens_namehash_hex(CLEARED)?.parse()?;
    logs.extend(transaction(
        124,
        4,
        vec![
            (
                V1.parse()?,
                NewOwner {
                    node: bigname_lookup::ens_namehash_hex(ALT)?.parse()?,
                    label: keccak256("cleared"),
                    owner,
                }
                .encode_log_data(),
            ),
            (
                V1.parse()?,
                NewResolver { node, resolver }.encode_log_data(),
            ),
        ],
    ));
    logs.extend(transaction(
        124,
        5,
        vec![
            (
                resolver,
                NameChanged {
                    node,
                    name: CLEARED.into(),
                }
                .encode_log_data(),
            ),
            (
                resolver,
                AddressChanged {
                    node,
                    coinType: U256::from(60),
                    newAddress: owner.to_vec().into(),
                }
                .encode_log_data(),
            ),
        ],
    ));
    seed_and_run_with_targets(
        &database,
        &logs,
        124,
        124,
        &[(124, 0, HOLDER)],
        &[(124, 0, FACTORY)],
        None,
    )
    .await?;
    let cleared_resource = physical_resource(&database, REGISTRY, "cleared").await?;
    empty_clock(&database, 125, T).await?;
    for resource in [resources["leaf"], cleared_resource] {
        let pointer:Value=sqlx::query_scalar("SELECT to_jsonb(row) FROM project_resource_pointer row WHERE chain_id=$1 AND resource_id=$2")
            .bind(PATH_CHAIN).bind(resource).fetch_one(&database.pool).await?;
        assert!(pointer["resolver_address"].is_null(), "{pointer:#}");
        assert_eq!(pointer["nonzero_resolver_address"], PROXY);
        let retirement: Value =
            sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE event_identity=$1")
                .bind(
                    pointer["pointer_position"]["event_identity"]
                        .as_str()
                        .unwrap(),
                )
                .fetch_one(&database.pool)
                .await?;
        if resource == resources["leaf"] {
            assert_eq!(retirement["source_event"], "RegistryPathExpired");
            assert_eq!(retirement["derived_from"], "interpreter_state");
            assert_eq!(
                retirement["terminal_reason"],
                "registry_name_binding_expired"
            );
        } else {
            // Expiry does not replace an already-null pointer with a synthetic clear.
            assert_eq!(retirement["source_event"], "ResolverUpdated");
            assert!(retirement["derived_from"].is_null());
            assert!(retirement["terminal_reason"].is_null());
        }
    }
    assert_eq!(
        physical(&database, resources["leaf"]).await?["after_state"]["resolver"],
        PROXY
    );
    let cleared = physical(&database, cleared_resource).await?;
    assert!(
        cleared["after_state"]["resolver"].is_null()
            || cleared["after_state"]["resolver"] == format!("{:#x}", Address::ZERO)
    );
    assert_eq!(cleared["after_state"]["source_event"], "ResolverUpdated");
    let expiry: String = sqlx::query_scalar(
        "SELECT expiry::text FROM project_ens_v2_entry_owner WHERE chain_id=$1 AND resource_id=$2",
    )
    .bind(PATH_CHAIN)
    .bind(resources["leaf"])
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(expiry.parse::<u64>()?, T + 10);
    let only_physical: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_resource_pointer WHERE chain_id=$1 AND resolver_address=$2",
    )
    .bind(PATH_CHAIN)
    .bind(PROXY)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        only_physical, 0,
        "no current pointer may accidentally select P"
    );
    let before = observe(&database, DIFFERENT).await?;
    let unchanged = invariant(&database).await?;
    let family_before = replay::families(&database).await?;
    let controls_before = vec![
        lookup_publication::assert_name_prepared_parity(&database, CLEARED).await?,
        lookup_publication::assert_name_prepared_parity(&database, DEEP).await?,
        lookup_publication::assert_name_prepared_parity(&database, RESET).await?,
    ];
    upgrade(&database, 126, UNKNOWN, T + 1).await?;
    let upgraded:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(event) FROM normalized_events event WHERE chain_id=$1 AND block_number=$2 AND consumer_visibility='activated' ORDER BY event_identity")
        .bind(PATH_CHAIN).bind(BASE+126).fetch_all(&database.pool).await?;
    assert_eq!(
        upgraded.len(),
        1,
        "only the proxy upgrade may trigger this block: {upgraded:#?}"
    );
    assert_eq!(upgraded[0]["event_kind"], "Upgraded");
    assert!(upgraded[0]["logical_name_id"].is_null() && upgraded[0]["resource_id"].is_null());
    let classification:Value=sqlx::query_scalar("SELECT to_jsonb(row) FROM project_resolver_classification row WHERE chain_id=$1 AND resolver_address=$2")
        .bind(PATH_CHAIN).bind(PROXY).fetch_one(&database.pool).await?;
    assert_eq!(classification["support_status"], "unsupported");
    assert_eq!(
        classification["unsupported_reason"],
        "resolver_implementation_not_declared"
    );
    assert_eq!(
        invariant(&database).await?,
        unchanged,
        "classification changes no authority, clocks, relations or pointer state"
    );
    assert_eq!(
        physical(&database, resources["leaf"]).await?["after_state"]["resolver"],
        PROXY
    );
    let after = observe(&database, UNKNOWN_PATH).await?;
    for field in [
        "authority",
        "registration_id",
        "status",
        "owner",
        "manager",
        "ens_v1",
        "created_at",
    ] {
        assert_eq!(
            before["detail"]["data"][field], after["detail"]["data"][field],
            "{field}"
        );
    }
    let controls_after = vec![
        lookup_publication::assert_name_prepared_parity(&database, CLEARED).await?,
        lookup_publication::assert_name_prepared_parity(&database, DEEP).await?,
        lookup_publication::assert_name_prepared_parity(&database, RESET).await?,
    ];
    assert_eq!(controls_after, controls_before);
    let family_after = replay::families(&database).await?;
    assert_eq!(
        bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 125).await?,
        1
    );
    assert_eq!(replay::families(&database).await?, family_before);
    observe(&database, DIFFERENT).await?;
    publish(&database, 126).await?;
    assert_eq!(replay::families(&database).await?, family_after);
    observe(&database, UNKNOWN_PATH).await?;
    replay::assert_rebuild(&database, 126).await?;
    observe(&database, UNKNOWN_PATH).await?;
    upgrade(&database, 127, IMPLEMENTATION, T + 2).await?;
    observe(&database, DIFFERENT).await?;
    let before_record = lookup_publication::components(&database).await?;
    let record = transaction(
        128,
        0,
        vec![(
            resolver,
            AddressChanged {
                node: bigname_lookup::ens_namehash_hex(LEAF)?.parse()?,
                coinType: U256::from(60),
                newAddress: GRANTEE.parse::<Address>()?.to_vec().into(),
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(&database, &record, 128, 128, &[], Some((T + 3) as i64)).await?;
    observe(&database, DIFFERENT).await?;
    assert_eq!(
        lookup_publication::components(&database).await?,
        before_record,
        "withheld record update does not rewrite stored components"
    );
    let evidence = json!({"database":database.database_name,"compiled_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH,
        "chain":PATH_CHAIN,"classifier_block":BASE+126,"record_block":BASE+128,"clock":T+1,
        "proxy":PROXY,"implementation":IMPLEMENTATION,"unknown_implementation":UNKNOWN,
        "resource":resources["leaf"],"cleared_resource":cleared_resource,"classification":classification,
        "leaf":format!("ens:{}",bigname_lookup::ens_namehash_hex(LEAF)?),
        "cleared":format!("ens:{}",bigname_lookup::ens_namehash_hex(CLEARED)?),
        "unrelated":format!("ens:{}",bigname_lookup::ens_namehash_hex(DEEP)?),"before":before,"after":after});
    if let Ok(path) = std::env::var("BIGNAME_TYR259_CLASSIFIER_EVIDENCE") {
        std::fs::write(path, serde_json::to_vec_pretty(&evidence)?)?;
        database.pool.close().await;
        database.lookup_pool.close().await;
        Ok(())
    } else {
        database.cleanup().await
    }
}
