use super::*;

#[tokio::test]
async fn factory_origin_before_delayed_initialization_does_not_admit_a_registry() -> Result<()> {
    let mut logs = Logs::default();
    logs.user(0, PARENT)
        .register(1, PARENT, LABEL, ALICE, (TIME + 100) as u64, U256::ZERO)
        // Empty initialization data: the factory creates the proxy, but it emits no RegistryCreated.
        .upgrade(2, WRAPPER, WRAPPER_IMPL)
        .factory(2, FACTORY, WRAPPER, ADMIN, WRAPPER_IMPL)
        .initialize_wrapper(5, WRAPPER, PARENT, LABEL, bitmap())
        .approve(6, PARENT, ALICE, OPERATOR, true);
    let database = setup(&logs).await?;
    interpret(&database, true, true).await?;
    let origin: i64 = sqlx::query_scalar(
        "SELECT block_number FROM normalized_events
        WHERE event_kind = 'ContractDiscovered' AND lower(after_state ->> 'proxy_address') = $1",
    )
    .bind(WRAPPER)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(origin, BASE + 2);
    let admission: i64 = sqlx::query_scalar(
        "SELECT active_from_block_number FROM contract_instance_addresses
        WHERE chain_id = $1 AND lower(address) = $2 AND deactivated_at IS NULL",
    )
    .bind(CHAIN)
    .bind(WRAPPER)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        admission,
        BASE + 5,
        "the retained factory fact does not widen admission"
    );
    publish(&database, 4).await?;
    assert!(
        registry_page(&database, WRAPPER).await?["data"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    publish(&database, 6).await?;
    let page = registry_page(&database, WRAPPER).await?;
    assert_derived(&page, ALICE, "holder", PARENT, ALICE);
    assert_derived(&page, OPERATOR, "operator", PARENT, ALICE);
    database.cleanup().await
}

#[tokio::test]
async fn self_announced_unknown_and_wrong_factory_registries_do_not_derive_or_replace() -> Result<()>
{
    let mut logs = Logs::default();
    logs.user(0, PARENT)
        .register(1, PARENT, LABEL, ALICE, (TIME + 100) as u64, U256::ZERO)
        .initialize_wrapper(2, WRAPPER, PARENT, LABEL, bitmap() | admin_bitmap())
        .factory(2, UNKNOWN, WRAPPER, ADMIN, WRAPPER_IMPL)
        .initialize_wrapper(3, CHILD, PARENT, LABEL, bitmap() | admin_bitmap())
        .factory(3, FACTORY, CHILD, ADMIN, UNKNOWN)
        .roles(4, WRAPPER, U256::ZERO, ALICE, U256::ZERO, bit(20))
        .roles(4, CHILD, U256::ZERO, ALICE, U256::ZERO, bit(20))
        .approve(4, PARENT, ALICE, OPERATOR, true);
    let database = setup(&logs).await?;
    interpret(&database, false, false).await?;
    publish(&database, 4).await?;
    for registry in [WRAPPER, CHILD] {
        let page = registry_page(&database, registry).await?;
        let alice = for_subject(&page, ALICE);
        assert_eq!(alice.len(), 1, "{page:#}");
        assert_eq!(alice[0]["powers"], json!(["set_subregistry"]));
        assert!(alice[0].get("grant_relation").is_none());
        assert!(for_subject(&page, OPERATOR).is_empty());
    }
    let origins: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events
        WHERE event_kind = 'ContractDiscovered' AND lower(after_state ->> 'proxy_address') IN ($1, $2)")
        .bind(WRAPPER).bind(CHILD).fetch_one(&database.pool).await?;
    assert_eq!(origins, 0);
    database.cleanup().await
}

#[tokio::test]
async fn canonical_departure_and_return_disqualify_both_wrapper_and_user_parent() -> Result<()> {
    for registry in [WRAPPER, PARENT] {
        let known = if registry == WRAPPER {
            WRAPPER_IMPL
        } else {
            USER_IMPL
        };
        let mut logs = manual_logs(U256::ZERO, (TIME + 100) as u64);
        // These start from known upgrade authority, whose first departure emits Upgraded.
        // The unknown implementation can subsequently announce a familiar implementation;
        // that final event cannot prove an uninterrupted storage history.
        // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1171 @ ens_v2_sepolia_20261001@07e55a05)
        logs.upgrade(5, registry, known)
            .upgrade(7, registry, UNKNOWN)
            .upgrade(7, registry, known);
        let database = setup(&logs).await?;
        interpret(&database, false, false).await?;
        publish(&database, 4).await?;
        assert_derived(
            &registry_page(&database, WRAPPER).await?,
            ALICE,
            "holder",
            PARENT,
            ALICE,
        );
        publish(&database, 5).await?;
        assert_derived(
            &registry_page(&database, WRAPPER).await?,
            ALICE,
            "holder",
            PARENT,
            ALICE,
        );
        publish(&database, 7).await?;
        let departed = registry_page(&database, WRAPPER).await?;
        assert_eq!(
            for_subject(&departed, ALICE)[0]["powers"],
            json!(["set_subregistry"])
        );
        assert!(
            for_subject(&departed, ALICE)[0]
                .get("grant_relation")
                .is_none()
        );
        assert_eq!(
            for_subject(&departed, OPERATOR)[0]["powers"],
            json!(["set_resolver"])
        );
        // The actual departure block leaves the readable chain. Project undoes it and the
        // indexed proof sees the earlier canonical history; no classification row is toggled.
        head(&database, 6).await?;
        sqlx::query(
            "UPDATE chain_lineage SET canonicality_state = 'orphaned'
            WHERE chain_id = $1 AND block_number >= $2",
        )
        .bind(CHAIN)
        .bind(BASE + 7)
        .execute(&database.pool)
        .await?;
        publish(&database, 6).await?;
        let restored = registry_page(&database, WRAPPER).await?;
        assert_derived(&restored, ALICE, "holder", PARENT, ALICE);
        assert_derived(&restored, OPERATOR, "operator", PARENT, ALICE);
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn factory_migration_roots_and_deeper_wrappers_work_in_warm_cold_and_split_replay()
-> Result<()> {
    let mut logs = Logs::default();
    // Creation segments of locked second-level and deeper migration. The normal migration
    // bitmap has no set_parent bit; it grants registrar/renew/upgrade/can-name and admins.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L110-L164 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L211-L226 @ ens_v2_sepolia_20261001@07e55a05)
    let migration_roles =
        bit(0) | bit(16) | bit(120) | bit(124) | bit(128) | bit(144) | bit(248) | bit(252);
    logs.roles(0, ETH, U256::ZERO, ADMIN, U256::ZERO, parent_admin())
        .register(1, ETH, LABEL, ALICE, (TIME + 100) as u64, U256::ZERO)
        .wrapper(2, WRAPPER, ETH, LABEL, LOCKED, migration_roles)
        .approve(3, ETH, ALICE, OPERATOR, true)
        .register_by(
            3,
            WRAPPER,
            "child",
            (BOB, ALICE),
            (TIME + 100) as u64,
            U256::ZERO,
        )
        .wrapper(4, CHILD, WRAPPER, "child", WRAPPER, migration_roles)
        .approve(5, WRAPPER, BOB, OPERATOR, true);
    let mut expected: Option<Vec<Value>> = None;
    for (split, cold) in [(false, false), (true, false), (true, true)] {
        let database = setup(&logs).await?;
        interpret(&database, split, cold).await?;
        publish(&database, 5).await?;
        let mut pages = Vec::new();
        for (registry, owner, parent) in [(WRAPPER, ALICE, ETH), (CHILD, BOB, WRAPPER)] {
            let page = registry_page(&database, registry).await?;
            let holder = for_subject(&page, owner);
            let operator = for_subject(&page, OPERATOR);
            assert_eq!(holder.len(), 1, "{page:#}");
            assert_eq!(operator.len(), 1, "{page:#}");
            assert_eq!(holder[0]["grant_relation"], "holder");
            assert_eq!(operator[0]["grant_relation"], "operator");
            assert_eq!(holder[0]["powers"], operator[0]["powers"]);
            assert_eq!(
                operator[0]["grant_scope"]["detail"]["authority_contract"],
                parent
            );
            pages.push(page["data"].clone());
        }
        if let Some(expected) = &expected {
            assert_eq!(expected, &pages);
        } else {
            expected = Some(pages);
        }
        let correlations: i64 = sqlx::query_scalar("SELECT count(DISTINCT registry_address)
            FROM migration_discovery_associations WHERE chain_id = $1 AND consumer_visibility = 'activated'")
            .bind(CHAIN).fetch_one(&database.pool).await?;
        assert_eq!(
            correlations, 2,
            "both actual creation segments preserve ordinary migration correlation"
        );
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn missing_reset_or_wrong_hash_publication_refuses_wrapper_permission_reads() -> Result<()> {
    let database = setup(&manual_logs(U256::ZERO, (TIME + 100) as u64)).await?;
    interpret(&database, false, false).await?;
    publish(&database, 4).await?;
    let uri = format!("/v1/permissions?registry=11155111:{WRAPPER}");
    let saved: Value = sqlx::query_scalar(
        "SELECT to_jsonb(marker) FROM project_family_marker marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&database.pool)
    .await?;
    for mutation in [
        "UPDATE project_family_marker SET state = 'bootstrap_pending' WHERE chain_id = $1",
        "UPDATE project_family_marker SET input_content_hash = 'wrong' WHERE chain_id = $1",
    ] {
        sqlx::query(mutation)
            .bind(CHAIN)
            .execute(&database.pool)
            .await?;
        let (status, _) = response(&database, &uri).await?;
        assert_eq!(status, StatusCode::CONFLICT);
        sqlx::query(
            "UPDATE project_family_marker SET state = $2,
            input_content_hash = $3 WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .bind(saved["state"].as_str().unwrap())
        .bind(saved["input_content_hash"].as_str().unwrap())
        .execute(&database.pool)
        .await?;
    }
    sqlx::query("DELETE FROM project_family_marker WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(&database.pool)
        .await?;
    let (status, _) = response(&database, &uri).await?;
    assert_eq!(status, StatusCode::CONFLICT);
    database.cleanup().await
}

#[tokio::test]
async fn unsupported_parent_origin_is_not_rehabilitated_by_a_later_known_implementation()
-> Result<()> {
    let mut logs = Logs::default();
    // Unknown code can emit the registry ABI. A later familiar implementation does not prove
    // the parent's ownership storage history, even while W itself has an exact factory origin.
    logs.upgrade(0, PARENT, UNKNOWN)
        .push(0, PARENT, RegistryCreated {}.encode_log_data())
        .roles(0, PARENT, U256::ZERO, ADMIN, U256::ZERO, parent_admin())
        .factory(0, FACTORY, PARENT, ADMIN, UNKNOWN)
        .register(1, PARENT, LABEL, ALICE, (TIME + 100) as u64, U256::ZERO)
        .wrapper(2, WRAPPER, PARENT, LABEL, ADMIN, bitmap() | admin_bitmap())
        .roles(3, WRAPPER, U256::ZERO, ALICE, U256::ZERO, bit(20))
        .upgrade(3, PARENT, USER_IMPL)
        .approve(4, PARENT, ALICE, OPERATOR, true);
    let database = setup(&logs).await?;
    interpret(&database, false, false).await?;
    publish(&database, 4).await?;
    let page = registry_page(&database, WRAPPER).await?;
    let owner = for_subject(&page, ALICE);
    assert_eq!(owner.len(), 1, "{page:#}");
    assert_eq!(owner[0]["powers"], json!(["set_subregistry"]));
    assert!(owner[0].get("grant_relation").is_none());
    assert!(for_subject(&page, OPERATOR).is_empty());
    database.cleanup().await
}
