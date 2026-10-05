use super::*;

#[tokio::test]
async fn manual_factory_wrapper_derives_owner_without_parent_roles_and_pages_shared_rows()
-> Result<()> {
    let logs = manual_logs(U256::ZERO, (TIME + 100) as u64);
    let database = setup(&logs).await?;
    interpret(&database, false, false).await?;
    let origins: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
        WHERE source_family = 'ens_v2_migration_l1' AND event_kind = 'ContractDiscovered'
          AND lower(after_state ->> 'proxy_address') IN ($1, $2)",
    )
    .bind(PARENT)
    .bind(WRAPPER)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        origins, 2,
        "baseline drops the two independent known factory origins"
    );
    publish(&database, 4).await?;
    let page = registry_page(&database, WRAPPER).await?;
    assert_derived(&page, ALICE, "holder", PARENT, ALICE);
    assert_derived(&page, OPERATOR, "operator", PARENT, ALICE);
    assert_eq!(
        page["meta"]["unlisted_permission_surfaces"],
        json!(["ens_v2_registry_operators"])
    );
    let parent_roles: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_grant
        WHERE chain_id = $1 AND subject = $2 AND scope_detail ->> 'registry_address' = $3",
    )
    .bind(CHAIN)
    .bind(ALICE)
    .bind(PARENT)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        parent_roles, 0,
        "ownership, not a parent role grant, supplies the holder"
    );
    let owner_rows = for_subject(&page, ALICE);
    assert_eq!(
        owner_rows.len(),
        1,
        "self approval does not duplicate the holder"
    );
    for subject in [ALICE, OPERATOR] {
        let address_page = payload(
            &database,
            &format!("/v1/permissions?address={subject}&namespace=ens"),
        )
        .await?;
        let expected = for_subject(&page, subject)[0];
        assert!(
            address_page["data"].as_array().unwrap().contains(expected),
            "{address_page:#}"
        );
    }
    let paged = pages(
        &database,
        &format!("/v1/permissions?registry=11155111:{WRAPPER}&page_size=1"),
    )
    .await?;
    let rows: Vec<Value> = paged
        .iter()
        .flat_map(|page| page["data"].as_array().unwrap().clone())
        .collect();
    assert_eq!(rows, *page["data"].as_array().unwrap());
    let filtered = payload(
        &database,
        &format!("/v1/permissions?registry=11155111:{WRAPPER}&address={OPERATOR}"),
    )
    .await?;
    assert_derived(&filtered, OPERATOR, "operator", PARENT, ALICE);
    database.cleanup().await
}

#[tokio::test]
async fn empty_replacement_dormant_grants_transfer_expiry_renewal_and_approval_revocation()
-> Result<()> {
    let mut logs = manual_logs(bit(156), (TIME + 11) as u64);
    // Preserve the initialized can_transfer_admin bit. ADMIN's ordinary set-parent role was legally
    // granted before they were stripped; changing to an empty virtual parent exercises the
    // same replacement without trying to recreate a forbidden root admin grant.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L250-L287 @ ens_v2_sepolia_20261001@07e55a05)
    logs.user(0, EMPTY_PARENT)
        .register(1, EMPTY_PARENT, LABEL, ALICE, (TIME + 11) as u64, bit(156))
        .approve(4, EMPTY_PARENT, ALICE, OPERATOR, true)
        .parent(5, WRAPPER, EMPTY_PARENT, LABEL, ADMIN)
        .approve(6, EMPTY_PARENT, ALICE, OPERATOR, false)
        .approve(7, EMPTY_PARENT, ALICE, OPERATOR, true)
        .push(
            8,
            EMPTY_PARENT,
            TransferSingle {
                operator: address(ALICE),
                from: address(ALICE),
                to: address(BOB),
                id: id(LABEL),
                value: U256::from(1),
            }
            .encode_log_data(),
        )
        .roles(8, EMPTY_PARENT, id(LABEL), ALICE, bit(156), U256::ZERO)
        .roles(8, EMPTY_PARENT, id(LABEL), BOB, U256::ZERO, bit(156))
        .push(
            8,
            PARENT,
            TransferSingle {
                operator: address(ALICE),
                from: address(ALICE),
                to: address(BOB),
                id: id(LABEL),
                value: U256::from(1),
            }
            .encode_log_data(),
        )
        .roles(8, PARENT, id(LABEL), ALICE, bit(156), U256::ZERO)
        .roles(8, PARENT, id(LABEL), BOB, U256::ZERO, bit(156))
        .parent(9, WRAPPER, PARENT, LABEL, ADMIN)
        .approve(10, PARENT, BOB, OPERATOR, true)
        .renew(12, PARENT, LABEL, (TIME + 100) as u64);
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
    let empty = registry_page(&database, WRAPPER).await?;
    assert!(
        for_subject(&empty, ALICE).is_empty() && for_subject(&empty, OPERATOR).is_empty(),
        "{empty:#}"
    );
    let dormant: Vec<Value> = sqlx::query_scalar(
        "SELECT effective_powers FROM project_grant
        WHERE chain_id = $1 AND subject IN ($2, $3) AND scope = 'root'
          AND scope_detail ->> 'registry_address' = $4 ORDER BY subject",
    )
    .bind(CHAIN)
    .bind(ALICE)
    .bind(OPERATOR)
    .bind(WRAPPER)
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(
        dormant,
        vec![json!(["set_subregistry"]), json!(["set_resolver"])]
    );
    publish(&database, 6).await?;
    let revoked = registry_page(&database, WRAPPER).await?;
    assert_eq!(
        for_subject(&revoked, OPERATOR)[0]["powers"],
        json!(["set_resolver"])
    );
    assert!(
        for_subject(&revoked, OPERATOR)[0]
            .get("grant_relation")
            .is_none()
    );
    publish(&database, 7).await?;
    assert!(for_subject(&registry_page(&database, WRAPPER).await?, OPERATOR).is_empty());
    publish(&database, 8).await?;
    let transferred = registry_page(&database, WRAPPER).await?;
    assert_eq!(
        for_subject(&transferred, ALICE)[0]["powers"],
        json!(["set_subregistry"])
    );
    assert_eq!(
        for_subject(&transferred, OPERATOR)[0]["powers"],
        json!(["set_resolver"])
    );
    assert!(for_subject(&transferred, BOB).is_empty());
    publish(&database, 10).await?;
    let new_owner = registry_page(&database, WRAPPER).await?;
    assert_derived(&new_owner, BOB, "holder", PARENT, BOB);
    assert_derived(&new_owner, OPERATOR, "operator", PARENT, BOB);
    publish(&database, 11).await?;
    let expired = registry_page(&database, WRAPPER).await?;
    assert!(
        for_subject(&expired, BOB).is_empty(),
        "expiry equals publication time"
    );
    assert_eq!(
        for_subject(&expired, OPERATOR)[0]["powers"],
        json!(["set_resolver"])
    );
    publish(&database, 12).await?;
    let renewed = registry_page(&database, WRAPPER).await?;
    assert_derived(&renewed, BOB, "holder", PARENT, BOB);
    assert_derived(&renewed, OPERATOR, "operator", PARENT, BOB);
    database.cleanup().await
}

#[tokio::test]
async fn token_and_name_reads_keep_requested_identity_and_role_summary_excludes_root_rows()
-> Result<()> {
    let mut logs = manual_logs(U256::ZERO, (TIME + 100) as u64);
    logs.roles(0, ETH, U256::ZERO, ADMIN, U256::ZERO, parent_admin())
        .register(0, ETH, "garden", ADMIN, (TIME + 100) as u64, U256::ZERO)
        .push(
            0,
            ETH,
            SubregistryUpdated {
                tokenId: id("garden"),
                subregistry: address(PARENT),
                sender: address(ADMIN),
            }
            .encode_log_data(),
        )
        .parent(0, PARENT, ETH, "garden", ADMIN)
        .push(
            2,
            PARENT,
            SubregistryUpdated {
                tokenId: id(LABEL),
                subregistry: address(WRAPPER),
                sender: address(ADMIN),
            }
            .encode_log_data(),
        )
        .register(
            3,
            WRAPPER,
            "leaf",
            BOB,
            (TIME + 100) as u64,
            bit(24) | bit(156),
        );
    let database = setup(&logs).await?;
    interpret(&database, false, false).await?;
    publish(&database, 4).await?;
    let token: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM project_ens_v2_entry_owner
        WHERE chain_id = $1 AND registry = $2 AND entry_key = $3",
    )
    .bind(CHAIN)
    .bind(WRAPPER)
    .bind(format!("{:#066x}", id("leaf")))
    .fetch_one(&database.pool)
    .await?;
    for selector in [
        format!("registration_id={token}"),
        "name=leaf.holder.garden.eth".into(),
    ] {
        let page = payload(&database, &format!("/v1/permissions?{selector}")).await?;
        assert_derived(&page, ALICE, "holder", PARENT, ALICE);
        assert_derived(&page, OPERATOR, "operator", PARENT, ALICE);
        for row in page["data"].as_array().unwrap() {
            assert_eq!(row["registration_id"], token.to_string());
            assert_eq!(row["name"], "leaf.holder.garden.eth");
            assert_eq!(
                row["authority_context"],
                if selector.starts_with("name=") {
                    "current_for_name"
                } else {
                    "resource_audit"
                }
            );
        }
        let paged = pages(
            &database,
            &format!("/v1/permissions?{selector}&page_size=1"),
        )
        .await?;
        assert_eq!(
            paged
                .iter()
                .flat_map(|page| page["data"].as_array().unwrap().clone())
                .collect::<Vec<_>>(),
            *page["data"].as_array().unwrap()
        );
    }
    let names = payload(
        &database,
        &format!("/v1/addresses/{BOB}/names?namespace=ens&include=role_summary"),
    )
    .await?;
    let leaf = names["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "leaf.holder.garden.eth")
        .context("leaf name")?;
    let summary = leaf["role_summary"]
        .as_array()
        .context("leaf role summary")?;
    assert!(
        summary
            .iter()
            .all(|row| row["address"] != ALICE && row["address"] != OPERATOR),
        "{leaf:#}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn current_parent_and_raw_label_choose_owner_without_recursive_parent_classification()
-> Result<()> {
    let mut logs = manual_logs(U256::ZERO, (TIME + 100) as u64);
    logs.roles(0, ETH, U256::ZERO, ADMIN, U256::ZERO, parent_admin())
        .register(0, ETH, "other", BOB, (TIME + 100) as u64, U256::ZERO)
        .roles(
            2,
            WRAPPER,
            U256::ZERO,
            ETH,
            U256::ZERO,
            bit(0) | bit(8) | bit(16),
        )
        .parent(5, WRAPPER, ETH, "other", ADMIN)
        .parent(6, WRAPPER, ZERO, "other", ADMIN)
        .parent(7, WRAPPER, PARENT, LABEL, ADMIN);
    let database = setup(&logs).await?;
    interpret(&database, true, true).await?;
    publish(&database, 4).await?;
    assert_derived(
        &registry_page(&database, WRAPPER).await?,
        ALICE,
        "holder",
        PARENT,
        ALICE,
    );
    publish(&database, 5).await?;
    let changed = registry_page(&database, WRAPPER).await?;
    assert_derived_powers(
        &changed,
        BOB,
        "holder",
        ETH,
        BOB,
        json!(["registrar", "set_parent", "renew"]),
    );
    assert_eq!(
        for_subject(&changed, ALICE)[0]["powers"],
        json!(["set_subregistry"])
    );
    publish(&database, 6).await?;
    assert!(for_subject(&registry_page(&database, WRAPPER).await?, BOB).is_empty());
    publish(&database, 7).await?;
    assert_derived(
        &registry_page(&database, WRAPPER).await?,
        ALICE,
        "holder",
        PARENT,
        ALICE,
    );
    database.cleanup().await
}

#[tokio::test]
async fn self_parent_combines_token_and_root_operator_powers_under_one_account_key() -> Result<()> {
    let mut logs = manual_logs(U256::ZERO, (TIME + 100) as u64);
    // A mutable WrapperRegistry can register its own parent label and then setParent to itself.
    // Recognition proves W once; getRoles uses the ordinary current entry owner, without
    // recursively trying to classify the ancestors of that entry.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L164-L181 @ ens_v2_sepolia_20261001@07e55a05)
    logs.roles(0, ETH, U256::ZERO, ADMIN, U256::ZERO, parent_admin())
        .register(0, ETH, "garden", ADMIN, (TIME + 100) as u64, U256::ZERO)
        .push(
            0,
            ETH,
            SubregistryUpdated {
                tokenId: id("garden"),
                subregistry: address(PARENT),
                sender: address(ADMIN),
            }
            .encode_log_data(),
        )
        .parent(0, PARENT, ETH, "garden", ADMIN)
        .push(
            2,
            PARENT,
            SubregistryUpdated {
                tokenId: id(LABEL),
                subregistry: address(WRAPPER),
                sender: address(ADMIN),
            }
            .encode_log_data(),
        )
        .register(4, WRAPPER, "self", ALICE, (TIME + 100) as u64, bit(24))
        .roles(
            2,
            WRAPPER,
            U256::ZERO,
            WRAPPER,
            U256::ZERO,
            bit(0) | bit(8) | bit(16),
        )
        .parent(5, WRAPPER, WRAPPER, "self", ADMIN)
        .approve(6, WRAPPER, ALICE, OPERATOR, true);
    let database = setup(&logs).await?;
    interpret(&database, false, false).await?;
    publish(&database, 6).await?;
    let token: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM project_ens_v2_entry_owner
        WHERE chain_id = $1 AND registry = $2 AND entry_key = $3",
    )
    .bind(CHAIN)
    .bind(WRAPPER)
    .bind(format!("{:#066x}", id("self")))
    .fetch_one(&database.pool)
    .await?;
    let uri = format!("/v1/permissions?registration_id={token}");
    let page = payload(&database, &uri).await?;
    let operators = for_subject(&page, OPERATOR);
    assert_eq!(operators.len(), 1, "one candidate account key: {page:#}");
    assert_eq!(operators[0]["grant_relation"], "operator");
    assert_eq!(
        operators[0]["grant_scope"]["detail"]["authority_contract"],
        WRAPPER
    );
    let powers: std::collections::BTreeSet<&str> = operators[0]["powers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|power| power.as_str().unwrap())
        .collect();
    assert_eq!(
        powers,
        std::collections::BTreeSet::from(["registrar", "set_parent", "renew", "set_resolver",])
    );
    // The bounded expansion used by address-name role_summary keeps only token roles,
    // even when a self-parent approval shares the root operator's account key.
    let inline = bigname_storage::load_bounded_effective_permissions_by_resource_ids(
        &database.lookup_pool,
        &[token],
        Some("ens"),
        200,
    )
    .await?;
    let inline_operator = inline
        .iter()
        .find(|row| row.subject == OPERATOR)
        .context("inline token operator")?;
    assert_eq!(inline_operator.effective_powers, json!(["set_resolver"]));
    let paged = pages(&database, &format!("{uri}&page_size=1")).await?;
    let rows: Vec<Value> = paged
        .iter()
        .flat_map(|page| page["data"].as_array().unwrap().clone())
        .collect();
    assert_eq!(rows, *page["data"].as_array().unwrap());
    database.cleanup().await
}
