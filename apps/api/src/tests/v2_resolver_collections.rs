#[tokio::test]
async fn v2_resolver_collection_aliases_exhaustive_scoped_and_latest() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;
    // Include the overview's binding-alias group, not only AliasChanged history.
    sqlx::query("DELETE FROM bigname_phase.address_names_current WHERE surface_binding_id IN (SELECT surface_binding_id FROM bigname_phase.name_current WHERE raw_name = 'alpha.eth')")
        .execute(&database.pool).await?;
    sqlx::query(r#"WITH bindings AS (
        UPDATE bigname_phase.surface_bindings SET binding_kind = 'resolver_alias_path'
        WHERE surface_binding_id IN (SELECT surface_binding_id FROM bigname_phase.name_current WHERE raw_name = 'alpha.eth')
        RETURNING surface_binding_id
    ) UPDATE bigname_phase.name_current nc SET binding_kind = 'resolver_alias_path',
        provenance = provenance || '{"resolver_pointer_source_family":"ens_v2_registry_l1"}'::jsonb
      FROM bindings WHERE nc.surface_binding_id = bindings.surface_binding_id"#)
        .execute(&database.pool).await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xaliases150",
            None,
            150,
            1_700_000_150,
        )],
    )
    .await?;
    let alias = |index: usize, identity: &str, active: bool, resolver: &str| {
        let mut event = history_event(
            identity,
            None,
            None,
            Some("ethereum-mainnet"),
            Some(150),
            Some("0xaliases150"),
            Some("0xaliastx"),
            Some(index as i64),
            CanonicalityState::Canonical,
        );
        event.event_kind = "AliasChanged".to_owned();
        event.after_state = json!({"resolver":resolver, "from_namehash":format!("key-{index:04}"),
            "from_name":"same.eth", "to_name":format!("target-{index:04}.eth"),
            "alias_state":"active", "active":active});
        event
    };
    let mut events = (0..105)
        .map(|index| alias(index, &format!("alias-{index}"), true, V2_RESOLVER_ADDRESS))
        .collect::<Vec<_>>();
    let mut removed = alias(0, "alias-removed", false, V2_RESOLVER_ADDRESS);
    removed.log_index = Some(200);
    events.push(removed);
    events.push(alias(
        500,
        "alias-other",
        true,
        "0x0000000000000000000000000000000000000bbb",
    ));
    let mut orphan = alias(501, "alias-orphan", true, V2_RESOLVER_ADDRESS);
    orphan.canonicality_state = CanonicalityState::Orphaned;
    events.push(orphan);
    events.push(alias(502, "alias-candidate", true, V2_RESOLVER_ADDRESS));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET consumer_visibility = 'candidate', migration_correlation_ids = ARRAY['alias-candidate-correlation'] WHERE event_identity = 'alias-candidate'")
        .execute(&database.pool).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/aliases?page_size=37");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    assert_eq!(first["page"]["total_count"], 105);
    assert_eq!(first["data"][0]["name"], "alpha.eth");
    let token = first["meta"]["as_of_token"].clone();
    let mut all = first["data"].as_array().unwrap().clone();
    let mut page = first;
    while let Some(cursor) = page["page"]["next_cursor"].as_str() {
        page =
            v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(page["meta"]["as_of_token"], token);
        assert_eq!(page["page"]["total_count"], 105);
        all.extend(page["data"].as_array().unwrap().clone());
    }
    assert_eq!(all.len(), 105);
    let targets = all
        .iter()
        .filter_map(|row| row["to_name"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(targets.len(), 104);
    assert!(!targets.contains("target-0000.eth"));
    assert!(!targets.contains("target-0500.eth"));
    assert!(!targets.contains("target-0501.eth"));
    assert!(!targets.contains("target-0502.eth"));
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_collection_roles_page_per_registration_and_scope() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(&database, &[resolver]).await?;
    let mut permissions = Vec::new();
    let resources = (0..207)
        .map(|i| resource(Uuid::from_u128(0x5500 + i)))
        .collect::<Vec<_>>();
    upsert_test_resources(&database.pool, &resources).await?;
    for (index, resource) in resources.iter().enumerate() {
        let mut permission = permission_current_row(
            resource.resource_id,
            &format!("0x{:040x}", index / 3 + 1),
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: if index == 205 {
                    "0x0000000000000000000000000000000000000bbb"
                } else {
                    V2_RESOLVER_ADDRESS
                }
                .to_owned(),
            },
            7,
            160,
        );
        permission.provenance["normalized_event_ids"] = json!([]);
        if index == 206 {
            permission.effective_powers = json!([]);
        }
        permissions.push(permission);
    }
    upsert_phase_permissions_current_rows(&database.pool, &permissions).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles?page_size=100");
    let mut page = v2_resolver_payload_for_database(&database, &base).await?;
    let first_cursor = page["page"]["next_cursor"].as_str().unwrap().to_owned();
    let mut ids = std::collections::BTreeSet::new();
    loop {
        assert_eq!(page["page"]["total_count"], 205);
        for row in page["data"].as_array().unwrap() {
            assert!(ids.insert(row["registration_id"].as_str().unwrap().to_owned()));
            assert_eq!(row["powers"], json!(["set_resolver", "set_records"]));
            assert!(row.get("grant_event").is_none());
        }
        let Some(cursor) = page["page"]["next_cursor"].as_str() else {
            break;
        };
        page =
            v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    }
    assert_eq!(ids.len(), 205);
    for uri in [
        format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/aliases?cursor={first_cursor}"),
        format!(
            "/v1/resolvers/1/0x0000000000000000000000000000000000000bbb/roles?cursor={first_cursor}"
        ),
        format!("{base}&namespace=ens"),
    ] {
        let response = v2_resolver_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    // A real same-height republish changes the project row generation.
    sqlx::query("UPDATE bigname_phase.chain_phase_state SET updated_at = now() WHERE phase_name = 'project' AND chain_id = 'ethereum-mainnet'")
        .execute(&database.pool).await?;
    let response =
        v2_resolver_response_for_database(&database, &format!("{base}&cursor={first_cursor}"))
            .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_collection_unsupported_is_not_empty_supported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = unsupported_resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(&database, &[resolver]).await?;
    for section in ["aliases", "roles"] {
        let payload = v2_resolver_payload_for_database(
            &database,
            &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/{section}"),
        )
        .await?;
        assert_eq!(payload["data"], json!([]));
        assert!(payload["page"]["total_count"].is_null());
        assert_eq!(payload["meta"]["completeness"], "unsupported");
        assert!(payload["meta"]["unsupported_reason"].is_string());
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_collection_role_provenance_stays_registration_scoped() -> Result<()> {
    const HOLDER: &str = "0x0000000000000000000000000000000000000abc";
    let database = TestDatabase::new_migrated().await?;
    let resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(&database, &[resolver]).await?;
    let resources = [
        resource(Uuid::from_u128(0x6500)),
        resource(Uuid::from_u128(0x6501)),
    ];
    upsert_test_resources(&database.pool, &resources).await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xroles150", None, 150, 1_700_000_150),
            raw_block("ethereum-mainnet", "0xroles160", None, 160, 1_700_000_160),
        ],
    )
    .await?;
    for (index, resource) in resources.iter().enumerate() {
        let block = 150 + index as i64 * 10;
        let identity = format!("per-resource-{index}");
        let mut event = history_event(
            &identity,
            None,
            Some(resource.resource_id),
            Some("ethereum-mainnet"),
            Some(block),
            Some(&format!("0xroles{block}")),
            Some(&format!("0xrole-tx-{index}")),
            Some(3),
            CanonicalityState::Canonical,
        );
        event.event_kind = "PermissionChanged".to_owned();
        event.after_state = json!({"subject":HOLDER});
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
        let id: i64 = sqlx::query_scalar("SELECT normalized_event_id FROM bigname_phase.normalized_events WHERE event_identity = $1")
            .bind(&identity).fetch_one(&database.pool).await?;
        let mut permission = permission_current_row(
            resource.resource_id,
            HOLDER,
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: V2_RESOLVER_ADDRESS.to_owned(),
            },
            7,
            160,
        );
        permission.provenance["normalized_event_ids"] = json!([id]);
        upsert_phase_permissions_current_rows(&database.pool, &[permission]).await?;
    }
    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles"),
    )
    .await?;
    assert_eq!(payload["page"]["total_count"], 2);
    assert_eq!(payload["data"][0]["grant_event"]["block_number"], 150);
    assert_eq!(payload["data"][1]["grant_event"]["block_number"], 160);
    assert_ne!(
        payload["data"][0]["registration_id"],
        payload["data"][1]["registration_id"]
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_collection_overview_cursor_rejects_same_height_republish() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    let cursor = first["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .unwrap();
    let mut wrong_generation = crate::v2::decode(cursor).expect("decode overview cursor");
    wrong_generation.last_item.insert(
        "resolver_generation".to_owned(),
        "old-selected-chain".to_owned(),
    );
    let response = v2_resolver_response_for_database(
        &database,
        &format!("{base}&cursor={}", crate::v2::encode(&wrong_generation)),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    sqlx::query("UPDATE bigname_phase.chain_phase_state SET updated_at = now() WHERE phase_name = 'project' AND chain_id = 'ethereum-mainnet'")
        .execute(&database.pool).await?;
    let response =
        v2_resolver_response_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    database.cleanup().await
}
