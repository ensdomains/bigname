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
async fn v2_resolver_collection_links_pages_latest_link_per_node_in_record_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(&database, &[resolver]).await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xlinks150", None, 150, 1_700_000_150),
            raw_block("ethereum-mainnet", "0xlinks300", None, 300, 1_700_000_300),
        ],
    )
    .await?;
    let node = |index: u64| format!("0x{:064x}", index + 0x1000);
    let link = |identity: &str, block: i64, log: i64, node: &str, record: &str, resolver: &str| {
        let (hash, tx) = if block == 300 {
            ("0xlinks300", "0xlinktx300")
        } else {
            ("0xlinks150", "0xlinktx150")
        };
        let mut event = history_event(
            identity,
            None,
            None,
            Some("ethereum-mainnet"),
            Some(block),
            Some(hash),
            Some(tx),
            Some(log),
            CanonicalityState::Canonical,
        );
        event.event_kind = "ResolverRecordLinked".to_owned();
        event.after_state = json!({"source_event":"Linked", "storage_model":"resolver_record_id",
            "resolver":resolver, "node":node, "resolver_record_id":record});
        // A record-ID resolver emits its own Linked logs; the read filters on the emitter.
        event.raw_fact_ref["emitting_address"] = json!(resolver);
        event
    };
    // Records 1, 2, 3 and 10: a text sort would place "10" before "2".
    let records = ["1", "2", "3", "10"];
    let mut events = (0..105u64)
        .map(|index| {
            link(
                &format!("link-{index}"),
                150,
                index as i64,
                &node(index),
                records[(index % 4) as usize],
                V2_RESOLVER_ADDRESS,
            )
        })
        .collect::<Vec<_>>();
    // Relinked: only the latest link per node counts.
    events.push(link("relink-0", 150, 500, &node(0), "3", V2_RESOLVER_ADDRESS));
    // Unlinked: record 0 removes the node.
    events.push(link("unlink-1", 150, 501, &node(1), "0", V2_RESOLVER_ADDRESS));
    // The default record: the empty-name node.
    events.push(link(
        "default",
        150,
        502,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "2",
        V2_RESOLVER_ADDRESS,
    ));
    let mut orphan = link("orphan-2", 150, 503, &node(2), "9", V2_RESOLVER_ADDRESS);
    orphan.canonicality_state = CanonicalityState::Orphaned;
    events.push(orphan);
    events.push(link("candidate-2", 150, 504, &node(2), "9", V2_RESOLVER_ADDRESS));
    events.push(link(
        "other-resolver",
        150,
        505,
        &node(700),
        "1",
        "0x0000000000000000000000000000000000000bbb",
    ));
    // Above the selected height (202): not yet visible.
    events.push(link("future-3", 300, 0, &node(3), "5", V2_RESOLVER_ADDRESS));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET consumer_visibility = 'candidate', migration_correlation_ids = ARRAY['candidate-2'] WHERE event_identity = 'candidate-2'")
        .execute(&database.pool).await?;
    // Only node 4 has an active name surface; every other node is served by namehash
    // alone. Node 5's surface is shadow -- withheld from readers, and its raw name is
    // not even normalizable -- so it must neither be shown nor break the page.
    sqlx::query("INSERT INTO bigname_phase.name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, deactivation_reason, deactivated_at, chain_id, block_hash, block_number, canonicality_state) VALUES ('ens:' || $1, 'ens', 'Linked.eth', ARRAY['linked','eth'], '\\x066c696e6b656403657468'::bytea, $1, ARRAY['labelhash:linked','labelhash:eth'], 'fixture', 'active', NULL, NULL, 'ethereum-mainnet', '0xlinks150', 150, 'canonical'), ('ens:' || $2, 'ens', 'bad..name', ARRAY['bad','','name'], '\\x00'::bytea, $2, ARRAY['a','b','c'], 'fixture', 'shadow', 'fixture', now(), 'ethereum-mainnet', '0xlinks150', 150, 'canonical')")
        .bind(node(4)).bind(node(5)).execute(&database.pool).await?;

    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/links?page_size=40");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    // 105 links, minus the unlinked node, plus the default record.
    assert_eq!(first["page"]["total_count"], 105);
    assert_eq!(first["page"]["has_more"], true);
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
    let keys = all
        .iter()
        .map(|row| {
            (
                row["record_id"].as_str().unwrap().parse::<u64>().unwrap(),
                row["namehash"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "links sort by numeric record then namehash");
    assert_eq!(keys.last().unwrap().0, 10);
    let by_node = all
        .iter()
        .map(|row| (row["namehash"].as_str().unwrap().to_owned(), row.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(by_node[&node(0)]["record_id"], "3");
    assert!(!by_node.contains_key(&node(1)));
    assert_eq!(by_node[&node(2)]["record_id"], "3");
    assert_eq!(by_node[&node(3)]["record_id"], "10");
    assert!(!by_node.contains_key(&node(700)));
    let default = &by_node["0x0000000000000000000000000000000000000000000000000000000000000000"];
    assert_eq!(default["default"], true);
    assert_eq!(default["record_id"], "2");
    assert!(default.get("name").is_none());
    let named = &by_node[&node(4)];
    assert_eq!(named["name"], "linked.eth");
    assert_eq!(named["display_name"], "linked.eth");
    assert_eq!(named["namespace"], "ens");
    assert_eq!(named["default"], false);
    assert_eq!(
        named["link_event"],
        json!({
            "block_number": 150,
            "timestamp": "2023-11-14T22:15:50Z",
            "transaction_hash": "0xlinktx150",
            "log_index": 4
        })
    );
    assert!(by_node[&node(5)].get("name").is_none(), "shadow surface must not name a link");
    assert!(all.iter().all(|row| row.get("logical_name_id").is_none()
        && row.get("normalized_event_id").is_none()
        && row.get("chain_position").is_none()));
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
    for section in ["aliases", "links", "roles"] {
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
