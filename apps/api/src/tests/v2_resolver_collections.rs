#[tokio::test]
async fn v2_resolver_collection_roles_page_per_registration_and_scope() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let manifest = seed_permissioned_collection_inputs(&database).await?;
    let mut events = Vec::new();
    for index in 0..207 {
        let resource = Uuid::from_u128(0x5500 + index);
        insert_collection_permission_resource(&database.pool, resource).await?;
        events.push(collection_role_event(
            resource,
            &format!("0x{:040x}", index / 3 + 1),
            if index == 205 {
                "0x0000000000000000000000000000000000000bbb"
            } else {
                V2_RESOLVER_ADDRESS
            },
            160,
            index as i64,
            if index == 206 {
                json!([])
            } else {
                json!(["set_addr", "set_text"])
            },
            manifest,
        ));
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_resolver_collection_inputs(&database).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles?page_size=100");
    let mut page = v2_resolver_payload_for_database(&database, &base).await?;
    let first_cursor = page["page"]["next_cursor"].as_str().unwrap().to_owned();
    let mut ids = std::collections::BTreeSet::new();
    loop {
        assert_eq!(page["page"]["total_count"], 205);
        for row in page["data"].as_array().unwrap() {
            assert!(ids.insert(row["registration_id"].as_str().unwrap().to_owned()));
            assert_eq!(row["powers"], json!(["set_addr", "set_text"]));
            assert_eq!(row["grant_event"]["block_number"], 160);
        }
        let Some(cursor) = page["page"]["next_cursor"].as_str() else {
            break;
        };
        page =
            v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    }
    assert_eq!(ids.len(), 205);
    for uri in [
        format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/links?cursor={first_cursor}"),
        format!(
            "/v1/resolvers/1/0x0000000000000000000000000000000000000bbb/roles?cursor={first_cursor}"
        ),
        format!("{base}&namespace=ens"),
    ] {
        let response = v2_resolver_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    publish_resolver_collection_inputs(&database).await?;
    let response =
        v2_resolver_response_for_database(&database, &format!("{base}&cursor={first_cursor}"))
            .await?;
    assert_eq!(response.status(), StatusCode::OK);
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_collection_links_pages_latest_link_per_node_in_record_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_permissioned_collection_inputs(&database).await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &[
            raw_block(
                "ethereum-mainnet",
                "0xcollection150",
                None,
                150,
                1_700_000_150,
            ),
            raw_block("ethereum-mainnet", "0xlinks300", None, 300, 1_700_000_300),
        ],
    )
    .await?;
    let node = |index: u64| format!("0x{:064x}", index + 0x1000);
    let link = |identity: &str, block: i64, log: i64, node: &str, record: &str, resolver: &str| {
        let (hash, tx) = if block == 300 {
            ("0xlinks300", "0xlinktx300")
        } else {
            ("0xcollection150", "0xlinktx150")
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
        event.source_family = "ens_v2_resolver_l1".into();
        event.after_state = json!({"source_event":"Linked", "storage_model":"resolver_record_id",
            "resolver":resolver, "node":node, "resolver_record_id":record});
        // A record-ID resolver emits its own Linked logs; the read filters on the emitter.
        // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L363-L367 @ ens_v2@a971bd64)
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
    events.push(link(
        "relink-0",
        150,
        500,
        &node(0),
        "3",
        V2_RESOLVER_ADDRESS,
    ));
    // Unlinked: record 0 removes the node.
    events.push(link(
        "unlink-1",
        150,
        501,
        &node(1),
        "0",
        V2_RESOLVER_ADDRESS,
    ));
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
    events.push(link(
        "candidate-2",
        150,
        504,
        &node(2),
        "9",
        V2_RESOLVER_ADDRESS,
    ));
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
    // not even normalizable -- so it must neither be shown nor break the page. The
    // root surface (the empty name at the all-zero node) exists on every ENS chain and
    // must not attach to the default record's link.
    sqlx::query("INSERT INTO bigname_phase.name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, deactivation_reason, deactivated_at, chain_id, block_hash, block_number, canonicality_state) VALUES ('ens:' || $1, 'ens', 'Linked.eth', ARRAY['linked','eth'], '\\x066c696e6b656403657468'::bytea, $1, ARRAY['labelhash:linked','labelhash:eth'], 'fixture', 'active', NULL, NULL, 'ethereum-mainnet', '0xcollection150', 150, 'canonical'), ('ens:' || $2, 'ens', 'bad..name', ARRAY['bad','','name'], '\\x00'::bytea, $2, ARRAY['a','b','c'], 'fixture', 'shadow', 'fixture', now(), 'ethereum-mainnet', '0xcollection150', 150, 'canonical'), ('ens:' || $3, 'ens', '', ARRAY[]::text[], '\\x00'::bytea, $3, ARRAY[]::text[], 'fixture', 'active', NULL, NULL, 'ethereum-mainnet', '0xcollection150', 150, 'canonical')")
        .bind(node(4)).bind(node(5)).bind("0x0000000000000000000000000000000000000000000000000000000000000000").execute(&database.pool).await?;

    publish_resolver_collection_inputs(&database).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/links?page_size=40");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    // 105 links, minus the unlinked node, plus the default record.
    assert_eq!(first["page"]["total_count"], 105);
    assert_eq!(first["page"]["has_more"], true);
    let token = first["meta"]["as_of_token"].clone();
    let mut all = first["data"].as_array().unwrap().clone();
    let mut page = first;
    while let Some(cursor) = page["page"]["next_cursor"].as_str() {
        publish_resolver_collection_inputs(&database).await?;
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
            "timestamp": "1700000150",
            "transaction_hash": "0xlinktx150",
            "log_index": 4
        })
    );
    assert!(
        by_node[&node(5)].get("name").is_none(),
        "shadow surface must not name a link"
    );
    assert!(all.iter().all(|row| row.get("logical_name_id").is_none()
        && row.get("normalized_event_id").is_none()
        && row.get("chain_position").is_none()));
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_collection_unsupported_is_not_empty_supported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_overview(&database, false).await?;
    for section in ["links", "roles"] {
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
    let manifest = seed_permissioned_collection_inputs(&database).await?;
    for index in 0..2 {
        let resource = Uuid::from_u128(0x6500 + index);
        insert_collection_permission_resource(&database.pool, resource).await?;
        let event = collection_role_event(
            resource,
            HOLDER,
            V2_RESOLVER_ADDRESS,
            150 + index as i64 * 10,
            3,
            json!(["set_text"]),
            manifest,
        );
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    }
    publish_resolver_collection_inputs(&database).await?;
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

// The overview's bound-names cursor carries no publication or resolver generation, so a
// same-height republish between pages does not refuse it; a cursor that still carries the
// resolver generation answers 400.
#[tokio::test]
async fn v2_resolver_overview_cursor_continues_across_a_same_height_republish() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    let cursor = first["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .unwrap();
    let second =
        v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    let mut with_generation = crate::v2::decode(cursor).expect("decode overview cursor");
    with_generation.last_item.insert(
        "resolver_generation".to_owned(),
        "old-selected-chain".to_owned(),
    );
    let response = v2_resolver_response_for_database(
        &database,
        &format!("{base}&cursor={}", crate::v2::encode(&with_generation)),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 203, "0xresolvercb").await?;
    let again =
        v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(again["data"]["bound_names"], second["data"]["bound_names"]);
    database.cleanup().await
}

/// Runs one request, republishes the collection publication once the handler reaches
/// `finish()` (after its last generation check), and returns the stale error message.
async fn resolver_publication_replaced_before_finish(
    database: &TestDatabase,
    uri: String,
) -> Result<String> {
    let (_guard, control) =
        crate::v2::collection_snapshot::finish_test_hooks::install(&database.pool).await?;
    let state = database.app_state();
    let request = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        control.wait_until_reached(),
    )
    .await
    .context("request never reached the collection publication finish")?;
    commit_family_block(&database.pool).await?;
    control.resume().await;
    let response = request
        .await
        .context("resolver request task panicked")?
        .context("resolver request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload:#}");
    assert_eq!(payload["error"]["code"], "stale");
    Ok(payload["error"]["message"].as_str().unwrap().to_owned())
}

const RETRY_REQUEST: &str = "collection publication changed during the read; retry the request";

// A publication during the continuation's own read still refuses it, but its cursor is
// not bound to the publication it replaced, so the answer asks for a retry, and the same cursor
// then continues.
#[tokio::test]
async fn v2_resolver_overview_continuation_retries_when_publication_changes_before_finish()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    let cursor = first["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .expect("overview first page carries a cursor")
        .to_owned();
    let continued =
        resolver_publication_replaced_before_finish(&database, format!("{base}&cursor={cursor}"))
            .await?;
    assert_eq!(continued, RETRY_REQUEST);
    v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    let cursorless = resolver_publication_replaced_before_finish(&database, base).await?;
    assert_eq!(cursorless, RETRY_REQUEST);
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_collection_continuation_retries_when_publication_changes_before_finish()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_roles_pages(&database).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles?page_size=1");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("roles first page carries a cursor")
        .to_owned();
    let continued =
        resolver_publication_replaced_before_finish(&database, format!("{base}&cursor={cursor}"))
            .await?;
    assert_eq!(continued, RETRY_REQUEST);
    v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    let cursorless = resolver_publication_replaced_before_finish(&database, base).await?;
    assert_eq!(cursorless, RETRY_REQUEST);
    database.cleanup().await
}

/// A resolver with three role holders, so `/roles?page_size=1` has continuations.
async fn seed_v2_resolver_roles_pages(database: &TestDatabase) -> Result<()> {
    let manifest = seed_permissioned_collection_inputs(database).await?;
    for index in 0..3 {
        let resource = Uuid::from_u128(0x5600 + index);
        insert_collection_permission_resource(&database.pool, resource).await?;
        let event = collection_role_event(
            resource,
            &format!("0x{:040x}", index + 1),
            V2_RESOLVER_ADDRESS,
            160,
            index as i64,
            json!(["set_text"]),
            manifest,
        );
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    }
    publish_resolver_collection_inputs(database).await
}

/// Declare an implementation and observe its upgrade before the collection events. Publication
/// is separate so each case can finish its retained inputs before reducing them.
async fn seed_permissioned_collection_inputs(database: &TestDatabase) -> Result<i64> {
    let blocks = [140, 150, 160].map(|block| {
        raw_block(
            "ethereum-mainnet",
            &format!("0xcollection{block}"),
            None,
            block,
            1_700_000_000 + block,
        )
    });
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    let existing: Option<(i64, String, OffsetDateTime)> = sqlx::query_as(
        "SELECT h.latest_block_number, h.latest_block_hash, l.block_timestamp FROM chain_heads h
         JOIN chain_lineage l ON l.chain_id = h.chain_id AND l.block_hash = h.latest_block_hash
         WHERE h.chain_id = 'ethereum-mainnet'",
    )
    .fetch_optional(&database.pool)
    .await?;
    let (block, hash, at) = existing.unwrap_or((
        202,
        "0xresolverc8".into(),
        OffsetDateTime::from_unix_timestamp(1_700_000_202)?,
    ));
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
        "chain_id":"ethereum-mainnet", "block_number":block, "block_hash":hash,
        "timestamp":bigname_storage::UnixSeconds::from(at).internal_string()}}))
        .await?;
    let implementation = "0x0000000000000000000000000000000000000fed";
    let payload = json!({"contracts":[],
        "resolver_implementations":[{"role":"permissioned_resolver", "address":implementation}],
        "abi":{"events":[{"name":"Linked", "fragment":"event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name)",
            "normalized_events":["ResolverRecordLinked", "PreimageObserved"]}]}});
    let manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v2_resolver_l1','ethereum-mainnet','fixture','active','fixture','fixture/collection-resolver.toml',$1) RETURNING manifest_id")
        .bind(&payload).fetch_one(&database.pool).await?;
    seed_fixture_manifest_update(
        &database.pool,
        manifest,
        "ethereum-mainnet",
        "ens",
        "ens_v2_resolver_l1",
        &payload,
    )
    .await?;
    let mut event = history_event(
        "collection-resolver-upgrade",
        None,
        None,
        Some("ethereum-mainnet"),
        Some(140),
        Some("0xcollection140"),
        Some("0xupgrade"),
        Some(0),
        CanonicalityState::Canonical,
    );
    event.event_kind = "Upgraded".into();
    event.source_family = "ens_v2_resolver_l1".into();
    event.source_manifest_id = Some(manifest);
    event.manifest_version = 1;
    event.before_state = json!({});
    event.after_state = json!({"source_event":"Upgraded", "proxy_address":V2_RESOLVER_ADDRESS, "implementation":implementation});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    Ok(manifest)
}

async fn publish_resolver_collection_inputs(database: &TestDatabase) -> Result<()> {
    let (block, hash): (i64, String) = sqlx::query_as("SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = 'ethereum-mainnet'")
        .fetch_one(&database.pool).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block, &hash).await
}

async fn insert_collection_permission_resource(pool: &PgPool, resource: Uuid) -> Result<()> {
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
        VALUES ($1, 'ethereum-mainnet', 140, '0xcollection140', 'canonical')")
        .bind(resource).execute(pool).await?;
    Ok(())
}

fn collection_role_event(
    resource: Uuid,
    subject: &str,
    resolver: &str,
    block: i64,
    log: i64,
    powers: Value,
    manifest: i64,
) -> NormalizedEvent {
    let mut event = history_event(
        &format!("collection-role-{resource}-{block}-{log}"),
        None,
        Some(resource),
        Some("ethereum-mainnet"),
        Some(block),
        Some(&format!("0xcollection{block}")),
        Some(&format!("0xrole-tx-{block}")),
        Some(log),
        CanonicalityState::Canonical,
    );
    event.event_kind = "PermissionChanged".into();
    event.source_family = "ens_v2_resolver_l1".into();
    event.source_manifest_id = Some(manifest);
    event.manifest_version = 1;
    event.raw_fact_ref["emitting_address"] = json!(resolver);
    event.before_state = json!({});
    let node = format!("0x{:064x}", resource.as_u128());
    event.after_state = json!({"subject":subject, "scope":{"kind":"resolver", "chain_id":"ethereum-mainnet", "resolver_address":resolver},
        "effective_powers":powers, "source_event":"EACRolesChanged", "upstream_resource":node,
        "resource":node, "root_resource":false, "storage_model":"resolver_record_id", "resolver":resolver,
        "resolver_record_id":"0", "record_key":"permission",
        "grant_source":{"kind":"raw_log", "source_event":"EACRolesChanged", "upstream_resource":node,
            "root_resource":false, "changed_powers":powers}, "revocation_source":null,
        "inheritance_path":[], "transfer_behavior":{}});
    event
}
