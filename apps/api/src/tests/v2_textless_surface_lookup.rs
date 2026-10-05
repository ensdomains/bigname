// Lookup, verified resolution, token ids and the resolver routes for a name surface that stores
// no raw bytes, on the fixture of v2_textless_surface.rs.

#[tokio::test]
async fn v2_textless_names_are_read_in_a_lookup_batch() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let names = [
        "alpha.eth",
        fixture.first_name.as_str(),
        fixture.nested_name.as_str(),
        fixture.known_name.as_str(),
    ];
    let inputs: Vec<Value> = names
        .iter()
        .enumerate()
        .map(|(index, name)| json!({"id": index.to_string(), "name": name}))
        .collect();

    for profile in ["detail", "feed"] {
        let response = v2_lookup_response_for_database_with_public_namespaces(
            &database,
            "/v1/lookup",
            json!({"inputs": inputs, "profile": profile}),
            &["ens"],
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK, "{profile}");
        let body: Value = read_json(response).await?;
        let results = body["data"].as_array().expect("results");
        assert_eq!(results.len(), names.len(), "{profile}: {body:#}");
        for (result, name) in results.iter().zip(names) {
            assert!(result.get("normalization").is_none(), "{profile}: {result:#}");
            let record = &result["record"];
            assert_eq!(record["name"], json!(name), "{profile}: {result:#}");
            assert_eq!(record["display_name"], json!(name), "{profile}: {result:#}");
        }
        if profile == "detail" {
            // The batch record is the name's detail.
            let detail =
                tl_get(&database, &format!("/v1/names/{}", tl_path(&fixture.first_name))).await?;
            for field in ["namehash", "owner", "manager", "registration_status", "resolver"] {
                assert_eq!(results[1]["record"][field], detail["data"][field], "{field}: {body:#}");
            }
        }
    }

    database.cleanup().await
}

/// Remove the stored bytes of the lookup fixture's `alice.eth` surface, leaving its label-hash
/// path: the row an observation of the node without its label bytes would have written.
async fn drop_alice_surface_bytes(database: &TestDatabase) -> Result<()> {
    let changed = sqlx::query(
        "UPDATE name_surfaces
         SET raw_name = NULL, raw_labels = NULL, dns_encoded_name = NULL,
             preimage_event_identity = NULL
         WHERE raw_name = 'alice.eth'",
    )
    .execute(&database.lookup_pool().await?)
    .await?
    .rows_affected();
    anyhow::ensure!(changed == 1, "the fixture has one alice.eth surface");
    Ok(())
}

#[tokio::test]
async fn verified_lookup_of_a_textless_name_is_unsupported_without_a_provider_call() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, true).await?;
    drop_alice_surface_bytes(&database).await?;
    // Only `eth` is known: the leaf label has no bytes to send. Nothing listens at this
    // address, so a provider call would fail the request.
    insert_family_label_preimage(&database.pool, b"eth").await?;
    let unreachable = unreachable_rpc_url()?;
    let alice = format!("{}.eth", tl_bracket(&child_labelhash("alice")));

    for route in ["alice.eth".to_owned(), tl_path(&alice)] {
        let uri = format!("/v1/names/{route}?source=verified");
        let (status, detail) = sepolia_verified_get(&database, &unreachable, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {detail}");
        let data = &detail["data"];
        assert_eq!(data["name"], json!(alice), "{uri}: {detail}");
        assert_eq!(data["status"], json!("unsupported"), "{uri}: {detail}");
        assert_eq!(
            data["unsupported_reason"],
            json!("verified_records_not_supported"),
            "{uri}: {detail}"
        );
        // Registration facts stay indexed.
        assert_eq!(data["registration_status"], json!("active"), "{uri}: {detail}");
    }
    // The indexed read of the same name is unaffected.
    let (status, indexed) =
        sepolia_verified_get(&database, &unreachable, &format!("/v1/names/{}", tl_path(&alice)))
            .await?;
    assert_eq!(status, StatusCode::OK, "{indexed}");
    assert_eq!(indexed["data"]["status"], json!("ok"), "{indexed}");

    database.cleanup().await
}

#[tokio::test]
async fn verified_lookup_of_a_textless_name_runs_once_every_label_has_verified_bytes() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, true).await?;
    drop_alice_surface_bytes(&database).await?;
    for label in ["alice", "eth"] {
        insert_family_label_preimage(&database.pool, label.as_bytes()).await?;
    }
    let (rpc_url, rpc_handle) = spawn_record_getter_mock_rpc(1, profile_answer).await?;

    let (status, detail) =
        sepolia_verified_get(&database, &rpc_url, "/v1/names/alice.eth?source=verified").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let data = &detail["data"];
    assert_eq!(data["name"], json!("alice.eth"), "{detail}");
    assert_eq!(data["status"], json!("ok"), "{detail}");
    assert_eq!(data["primary_address"], json!(SEPOLIA_DETAIL_EXECUTED), "{detail}");
    assert_eq!(joined_keys(rpc_handle).await?, ["addr:60"]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_eth_second_level_name_serves_its_labelhash_as_token_id() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let labelhash = format!("0x{}", "5a".repeat(32));
    let (id, resource) = seed_textless_family_name(
        &database,
        &[labelhash.clone(), child_labelhash("eth")],
        0x8e1_0000,
        "ens_v1",
        202,
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &textless_child_events(
            &id,
            resource,
            &bigname_lookup::ens_namehash_hex("eth")?,
            &labelhash,
            RC_OWNER,
            202,
        ),
    )
    .await?;
    publish_test_families(&database, 240).await?;
    let token_id = alloy_primitives::U256::from_str_radix(&"5a".repeat(32), 16)?.to_string();
    let route = format!("/v1/names/{}.eth", tl_path(&tl_bracket(&labelhash)));

    // With no preimage even `eth` is served as its hash; the token id is the leaf labelhash
    // either way, never a hash of the bracketed text.
    let unnamed = tl_get(&database, &route).await?;
    assert_eq!(
        unnamed["data"]["name"],
        json!(format!("{}.{}", tl_bracket(&labelhash), tl_bracket(&child_labelhash("eth")))),
        "{unnamed:#}"
    );
    assert_eq!(unnamed["data"]["token_id"], json!(token_id), "{unnamed:#}");
    insert_family_label_preimage(&database.pool, b"eth").await?;
    let named = tl_get(&database, &route).await?;
    assert_eq!(
        named["data"]["name"],
        json!(format!("{}.eth", tl_bracket(&labelhash))),
        "{named:#}"
    );
    assert_eq!(named["data"]["token_id"], json!(token_id), "{named:#}");

    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_name_is_a_bound_name_of_its_resolver() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    for page_size in [1, 10] {
        let uri = format!("/v1/resolvers/1/{FAMILY_RESOLVER}?page_size={page_size}");
        let pages = read_family_pages_in(&database, &uri, "/data/bound_names").await?;
        assert_eq!(tl_column(&pages, "name"), [fixture.first_name.as_str()], "{pages:#?}");
        assert_eq!(
            tl_column(&pages, "display_name"),
            [fixture.first_name.as_str()],
            "{pages:#?}"
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_node_is_named_on_its_resolver_link() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_permissions_fixture(&database).await?;
    // A node below alpha.eth observed without its label bytes at 241, linked to record 3.
    let labelhash = format!("0x{}", "6b".repeat(32));
    let id = insert_textless_surface(
        &database.pool,
        "ens",
        FAMILY_CHAIN,
        &[labelhash.clone(), child_labelhash("alpha"), child_labelhash("eth")],
        241,
    )
    .await?;
    let node = id.strip_prefix("ens:").expect("ens id");
    let manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM bigname_phase.manifest_versions
         WHERE file_path = 'fixture/family-v2-resolver.toml'",
    )
    .fetch_one(&database.pool)
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[family_v2_resolver_event(
            "tl-link",
            None,
            "ResolverRecordLinked",
            (241, 0),
            "ens_v2_resolver",
            manifest_id,
            json!({"source_event": "Linked", "storage_model": "resolver_record_id",
                   "resolver": FAMILY_V2_RESOLVER, "node": node,
                   "resolver_record_id": "3"}),
        )],
    )
    .await?;
    publish_test_families(&database, 241).await?;
    let uri = format!("/v1/resolvers/1/{FAMILY_V2_RESOLVER}/links?page_size=10");
    let linked = |pages: &[Value]| {
        rows_of(pages)
            .into_iter()
            .find(|row| row["namehash"] == json!(node))
            .expect("the node's link")
    };

    let link = linked(&read_family_pages(&database, &uri).await?);
    let placeholders = format!(
        "{}.{}.{}",
        tl_bracket(&labelhash),
        tl_bracket(&child_labelhash("alpha")),
        tl_bracket(&child_labelhash("eth"))
    );
    assert_eq!(link["record_id"], json!("3"), "{link:#}");
    assert_eq!(link["name"], json!(placeholders), "{link:#}");
    assert_eq!(link["display_name"], json!(placeholders), "{link:#}");
    for label in ["alpha", "eth"] {
        insert_family_label_preimage(&database.pool, label.as_bytes()).await?;
    }
    let pages = read_family_pages(&database, &uri).await?;
    let link = linked(&pages);
    let name = format!("{}.alpha.eth", tl_bracket(&labelhash));
    assert_eq!(link["name"], json!(name), "{link:#}");
    assert_eq!(link["display_name"], json!(name), "{link:#}");
    // The link of a name with bytes is served as before.
    assert!(
        rows_of(&pages).iter().any(|row| row["name"] == json!("alpha.eth")),
        "{pages:#?}"
    );

    database.cleanup().await
}

/// The label of the two names [`decoded_textless_events`] adds. It is normalized as it stands
/// and its display form adds variation selectors, so a name and its beautified form sort apart.
const TL_DECODED_LABEL: &str = "🅰🅱";

/// Two more children without bytes, `🅰🅱` below alpha.eth (209) and below `first` (210), with a
/// preimage for every label of both paths.
async fn decoded_textless_events(
    database: &TestDatabase,
    alpha_node: &str,
    first_node: &str,
) -> Result<Vec<NormalizedEvent>> {
    for preimage in [TL_DECODED_LABEL.as_bytes(), b"first"] {
        insert_family_label_preimage(&database.pool, preimage).await?;
    }
    let hash = |label: &[u8]| format!("{:#x}", alloy_primitives::keccak256(label));
    let label = hash(TL_DECODED_LABEL.as_bytes());
    let alpha_path = vec![hash(b"alpha"), hash(b"eth")];
    let first_path = [vec![hash(b"first")], alpha_path.clone()].concat();
    let mut events = Vec::new();
    for (index, (parent, parent_node)) in
        [(alpha_path, alpha_node), (first_path, first_node)].into_iter().enumerate()
    {
        let block = 209 + index as i64;
        let (id, resource) = seed_textless_family_name(
            database,
            &[vec![label.clone()], parent].concat(),
            0x8d1_0000 + 0x10 * index as u128,
            "ens_v1",
            block,
        )
        .await?;
        events.extend(textless_child_events(&id, resource, parent_node, &label, RC_OWNER, block));
    }
    Ok(events)
}

/// A capped address read walks its names by a stored key. For a surface without bytes that key
/// is the rendered name the row and the cursor carry, also once every label is known and the
/// name no longer shows a bracket.
#[tokio::test]
async fn v2_textless_names_page_alike_on_the_capped_address_walk() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_textless_fixture_with(&database, true, true).await?;
    let decoded = [
        format!("{TL_DECODED_LABEL}.alpha.eth"),
        format!("{TL_DECODED_LABEL}.first.alpha.eth"),
    ];

    for order in ["asc", "desc"] {
        for page_size in [1, 2, 50] {
            let uri = format!(
                "/v1/addresses/{RC_OWNER}/names?namespace=ens&sort=name&order={order}&page_size={page_size}"
            );
            let exact = walk_all_pages(&database, &uri).await?;
            let (walked, paths) =
                with_paths(with_exact_total_cap(0, walk_all_pages(&database, &uri))).await;
            let walked = walked?;
            assert!(paths.iter().all(|path| *path == "walk"), "{uri}: {paths:?}");
            assert_eq!(
                exact.iter().map(page_body).collect::<Vec<_>>(),
                walked.iter().map(page_body).collect::<Vec<_>>(),
                "{uri}"
            );
            let rows: Vec<&Value> =
                walked.iter().flat_map(|page| page["data"].as_array().expect("rows")).collect();
            assert_eq!(rows.len(), 8, "{uri}: {rows:#?}");
            for name in &decoded {
                let row = rows.iter().find(|row| row["name"] == json!(name));
                let row = row.unwrap_or_else(|| panic!("{uri}: {name} is not listed"));
                assert_eq!(row["display_name"], row["name"], "{uri}");
            }
        }
    }

    // The name row and the batch identity read serve the same two fields.
    let inputs: Vec<Value> = decoded.iter().map(|name| json!({"name": name})).collect();
    for profile in ["detail", "feed"] {
        let response = v2_lookup_response_for_database_with_public_namespaces(
            &database,
            "/v1/lookup",
            json!({"inputs": inputs, "profile": profile}),
            &["ens"],
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK, "{profile}");
        let body: Value = read_json(response).await?;
        for (result, name) in body["data"].as_array().expect("results").iter().zip(&decoded) {
            assert_eq!(result["record"]["name"], json!(name), "{profile}: {result:#}");
            assert_eq!(result["record"]["display_name"], json!(name), "{profile}: {result:#}");
        }
    }
    for name in &decoded {
        let detail = tl_get(&database, &format!("/v1/names/{}", tl_path(name))).await?;
        assert_eq!(detail["data"]["name"], json!(name));
        assert_eq!(detail["data"]["display_name"], json!(name));
    }

    database.cleanup().await
}
