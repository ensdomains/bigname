// An address holding many names: every composition an address read makes stays within one
// chunk, the walk composes about a page, and every path serves the same pages.

const BULK_ADDRESS: &str = "0x00000000000000000000000000000000000b0b0b";
const BULK_REGISTRY: &str = "0x0000000000000000000000000000000000000b23";
// Names whose served spelling `ens_beautify` changes, so name order must follow the served name.
const BULK_EMOJI_LABELS: [&str; 4] = ["❤", "☺", "💩", "⌚"];

fn bulk_time(index: usize) -> (i64, String, String) {
    let at = format!("2023-{:02}-02T00:00:00Z", index + 1);
    (3000 + index as i64, format!("0xbulk-time-{index}"), at)
}

fn bulk_label(index: usize) -> String {
    if index < BULK_EMOJI_LABELS.len() {
        BULK_EMOJI_LABELS[index].to_owned()
    } else {
        format!("bulk{index:05}")
    }
}

/// `count` names, each registered to and owned by [`BULK_ADDRESS`], with repeating creation and
/// registration times and expiries so every sort has ties.
async fn seed_bulk_address_names(database: &TestDatabase, count: usize) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({"base":{
            "chain_id":"base-mainnet", "block_number":1, "block_hash":"0xcount-base-empty",
            "timestamp":"2024-01-01T00:00:00Z"
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 1, "0xcount-base-empty").await?;
    for index in 0..10 {
        let (block, hash, at) = bulk_time(index);
        sqlx::query(
            "INSERT INTO chain_lineage
                (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ('ethereum-mainnet', $1, $2, $3::timestamptz, 'canonical')",
        )
        .bind(&hash)
        .bind(block)
        .bind(&at)
        .execute(&database.pool)
        .await?;
    }
    let (mut tokens, mut resources, mut surfaces, mut bindings, mut events) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for index in 0..count {
        let name = format!("{}.eth", bulk_label(index));
        let normalized = bigname_domain::normalization::normalize_name(&name)?;
        let (logical, namehash) = phase_logical_identity("ens", &normalized.normalized_name)?;
        let (created_block, created_hash, created_at) = bulk_time(index % 5);
        let (registered_block, registered_hash, _) = bulk_time(5 + index % 4);
        let resource = Uuid::from_u128(0x7000_0000 + index as u128 * 4);
        let token = Uuid::from_u128(0x7000_0001 + index as u128 * 4);
        let binding = Uuid::from_u128(0x7000_0002 + index as u128 * 4);
        tokens.push(TokenLineage {
            token_lineage_id: token,
            chain_id: "ethereum-mainnet".into(),
            block_number: created_block,
            block_hash: created_hash.clone(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        });
        resources.push(Resource {
            resource_id: resource,
            token_lineage_id: Some(token),
            chain_id: "ethereum-mainnet".into(),
            block_number: created_block,
            block_hash: created_hash.clone(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        });
        surfaces.push(NameSurface {
            logical_name_id: logical.clone(),
            namespace: "ens".into(),
            input_name: name.clone(),
            canonical_display_name: normalized.canonical_display_name,
            normalized_name: normalized.normalized_name.clone(),
            dns_encoded_name: Some(normalized.dns_encoded_name),
            namehash,
            labelhashes: vec![],
            normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION.into(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".into(),
            block_number: created_block,
            block_hash: created_hash.clone(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        });
        bindings.push(SurfaceBinding {
            surface_binding_id: binding,
            logical_name_id: format!("ens:{name}"),
            resource_id: resource,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            authority_arm: "ens_v1".into(),
            active_from: parse_rfc3339_utc_timestamp(&created_at)
                .map_err(|error| anyhow::anyhow!("{error}"))?,
            active_to: None,
            chain_id: "ethereum-mainnet".into(),
            block_number: created_block,
            block_hash: created_hash.clone(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        });
        let logical_event = bigname_storage::logical_name_id_for_name("ens", &name);
        let node = bigname_lookup::ens_namehash_hex(&normalized.normalized_name)?;
        let expiry = 1_800_000_000_i64 + (index % 37) as i64 * 86_400;
        for (block, hash, kind, family, after) in [
            (
                created_block,
                &created_hash,
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                json!({"source_event":"Transfer", "node":node, "owner":BULK_ADDRESS,
                    "owner_getter":BULK_ADDRESS, "registry_contract":BULK_REGISTRY,
                    "emitter_role":"registry"}),
            ),
            (
                registered_block,
                &registered_hash,
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                json!({"authority_kind":"registrar", "registrant":BULK_ADDRESS,
                    "expiry":expiry}),
            ),
        ] {
            events.push(address_fixture_event(
                &format!("bulk-{resource}-{kind}"),
                Some(&logical_event),
                Some(resource),
                kind,
                family,
                block,
                hash,
                NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
                after,
            ));
        }
    }
    for chunk in tokens.chunks(1000) {
        upsert_test_token_lineages(&database.pool, chunk).await?;
    }
    for chunk in resources.chunks(1000) {
        upsert_test_resources(&database.pool, chunk).await?;
    }
    for chunk in surfaces.chunks(1000) {
        upsert_test_name_surfaces(&database.pool, chunk).await?;
    }
    for chunk in bindings.chunks(1000) {
        upsert_test_surface_bindings(&database.pool, chunk).await?;
    }
    for chunk in events.chunks(1000) {
        bigname_storage::insert_normalized_event_fixtures(&database.pool, chunk).await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('ethereum-mainnet', '0xbulk-head', 4000, '2024-05-31T18:26:47Z', 'canonical')",
    )
    .execute(&database.pool)
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":"ethereum-mainnet", "block_number":4000, "block_hash":"0xbulk-head",
            "timestamp":"2024-05-31T18:26:47Z"
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 4000, "0xbulk-head").await
}

async fn bulk_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    v2_address_names_payload_for_database(database, uri).await
}

/// How a capped page above the cap reads: `sort=created_at` has no stored key to walk by.
fn capped_path(uri: &str) -> &'static str {
    if uri.contains("sort=created_at") {
        "full"
    } else {
        "walk"
    }
}

/// Every page of `uri` at `page_size`, following `next_cursor`.
async fn walk_all_pages(database: &TestDatabase, uri: &str) -> Result<Vec<Value>> {
    walk_pages(database, uri, 1000).await
}

/// The first `limit` pages of `uri`, following `next_cursor`.
async fn walk_pages(database: &TestDatabase, uri: &str, limit: usize) -> Result<Vec<Value>> {
    let separator = if uri.contains('?') { '&' } else { '?' };
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page_uri = match &cursor {
            Some(cursor) => format!("{uri}{separator}cursor={cursor}"),
            None => uri.to_owned(),
        };
        let payload = bulk_payload(database, &page_uri).await?;
        cursor = payload["page"]["next_cursor"].as_str().map(str::to_owned);
        pages.push(payload);
        if cursor.is_none() || pages.len() == limit {
            return Ok(pages);
        }
    }
}

/// The parts of a page every path must serve alike: rows, cursor and whether more follow.
fn page_body(payload: &Value) -> Value {
    json!({
        "data": payload["data"],
        "next_cursor": payload["page"]["next_cursor"],
        "has_more": payload["page"]["has_more"],
    })
}

async fn with_paths<F: std::future::Future>(future: F) -> (F::Output, Vec<&'static str>) {
    use bigname_storage::families::records::seams::with_address_name_paths;
    let paths = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = with_address_name_paths(paths.clone(), future).await;
    let paths = paths.lock().unwrap().clone();
    (output, paths)
}

async fn with_exact_total_cap<F: std::future::Future>(cap: usize, future: F) -> F::Output {
    bigname_storage::families::records::seams::with_exact_total_cap(cap, future).await
}

async fn with_batches<F: std::future::Future>(future: F) -> (F::Output, Vec<usize>) {
    use bigname_storage::families::records::seams::with_composed_name_batches;
    let batches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = with_composed_name_batches(batches.clone(), future).await;
    let batches = batches.lock().unwrap().clone();
    (output, batches)
}


#[tokio::test]
async fn v2_address_names_compose_a_large_address_in_bounded_chunks() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 600).await?;

    // The exact path, which serves 600 names below the cap, composes 64 names at a time.
    let names = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=1");
    let ((payload, paths), batches) =
        with_batches(with_compose_chunk(64, with_paths(bulk_payload(&database, &names)))).await;
    let payload = payload?;
    assert_eq!(paths, ["exact"]);
    assert_eq!(payload["page"]["total_count"], json!(600), "{payload}");
    assert_eq!(payload["page"]["has_more"], json!(true), "{payload}");
    assert!(batches.len() >= 10, "{batches:?}");
    assert!(batches.iter().all(|batch| *batch <= 64), "{batches:?}");
    assert_eq!(batches.iter().sum::<usize>(), 600, "{batches:?}");

    let history = format!("/v1/addresses/{BULK_ADDRESS}/history?page_size=1");
    let (payload, batches) =
        with_batches(with_compose_chunk(64, bulk_payload(&database, &history))).await;
    let payload = payload?;
    assert_eq!(payload["data"].as_array().unwrap().len(), 1, "{payload}");
    assert_eq!(payload["page"]["total_count"], Value::Null, "{payload}");
    assert_eq!(payload["page"]["has_more"], json!(true), "{payload}");
    assert!(payload["page"]["next_cursor"].is_string(), "{payload}");
    // History reads the published catalogue without composing the address's names.
    assert!(batches.is_empty(), "{batches:?}");

    // Former owners compose every indexed name in chunks, then the page's names in full.
    let former = format!("/v1/addresses/{BULK_ADDRESS}/names?relation=former_owner&page_size=1");
    let (payload, batches) =
        with_batches(with_compose_chunk(64, bulk_payload(&database, &former))).await;
    payload?;
    assert!(batches.len() >= 11, "{batches:?}");
    assert!(batches.iter().all(|batch| *batch <= 64), "{batches:?}");
    assert!(*batches.last().unwrap() <= 1, "{batches:?}");

    // Above the cap the first page composes one walk batch, not the address.
    let (payload, batches) = with_batches(with_exact_total_cap(
        100,
        with_paths(bulk_payload(&database, &names)),
    ))
    .await;
    let (payload, paths) = payload;
    let payload = payload?;
    assert_eq!(paths, ["walk"]);
    assert_eq!(payload["page"]["total_count"], Value::Null, "{payload}");
    assert_eq!(payload["page"]["has_more"], json!(true), "{payload}");
    assert!(batches.iter().sum::<usize>() <= 32, "{batches:?}");

    // include=total_count asks for the exact read whatever the cap.
    let exact = format!("{names}&include=total_count");
    let (payload, paths) =
        with_exact_total_cap(100, with_paths(bulk_payload(&database, &exact))).await;
    assert_eq!(paths, ["exact"]);
    assert_eq!(payload?["page"]["total_count"], json!(600));
    database.cleanup().await
}


#[tokio::test]
async fn v2_address_names_walk_and_chunks_serve_identical_pages() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    // 65 names: three pages of 25, a partial last chunk of 7, and two 5-row pages that each
    // compose one 32-name walk batch.
    seed_bulk_address_names(&database, 65).await?;
    for sort in ["name", "expires_at", "registered_at", "created_at"] {
        for order in ["asc", "desc"] {
            for dedupe in ["name", "registration"] {
                let uri = format!(
                    "/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&order={order}&dedupe={dedupe}&page_size=25"
                );
                let exact = walk_all_pages(&database, &uri).await?;
                let chunked = with_compose_chunk(7, walk_all_pages(&database, &uri)).await?;
                assert_eq!(exact, chunked, "{uri}");
                let (walked, paths) =
                    with_paths(with_exact_total_cap(0, walk_all_pages(&database, &uri))).await;
                let walked = walked?;
                assert!(paths.iter().all(|path| *path == capped_path(&uri)), "{uri}: {paths:?}");
                assert_eq!(
                    exact.iter().map(page_body).collect::<Vec<_>>(),
                    walked.iter().map(page_body).collect::<Vec<_>>(),
                    "{uri}"
                );
                assert_eq!(exact[0]["page"]["total_count"], json!(65), "{uri}");
                assert!(
                    walked.iter().all(|page| page["page"]["total_count"].is_null()),
                    "{uri}"
                );
            }
        }
    }
    // Small pages stop the walk early: the first pages and their cursors match the full read,
    // and the first page composes a fraction of the address.
    for sort in ["name", "expires_at", "registered_at"] {
        for order in ["asc", "desc"] {
            for dedupe in ["name", "registration"] {
                let uri = format!(
                    "/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&order={order}&dedupe={dedupe}&page_size=5"
                );
                let exact = walk_pages(&database, &uri, 2).await?;
                let (walked, batches) =
                    with_batches(with_exact_total_cap(0, walk_pages(&database, &uri, 2))).await;
                assert_eq!(
                    exact.iter().map(page_body).collect::<Vec<_>>(),
                    walked?.iter().map(page_body).collect::<Vec<_>>(),
                    "{uri}"
                );
                assert!(batches.iter().sum::<usize>() < 65, "{uri}: {batches:?}");
            }
        }
    }
    // Selective filters walk past most candidates without keeping their rows.
    for query in ["q=bulk0001", "q=zzz", "q=bulk00&match=contains&sort=created_at&dedupe=registration"] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=3&{query}");
        let exact = walk_all_pages(&database, &uri).await?;
        let walked = with_exact_total_cap(0, walk_all_pages(&database, &uri)).await?;
        assert_eq!(
            exact.iter().map(page_body).collect::<Vec<_>>(),
            walked.iter().map(page_body).collect::<Vec<_>>(),
            "{uri}"
        );
    }
    // Served names, not stored spellings, decide name order: the emoji names sort as served.
    let first = bulk_payload(
        &database,
        &format!("/v1/addresses/{BULK_ADDRESS}/names?order=desc&page_size=4"),
    )
    .await?;
    let served: Vec<&str> = first["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["display_name"].as_str().unwrap())
        .collect();
    assert!(served.iter().any(|name| name.contains('\u{fe0f}')), "{served:?}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_walk_falls_back_when_a_stored_sort_key_disagrees() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 120).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort=expires_at&page_size=10");
    let exact = bulk_payload(&database, &uri).await?;
    // A stale stored expiry that walks the name first; composition still serves the real one.
    let logical = phase_logical_identity("ens", "bulk00050.eth")?.0;
    let updated = sqlx::query(
        "UPDATE bigname_phase.project_name_summary SET expires_at = 1 WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);
    let (walked, paths) =
        with_paths(with_exact_total_cap(0, bulk_payload(&database, &uri))).await;
    assert_eq!(paths, ["fallback"]);
    let walked = walked?;
    assert_eq!(page_body(&exact), page_body(&walked));
    assert!(walked["page"]["total_count"].is_null(), "{walked}");
    database.cleanup().await
}

/// The rich fixture's query matrix: one chunk at a time and the walk serve the default pages.
#[tokio::test]
async fn v2_address_names_rich_fixture_is_identical_across_paths() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let queries = [
        "",
        "relation=owner",
        "relation=manager",
        "relation=owner,manager",
        "relation=role_holder",
        "q=sha",
        "q=a&match=contains",
        "authority=ens_v1",
        "authority=ens_v0,ens_v2",
        "is_migrated=false",
        "parent=eth",
        "include=role_summary",
        "include=counts",
        "dedupe=registration",
        "dedupe=registration&order=desc",
        "sort=expires_at",
        "sort=expires_at&order=desc&dedupe=registration",
        "sort=registered_at",
        "sort=registered_at&order=desc",
        "sort=created_at&dedupe=registration",
        "sort=created_at&order=desc",
    ];
    for query in queries {
        for page_size in [1, 2, 50] {
            let uri = format!("/v1/addresses/{V2_ADDRESS}/names?page_size={page_size}&{query}");
            let exact = walk_all_pages(&database, &uri).await?;
            let chunked = with_compose_chunk(1, walk_all_pages(&database, &uri)).await?;
            assert_eq!(exact, chunked, "{uri}");
            let (walked, paths) =
                with_paths(with_exact_total_cap(0, walk_all_pages(&database, &uri))).await;
            let walked = walked?;
            assert!(paths.iter().all(|path| *path == capped_path(&uri)), "{uri}: {paths:?}");
            assert_eq!(
                exact.iter().map(page_body).collect::<Vec<_>>(),
                walked.iter().map(page_body).collect::<Vec<_>>(),
                "{uri}"
            );
        }
    }
    for query in ["relation=resolves_to", "relation=former_owner"] {
        let uri = format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1&{query}");
        let exact = walk_all_pages(&database, &uri).await?;
        let chunked = with_compose_chunk(1, walk_all_pages(&database, &uri)).await?;
        assert_eq!(exact, chunked, "{uri}");
    }
    let history = format!("/v1/addresses/{V2_ADDRESS}/history?page_size=2");
    let exact = walk_all_pages(&database, &history).await?;
    let chunked = with_compose_chunk(1, walk_all_pages(&database, &history)).await?;
    assert_eq!(exact, chunked);
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_roles_fixture_is_identical_across_paths() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!(["renew"])).await?;
    for address in [ROLE_HOLDER, V2_ADDRESS] {
        for query in [
            "",
            "relation=role_holder",
            "relation=owner",
            "dedupe=registration",
            "sort=expires_at&order=desc",
            "sort=registered_at&dedupe=registration",
            "sort=created_at&order=desc&dedupe=registration",
            "order=desc&relation=role_holder",
        ] {
            let uri = format!("/v1/addresses/{address}/names?page_size=1&{query}");
            let exact = walk_all_pages(&database, &uri).await?;
            let chunked = with_compose_chunk(1, walk_all_pages(&database, &uri)).await?;
            assert_eq!(exact, chunked, "{uri}");
            let walked = with_exact_total_cap(0, walk_all_pages(&database, &uri)).await?;
            assert_eq!(
                exact.iter().map(page_body).collect::<Vec<_>>(),
                walked.iter().map(page_body).collect::<Vec<_>>(),
                "{uri}"
            );
        }
    }
    database.cleanup().await
}

/// Surface-less registry children the index also lists do not push a capped address off the walk.
#[tokio::test]
async fn v2_address_names_walk_serves_registry_children() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    for query in ["", "order=desc&dedupe=registration", "sort=expires_at", "sort=registered_at&order=desc"] {
        let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=2&{query}");
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
    }
    database.cleanup().await
}
