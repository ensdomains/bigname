// `relation=resolves_to` on an address many names resolve to: above the cap the page walks the
// candidates as the ownership relations' page does, and every path serves the same pages. The
// fixtures and helpers are `v2_address_names_bounded.rs`'s.

/// Every bulk name resolves to [`BULK_ADDRESS`]: a third on coin 60 alone, a third through the
/// ENSIP-19 default alone, a third through both, and every fifth on coin 61 too.
fn bulk_resolving(index: usize) -> BulkRecords {
    let mut writes = Vec::new();
    if index % 3 != 1 {
        writes.push(("addr:60".to_owned(), BULK_ADDRESS.to_owned()));
    }
    if !index.is_multiple_of(3) {
        writes.push(("addr:2147483648".to_owned(), BULK_ADDRESS.to_owned()));
    }
    if index.is_multiple_of(5) {
        writes.push(("addr:61".to_owned(), BULK_ADDRESS.to_owned()));
    }
    BulkRecords {
        writes,
        cleared: false,
    }
}

/// Names the inverse address index lists for [`BULK_ADDRESS`] that no longer resolve to it,
/// interleaved with names that do: every fourth name's records were cleared by a later record
/// version, and every fourth after it answers coin 60 elsewhere while its ENSIP-19 default
/// still matches, which the exact entry shadows for coin 60.
fn bulk_partly_resolving(index: usize) -> BulkRecords {
    match index % 4 {
        0 => BulkRecords {
            writes: vec![("addr:60".to_owned(), BULK_ADDRESS.to_owned())],
            cleared: true,
        },
        1 => BulkRecords {
            writes: vec![
                ("addr:60".to_owned(), BULK_OTHER_ADDRESS.to_owned()),
                ("addr:2147483648".to_owned(), BULK_ADDRESS.to_owned()),
            ],
            cleared: false,
        },
        _ => bulk_resolving(index),
    }
}

/// `count` names owned by another address whose resolver records are `records(index)`.
async fn seed_bulk_resolves_to_names(
    database: &TestDatabase,
    count: usize,
    aliases: &[(usize, usize)],
    records: fn(usize) -> BulkRecords,
) -> Result<()> {
    seed_bulk_names_with_records(database, count, |_| BULK_OTHER_ADDRESS, aliases, records).await
}

fn resolves_to_uri(query: &str) -> String {
    format!("/v1/addresses/{BULK_ADDRESS}/names?relation=resolves_to&{query}")
}

fn bulk_rows<'a>(pages: impl IntoIterator<Item = &'a Value>) -> Vec<Value> {
    pages
        .into_iter()
        .flat_map(|page| page["data"].as_array().unwrap().clone())
        .collect()
}

async fn with_filter_names<F: std::future::Future>(future: F) -> (F::Output, Vec<usize>) {
    use bigname_storage::families::records::seams::with_walk_filter_names;
    let names = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = with_walk_filter_names(names.clone(), future).await;
    let names = names.lock().unwrap().clone();
    (output, names)
}

/// Every page of `query` at page size 25 above a cap of 40 is walked and serves the rows of the
/// full read, in order, with every composition within one chunk.
async fn assert_walk_serves_the_full_read(database: &TestDatabase, query: &str) -> Result<()> {
    let uri = resolves_to_uri(query);
    let exact = walk_all_pages(database, &format!("{uri}&page_size=200")).await?;
    let walked = capped_pages(
        database,
        &format!("{uri}&page_size=25"),
        1000,
        Some(40),
        false,
    )
    .await?;
    let expected = bulk_rows(&exact);
    assert!(!expected.is_empty(), "{uri}");
    assert_eq!(
        bulk_rows(walked.iter().map(|page| &page.payload)),
        expected,
        "{uri}"
    );
    assert_eq!(walked.len(), expected.len().div_ceil(25), "{uri}");
    for page in &walked {
        assert_eq!(page.paths, ["walk"], "{uri}");
        assert!(page.batches.iter().all(|batch| *batch <= 256), "{uri}");
    }
    Ok(())
}

/// Row 1: the walk serves the full read for every sort, order, dedupe and coin form, in the
/// ENS namespace and with none, over registrations with a second name far from the first.
#[tokio::test]
async fn v2_resolves_to_walk_and_full_read_serve_identical_pages() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 300, &[(10, 200), (20, 21)], bulk_resolving).await?;
    for coin in ["60", "2147483658", "evm"] {
        for sort in ["name", "expires_at", "registered_at"] {
            for (order, dedupe) in [("asc", "name"), ("desc", "registration")] {
                let query = format!(
                    "namespace=ens&coin_type={coin}&sort={sort}&order={order}&dedupe={dedupe}"
                );
                assert_walk_serves_the_full_read(&database, &query).await?;
            }
        }
    }
    for query in [
        "coin_type=60&dedupe=registration",
        "coin_type=60&order=desc",
        "coin_type=61",
        "coin_type=evm&sort=expires_at&dedupe=registration",
    ] {
        assert_walk_serves_the_full_read(&database, query).await?;
    }
    database.cleanup().await
}

/// Row 2: a first page composes the page and the name after it, and a resumed page the
/// cursor's own name too.
#[tokio::test]
async fn v2_resolves_to_walk_first_and_resumed_pages_compose_one_page() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 300, &[], bulk_resolving).await?;
    for coin in ["60", "evm"] {
        let uri = resolves_to_uri(&format!("coin_type={coin}&page_size=50"));
        let pages =
            with_compose_chunk(4096, capped_pages(&database, &uri, 2, Some(0), false)).await?;
        let exact = walk_pages(&database, &uri, 2).await?;
        for (page, exact) in pages.iter().zip(&exact) {
            assert_eq!(page.paths, ["walk"], "{uri}");
            assert_eq!(page_body(&page.payload), page_body(exact), "{uri}");
        }
        assert_eq!(pages[0].batches, [51], "{uri}");
        assert_eq!(pages[1].batches, [52], "{uri}");
    }
    database.cleanup().await
}

/// Row 3: the cap is exclusive. At as many candidates as the cap the page is the full read, one
/// above it the walk, and both serve the same pages.
#[tokio::test]
async fn v2_resolves_to_below_the_cap_is_the_full_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 300, &[], bulk_resolving).await?;
    for query in [
        "coin_type=60&page_size=50",
        "coin_type=evm&page_size=50&order=desc",
    ] {
        let uri = resolves_to_uri(query);
        let at_cap = capped_pages(&database, &uri, 1000, Some(300), false).await?;
        let above = capped_pages(&database, &uri, 1000, Some(299), false).await?;
        assert!(at_cap.iter().all(|page| page.paths == ["exact"]), "{uri}");
        assert!(above.iter().all(|page| page.paths == ["walk"]), "{uri}");
        assert_eq!(
            at_cap
                .iter()
                .map(|page| page_body(&page.payload))
                .collect::<Vec<_>>(),
            above
                .iter()
                .map(|page| page_body(&page.payload))
                .collect::<Vec<_>>(),
            "{uri}"
        );
    }
    database.cleanup().await
}

/// Row 4: a cursor the full read minted resumes on the walk, and the other way round, for both
/// coin forms.
#[tokio::test]
async fn v2_resolves_to_cursor_crosses_the_cap_unchanged() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 120, &[(3, 90)], bulk_resolving).await?;
    for query in [
        "coin_type=60&page_size=9",
        "coin_type=evm&page_size=9&dedupe=registration&order=desc",
        "coin_type=60&page_size=9&sort=expires_at",
    ] {
        let uri = resolves_to_uri(query);
        let exact = walk_pages(&database, &uri, 2).await?;
        let cursor = exact[0]["page"]["next_cursor"].as_str().unwrap().to_owned();
        let resumed = format!("{uri}&cursor={cursor}");
        let (walked, paths) =
            with_paths(with_exact_total_cap(0, bulk_payload(&database, &resumed))).await;
        assert_eq!(paths, ["walk"], "{uri}");
        assert_eq!(page_body(&walked?), page_body(&exact[1]), "{uri}");

        let first = with_exact_total_cap(0, bulk_payload(&database, &uri)).await?;
        assert_eq!(page_body(&first), page_body(&exact[0]), "{uri}");
        let cursor = first["page"]["next_cursor"].as_str().unwrap();
        let (second, paths) =
            with_paths(bulk_payload(&database, &format!("{uri}&cursor={cursor}"))).await;
        assert_eq!(paths, ["exact"], "{uri}");
        assert_eq!(page_body(&second?), page_body(&exact[1]), "{uri}");
    }
    database.cleanup().await
}

/// Row 5: names the index lists that no longer resolve to the address, a cleared record version
/// and a coin-60 entry for another address shadowing the default, are walked past.
#[tokio::test]
async fn v2_resolves_to_walk_steps_past_names_that_no_longer_resolve() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 300, &[], bulk_partly_resolving).await?;
    for query in [
        "coin_type=60",
        "coin_type=60&order=desc&dedupe=registration",
        "coin_type=2147483658&sort=expires_at",
        "coin_type=evm",
    ] {
        assert_walk_serves_the_full_read(&database, query).await?;
    }
    database.cleanup().await
}

/// Row 6: a registration with two names far apart in walk order. In descending order the walk
/// reaches the higher name first and composes the lower one with it, whose entry the group is:
/// the page is not final until the walk passes it.
#[tokio::test]
async fn v2_resolves_to_walk_closes_a_registration_split_across_steps() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 71, &[(10, 70)], bulk_resolving).await?;
    for query in [
        "coin_type=60&dedupe=registration&order=desc&page_size=1",
        "coin_type=evm&dedupe=registration&order=desc&page_size=1",
        "coin_type=60&dedupe=registration&page_size=5",
    ] {
        let uri = resolves_to_uri(query);
        let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
        assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
        let names: Vec<&Value> = pages
            .iter()
            .flat_map(|page| page.payload["data"].as_array().unwrap())
            .map(|row| &row["name"])
            .collect();
        assert!(names.contains(&&json!("bulk00010.eth")), "{uri}: {names:?}");
        assert!(
            !names.contains(&&json!("bulk00070.eth")),
            "{uri}: {names:?}"
        );
    }
    database.cleanup().await
}

/// Row 7: a stored sort key that disagrees with composition sends the page to the full read.
#[tokio::test]
async fn v2_resolves_to_walk_falls_back_when_a_stored_sort_key_disagrees() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 120, &[], bulk_resolving).await?;
    let logical = bulk_logical_name_id(50)?;
    for (sort, column, kind, value) in [
        ("expires_at", "expires_at", "numeric", "1"),
        (
            "registered_at",
            "registered_at",
            "timestamptz",
            "2001-01-01T00:00:00Z",
        ),
    ] {
        let saved: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {column}::text FROM bigname_phase.project_name_summary
             WHERE logical_name_id = $1"
        ))
        .bind(&logical)
        .fetch_one(&database.pool)
        .await?;
        let store = |value: Option<String>| {
            let (database, logical) = (&database, &logical);
            async move {
                let updated = sqlx::query(&format!(
                    "UPDATE bigname_phase.project_name_summary SET {column} = $2::{kind}
                     WHERE logical_name_id = $1"
                ))
                .bind(logical)
                .bind(value)
                .execute(&database.pool)
                .await?;
                anyhow::ensure!(updated.rows_affected() == 1, "one stored summary");
                anyhow::Ok(())
            }
        };
        for coin in ["60", "evm"] {
            let uri = resolves_to_uri(&format!("coin_type={coin}&sort={sort}&page_size=10"));
            let exact = bulk_payload(&database, &uri).await?;
            let (walked, paths) =
                with_paths(with_exact_total_cap(0, bulk_payload(&database, &uri))).await;
            assert_eq!(paths, ["walk"], "{uri}");
            assert_eq!(page_body(&walked?), page_body(&exact), "{uri}");
            // A stale stored key that walks the name first; composition still serves the real one.
            store(Some(value.to_owned())).await?;
            let (walked, paths) =
                with_paths(with_exact_total_cap(0, bulk_payload(&database, &uri))).await;
            assert_eq!(paths, ["fallback"], "{uri}");
            assert_eq!(page_body(&walked?), page_body(&exact), "{uri}");
            store(saved.clone()).await?;
        }
    }
    database.cleanup().await
}

/// Row 8: selective filters walk past the candidates they reject, a step's rows at a time.
#[tokio::test]
async fn v2_resolves_to_walk_keeps_only_filtered_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 300, &[(10, 200)], bulk_resolving).await?;
    for filter in [
        "q=zzz",
        "q=bulk0001",
        "q=bulk00&match=contains",
        "parent=eth",
        "authority=ens_v1",
    ] {
        for dedupe in ["name", "registration"] {
            let uri = resolves_to_uri(&format!(
                "coin_type=60&dedupe={dedupe}&page_size=3&{filter}"
            ));
            let exact = walk_all_pages(&database, &uri).await?;
            let (walked, filters) = with_filter_names(with_paths(with_exact_total_cap(
                0,
                walk_all_pages(&database, &uri),
            )))
            .await;
            let (walked, paths) = walked;
            let walked = walked?;
            assert!(
                !paths.is_empty() && paths.iter().all(|path| *path == "walk"),
                "{uri}: {paths:?}"
            );
            assert_eq!(
                exact.iter().map(page_body).collect::<Vec<_>>(),
                walked.iter().map(page_body).collect::<Vec<_>>(),
                "{uri}"
            );
            assert!(!filters.is_empty(), "{uri}");
            assert!(
                !filters.is_empty() && filters.iter().all(|names| *names < 300),
                "{uri}: {filters:?}"
            );
        }
    }
    database.cleanup().await
}

/// Row 9: a `coin_type=evm` row past the per-row coin-type limit is still refused on the walk.
#[tokio::test]
async fn v2_resolves_to_evm_walk_still_rejects_a_row_past_the_coin_type_limit() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_evm_bound_fixture(&database).await?;
    for query in ["&page_size=1", "&dedupe=registration&page_size=1", ""] {
        let uri = format!(
            "/v1/addresses/{V2_EVM_WIDE_ADDRESS}/names?relation=resolves_to&coin_type=evm{query}"
        );
        let exact = bulk_response(&database, &uri).await?;
        let (walked, paths) =
            with_paths(with_exact_total_cap(0, bulk_response(&database, &uri))).await;
        assert_eq!(paths, ["walk"], "{uri}");
        assert_eq!(exact.0, StatusCode::UNPROCESSABLE_ENTITY, "{uri}");
        assert_eq!(walked?, exact, "{uri}");
    }
    database.cleanup().await
}

/// Row 10: a candidate on a chain with no served publication answers stale above the cap as
/// below it.
#[tokio::test]
async fn v2_resolves_to_walk_answers_stale_for_an_unpublished_chain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 40, &[], bulk_resolving).await?;
    let uri = resolves_to_uri("coin_type=60&page_size=5");
    let (served, paths) = with_paths(with_exact_total_cap(0, bulk_payload(&database, &uri))).await;
    assert_eq!(paths, ["walk"]);
    assert_eq!(served?["data"].as_array().unwrap().len(), 5);
    // The inverse index lists the address on a chain nothing has published, at a resolver
    // pointer a name there is bound to.
    let resource = Uuid::from_u128(0x7e44_0001);
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_record_node_index
            (address, coin_type, chain_id, resolver_address, node, logical_name_id)
         VALUES ($1, '60', 'unpublished-chain', $2, '0xunpublished', '')",
    )
    .bind(BULK_ADDRESS)
    .bind(V2_RESOLVES_TO_RESOLVER)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.project_resource_pointer
            (chain_id, resource_id, block_number, event_identity, resolver_address, namehash,
             namespace, source_family)
         VALUES ('unpublished-chain', $1, 1, 'unpublished-pointer', $2, '0xunpublished', 'ens',
                 'ens_v1_registry_l1')",
    )
    .bind(resource)
    .bind(V2_RESOLVES_TO_RESOLVER)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.project_binding_candidate
            (surface_binding_id, logical_name_id, namespace, chain_id, authority_arm,
             resource_id, binding_kind, canonicality_state, block_number, event_identity)
         VALUES ($1, 'ens:unpublished', 'ens', 'unpublished-chain', 'ens_v1', $2,
                 'declared_registry_path', 'canonical', 1, 'unpublished-binding')",
    )
    .bind(Uuid::from_u128(0x7e44_0002))
    .bind(resource)
    .execute(&database.pool)
    .await?;
    let exact = bulk_response(&database, &uri).await?;
    assert_eq!(exact.0, StatusCode::CONFLICT, "{}", exact.1);
    assert_eq!(exact.1["error"]["code"], json!("stale"), "{}", exact.1);
    // The walk refuses before its first step, so it records no path.
    let (walked, paths) = with_paths(with_exact_total_cap(0, bulk_response(&database, &uri))).await;
    assert!(paths.is_empty(), "{paths:?}");
    assert_eq!(walked?, exact);
    database.cleanup().await
}

/// Row 11: pages that end exactly where a step ends, with and without a name after them.
#[tokio::test]
async fn v2_resolves_to_walk_page_ending_at_the_step_boundary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 24, &[], bulk_resolving).await?;
    for page_size in (5..=13).chain([23, 24]) {
        for query in [
            "coin_type=60",
            "coin_type=evm&order=desc",
            "coin_type=60&sort=expires_at",
            "coin_type=60&sort=registered_at&order=desc&dedupe=registration",
        ] {
            let uri = resolves_to_uri(&format!("page_size={page_size}&{query}"));
            let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
            assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
            assert_eq!(pages.len(), 24_usize.div_ceil(page_size), "{uri}");
            assert_eq!(
                pages.last().unwrap().payload["page"]["has_more"],
                json!(false),
                "{uri}"
            );
            if query.contains("dedupe=registration") {
                continue;
            }
            let counts: Vec<usize> = pages.iter().map(composed).collect();
            match page_size {
                12 => assert_eq!(counts, [13, 13], "{uri}"),
                11 => assert_eq!(counts, [12, 13, 3], "{uri}"),
                24 => assert_eq!(counts, [24], "{uri}"),
                23 => assert_eq!(counts, [24, 2], "{uri}"),
                _ => {}
            }
        }
    }
    database.cleanup().await
}

/// Row 12b: 3,300 candidates of which only the last three resolve. The steps grow to 1,024
/// names and repeat that size, as the ownership relations' steps do.
#[tokio::test]
async fn v2_resolves_to_walk_repeats_the_largest_step() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 3_300, &[], |index| {
        if index >= 3_297 {
            bulk_resolving(index)
        } else {
            BulkRecords {
                writes: vec![("addr:60".to_owned(), BULK_ADDRESS.to_owned())],
                cleared: true,
            }
        }
    })
    .await?;
    let uri = resolves_to_uri("coin_type=60&page_size=5");
    let exact = walk_all_pages(&database, &uri).await?;
    let pages = with_compose_chunk(4096, capped_pages(&database, &uri, 1, Some(0), false)).await?;
    assert_eq!(pages[0].paths, ["walk"]);
    assert_eq!(page_body(&pages[0].payload), page_body(&exact[0]));
    assert_eq!(pages[0].payload["data"].as_array().unwrap().len(), 3);
    assert_eq!(
        pages[0].batches,
        [8, 24, 64, 128, 256, 512, 1024, 1024, 260]
    );
    database.cleanup().await
}

/// Row 13b: one registration bound to 700 candidate names, of which `q` keeps one. The walk
/// composes the registration's other names when it first reaches it and filters them a chunk at
/// a time, so no filter statement reads more than one chunk of names.
#[tokio::test]
async fn v2_resolves_to_walk_closure_filters_each_chunk() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    let aliases: Vec<(usize, usize)> = (1..700).map(|index| (0, index)).collect();
    seed_bulk_resolves_to_names(&database, 700, &aliases, bulk_resolving).await?;
    let uri = resolves_to_uri("coin_type=60&dedupe=registration&q=bulk00350&page_size=5");
    let exact = walk_all_pages(&database, &uri).await?;
    assert_eq!(bulk_rows(&exact).len(), 1);
    let ((walked, paths), filters) = with_filter_names(with_compose_chunk(
        100,
        with_paths(with_exact_total_cap(0, walk_all_pages(&database, &uri))),
    ))
    .await;
    assert!(
        !paths.is_empty() && paths.iter().all(|path| *path == "walk"),
        "{paths:?}"
    );
    assert_eq!(
        exact.iter().map(page_body).collect::<Vec<_>>(),
        walked?.iter().map(page_body).collect::<Vec<_>>()
    );
    assert!(filters.len() > 7, "{filters:?}");
    assert!(
        !filters.is_empty() && filters.iter().all(|names| *names <= 100),
        "{filters:?}"
    );
    database.cleanup().await
}

/// `sort=created_at` above the cap: the walk first composes every candidate a chunk at a time
/// for the names' served first observations, then serves the full read's pages, ties included.
#[tokio::test]
async fn v2_resolves_to_created_at_above_the_cap_walks_after_reading_every_candidate() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 120, &[(10, 100)], bulk_partly_resolving).await?;
    for query in [
        "coin_type=60&sort=created_at",
        "coin_type=60&sort=created_at&order=desc&dedupe=registration",
        "coin_type=evm&sort=created_at&dedupe=registration",
        "coin_type=2147483658&sort=created_at&order=desc&q=bulk00&match=contains",
    ] {
        let uri = resolves_to_uri(&format!("{query}&page_size=7"));
        let exact = walk_all_pages(&database, &uri).await?;
        let ((walked, paths), filters) = with_filter_names(with_paths(with_exact_total_cap(
            0,
            walk_all_pages(&database, &uri),
        )))
        .await;
        let walked = walked?;
        assert!(
            !paths.is_empty() && paths.iter().all(|path| *path == "walk"),
            "{uri}: {paths:?}"
        );
        assert_eq!(
            exact.iter().map(page_body).collect::<Vec<_>>(),
            walked.iter().map(page_body).collect::<Vec<_>>(),
            "{uri}"
        );
        assert!(
            !filters.is_empty() && filters.iter().all(|names| *names <= 256),
            "{uri}: {filters:?}"
        );
    }
    database.cleanup().await
}

/// Fixtures with names served through a retained registry pointer alone, an ENSv2 root TLD, the
/// ENSIP-19 default and its shadowing, and registrations with two names: the walk serves the
/// full read's pages.
#[tokio::test]
async fn v2_resolves_to_fixtures_identical_across_paths() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_evm_records(&database).await?;
    for (name, seed) in [("ownerless-a.eth", 0xa600), ("ownerless-b.eth", 0xc600)] {
        seed_resolves_to_ownerless_name(&database, name, seed).await?;
    }
    for query in [
        "",
        "coin_type=2147483658",
        "coin_type=2147483648",
        "coin_type=evm",
        "dedupe=registration&order=desc",
        "coin_type=evm&dedupe=registration&sort=expires_at",
        "sort=registered_at&order=desc",
        "sort=created_at&dedupe=registration",
        "q=a&match=contains",
        "authority=ens_v1",
    ] {
        for page_size in [1, 2, 50] {
            let uri = format!(
                "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size={page_size}&{query}"
            );
            let exact = walk_all_pages(&database, &uri).await?;
            let (walked, paths) =
                with_paths(with_exact_total_cap(0, walk_all_pages(&database, &uri))).await;
            let walked = walked?;
            assert!(
                !paths.is_empty() && paths.iter().all(|path| *path == "walk"),
                "{uri}: {paths:?}"
            );
            assert_eq!(
                exact.iter().map(page_body).collect::<Vec<_>>(),
                walked.iter().map(page_body).collect::<Vec<_>>(),
                "{uri}"
            );
        }
    }
    database.cleanup().await
}
