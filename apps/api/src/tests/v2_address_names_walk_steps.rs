// The address-names walk's steps: how many names a page composes, in how many steps, and that
// no step size changes a page. The fixtures and helpers are `v2_address_names_bounded.rs`'s.

/// The names of the bulk fixture [`BULK_ADDRESS`] holds when another address holds those the
/// index also lists for it: the names whose served spelling differs sort by collation, so they
/// belong to the other address and stay out of the candidates.
fn bulk_owner_below(index: usize, first_held: usize) -> &'static str {
    if index < first_held {
        BULK_OTHER_ADDRESS
    } else {
        BULK_ADDRESS
    }
}

async fn bulk_response(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = v2_address_names_response_for_database(database, uri).await?;
    let status = response.status();
    Ok((status, read_json::<Value>(response).await?))
}

/// Six pages of 200 over a holder of 1,127 names, above the route's own 1,000-candidate cap:
/// each page composes itself and what ends it, where the walk's first step sizes composed four
/// pages while that many names remained.
#[tokio::test]
async fn v2_address_names_walk_six_pages_of_a_holder_above_the_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 1_127).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=200");
    let first = capped_pages(&database, &uri, 1000, None, true).await?;
    let current = capped_pages(&database, &uri, 1000, None, false).await?;
    let exact = with_exact_total_cap(usize::MAX, walk_all_pages(&database, &uri)).await?;
    for pages in [&first, &current] {
        assert!(pages.iter().all(|page| page.paths == ["walk"]));
        assert_eq!(
            pages
                .iter()
                .map(|page| page.payload["data"].as_array().unwrap().len())
                .collect::<Vec<_>>(),
            [200, 200, 200, 200, 200, 127]
        );
        assert_eq!(
            exact.iter().map(page_body).collect::<Vec<_>>(),
            pages
                .iter()
                .map(|page| page_body(&page.payload))
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(
        first.iter().map(|page| &page.payload).collect::<Vec<_>>(),
        current.iter().map(|page| &page.payload).collect::<Vec<_>>()
    );
    assert_eq!(
        first.iter().map(composed).collect::<Vec<_>>(),
        [804, 804, 728, 528, 328, 128]
    );
    assert_eq!(
        current.iter().map(composed).collect::<Vec<_>>(),
        [201, 202, 202, 202, 202, 128]
    );
    let explicit =
        capped_pages(&database, &format!("{uri}&dedupe=name"), 1000, None, false).await?;
    assert_eq!(
        current.iter().map(|page| &page.payload).collect::<Vec<_>>(),
        explicit
            .iter()
            .map(|page| &page.payload)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        current.iter().map(|page| &page.batches).collect::<Vec<_>>(),
        explicit
            .iter()
            .map(|page| &page.batches)
            .collect::<Vec<_>>()
    );
    let registration_uri = format!("{uri}&dedupe=registration");
    let old = capped_pages(&database, &registration_uri, 1000, None, true).await?;
    let registration = capped_pages(&database, &registration_uri, 1000, None, false).await?;
    assert_eq!(
        old.iter().map(|page| &page.payload).collect::<Vec<_>>(),
        registration
            .iter()
            .map(|page| &page.payload)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        old.iter().map(|page| &page.batches).collect::<Vec<_>>(),
        registration
            .iter()
            .map(|page| &page.batches)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        registration.iter().map(composed).collect::<Vec<_>>(),
        [804, 804, 728, 528, 328, 128]
    );
    database.cleanup().await
}

/// A registration with two names far apart in the walk's order. In descending name order the
/// walk reaches the higher name first and gathers the whole registration, whose entry sorts at
/// the lower name: the page is not final until the walk passes that name, several steps on.
#[tokio::test]
async fn v2_address_names_walk_closes_a_registration_split_across_steps() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // bulk00010 and bulk00070 share a registration. Between them the index lists 59 names of
    // another address, which compose no row.
    seed_bulk_names(
        &database,
        71,
        |index| {
            if (4..=10).contains(&index) || index == 70 {
                BULK_ADDRESS
            } else {
                BULK_OTHER_ADDRESS
            }
        },
        &[(10, 70)],
    )
    .await?;
    list_bulk_names_without_a_relation(&database, 11..70).await?;
    let names = format!("/v1/addresses/{BULK_ADDRESS}/names");
    let by_name = bulk_payload(&database, &format!("{names}?dedupe=name")).await?;
    let by_registration = bulk_payload(&database, &format!("{names}?dedupe=registration")).await?;
    assert_eq!(by_name["page"]["total_count"], json!(8), "{by_name}");
    assert_eq!(
        by_registration["page"]["total_count"],
        json!(7),
        "{by_registration}"
    );

    let uri = format!("{names}?dedupe=registration&order=desc&page_size=1");
    let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
    assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
    assert_eq!(pages.len(), 7, "{uri}");
    assert_eq!(
        pages[0].payload["data"][0]["name"],
        json!("bulk00010.eth"),
        "{uri}"
    );
    // The old registration partitions include a separate group-closure composition.
    let old = capped_pages(&database, &uri, 1, Some(0), true).await?;
    assert_eq!(pages[0].batches, old[0].batches);

    // Ascending, the first step reaches the lower name and gathers the higher one with it.
    let uri = format!("{names}?dedupe=registration&page_size=5");
    let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
    assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
    assert_eq!(
        pages[0].payload["data"][4]["name"],
        json!("bulk00008.eth"),
        "{uri}"
    );
    let uri = format!("{names}?dedupe=registration&sort=expires_at&page_size=2");
    assert_step_sizes_change_no_page(&database, &uri).await?;
    database.cleanup().await
}

/// Pages that end exactly where a step ends, with and without a name after them: 24 names at
/// every page size from 5 to 13, so the last page is full at 6, 8 and 12 and holds one name at
/// 23.
#[tokio::test]
async fn v2_address_names_walk_page_ending_at_the_step_boundary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 24).await?;
    for page_size in (5..=13).chain([23, 24]) {
        for query in [
            "",
            "order=desc",
            "sort=expires_at",
            "sort=registered_at&order=desc&dedupe=registration",
        ] {
            let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size={page_size}&{query}");
            let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
            assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
            assert_eq!(pages.len(), 24_usize.div_ceil(page_size), "{uri}");
            let last = pages.last().unwrap();
            assert_eq!(last.payload["page"]["has_more"], json!(false), "{uri}");
            let counts: Vec<usize> = pages.iter().map(composed).collect();
            if query.contains("dedupe=registration") {
                let old = capped_pages(&database, &uri, 1000, Some(0), true).await?;
                assert_eq!(
                    counts,
                    old.iter().map(composed).collect::<Vec<_>>(),
                    "{uri}"
                );
                continue;
            }
            match page_size {
                // The first step holds the page and the name after it. The second starts on
                // the cursor's name and ends with the candidates, on a full page.
                12 => assert_eq!(counts, [13, 13], "{uri}"),
                11 => assert_eq!(counts, [12, 13, 3], "{uri}"),
                // One step past the page finds no candidate: the walk ends with them.
                24 => assert_eq!(counts, [24], "{uri}"),
                23 => assert_eq!(counts, [24, 2], "{uri}"),
                _ => {}
            }
        }
    }
    database.cleanup().await
}

/// Names the index lists for the address but that compose no row for it, first in name order:
/// the walk steps past them to the names the address holds.
#[tokio::test]
async fn v2_address_names_walk_steps_past_candidates_with_no_relation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_names(&database, 60, |index| bulk_owner_below(index, 30), &[]).await?;
    list_bulk_names_without_a_relation(&database, 4..30).await?;
    for query in [
        "",
        "order=desc",
        "sort=expires_at",
        "dedupe=registration&sort=registered_at",
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=5&{query}");
        let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
        assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
        assert_eq!(pages.len(), 6, "{uri}");
        // Every candidate is composed at least once across the pages, the 26 without a row too.
        assert!(pages.iter().map(composed).sum::<usize>() >= 56, "{uri}");
    }
    database.cleanup().await
}

/// How many steps a walk takes when candidates yield no row. A step costs a fixed number of
/// statements whatever its size, so the step count is what a sparse page pays.
///
/// Two bounds against the walk's first step sizes, for the same candidates and request. A page
/// composes no more candidates: no step passes where one of those steps ended. And it takes at
/// most five more steps: the second step is at least a sixteenth of the first step then, and
/// every later step at least doubles before it is cut to such an end.
#[tokio::test]
async fn v2_address_names_walk_takes_bounded_steps_when_few_candidates_yield() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    // The composition calls of page `page` of `uri` under the first step sizes and the current
    // ones, with one call a step for the walked names.
    async fn calls(
        database: &TestDatabase,
        uri: &str,
        page: usize,
    ) -> Result<(Vec<usize>, Vec<usize>)> {
        assert_eq!(
            address_name_test_requests(uri),
            1,
            "the +5 bound is per HTTP request"
        );
        let mut read = Vec::new();
        for first_policy in [true, false] {
            let pages = with_compose_chunk(
                4096,
                capped_pages(database, uri, page + 1, Some(0), first_policy),
            )
            .await?;
            assert_eq!(pages[page].paths, ["walk"], "{uri}");
            read.push(pages.into_iter().nth(page).unwrap());
        }
        assert_eq!(read[0].payload, read[1].payload, "{uri}");
        let current = read.pop().unwrap().batches;
        Ok((read.pop().unwrap().batches, current))
    }

    // 556 candidates. In descending name order: 159 names the address holds, one name that
    // composes no row, 96 more names it holds, then 300 that compose no row.
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_names(
        &database,
        560,
        |index| {
            if index < 304 || index == 400 {
                BULK_OTHER_ADDRESS
            } else {
                BULK_ADDRESS
            }
        },
        &[],
    )
    .await?;
    list_bulk_names_without_a_relation(&database, 4..304).await?;
    list_bulk_names_without_a_relation(&database, 400..401).await?;
    let names = format!("/v1/addresses/{BULK_ADDRESS}/names");

    // A page of 200 one candidate short: the first step holds the name without a row, so it
    // finds 200 groups of the 201 wanted. The second step is a quarter of the first.
    let near_miss = format!("{names}?page_size=200&order=desc");
    let (first, current) = calls(&database, &near_miss, 0).await?;
    assert_eq!(first, [556]);
    assert_eq!(current, [201, 51]);

    // A nearly full first step, then nothing yields: the third page of 85 holds the last 85
    // names of the address, and only names without a row follow it. Six steps where the first
    // sizes took two, over the same 386 candidates. The fifth stops where their first ended.
    let nearly_full = format!("{names}?page_size=85&order=desc");
    let (first, current) = calls(&database, &nearly_full, 2).await?;
    assert_eq!(first, [344, 42]);
    assert_eq!(current, [87, 22, 44, 88, 103, 42]);
    assert!(current.len() <= first.len() + 5);

    // The names without a row come first: nothing is found, each step asks for six times the
    // candidates walked so far and stops where a step of the first sizes ended. Five steps
    // where they took four, over the same 480 candidates.
    let sparse_head = format!("{names}?page_size=5");
    let (first, current) = calls(&database, &sparse_head, 0).await?;
    assert_eq!(first, [32, 64, 128, 256]);
    assert_eq!(current, [8, 24, 64, 128, 256]);
    assert!(current.len() <= first.len() + 5);
    for uri in [&near_miss, &nearly_full] {
        assert_step_sizes_change_no_page(&database, uri).await?;
    }
    database.cleanup().await?;

    // Every wanted group gathered, the page's last entry not yet passed. Two registrations
    // have a second name each, the first two candidates in descending order. Their entries
    // sort at their lower names, after 58 names that compose no row, so the first step holds
    // the page and the entry after it and the walk still has to pass the lower name.
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_names(
        &database,
        71,
        |index| {
            if (4..=10).contains(&index) || index >= 69 {
                BULK_ADDRESS
            } else {
                BULK_OTHER_ADDRESS
            }
        },
        &[(10, 70), (9, 69)],
    )
    .await?;
    list_bulk_names_without_a_relation(&database, 11..69).await?;
    let unpassed = format!("{names}?dedupe=registration&order=desc&page_size=1");
    let (first, current) = calls(&database, &unpassed, 0).await?;
    // The second call of each read composes the two lower names the first step's
    // registrations reach. Both policies retain two walk steps and this closure call.
    assert_eq!(first, [32, 2, 35]);
    assert_eq!(current, [32, 2, 35]);
    assert!(current.len() - 1 <= first.len() - 1 + 5);
    let page = bulk_payload(&database, &unpassed).await?;
    assert_eq!(page["data"][0]["name"], json!("bulk00010.eth"), "{page}");
    assert_step_sizes_change_no_page(&database, &unpassed).await?;
    database.cleanup().await
}

/// A candidate on a chain with no served publication answers stale above the cap as below it,
/// whatever the step sizes: every chain is checked before the first step.
#[tokio::test]
async fn v2_address_names_walk_answers_stale_for_an_unpublished_chain() -> Result<()> {
    use bigname_storage::families::records::seams::with_legacy_walk_batches;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 40).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=5");
    let (served, paths) = with_paths(with_exact_total_cap(0, bulk_payload(&database, &uri))).await;
    assert_eq!(paths, ["walk"]);
    assert_eq!(served?["data"].as_array().unwrap().len(), 5);
    // The index lists one more name for the address, on a chain nothing has published.
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_name_index
            (address, logical_name_id, relation, chain_id)
         VALUES ($1, 'ens:unpublished', 'token_holder', 'unpublished-chain')",
    )
    .bind(BULK_ADDRESS)
    .execute(&database.pool)
    .await?;
    let exact = bulk_response(&database, &uri).await?;
    assert_eq!(exact.0, StatusCode::CONFLICT, "{}", exact.1);
    assert_eq!(exact.1["error"]["code"], json!("stale"), "{}", exact.1);
    let current = with_exact_total_cap(0, bulk_response(&database, &uri)).await?;
    let first =
        with_legacy_walk_batches(with_exact_total_cap(0, bulk_response(&database, &uri))).await?;
    assert_eq!(exact, current);
    assert_eq!(exact, first);
    database.cleanup().await
}

/// The walk checks the stored sort key of every name it composes, and of no other name. A
/// stored expiry that disagrees with the composed one sends the page to the full read when the
/// walk composes that name. The stored expiry of a name the walk does not compose is trusted:
/// here it ranks the name after the page, where its composed expiry ranks it too, so the page
/// is the full read's either way.
#[tokio::test]
async fn v2_address_names_walk_trusts_stored_keys_beyond_the_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 120).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort=expires_at&page_size=5");
    let exact = bulk_payload(&database, &uri).await?;
    // bulk00036 holds the latest expiry. 28 names expire before the stale stored value below.
    let stale = bulk_logical_name_id(36)?;
    let store = |expires_at: i64| {
        let (database, stale) = (&database, &stale);
        async move {
            let updated = sqlx::query(&format!(
                "UPDATE bigname_phase.project_name_summary SET expires_at = {expires_at}
                 WHERE logical_name_id = $1"
            ))
            .bind(stale)
            .execute(&database.pool)
            .await?;
            anyhow::ensure!(updated.rows_affected() == 1, "one stored summary");
            anyhow::Ok(())
        }
    };
    let read = |first_policy: bool| {
        let (database, uri) = (&database, &uri);
        async move {
            let pages = capped_pages(database, uri, 1, Some(0), first_policy).await?;
            anyhow::Ok(pages.into_iter().next().unwrap())
        }
    };

    // Ranked 29th by the stale value: past the 8 names this page composes, so the walk serves
    // the page. The first step sizes composed 32 names, this one among them, and fell back.
    store(1_800_000_000 + 6 * 86_400 + 1).await?;
    let current = read(false).await?;
    assert_eq!(current.paths, ["walk"]);
    assert_eq!(composed(&current), 8);
    assert_eq!(page_body(&exact), page_body(&current.payload));
    let first = read(true).await?;
    assert_eq!(first.paths, ["fallback"]);
    assert_eq!(page_body(&exact), page_body(&first.payload));

    // Ranked first by the stale value: the walk composes the name, sees the disagreement and
    // gives way to the full read, as it did with its first step sizes.
    store(1).await?;
    for first_policy in [false, true] {
        let page = read(first_policy).await?;
        assert_eq!(page.paths, ["fallback"]);
        assert_eq!(page_body(&exact), page_body(&page.payload));
        assert!(page.payload["page"]["total_count"].is_null());
    }
    database.cleanup().await
}

/// Row 12: 3,300 candidates of which only the last three compose a row. Name grouping's steps
/// grow to 1,024 names and repeat that size. Registration grouping keeps the old doubling, also
/// capped at 1,024.
#[tokio::test]
async fn v2_address_names_walk_repeats_the_largest_step_on_a_long_run_without_rows() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_names(
        &database,
        3_304,
        |index| bulk_owner_below(index, 3_301),
        &[],
    )
    .await?;
    list_bulk_names_without_a_relation(&database, 4..3_301).await?;
    for (query, steps) in [
        ("", vec![8, 24, 64, 128, 256, 512, 1024, 1024, 260]),
        (
            "&dedupe=registration",
            vec![32, 64, 128, 256, 512, 1024, 1024, 260],
        ),
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=5{query}");
        let exact = walk_all_pages(&database, &uri).await?;
        let pages =
            with_compose_chunk(4096, capped_pages(&database, &uri, 1, Some(0), false)).await?;
        assert_eq!(pages[0].paths, ["walk"], "{uri}");
        assert_eq!(page_body(&pages[0].payload), page_body(&exact[0]), "{uri}");
        assert_eq!(
            pages[0].payload["data"].as_array().unwrap().len(),
            3,
            "{uri}"
        );
        assert_eq!(pages[0].batches, steps, "{uri}");
    }
    database.cleanup().await
}

async fn with_walk_filters<F: std::future::Future>(future: F) -> (F::Output, Vec<usize>) {
    use bigname_storage::families::records::seams::with_walk_filter_names;
    let names = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = with_walk_filter_names(names.clone(), future).await;
    let names = names.lock().unwrap().clone();
    (output, names)
}

/// Row 13: one registration bound to 700 candidate names, of which `q` keeps one. When the walk
/// first reaches the registration it composes the other names a chunk at a time and filters
/// each chunk, so no filter statement reads more than one chunk of names.
#[tokio::test]
async fn v2_address_names_walk_closure_filters_each_chunk() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    let aliases: Vec<(usize, usize)> = (1..700).map(|index| (0, index)).collect();
    seed_bulk_names(&database, 700, |_| BULK_ADDRESS, &aliases).await?;
    let uri =
        format!("/v1/addresses/{BULK_ADDRESS}/names?dedupe=registration&q=bulk00350&page_size=5");
    let exact = walk_all_pages(&database, &uri).await?;
    assert_eq!(exact[0]["data"].as_array().unwrap().len(), 1);
    let ((walked, paths), filters) = with_walk_filters(with_compose_chunk(
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

/// `include=total_count` above the cap: every candidate is composed and filtered a chunk at a
/// time and the kept groups are counted, which is the summary statement's count, and the page
/// walks.
#[tokio::test]
async fn v2_address_names_total_count_above_the_cap_counts_every_candidate() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_names(
        &database,
        300,
        |index| {
            if index % 7 == 3 {
                BULK_OTHER_ADDRESS
            } else {
                BULK_ADDRESS
            }
        },
        &[(10, 200), (20, 21), (30, 31)],
    )
    .await?;
    list_bulk_names_without_a_relation(&database, 300..310).await?;
    for query in [
        "",
        "dedupe=registration",
        "relation=owner&dedupe=registration&parent=eth",
        "q=bulk00&match=contains&dedupe=registration",
        "q=zzz",
        "sort=created_at&order=desc",
        "sort=expires_at&dedupe=registration",
    ] {
        let uri =
            format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=7&include=total_count&{query}");
        let (exact, paths) = with_paths(walk_pages(&database, &uri, 2)).await;
        let exact = exact?;
        assert!(
            !paths.is_empty() && paths.iter().all(|path| *path == "exact"),
            "{uri}: {paths:?}"
        );
        let ((counted, paths), filters) = with_walk_filters(with_paths(with_exact_total_cap(
            100,
            walk_pages(&database, &uri, 2),
        )))
        .await;
        let counted = counted?;
        assert!(
            !paths.is_empty() && paths.iter().all(|path| *path == "walk"),
            "{uri}: {paths:?}"
        );
        assert_eq!(exact, counted, "{uri}");
        assert!(exact[0]["page"]["total_count"].is_u64(), "{uri}");
        assert!(
            !filters.is_empty() && filters.iter().all(|names| *names <= 256),
            "{uri}: {filters:?}"
        );
    }
    database.cleanup().await
}

/// `sort=created_at` above the cap: the walk first composes every candidate a chunk at a time
/// for the kept names' served first observations, then walks in that order. Its pages are the
/// full read's, ties among equal times included, and no statement reads more than a chunk.
#[tokio::test]
async fn v2_address_names_created_at_above_the_cap_walks_after_reading_every_candidate()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_names(&database, 300, |_| BULK_ADDRESS, &[(10, 200), (20, 21)]).await?;
    for query in [
        "sort=created_at",
        "sort=created_at&order=desc",
        "sort=created_at&dedupe=registration",
        "sort=created_at&order=desc&dedupe=registration&q=bulk00&match=contains",
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=7&{query}");
        let exact = walk_all_pages(&database, &uri).await?;
        let pages = capped_pages(&database, &uri, 1000, Some(100), false).await?;
        assert_eq!(
            exact.iter().map(page_body).collect::<Vec<_>>(),
            pages
                .iter()
                .map(|page| page_body(&page.payload))
                .collect::<Vec<_>>(),
            "{uri}"
        );
        for page in &pages {
            assert_eq!(page.paths, ["walk"], "{uri}");
            assert!(page.payload["page"]["total_count"].is_null(), "{uri}");
            assert!(page.batches.iter().all(|batch| *batch <= 256), "{uri}");
        }
        // Equal first observations come in name-id order, as the full read serves them.
        let times: Vec<&Value> = exact
            .iter()
            .flat_map(|page| page["data"].as_array().unwrap())
            .map(|row| &row["created_at"])
            .collect();
        assert!(times.windows(2).any(|pair| pair[0] == pair[1]), "{uri}");
    }
    database.cleanup().await
}

/// A served first observation that disagrees with the one the walk ordered a name by sends the
/// page to the full read once the walk composes that name.
#[tokio::test]
async fn v2_address_names_created_at_walk_falls_back_when_a_key_disagrees() -> Result<()> {
    use bigname_storage::families::records::seams::with_walk_created_at_cleared;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_names(&database, 120, |_| BULK_ADDRESS, &[]).await?;
    for query in ["sort=created_at", "sort=created_at&dedupe=registration"] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=10&{query}");
        let exact = bulk_payload(&database, &uri).await?;
        let (walked, paths) = with_paths(with_walk_created_at_cleared(
            bulk_logical_name_id(50)?,
            with_exact_total_cap(0, bulk_payload(&database, &uri)),
        ))
        .await;
        assert_eq!(paths, ["fallback"], "{uri}");
        assert_eq!(page_body(&walked?), page_body(&exact), "{uri}");
    }
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 120, &[], bulk_resolving).await?;
    for query in [
        "sort=created_at",
        "sort=created_at&dedupe=registration&coin_type=evm",
    ] {
        let uri = resolves_to_uri(&format!("page_size=10&{query}"));
        let exact = bulk_payload(&database, &uri).await?;
        let (walked, paths) = with_paths(with_walk_created_at_cleared(
            bulk_logical_name_id(50)?,
            with_exact_total_cap(0, bulk_payload(&database, &uri)),
        ))
        .await;
        assert_eq!(paths, ["fallback"], "{uri}");
        assert_eq!(page_body(&walked?), page_body(&exact), "{uri}");
    }
    database.cleanup().await
}
