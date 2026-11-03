// The resolver overview's `include=total_count`: exact up to the cap and `null` above it, on
// every page, with the page's rows, cursor and `has_more` unchanged. The cap is lowered through
// `with_bound_count_cap`. The storage plan test (`bound_page_plan_tests.rs`) bounds the rows the
// count reads at a million names.

const BOUND_COUNT_TAG: &str = "families.name.bound_count";

/// The payload of `uri` and the statements the request sent, on connections of its own.
async fn counted_resolver_page(
    database: &TestDatabase,
    uri: &str,
) -> Result<(Value, bigname_test_support::StatementCounts)> {
    let counter = bigname_test_support::StatementCounter::connect(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        counter.pool().clone(),
        bigname_lookup::ChainRpcUrls::default(),
    )
    .with_public_namespaces_for_test(["ens", "basenames"]);
    let request = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request must build");
    let (response, sent) = counter.count(app_router(state).oneshot(request)).await;
    let response = response.context("counted resolver request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    counter.close().await;
    assert_eq!(status, StatusCode::OK, "{payload}");
    Ok((payload, sent))
}

/// The parts of a bound-names page the count never changes.
fn bound_page_body(payload: &Value) -> Value {
    let bound = &payload["data"]["bound_names"];
    json!({
        "data": bound["data"],
        "next_cursor": bound["page"]["next_cursor"],
        "has_more": bound["page"]["has_more"],
    })
}

/// The paging fixture's resolver is an ENSv1 resolver, whose binding enumeration is not
/// `supported`, so its listing reads the exempt index. Its 27 names are counted exactly up to
/// a cap of 27 and are `null` above it, on every page, latest and pinned, while the pages keep
/// the rows, cursors and `has_more` of a page without the flag.
#[tokio::test]
async fn resolver_bound_count_is_null_above_the_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bound_paging(&database).await?;
    let classification = bigname_storage::families::topology::load_family_resolver_current(
        &database.pool,
        FAMILY_CHAIN,
        FAMILY_RESOLVER,
    )
    .await?
    .context("the resolver's classification")?;
    assert_ne!(
        classification.declared_summary["bindings"]["status"],
        json!("supported"),
        "{:#}",
        classification.declared_summary
    );
    let listed = paging_parity(&database, None).await?;
    let size = u64::try_from(listed.len())?;
    assert_eq!(size, 27, "{listed:?}");
    let overview = format!("/v1/resolvers/1/{FAMILY_RESOLVER}");
    let counted = format!("{overview}?include=total_count");

    // Rows 1 and 2: without the flag no count statement runs, with it one does, exact below
    // the cap.
    let (plain, sent) = counted_resolver_page(&database, &overview).await?;
    assert_eq!(plain["data"]["bound_names"]["page"]["total_count"], Value::Null, "{plain}");
    assert_eq!(sent.tagged(BOUND_COUNT_TAG), 0, "{:?}", sent.by_tag());
    let (exact, sent) = counted_resolver_page(&database, &counted).await?;
    assert_eq!(exact["data"]["bound_names"]["page"]["total_count"], json!(size), "{exact}");
    assert_eq!(sent.tagged(BOUND_COUNT_TAG), 1, "{:?}", sent.by_tag());

    // Row 3: a count equal to the cap is exact, on every page.
    crate::v2::with_bound_count_cap(size, async {
        let pages = read_family_pages_in(&database, &format!("{counted}&page_size=7"), "/data/bound_names").await?;
        assert_eq!(tl_column(&pages, "name"), listed);
        for page in &pages {
            assert_eq!(page["total_count"], json!(size), "{page:#}");
        }
        anyhow::Ok(())
    })
    .await?;

    // Rows 4, 9 and 10: one name over the cap the count is null, on a full page, on a page of
    // one, and pinned to the selected block. The rows, cursor and `has_more` are those of the
    // page without the flag.
    let (_, latest) = read_family_response(&database, &overview).await?;
    let token = latest["meta"]["as_of_token"].as_str().context("a snapshot token")?.to_owned();
    let pinned_query = format!("page_size=50&at={token}");
    crate::v2::with_bound_count_cap(size - 1, async {
        for query in ["page_size=50", "page_size=1", pinned_query.as_str()] {
            let (status, capped) =
                read_family_response(&database, &format!("{overview}?{query}&include=total_count")).await?;
            assert_eq!(status, StatusCode::OK, "{capped:#}");
            let (status, unasked) = read_family_response(&database, &format!("{overview}?{query}")).await?;
            assert_eq!(status, StatusCode::OK, "{unasked:#}");
            assert_eq!(capped["data"]["bound_names"]["page"]["total_count"], Value::Null, "{query}: {capped:#}");
            assert_eq!(bound_page_body(&capped), bound_page_body(&unasked), "{query}");
        }
        let (_, one) = read_family_response(&database, &format!("{counted}&page_size=1")).await?;
        assert_eq!(one["data"]["bound_names"]["data"].as_array().map(Vec::len), Some(1), "{one:#}");
        let pinned = read_family_pages_in(&database, &format!("{counted}&page_size=7&at={token}"), "/data/bound_names").await?;
        assert_eq!(tl_column(&pinned, "name"), listed);
        assert!(pinned.iter().all(|page| page["total_count"].is_null()), "{pinned:#?}");
        anyhow::Ok(())
    })
    .await?;

    // Row 5: far above the cap the count is null on the first page and on every page its
    // cursor reaches.
    crate::v2::with_bound_count_cap(size / 3, async {
        let pages = read_family_pages_in(&database, &format!("{counted}&page_size=7"), "/data/bound_names").await?;
        assert!(pages.len() > 1, "{pages:#?}");
        assert_eq!(tl_column(&pages, "name"), listed);
        assert!(pages.iter().all(|page| page["total_count"].is_null()), "{pages:#?}");
        anyhow::Ok(())
    })
    .await?;

    // Row 11: the listing still holds the walk's names, with the exact count on every page.
    assert_eq!(paging_parity(&database, None).await?, listed);
    database.cleanup().await
}

/// Row 6, the other index: a resolver whose binding enumeration is `supported` lists a name
/// that is not exempt from the capability gate, which only the supported listing serves. Its
/// one name is exact at a cap of one and `null` at a cap of none, the row still served.
#[tokio::test]
async fn resolver_bound_count_of_a_supported_resolver_is_null_above_the_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_ownerless_resolver_name(&database, true).await?;
    let uri = format!("/v1/resolvers/8453/{V2_RESOLVER_ADDRESS}?include=total_count");
    for (cap, total) in [(1, json!(1)), (0, Value::Null)] {
        let body = crate::v2::with_bound_count_cap(cap, v2_resolver_payload_for_database(&database, &uri)).await?;
        let rows = body["data"]["bound_names"]["data"].as_array().context("bound names")?;
        assert_eq!(names(rows), vec!["ownerless.base.eth"], "cap {cap}");
        assert_eq!(body["data"]["bound_names"]["page"]["total_count"], total, "cap {cap}: {body:#}");
    }
    database.cleanup().await
}
