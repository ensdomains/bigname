// TYR-462: `include=total_count` on a `/v1/events` read that only an emitting contract anchors,
// and the registry overview's `counts.events` on the current position, come from the stored
// event counts (`docs/projections.md`, "Event counts"): exact and unbounded. Every other read,
// and every read whose publication or privilege does not allow it, counts rows as before.
//
// A test cap of 0 tells the two apart: a row count of one row or more is then `null`, and a
// stored count is the exact number.
//
// | Row | Input class | Expected | Test |
// | --- | --- | --- | --- |
// | R1 | one emitted row | 1 with the flag, `null` without, the same rows | `stored_count_of_one_emitted_row` |
// | R2 | one emitter, several kinds, two namespaces; `namespace`, `type`, `exclude_type`, `kind` | each count is the rows one page of 200 lists, served stored | `stored_counts_follow_the_namespace_and_kind_filters` |
// | R3 | 10,004 emitted rows and two pointer rows, the production cap | exact 10,006 / 10,004, not `null`; the page unchanged | `stored_count_is_exact_above_ten_thousand` |
// | R4 | the stored statement, custom and generic plans | the key's primary key index, no sequential scan | `stored_count_reads_the_key_by_its_index` |
// | R5 | a publication between capture and count | rows counted at the captured block | `a_publication_after_capture_counts_rows_at_the_captured_block` |
// | R6 | marker not live, of another build, or orphaned, inside the read | rows counted | `an_unservable_marker_inside_the_read_counts_rows` |
// | R7 | `record_key`, `from_block`, `to_block`, `from_timestamp`, `to_timestamp`, a resolver with another `contract_address` | rows counted | `filters_the_key_does_not_hold_count_rows` |
// | R8 | registry overview at a past `at` | rows counted through the block | `registry_event_count_is_null_above_the_cap` row A3 (`v2_registry_event_count_cap.rs`) |
// | R9 | registry overview, current position, above the cap | exact, equal to the feed; other counts unchanged | `registry_event_count_is_null_above_the_cap` rows A2 and A5 |
// | R10 | an empty kind set | 0 | `stored_count_of_an_empty_kind_set_and_a_silent_contract_is_zero` |
// | R11 | an emitter, and a registry, with no events | 0 | `stored_count_of_an_empty_kind_set_and_a_silent_contract_is_zero` |
// | R12 | one read over two chains | the sum of both, each chain fenced | `stored_count_sums_and_fences_every_chain_of_the_read` |
// | R13 | every servable request below the cap | the body equals the row count's | R1, R2, R10 and R11 |
// | R14 | a role without SELECT on the table | both routes count rows, the same rows; startup WARN names the table | `a_role_without_the_grant_counts_rows_and_warns` |
const SC_CHAIN: &str = "ethereum-mainnet";
const SC_EMITTER: &str = "0x00000000000000000000000000000000000000d1";
const SC_ONE: &str = "0x00000000000000000000000000000000000000d2";
const SC_LARGE: &str = "0x00000000000000000000000000000000000000d3";
const SC_POINTER_EMITTER: &str = "0x00000000000000000000000000000000000000d4";
/// The other resolver of SC_POINTER_EMITTER's pointer changes.
const SC_SILENT: &str = "0x00000000000000000000000000000000000000d9";
/// A contract no event names.
const SC_NOBODY: &str = "0x00000000000000000000000000000000000000d8";
const SC_BASE_BLOCK_HASH: &str = "0xstoredbase1";

/// One event `emitter` emitted, with no name or registration.
fn sc_emitted(identity: &str, kind: &str, block: i64, emitter: &str, namespace: &str) -> NormalizedEvent {
    let mut event = emitted_by(v2_history_event(identity, None, None, kind, block), emitter);
    event.namespace = namespace.to_owned();
    event
}

/// The history fixture, then `events`, then the families rebuilt at the fixture's publication,
/// so the stored counts hold every row a read counts.
async fn sc_seed(database: &TestDatabase, events: &[NormalizedEvent]) -> Result<()> {
    seed_v2_history_fixture(database).await?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, events).await?;
    rebuild_fixture_families(&database.pool, SC_CHAIN, 21_000_003, "0xbinding").await
}

/// SC_EMITTER's rows: four kinds over blocks 101..=110 in `ens`, and two in `basenames`.
/// SC_POINTER_EMITTER sets the resolver to SC_EMITTER and clears it again. SC_ONE emits one row.
fn sc_small_events() -> Vec<NormalizedEvent> {
    let mut events = Vec::new();
    for (n, kind) in ["RecordChanged", "RecordChanged", "RecordChanged", "ResolverChanged",
        "PermissionChanged", "TokenControlTransferred", "RecordChanged", "ResolverChanged"]
        .into_iter()
        .enumerate()
    {
        let namespace = if n >= 6 { "basenames" } else { "ens" };
        events.push(sc_emitted(&format!("sc-emitted-{n}"), kind, 101 + n as i64, SC_EMITTER, namespace));
    }
    for (identity, block, before, after) in [
        ("sc-pointer-set", 102, SC_SILENT, SC_EMITTER),
        ("sc-pointer-clear", 109, SC_EMITTER, SC_SILENT),
    ] {
        let mut pointer = sc_emitted(identity, "ResolverChanged", block, SC_POINTER_EMITTER, "ens");
        pointer.before_state = json!({"resolver": before});
        pointer.after_state["resolver"] = json!(after);
        events.push(pointer);
    }
    events.push(sc_emitted("sc-one", "RecordChanged", 105, SC_ONE, "ens"));
    events
}

async fn sc_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    v2_history_payload_for_database(database, uri).await
}

/// The response with the count read from the rows, as before the stored counts served.
async fn sc_rows_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    crate::v2::with_row_counts_only(sc_payload(database, uri)).await
}

/// R13: a servable request's body equals the body of the same request counted from its rows.
async fn sc_assert_same_as_rows(database: &TestDatabase, uri: &str) -> Result<Value> {
    let stored = sc_payload(database, uri).await?;
    let rows = sc_rows_payload(database, uri).await?;
    assert_eq!(stored.to_string(), rows.to_string(), "{uri}");
    Ok(stored)
}

#[tokio::test]
async fn stored_count_of_one_emitted_row() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sc_seed(&database, &sc_small_events()).await?;
    let route = format!("/v1/events?contract_address={SC_ONE}");
    let plain = sc_payload(&database, &route).await?;
    assert_eq!(plain["page"]["total_count"], Value::Null);
    let counted = sc_assert_same_as_rows(&database, &format!("{route}&include=total_count")).await?;
    assert_eq!(counted["page"]["total_count"], json!(1));
    assert_eq!(counted["data"], plain["data"]);
    let stored = crate::v2::with_emitter_count_cap(0, sc_payload(&database, &format!("{route}&include=total_count"))).await?;
    assert_eq!(stored["page"]["total_count"], json!(1), "the stored count serves under any cap");
    database.cleanup().await
}

#[tokio::test]
async fn stored_counts_follow_the_namespace_and_kind_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sc_seed(&database, &sc_small_events()).await?;
    let resolver = format!("/v1/events?resolver=1:{SC_EMITTER}");
    let contract = format!("/v1/events?contract_address={SC_EMITTER}");
    let both = format!("{resolver}&contract_address={SC_EMITTER}");
    let mut failures = Vec::new();
    for (route, filters, expected) in [
        // A resolver read takes every namespace: eight emitted rows and two pointer rows.
        (&resolver, "", 10),
        (&resolver, "&namespace=ens", 8),
        (&resolver, "&type=record", 4),
        (&resolver, "&type=resolver", 4),
        (&resolver, "&exclude_type=record", 6),
        (&resolver, "&kind=ResolverChanged", 4),
        (&resolver, "&kind=PermissionChanged", 1),
        // A contract read defaults to ens.
        (&contract, "", 6),
        (&contract, "&type=record", 3),
        (&contract, "&exclude_type=record", 3),
        (&contract, "&kind=TokenControlTransferred", 1),
        (&both, "", 8),
        (&both, "&namespace=ens&type=resolver", 1),
    ] {
        let uri = format!("{route}{filters}&page_size=200&include=total_count");
        let page = sc_assert_same_as_rows(&database, &uri).await?;
        let listed = page_rows(&page).len();
        let stored = crate::v2::with_emitter_count_cap(0, sc_payload(&database, &uri)).await?;
        if listed != expected
            || page["page"]["total_count"] != json!(expected)
            || stored["page"]["total_count"] != json!(expected)
            || page["page"]["has_more"] != json!(false)
        {
            failures.push(format!("{uri}: listed {listed}, counted {}, stored {}, expected {expected}",
                page["page"]["total_count"], stored["page"]["total_count"]));
        }
    }
    database.cleanup().await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn stored_count_is_exact_above_ten_thousand() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut events = (0..LARGE_EMITTED)
        .map(|n| sc_emitted(&format!("sc-large-{n}"), "RecordChanged", 101 + (n % 10) as i64, SC_LARGE, "ens"))
        .collect::<Vec<_>>();
    events.extend(pointer_pair("sc-large-pointer", SC_LARGE, 102, 111));
    sc_seed(&database, &events).await?;
    let resolver = format!("/v1/events?resolver=1:{SC_LARGE}&page_size=3");
    let contract = format!("/v1/events?contract_address={SC_LARGE}&page_size=3");
    let both = format!("{resolver}&contract_address={SC_LARGE}");
    for (route, expected) in [
        (&resolver, LARGE_TOTAL),
        (&contract, LARGE_EMITTED as u64),
        (&both, LARGE_EMITTED as u64),
    ] {
        let plain = sc_payload(&database, route).await?;
        let counted = sc_payload(&database, &format!("{route}&include=total_count")).await?;
        assert_eq!(counted["page"]["total_count"], json!(expected), "{route}");
        assert_eq!(counted["data"], plain["data"], "{route}");
        assert_eq!(counted["page"]["next_cursor"], plain["page"]["next_cursor"], "{route}");
        assert_eq!(counted["page"]["has_more"], json!(true), "{route}");
        let cursor = counted["page"]["next_cursor"].as_str().context("a next page")?.to_owned();
        let next = sc_payload(&database, &format!("{route}&include=total_count&cursor={cursor}")).await?;
        assert_eq!(next["page"]["total_count"], json!(expected), "{route} continued");
        let rows = sc_rows_payload(&database, &format!("{route}&include=total_count")).await?;
        assert_eq!(rows["page"]["total_count"], Value::Null, "{route} counted from rows");
        assert_eq!(rows["data"], counted["data"], "{route}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn stored_count_reads_the_key_by_its_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // 120,000 keys of other anchors, and the anchor's own twenty.
    sqlx::query(r#"
        INSERT INTO bigname_phase.project_event_count
            (chain_id, anchor_kind, anchor, namespace, event_kind, event_count)
        SELECT 'ethereum-mainnet', kind, '0x' || lpad(to_hex(n), 40, '0'), 'ens', event_kind, 1 + n % 7
        FROM generate_series(1, 20000) n
        CROSS JOIN (VALUES (0::smallint), (1::smallint), (2::smallint)) kinds(kind)
        CROSS JOIN (VALUES ('RecordChanged'), ('ResolverChanged')) event_kinds(event_kind)
        UNION ALL
        SELECT 'ethereum-mainnet', kind, $1, namespace, event_kind, 3
        FROM (VALUES (0::smallint), (1::smallint)) kinds(kind)
        CROSS JOIN (VALUES ('ens'), ('basenames')) namespaces(namespace)
        CROSS JOIN (VALUES ('RecordChanged'), ('ResolverChanged'), ('PermissionChanged'),
                           ('TokenControlTransferred'), ('ExpiryChanged')) event_kinds(event_kind)
    "#)
    .bind(SC_EMITTER)
    .execute(&database.pool)
    .await?;
    sqlx::raw_sql("ANALYZE bigname_phase.project_event_count").execute(&database.pool).await?;
    let window = bigname_storage::HistoryBlockWindow {
        ranges: vec![bigname_storage::ChainBlockRange {
            chain_id: SC_CHAIN.to_owned(),
            from_block: None,
            to_block: Some(111),
        }],
    };
    let base = bigname_storage::EventHistoryFilter {
        event_kinds: crate::v2::product_history_event_kinds(),
        block_window: Some(window),
        publication_block_bounds: Some(BTreeMap::from([(SC_CHAIN.to_owned(), 111_i64)])),
        ..bigname_storage::EventHistoryFilter::default()
    };
    fn nodes<'a>(node: &'a Value, found: &mut Vec<&'a Value>) {
        found.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            nodes(child, found);
        }
    }
    let mut failures = Vec::new();
    for (label, filter) in [
        ("contract_address", bigname_storage::EventHistoryFilter {
            contract_address: Some(SC_EMITTER.to_owned()),
            namespace: Some("ens".to_owned()),
            ..base.clone()
        }),
        ("resolver", bigname_storage::EventHistoryFilter {
            resolver: Some(bigname_storage::EventHistoryResolverFilter {
                chain_id: SC_CHAIN.to_owned(),
                address: SC_EMITTER.to_owned(),
            }),
            ..base.clone()
        }),
    ] {
        for generic_plan in [false, true] {
            let label = format!("{label} generic_plan={generic_plan}");
            let plan = bigname_storage::explain_stored_event_count_for_test(
                &database.pool, filter.clone(), generic_plan).await?
                .with_context(|| format!("{label}: the read is a stored count's key"))?;
            let root = &plan[0]["Plan"];
            eprintln!("{label}: plan {root}");
            let mut found = Vec::new();
            nodes(root, &mut found);
            let scans = found
                .iter()
                .filter(|node| node["Relation Name"] == "project_event_count"
                    || node["Index Name"] == "project_event_count_pkey")
                .collect::<Vec<_>>();
            if scans.is_empty()
                || scans.iter().any(|scan| scan["Node Type"] == "Seq Scan"
                    || scan["Index Name"] != "project_event_count_pkey"
                        && scan["Node Type"] != "Bitmap Heap Scan")
            {
                failures.push(format!("{label}: not read by the key's index: {scans:?}"));
            }
            if found.iter().any(|node| node["Relation Name"] == "normalized_events") {
                failures.push(format!("{label}: the stored count reads normalized_events"));
            }
            let read: f64 = scans
                .iter()
                .filter(|scan| scan["Node Type"] != "Bitmap Index Scan")
                .map(|scan| scan["Actual Rows"].as_f64().unwrap_or(0.0) * scan["Actual Loops"].as_f64().unwrap_or(1.0))
                .sum();
            if read > 40.0 {
                failures.push(format!("{label}: read {read} key rows, more than the anchor's"));
            }
        }
    }
    // A record key is not a column of the key: no stored statement for it.
    let keyed = bigname_storage::EventHistoryFilter {
        contract_address: Some(SC_EMITTER.to_owned()),
        record_key: Some("addr:60".to_owned()),
        ..base.clone()
    };
    if bigname_storage::explain_stored_event_count_for_test(&database.pool, keyed, false).await?.is_some() {
        failures.push("a record key read has a stored count".to_owned());
    }
    database.cleanup().await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn a_publication_after_capture_counts_rows_at_the_captured_block() -> Result<()> {
    use bigname_storage::history_anchor_read_test_hooks::{HistoryReadHookPoint, install};
    let database = TestDatabase::new_migrated().await?;
    g_seed_records(&database).await?;
    seed_v2_history_blocks(&database, 146..=146).await?;
    let node = bigname_lookup::ens_namehash_hex(G_NAME)?;
    let above = event_data_event("g-above-publication", None, None, "RecordVersionChanged",
        "ens_v1_resolver_l1", 146, "0xtx146", 0, G_RESOLVER, version_after(&node, G_RESOLVER, 3));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[above]).await?;
    let uri = format!("/v1/events?contract_address={G_RESOLVER}&include=total_count&page_size=200");
    let bounded = hk_ok(&database, &uri).await?;
    assert_eq!(bounded["page"]["total_count"], json!(8));
    assert_eq!(hk_ids(&bounded).len(), 8);
    let (guard, control) = install(&database.pool, HistoryReadHookPoint::AfterAnchors).await?;
    let app = app_router(database.app_state());
    let request_uri = uri.clone();
    let read = tokio::spawn(async move {
        app.oneshot(Request::builder().uri(request_uri).body(Body::empty()).unwrap()).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), control.wait_until_reached())
        .await
        .context("the read did not reach the hook")?;
    // The read captured block 145. Block 146 is published before its page transaction begins,
    // so the stored counts now describe 146 and the read counts its rows through 145.
    seed_schema_v2_ens_lookup_head(&database.pool, 146, "0xhistory146", "2023-11-14T22:15:46Z").await?;
    rebuild_fixture_families(&database.pool, EVENT_DATA_CHAIN, 146, "0xhistory146").await?;
    control.resume().await;
    let response = read.await??;
    assert_eq!(response.status(), StatusCode::OK);
    let during: Value = read_json(response).await?;
    assert_eq!(during["page"]["total_count"], json!(8), "{during}");
    assert_eq!(hk_ids(&during), hk_ids(&bounded));
    drop(guard);
    let later = hk_ok(&database, &uri).await?;
    assert_eq!(later["page"]["total_count"], json!(9));
    assert_eq!(hk_ids(&later).len(), 9);
    database.cleanup().await
}

#[tokio::test]
async fn an_unservable_marker_inside_the_read_counts_rows() -> Result<()> {
    use bigname_storage::history_anchor_read_test_hooks::{HistoryReadHookPoint, install};
    let database = TestDatabase::new_migrated().await?;
    sc_seed(&database, &sc_small_events()).await?;
    let (lineage_state,): (String,) = sqlx::query_as(
        "SELECT lineage.canonicality_state::text FROM bigname_phase.project_family_marker marker
         JOIN bigname_phase.chain_lineage lineage ON lineage.chain_id = marker.chain_id
          AND lineage.block_hash = marker.current_block_hash
         WHERE marker.chain_id = $1",
    )
    .bind(SC_CHAIN)
    .fetch_one(&database.pool)
    .await?;
    let orphan = "UPDATE bigname_phase.chain_lineage lineage SET canonicality_state = 'orphaned'
        FROM bigname_phase.project_family_marker marker
        WHERE marker.chain_id = $1 AND lineage.chain_id = marker.chain_id
          AND lineage.block_hash = marker.current_block_hash".to_owned();
    let unorphan = format!("UPDATE bigname_phase.chain_lineage lineage
        SET canonicality_state = '{lineage_state}'
        FROM bigname_phase.project_family_marker marker
        WHERE marker.chain_id = $1 AND lineage.chain_id = marker.chain_id
          AND lineage.block_hash = marker.current_block_hash");
    let restore_hash = format!("UPDATE bigname_phase.project_family_marker SET input_content_hash = '{}'
        WHERE chain_id = $1", bigname_content_hash::PROJECTION_CONTENT_HASH);
    let cases = [
        ("not live", "UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending' WHERE chain_id = $1".to_owned(),
            "UPDATE bigname_phase.project_family_marker SET state = 'live' WHERE chain_id = $1".to_owned()),
        ("another build", "UPDATE bigname_phase.project_family_marker SET input_content_hash = 'another-build' WHERE chain_id = $1".to_owned(),
            restore_hash),
        ("orphaned", orphan, unorphan),
    ];
    for route in [
        format!("/v1/events?contract_address={SC_EMITTER}&include=total_count&page_size=200"),
        format!("/v1/events?resolver=1:{SC_EMITTER}&include=total_count&page_size=200"),
    ] {
        let served = crate::v2::with_emitter_count_cap(0, sc_payload(&database, &route)).await?;
        assert!(served["page"]["total_count"].as_u64().is_some_and(|count| count > 0), "{route}: {served}");
        for (label, break_marker, restore_marker) in &cases {
            let (anchors_guard, anchors) = install(&database.pool, HistoryReadHookPoint::AfterAnchors).await?;
            let (page_guard, page) = install(&database.pool, HistoryReadHookPoint::AfterPage).await?;
            let app = app_router(database.app_state_with_public_namespaces(&["ens"]));
            let request_uri = route.clone();
            let read = tokio::spawn(crate::v2::with_emitter_count_cap(0, async move {
                app.oneshot(Request::builder().uri(request_uri).body(Body::empty()).unwrap()).await
            }));
            tokio::time::timeout(std::time::Duration::from_secs(10), anchors.wait_until_reached())
                .await
                .context("the read did not reach the anchors hook")?;
            sqlx::query(break_marker).bind(SC_CHAIN).execute(&database.pool).await?;
            anchors.resume().await;
            tokio::time::timeout(std::time::Duration::from_secs(10), page.wait_until_reached())
                .await
                .context("the read did not reach the page hook")?;
            sqlx::query(restore_marker).bind(SC_CHAIN).execute(&database.pool).await?;
            page.resume().await;
            let response = read.await??;
            let status = response.status();
            let body: Value = read_json(response).await?;
            assert_eq!(status, StatusCode::OK, "{label} {route}: {body}");
            assert_eq!(body["page"]["total_count"], Value::Null, "{label} {route}: counted from rows");
            assert_eq!(body["data"], served["data"], "{label} {route}");
            drop((anchors_guard, page_guard));
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn filters_the_key_does_not_hold_count_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sc_seed(&database, &sc_small_events()).await?;
    let resolver = format!("/v1/events?resolver=1:{SC_EMITTER}");
    let contract = format!("/v1/events?contract_address={SC_EMITTER}");
    let mut failures = Vec::new();
    for (route, filters, listed) in [
        (&resolver, "&record_key=addr:60", 4),
        (&contract, "&record_key=addr:60", 3),
        (&resolver, "&from_block=103", 7),
        (&contract, "&to_block=104", 4),
        (&resolver, "&from_timestamp=1700000103", 7),
        (&contract, "&to_timestamp=1700000104", 4),
        // The resolver's pointer rows that SC_POINTER_EMITTER emitted.
        (&resolver, format!("&contract_address={SC_POINTER_EMITTER}").as_str(), 2),
    ] {
        let uri = format!("{route}{filters}&page_size=200&include=total_count");
        let exact = sc_payload(&database, &uri).await?;
        let capped = crate::v2::with_emitter_count_cap(0, sc_payload(&database, &uri)).await?;
        let rows = page_rows(&exact).len();
        if rows != listed
            || exact["page"]["total_count"] != json!(listed)
            || capped["page"]["total_count"] != Value::Null
            || capped["data"] != exact["data"]
        {
            failures.push(format!("{uri}: listed {rows}, counted {}, under cap 0 {}",
                exact["page"]["total_count"], capped["page"]["total_count"]));
        }
    }
    database.cleanup().await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn stored_count_of_an_empty_kind_set_and_a_silent_contract_is_zero() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sc_seed(&database, &sc_small_events()).await?;
    for uri in [
        format!("/v1/events?contract_address={SC_EMITTER}&type=record&exclude_type=record&include=total_count"),
        format!("/v1/events?resolver=1:{SC_EMITTER}&type=record&exclude_type=record&include=total_count"),
        format!("/v1/events?contract_address={SC_NOBODY}&include=total_count"),
        format!("/v1/events?resolver=1:{SC_NOBODY}&include=total_count"),
    ] {
        let page = sc_assert_same_as_rows(&database, &uri).await?;
        assert_eq!(page["page"]["total_count"], json!(0), "{uri}");
        assert_eq!(page_rows(&page).len(), 0, "{uri}");
    }
    database.cleanup().await?;

    // A registry with no events of its own.
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    seed_declared_registry(&database).await?;
    let uri = format!("/v1/registries/1/{DECLARED_REGISTRY}?include=counts");
    let stored = crate::v2::with_emitter_count_cap(0, registry_payload(&database, &uri)).await?;
    let rows = crate::v2::with_row_counts_only(registry_payload(&database, &uri)).await?;
    assert_eq!(stored["data"]["counts"]["events"], json!(0), "{stored}");
    assert_eq!(stored.to_string(), rows.to_string());
    database.cleanup().await
}

#[tokio::test]
async fn stored_count_sums_and_fences_every_chain_of_the_read() -> Result<()> {
    const BASE: &str = "base-mainnet";
    let database = TestDatabase::new_migrated().await?;
    let mut events = sc_small_events();
    upsert_phase_raw_blocks(&database.pool, &[raw_block(BASE, SC_BASE_BLOCK_HASH, None, 1, 1_700_000_001)]).await?;
    for n in 0..3 {
        let mut event = sc_emitted(&format!("sc-base-{n}"), "RecordChanged", 1, SC_EMITTER, "ens");
        event.chain_id = Some(BASE.to_owned());
        event.block_hash = Some(SC_BASE_BLOCK_HASH.to_owned());
        event.transaction_hash = Some(format!("0xbasetx{n}"));
        events.push(event);
    }
    sc_seed(&database, &events).await?;
    rebuild_fixture_families(&database.pool, BASE, 1, SC_BASE_BLOCK_HASH).await?;
    let filter = bigname_storage::EventHistoryFilter {
        contract_address: Some(SC_EMITTER.to_owned()),
        namespace: Some("ens".to_owned()),
        event_kinds: crate::v2::product_history_event_kinds(),
        block_window: Some(bigname_storage::HistoryBlockWindow {
            ranges: vec![
                bigname_storage::ChainBlockRange { chain_id: BASE.to_owned(), from_block: None, to_block: Some(1) },
                bigname_storage::ChainBlockRange { chain_id: SC_CHAIN.to_owned(), from_block: None, to_block: Some(21_000_003) },
            ],
        }),
        publication_block_bounds: Some(BTreeMap::from([
            (BASE.to_owned(), 1_i64),
            (SC_CHAIN.to_owned(), 21_000_003_i64),
        ])),
        ..bigname_storage::EventHistoryFilter::default()
    };
    let count = |mode| {
        let filter = filter.clone();
        let pool = database.pool.clone();
        async move {
            let page = bigname_storage::load_event_history_page(&pool, filter, true, None, 200, mode, false).await?;
            let summary = page.summary.context("a counted page")?;
            anyhow::Ok((page.rows.len(), summary.total_count, summary.count_cap))
        }
    };
    // Six ens rows on Mainnet and three on Base.
    assert_eq!(count(bigname_storage::HistorySummaryMode::CappedCount(100)).await?, (9, 9, Some(100)));
    assert_eq!(count(bigname_storage::HistorySummaryMode::StoredOrCapped(0)).await?, (9, 9, None));
    // One chain whose marker is not servable: the read counts its rows on both.
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending' WHERE chain_id = $1")
        .bind(BASE)
        .execute(&database.pool)
        .await?;
    assert_eq!(count(bigname_storage::HistorySummaryMode::StoredOrCapped(0)).await?, (9, 1, Some(0)));
    assert_eq!(count(bigname_storage::HistorySummaryMode::StoredOrCapped(100)).await?, (9, 9, Some(100)));
    database.cleanup().await
}

#[derive(Clone, Default)]
struct ScLogs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for ScLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ScLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn sc_logged<F: std::future::Future>(work: F) -> (F::Output, String) {
    use tracing::instrument::WithSubscriber;
    let logs = ScLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let output = work.with_subscriber(subscriber).await;
    let text = String::from_utf8_lossy(&logs.0.lock().expect("log buffer")).into_owned();
    (output, text)
}

/// A pool whose sessions run as `role`.
async fn sc_role_pool(database: &TestDatabase, role: &str) -> Result<PgPool> {
    let config = database.database_config(2)?;
    let options = PgConnectOptions::from_str(config.database_url.as_deref().context("test URL")?)?
        .options([("search_path", "bigname_phase".to_owned())]);
    let set_role = format!("SET ROLE {role}");
    Ok(PgPoolOptions::new()
        .max_connections(config.max_connections)
        .after_connect(move |connection, _metadata| {
            let set_role = set_role.clone();
            Box::pin(async move {
                sqlx::query(&set_role).execute(&mut *connection).await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?)
}

async fn sc_get(pool: &PgPool, uri: &str) -> Result<Value> {
    let state = AppState::new_with_rpc_urls(pool.clone(), bigname_lookup::ChainRpcUrls::default())
        .with_public_namespaces_for_test(["ens", "basenames"]);
    let response = app_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    ensure!(status == StatusCode::OK, "{uri}: {status} {body}");
    Ok(body)
}

#[tokio::test]
async fn a_role_without_the_grant_counts_rows_and_warns() -> Result<()> {
    const WARNING: &str = "the API role cannot read the stored event counts";
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let role = format!("sc_stored_counts_{}_{}", std::process::id(), NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed));
    let deployment = include_str!("../../../../docs/deployment.md")
        .split_once("## Surviving services")
        .context("deployment docs must contain the API service grants")?
        .1;
    let grants = deployment
        .split_once("GRANT SELECT ON TABLE\n")
        .context("deployment docs must contain the API SELECT grant")?
        .1
        .split_once("TO bigname_api;")
        .context("deployment docs must terminate the API SELECT grant")?
        .0;
    ensure!(grants.contains("bigname_phase.project_event_count"), "the documented grant omits the stored event counts");
    let guard = deployment
        .split_once("GRANT EXECUTE ON FUNCTION ")
        .context("deployment docs must contain the API guard grant")?
        .1
        .split_once("TO bigname_api;")
        .context("deployment docs must terminate the API guard grant")?
        .0;
    for statement in [
        format!("CREATE ROLE {role} NOLOGIN"),
        format!("GRANT USAGE ON SCHEMA bigname_phase TO {role}"),
        format!("GRANT SELECT ON TABLE {grants} TO {role}"),
        format!("GRANT EXECUTE ON FUNCTION {guard} TO {role}"),
    ] {
        sqlx::query(&statement).execute(&database.lookup_pool).await?;
    }
    let pool = sc_role_pool(&database, &role).await?;
    let overview = format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts");
    let routes = [
        format!("/v1/events?contract_address={ALPHA_REGISTRY}&include=total_count&page_size=200"),
        format!("/v1/events?resolver=1:{ALPHA_REGISTRY}&include=total_count&page_size=200"),
    ];

    // With the documented grant: stored counts, served under any cap, and no WARN.
    let (readable, logs) = sc_logged(crate::startup_preflight::warn_on_unreadable_stored_event_counts(&pool)).await;
    assert!(readable, "{logs}");
    assert!(!logs.contains(WARNING), "{logs}");
    let granted = crate::v2::with_emitter_count_cap(0, async {
        let mut bodies = vec![sc_get(&pool, &overview).await?];
        for route in &routes {
            bodies.push(sc_get(&pool, route).await?);
        }
        anyhow::Ok(bodies)
    })
    .await?;
    assert_eq!(granted[0]["data"]["counts"]["events"], json!(6), "{}", granted[0]);
    for (route, body) in routes.iter().zip(&granted[1..]) {
        assert!(body["page"]["total_count"].as_u64().is_some_and(|count| count > 0), "{route}: {body}");
    }

    // Without SELECT on the table: the same rows, counted from the rows, and a WARN naming it.
    sqlx::query(&format!("REVOKE SELECT ON bigname_phase.project_event_count FROM {role}"))
        .execute(&database.lookup_pool)
        .await?;
    pool.close().await;
    let pool = sc_role_pool(&database, &role).await?;
    let (readable, logs) = sc_logged(crate::startup_preflight::warn_on_unreadable_stored_event_counts(&pool)).await;
    assert!(!readable, "{logs}");
    let warned = logs
        .lines()
        .find(|line| line.contains("WARN") && line.contains(WARNING))
        .with_context(|| format!("no WARN for the stored event counts: {logs}"))?;
    assert!(warned.contains("bigname_phase.project_event_count"), "{warned}");
    let revoked = crate::v2::with_emitter_count_cap(0, async {
        let mut bodies = vec![sc_get(&pool, &overview).await?];
        for route in &routes {
            bodies.push(sc_get(&pool, route).await?);
        }
        anyhow::Ok(bodies)
    })
    .await?;
    assert_eq!(revoked[0]["data"]["counts"]["events"], Value::Null, "{}", revoked[0]);
    assert_eq!(revoked[0]["data"]["referenced_by"], granted[0]["data"]["referenced_by"]);
    assert_eq!(revoked[0]["data"]["counts"]["roles"], granted[0]["data"]["counts"]["roles"]);
    for ((route, body), stored) in routes.iter().zip(&revoked[1..]).zip(&granted[1..]) {
        assert_eq!(body["page"]["total_count"], Value::Null, "{route}: {body}");
        assert_eq!(body["data"], stored["data"], "{route}");
    }
    // Below the cap the row count equals the stored one.
    let overview_rows = sc_get(&pool, &overview).await?;
    assert_eq!(overview_rows["data"]["counts"]["events"], json!(6));
    for (route, stored) in routes.iter().zip(&granted[1..]) {
        let body = sc_get(&pool, route).await?;
        assert_eq!(body["page"]["total_count"], stored["page"]["total_count"], "{route}");
    }
    pool.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&database.lookup_pool)
        .await?;
    database.cleanup().await
}
