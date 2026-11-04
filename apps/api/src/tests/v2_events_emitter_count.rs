// TYR-452: `include=total_count` on a `/v1/events` read anchored only by an emitting contract
// (`resolver` or `contract_address`) is exact up to the opt-in cap and `null` above it, and its
// count statement stops reading at the cap. Other anchors keep the exact, uncapped opt-in.
const EMITTER_SMALL: &str = "0x00000000000000000000000000000000000000e1";
const EMITTER_LARGE: &str = "0x00000000000000000000000000000000000000e2";
const EMITTER_OTHER: &str = "0x00000000000000000000000000000000000000e9";
/// Rows the large emitter emitted, above the production opt-in cap of 10,000.
const LARGE_EMITTED: usize = 10_004;
/// Two pointer changes name the large emitter: one sets it, one clears it.
const LARGE_TOTAL: u64 = LARGE_EMITTED as u64 + 2;
/// A test opt-in cap below the large emitter's rows and above the production cap of 10,000.
const OVERRIDE_CAP: u64 = 10_002;

fn emitted_by(mut event: NormalizedEvent, emitter: &str) -> NormalizedEvent {
    event.raw_fact_ref["emitting_address"] = json!(emitter);
    event
}

/// `ResolverChanged` rows that set the resolver to `emitter` and clear it again.
fn pointer_pair(prefix: &str, emitter: &str, set_block: i64, clear_block: i64) -> Vec<NormalizedEvent> {
    let mut set = v2_history_event(&format!("{prefix}-set"), Some("ens:history.eth"), None, "ResolverChanged", set_block);
    set.after_state["resolver"] = json!(emitter);
    set.before_state = json!({"resolver": EMITTER_OTHER});
    let mut clear = v2_history_event(&format!("{prefix}-clear"), Some("ens:history.eth"), None, "ResolverChanged", clear_block);
    clear.after_state["resolver"] = json!(EMITTER_OTHER);
    clear.before_state = json!({"resolver": emitter});
    vec![set, clear]
}

fn page_rows(payload: &Value) -> &Vec<Value> {
    payload["data"].as_array().expect("events data")
}

#[tokio::test]
async fn events_emitter_total_count_is_exact_below_the_opt_in_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    // Five record writes emitted by the small emitter on blocks 105..=109, and a pointer set
    // on 102 and cleared on 110.
    let mut events = (105..=109)
        .map(|block| emitted_by(v2_history_event(&format!("small-emitted-{block}"),
            Some("ens:history.eth"), None, "RecordChanged", block), EMITTER_SMALL))
        .collect::<Vec<_>>();
    events.extend(pointer_pair("small-pointer", EMITTER_SMALL, 102, 110));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    let resolver = format!("/v1/events?resolver=1:{EMITTER_SMALL}");
    let contract = format!("/v1/events?contract_address={EMITTER_SMALL}");

    // Row 1: the default does not count, and the opt-in is exact below every cap.
    let default = v2_history_payload_for_database(&database, &resolver).await?;
    assert_eq!(default["page"]["total_count"], Value::Null);
    let counted = v2_history_payload_for_database(&database, &format!("{resolver}&include=total_count")).await?;
    assert_eq!(counted["page"]["total_count"], json!(7));
    assert_eq!(page_rows(&counted), page_rows(&default));

    // Row 4 below the cap: a contract read is exact with the flag and null without it.
    let contract_default = v2_history_payload_for_database(&database, &contract).await?;
    assert_eq!(contract_default["page"]["total_count"], Value::Null);
    let contract_counted =
        v2_history_payload_for_database(&database, &format!("{contract}&include=total_count")).await?;
    assert_eq!(contract_counted["page"]["total_count"], json!(5));
    let both = v2_history_payload_for_database(&database,
        &format!("{resolver}&contract_address={EMITTER_SMALL}&include=total_count")).await?;
    assert_eq!(both["page"]["total_count"], json!(5));

    // Row 10 and row 15: each filter's count equals the rows one page of 200 enumerates.
    for (filters, expected) in [
        ("&type=record", 5),
        ("&type=resolver", 2),
        ("&record_key=addr:60", 5),
        ("&from_block=106&to_block=108", 3),
        ("&from_timestamp=1700000107&to_timestamp=1700000110", 4),
        ("&kind=ResolverChanged&from_block=103", 1),
    ] {
        let route = format!("{resolver}{filters}&page_size=200&include=total_count");
        let page = v2_history_payload_for_database(&database, &route).await?;
        assert_eq!(page_rows(&page).len(), expected, "{route}");
        assert_eq!(page["page"]["total_count"], json!(expected), "{route}");
        assert_eq!(page["page"]["has_more"], json!(false), "{route}");
    }

    // Rows 11 and 12: payload flags never change the count.
    let lean = page_rows(&counted);
    assert!(lean.iter().all(|row| row.get("data").is_none() && row.get("kind").is_none()));
    let detailed = v2_history_payload_for_database(&database,
        &format!("{resolver}&include=data,raw,total_count")).await?;
    assert_eq!(detailed["page"]["total_count"], json!(7));
    assert!(page_rows(&detailed).iter().all(|row| row["kind"].is_string()));
    assert!(page_rows(&detailed).iter().any(|row| row["contract_address"] == json!(EMITTER_SMALL)));

    // Rows 13 and 14: the count is the collection's, on every page and in both orders.
    let first = v2_history_payload_for_database(&database,
        &format!("{resolver}&page_size=2&include=total_count")).await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("first page cursor").to_owned();
    let second = v2_history_payload_for_database(&database,
        &format!("{resolver}&page_size=2&include=total_count&cursor={cursor}")).await?;
    assert_eq!(second["page"]["total_count"], json!(7));
    let ascending = v2_history_payload_for_database(&database,
        &format!("{resolver}&order=asc&include=total_count")).await?;
    assert_eq!(ascending["page"]["total_count"], json!(7));
    let mut descending = history_blocks(&counted);
    descending.reverse();
    assert_eq!(history_blocks(&ascending), descending);

    // Row 9: an unanchored read never counts.
    let unanchored = v2_history_payload_for_database(&database,
        "/v1/events?namespace=ens&include=total_count").await?;
    assert_eq!(unanchored["page"]["total_count"], Value::Null);
    database.cleanup().await
}

#[tokio::test]
async fn events_emitter_total_count_is_null_above_the_opt_in_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let mut events = (0..LARGE_EMITTED)
        .map(|n| emitted_by(v2_history_event(&format!("large-emitted-{n}"),
            Some("ens:history.eth"), None, "RecordChanged", 101 + (n % 10) as i64), EMITTER_LARGE))
        .collect::<Vec<_>>();
    events.extend(pointer_pair("large-pointer", EMITTER_LARGE, 102, 111));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    let resolver = format!("/v1/events?resolver=1:{EMITTER_LARGE}&page_size=3");
    let contract = format!("/v1/events?contract_address={EMITTER_LARGE}&page_size=3");
    let both = format!("{resolver}&contract_address={EMITTER_LARGE}");

    // Row 2: below the opt-in cap, the flag gives the exact count. The default does not count.
    let default = v2_history_payload_for_database(&database, &resolver).await?;
    assert_eq!(default["page"]["total_count"], Value::Null);
    // The opt-in cap is raised above its production value of 10,000 for this row.
    crate::v2::with_emitter_count_cap(LARGE_TOTAL, async {
        for (route, expected) in [
            (resolver.clone(), LARGE_TOTAL),
            (contract.clone(), LARGE_EMITTED as u64),
            (both.clone(), LARGE_EMITTED as u64),
        ] {
            let page = v2_history_payload_for_database(&database, &format!("{route}&include=total_count")).await?;
            assert_eq!(page["page"]["total_count"], json!(expected), "{route}");
        }
        anyhow::Ok(())
    })
    .await?;

    crate::v2::with_emitter_count_cap(OVERRIDE_CAP, async {
        // Rows 3, 4 and 5: above the opt-in cap the count is null, and the page is the
        // default page.
        let counted = v2_history_payload_for_database(&database, &format!("{resolver}&include=total_count")).await?;
        assert_eq!(counted["page"]["total_count"], Value::Null);
        assert_eq!(counted["data"], default["data"]);
        assert_eq!(counted["page"]["next_cursor"], default["page"]["next_cursor"]);
        assert_eq!(counted["page"]["has_more"], json!(true));
        for route in [&contract, &both] {
            let page = v2_history_payload_for_database(&database, &format!("{route}&include=total_count")).await?;
            assert_eq!(page["page"]["total_count"], Value::Null, "{route}");
            assert_eq!(page_rows(&page).len(), 3, "{route}");
        }

        // Rows 13 and 14: null on a continuation page and in ascending order.
        let cursor = counted["page"]["next_cursor"].as_str().expect("first page cursor").to_owned();
        let second = v2_history_payload_for_database(&database,
            &format!("{resolver}&include=total_count&cursor={cursor}")).await?;
        assert_eq!(second["page"]["total_count"], Value::Null);
        assert_eq!(page_rows(&second).len(), 3);
        let ascending = v2_history_payload_for_database(&database,
            &format!("{resolver}&order=asc&include=total_count")).await?;
        assert_eq!(ascending["page"]["total_count"], Value::Null);
        assert_eq!(history_blocks(&ascending)[0], 101);

        // Row 6: a name anchor keeps the exact, uncapped opt-in above the override cap.
        let name = v2_history_payload_for_database(&database,
            "/v1/events?name=history.eth&type=record&page_size=1&include=total_count").await?;
        assert!(name["page"]["total_count"].as_u64().expect("exact name count") > OVERRIDE_CAP);
        anyhow::Ok(())
    })
    .await?;
    database.cleanup().await
}

/// The opt-in count of an emitter-anchored read reads the arms in index order under a `Limit`
/// and stops after `cap + 1` rows. An uncapped count would read every row the emitter has.
#[tokio::test]
async fn events_emitter_opt_in_count_plan_stops_at_the_cap() -> Result<()> {
    const PLAN_CAP: u64 = 200;
    const ARM_INDEXES: [&str; 3] = [
        "normalized_events_emitter_history_idx",
        "normalized_events_pointer_after_resolver_history_idx",
        "normalized_events_pointer_before_resolver_history_idx",
    ];
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 101..=111).await?;
    // The emitter has 1,200 emitted rows, 600 pointer sets and 600 pointer clears. Other
    // contracts emitted 30,000 rows and changed 6,000 pointers, so each arm reads a small part
    // of the table.
    sqlx::query(r#"
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family, manifest_version, chain_id,
             block_hash, block_number, transaction_hash, transaction_index, log_index,
             derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
        SELECT 'emitter-plan:' || arm || ':' || n, 'ens',
               CASE WHEN arm = 'emitted' OR arm = 'noise' THEN 'RecordChanged' ELSE 'ResolverChanged' END,
               CASE WHEN arm = 'emitted' OR arm = 'noise' THEN 'ens_v1_resolver_l1' ELSE 'ens_v1_registry_l1' END,
               1, 'ethereum-mainnet', '0xhistory' || (101 + n % 11), 101 + n % 11,
               'tx-emitter-plan-' || arm || '-' || n, 0, 0, 'ens_v1_unwrapped_authority',
               'canonical'::canonicality_state,
               CASE arm WHEN 'set' THEN jsonb_build_object('node', 'node-plan', 'resolver', $1::text)
                        WHEN 'emitted' THEN jsonb_build_object('record_key', 'text:plan')
                        WHEN 'noise' THEN jsonb_build_object('record_key', 'text:plan')
                        ELSE jsonb_build_object('node', 'node-plan', 'resolver', $2::text) END,
               CASE arm WHEN 'clear' THEN jsonb_build_object('resolver', $1::text)
                        ELSE jsonb_build_object('resolver', $2::text) END,
               CASE arm WHEN 'emitted' THEN jsonb_build_object('kind', 'raw_log', 'emitting_address', $1::text)
                        ELSE jsonb_build_object('kind', 'raw_log', 'emitting_address', $2::text) END
        FROM (VALUES ('emitted', 1200), ('set', 600), ('clear', 600),
                     ('noise', 30000), ('pointer_noise', 6000)) arms(arm, row_count)
        CROSS JOIN LATERAL generate_series(1, row_count) n
    "#)
    .bind(EMITTER_LARGE)
    .bind(EMITTER_OTHER)
    .execute(&database.pool)
    .await?;
    sqlx::raw_sql("ANALYZE").execute(&database.pool).await?;

    fn visited(node: &Value) -> f64 {
        let own = if node["Relation Name"] == "normalized_events" {
            (node["Actual Rows"].as_f64().unwrap_or(0.0)
                + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
                * node["Actual Loops"].as_f64().unwrap_or(1.0)
        } else {
            0.0
        };
        own + node["Plans"].as_array().map(|plans| plans.iter().map(visited).sum()).unwrap_or(0.0)
    }
    fn find<'a>(node: &'a Value, node_type: &str, found: &mut Vec<&'a Value>) {
        if node["Node Type"].as_str().is_some_and(|kind| kind.ends_with(node_type)) {
            found.push(node);
        }
        for child in node["Plans"].as_array().into_iter().flatten() {
            find(child, node_type, found);
        }
    }
    /// `normalized_events` scans of the statement itself, outside its correlated subqueries.
    fn event_scans<'a>(node: &'a Value, found: &mut Vec<&'a Value>) {
        if node["Relation Name"] == "normalized_events" || node["Node Type"] == "Bitmap Index Scan" {
            found.push(node);
        }
        for child in node["Plans"].as_array().into_iter().flatten() {
            if !matches!(child["Parent Relationship"].as_str(), Some("SubPlan" | "InitPlan")) {
                event_scans(child, found);
            }
        }
    }

    let bounds = BTreeMap::from([("ethereum-mainnet".to_owned(), 111_i64)]);
    let mut failures = Vec::new();
    for (label, raw, arms) in [
        ("resolver", crate::v2::RawQueryParams {
            resolver: Some(format!("1:{EMITTER_LARGE}")),
            include: Some("total_count".to_owned()),
            ..crate::v2::RawQueryParams::default()
        }, true),
        ("resolver and contract_address", crate::v2::RawQueryParams {
            resolver: Some(format!("1:{EMITTER_LARGE}")),
            contract_address: Some(EMITTER_LARGE.to_owned()),
            include: Some("total_count".to_owned()),
            ..crate::v2::RawQueryParams::default()
        }, false),
    ] {
        let params = crate::v2::QueryParams::try_from(raw).map_err(|error| anyhow::anyhow!("{error:?}"))?;
        let parsed = crate::v2::parse_events_filter(&params, None).map_err(|error| anyhow::anyhow!("{error:?}"))?;
        let mode = crate::v2::with_emitter_count_cap(PLAN_CAP, async {
            crate::v2::events_summary_mode(&params, parsed.anchored)
        })
        .await;
        let mut filter = parsed.storage_filter;
        filter.event_kinds = crate::v2::product_history_event_kinds();
        filter.block_window = Some(bigname_storage::HistoryBlockWindow {
            ranges: vec![bigname_storage::ChainBlockRange {
                chain_id: "ethereum-mainnet".to_owned(),
                from_block: None,
                to_block: Some(111),
            }],
        });
        filter.publication_block_bounds = Some(bounds.clone());
        for generic_plan in [false, true] {
            let label = format!("{label} generic_plan={generic_plan}");
            let plan = bigname_storage::explain_event_history_count_for_test(
                &database.pool, filter.clone(), mode, generic_plan).await?
                .with_context(|| format!("{label}: the opt-in runs no count"))?;
            let root = &plan[0]["Plan"];
            let read = visited(root);
            eprintln!("{label}: mode {mode:?}, visited {read}, plan {root}");
            if read > (3 * (PLAN_CAP + 1)) as f64 {
                failures.push(format!("{label}: the count visited {read} rows, over 3 x (cap + 1)"));
            }
            // The Limit that bounds the count is the one above the statement's own event scans.
            let mut limits = Vec::new();
            find(root, "Limit", &mut limits);
            let Some(limit) = limits.into_iter().find(|limit| {
                let mut scans = Vec::new();
                event_scans(limit, &mut scans);
                !scans.is_empty()
            }) else {
                failures.push(format!("{label}: no Limit bounds the count's event scans"));
                continue;
            };
            if !arms {
                continue;
            }
            let mut appends = Vec::new();
            find(limit, "Append", &mut appends);
            let Some(append) = appends.into_iter().next() else {
                failures.push(format!("{label}: no Append of the arms under the Limit"));
                continue;
            };
            let mut scans = Vec::new();
            event_scans(append, &mut scans);
            let indexes = scans
                .iter()
                .filter_map(|scan| scan["Index Name"].as_str())
                .collect::<std::collections::BTreeSet<_>>();
            if scans.iter().any(|scan| !scan["Node Type"].as_str().is_some_and(|kind| kind.starts_with("Index"))) {
                failures.push(format!("{label}: an arm does not read its index in order: {scans:?}"));
            }
            if indexes != std::collections::BTreeSet::from(ARM_INDEXES) {
                failures.push(format!("{label}: the arms read {indexes:?}, not {ARM_INDEXES:?}"));
            }
        }
    }
    database.cleanup().await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// The production opt-in cap, with no test override: an emitter-only count is exact up to
/// 10,000 rows and `null` above, on every emitter anchor. A name anchor stays exact.
#[tokio::test]
async fn events_emitter_total_count_is_null_above_ten_thousand() -> Result<()> {
    const EMITTER_BOUNDARY: &str = "0x00000000000000000000000000000000000000e3";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let emitted = |n: usize| emitted_by(v2_history_event(&format!("boundary-emitted-{n}"),
        Some("ens:history.eth"), None, "RecordChanged", 101 + (n % 10) as i64), EMITTER_BOUNDARY);
    let mut events = (0..10_000).map(emitted).collect::<Vec<_>>();
    events.extend(pointer_pair("boundary-pointer", EMITTER_BOUNDARY, 102, 111));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    let resolver = format!("/v1/events?resolver=1:{EMITTER_BOUNDARY}&page_size=3");
    let contract = format!("/v1/events?contract_address={EMITTER_BOUNDARY}&page_size=3");
    let both = format!("{resolver}&contract_address={EMITTER_BOUNDARY}");

    // At the cap: the contract anchors read the 10,000 emitted rows and count them exactly.
    for route in [&contract, &both] {
        let page = v2_history_payload_for_database(&database, &format!("{route}&include=total_count")).await?;
        assert_eq!(page["page"]["total_count"], json!(10_000), "{route}");
    }

    // Above the cap: the resolver read also holds the two pointer changes, 10,002 rows. The count is
    // null and the page is the page without the flag.
    let default = v2_history_payload_for_database(&database, &resolver).await?;
    let counted = v2_history_payload_for_database(&database, &format!("{resolver}&include=total_count")).await?;
    assert_eq!(counted["page"]["total_count"], Value::Null);
    assert_eq!(counted["data"], default["data"]);
    assert_eq!(counted["page"]["next_cursor"], default["page"]["next_cursor"]);
    assert_eq!(counted["page"]["has_more"], json!(true));

    // Above the cap on the contract anchors: one more emitted row takes them to 10,001.
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[emitted(10_000)]).await?;
    for route in [&contract, &both] {
        let page = v2_history_payload_for_database(&database, &format!("{route}&include=total_count")).await?;
        assert_eq!(page["page"]["total_count"], Value::Null, "{route}");
        assert_eq!(page_rows(&page).len(), 3, "{route}");
    }

    // A name anchor keeps the exact, uncapped opt-in above the cap.
    let name = v2_history_payload_for_database(&database,
        "/v1/events?name=history.eth&type=record&page_size=1&include=total_count").await?;
    assert!(name["page"]["total_count"].as_u64().expect("exact name count") > 10_000);
    database.cleanup().await
}
