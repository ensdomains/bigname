// TYR-428: the registry overview's `counts.events` follows the `/v1/events` emitter opt-in count.
// On the current position both are the stored event count, exact under any cap (TYR-462). A
// historical selection, and a read the stored counts cannot serve, count rows: exact up to the
// shared cap and `null` above it, and the statement stops reading at the cap.
const REGISTRY_EVENTS_AT_62: &str = "2023-11-14T22:14:22Z";

#[tokio::test]
async fn registry_event_count_is_null_above_the_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let overview = format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts");
    let historical = format!("{overview}&at={REGISTRY_EVENTS_AT_62}");
    let feed = format!("/v1/events?contract_address={ALPHA_REGISTRY}&include=total_count&page_size=1");

    // Rows A1 and A3 below every cap: six events in all, three through block 62.
    let uncapped = registry_payload(&database, &overview).await?;
    assert_eq!(uncapped["data"]["counts"]["events"], json!(6), "{uncapped}");
    let uncapped_historical = registry_payload(&database, &historical).await?;
    assert_eq!(uncapped_historical["data"]["counts"]["events"], json!(3), "{uncapped_historical}");

    // Row A4: without include=counts the field is absent.
    let plain = registry_payload(&database, &format!("/v1/registries/1/{ALPHA_REGISTRY}")).await?;
    assert!(plain["data"]["counts"].get("events").is_none(), "{plain}");

    // Row A1 at the boundary: a count equal to the cap is exact, on the overview and the feed.
    crate::v2::with_emitter_count_cap(6, async {
        let page = registry_payload(&database, &overview).await?;
        assert_eq!(page["data"]["counts"]["events"], json!(6), "{page}");
        let total = registry_payload(&database, &feed).await?;
        assert_eq!(total["page"]["total_count"], json!(6), "{total}");
        anyhow::Ok(())
    })
    .await?;

    // Rows A2 and A5 on the current position: above the cap the stored count is exact, equal to
    // the feed's, and the other counts are those of the uncapped read.
    crate::v2::with_emitter_count_cap(5, async {
        let page = registry_payload(&database, &overview).await?;
        assert_eq!(page["data"]["counts"]["events"], json!(6), "{page}");
        assert_eq!(page["data"]["counts"]["roles"], uncapped["data"]["counts"]["roles"]);
        assert_eq!(page["data"]["counts"]["labels"], uncapped["data"]["counts"]["labels"]);
        assert_eq!(page["data"]["referenced_by"], uncapped["data"]["referenced_by"]);
        let total = registry_payload(&database, &feed).await?;
        assert_eq!(total["page"]["total_count"], json!(6), "{total}");
        anyhow::Ok(())
    })
    .await?;

    // Rows A2 and A5 counted from the rows: above the cap the count is present and null, like
    // the feed's, and the other counts are those of the uncapped read.
    crate::v2::with_row_counts_only(crate::v2::with_emitter_count_cap(5, async {
        let page = registry_payload(&database, &overview).await?;
        assert_eq!(page["data"]["counts"]["events"], Value::Null, "{page}");
        assert!(page["data"]["counts"].get("events").is_some(), "{page}");
        assert_eq!(page["data"]["counts"]["roles"], uncapped["data"]["counts"]["roles"]);
        assert_eq!(page["data"]["counts"]["labels"], uncapped["data"]["counts"]["labels"]);
        assert_eq!(page["data"]["referenced_by"], uncapped["data"]["referenced_by"]);
        let total = registry_payload(&database, &feed).await?;
        assert_eq!(total["page"]["total_count"], Value::Null, "{total}");
        anyhow::Ok(())
    }))
    .await?;

    // Row A3: a historical selection counts through its block and caps the same way.
    crate::v2::with_emitter_count_cap(3, async {
        let page = registry_payload(&database, &historical).await?;
        assert_eq!(page["data"]["counts"]["events"], json!(3), "{page}");
        anyhow::Ok(())
    })
    .await?;
    crate::v2::with_emitter_count_cap(2, async {
        let page = registry_payload(&database, &historical).await?;
        assert_eq!(page["data"]["counts"]["events"], Value::Null, "{page}");
        assert_eq!(page["data"]["counts"]["roles"], uncapped_historical["data"]["counts"]["roles"]);
        anyhow::Ok(())
    })
    .await?;
    database.cleanup().await
}

/// Row A6: the overview's event count on a large emitter reads the emitter index under a
/// `Limit` and stops after `cap + 1` rows. An uncapped count reads every row the emitter has.
#[tokio::test]
async fn registry_event_count_plan_stops_at_the_cap() -> Result<()> {
    const PLAN_CAP: u64 = 200;
    const EMITTER: &str = "0x00000000000000000000000000000000000000b1";
    const OTHER: &str = "0x00000000000000000000000000000000000000b9";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 101..=111).await?;
    // The emitter has 1,200 events. Other contracts emitted 30,000, so the emitter's rows are a
    // small part of the table.
    sqlx::query(r#"
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family, manifest_version, chain_id,
             block_hash, block_number, transaction_hash, transaction_index, log_index,
             derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
        SELECT 'registry-plan:' || emitter || ':' || n, 'ens', 'RecordChanged',
               'ens_v1_resolver_l1', 1, 'ethereum-mainnet', '0xhistory' || (101 + n % 11),
               101 + n % 11, 'tx-registry-plan-' || emitter || '-' || n, 0, 0,
               'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
               jsonb_build_object('record_key', 'text:plan'), '{}'::jsonb,
               jsonb_build_object('kind', 'raw_log', 'emitting_address', emitter)
        FROM (VALUES ($1::text, 1200), ($2::text, 30000)) emitters(emitter, row_count)
        CROSS JOIN LATERAL generate_series(1, row_count) n
    "#)
    .bind(EMITTER)
    .bind(OTHER)
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
    fn limits<'a>(node: &'a Value, found: &mut Vec<&'a Value>) {
        if node["Node Type"] == "Limit" {
            found.push(node);
        }
        for child in node["Plans"].as_array().into_iter().flatten() {
            limits(child, found);
        }
    }

    let cap = crate::v2::with_emitter_count_cap(PLAN_CAP, async {
        crate::v2::registry_events_count_cap()
    })
    .await;
    let mut failures = Vec::new();
    for generic_plan in [false, true] {
        let label = format!("generic_plan={generic_plan}");
        let plan = bigname_storage::explain_contract_event_count_for_test(
            &database.pool, "ethereum-mainnet", EMITTER, &crate::v2::product_history_event_kinds(),
            Some(111), cap, generic_plan).await?;
        let root = &plan[0]["Plan"];
        let read = visited(root);
        eprintln!("{label}: cap {cap:?}, visited {read}, plan {root}");
        if read > (3 * (PLAN_CAP + 1)) as f64 {
            failures.push(format!("{label}: the count visited {read} rows, over 3 x (cap + 1)"));
        }
        let mut found = Vec::new();
        limits(root, &mut found);
        let Some(limit) = found.into_iter().find(|limit| {
            let mut scans = Vec::new();
            event_scans(limit, &mut scans);
            !scans.is_empty()
        }) else {
            failures.push(format!("{label}: no Limit bounds the count's event scans"));
            continue;
        };
        let mut scans = Vec::new();
        event_scans(limit, &mut scans);
        if !scans.iter().all(|scan| {
            scan["Node Type"].as_str().is_some_and(|kind| kind.starts_with("Index"))
                && scan["Index Name"] == "normalized_events_emitter_history_idx"
        }) {
            failures.push(format!("{label}: the count does not read the emitter index in order: {scans:?}"));
        }
    }
    database.cleanup().await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Rows A1 and A2 at the production cap, counted from the rows: with no test override,
/// `counts.events` is exact up to 10,000 events and `null` above, and the contract feed's opt-in
/// count agrees on both sides of the cap.
#[tokio::test]
async fn registry_event_count_is_null_above_ten_thousand() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    crate::v2::with_row_counts_only(registry_event_counts_null_above_ten_thousand(&database)).await?;
    database.cleanup().await
}

async fn registry_event_counts_null_above_ten_thousand(database: &TestDatabase) -> Result<()> {
    let overview = format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts");
    let feed = format!("/v1/events?contract_address={ALPHA_REGISTRY}&include=total_count&page_size=1");
    let created = |n: i64| {
        registry_event(&format!("registry-alpha-bulk-{n}"), None, "RegistryCreated", 65 + n % 19,
            ALPHA_REGISTRY, json!({ "source_event": "RegistryCreated", "registry": ALPHA_REGISTRY }))
    };
    let uncapped = registry_payload(&database, &overview).await?;
    assert_eq!(uncapped["data"]["counts"]["events"], json!(6), "{uncapped}");

    // The fixture's six events and 9,994 more reach the cap: exact on the overview and the feed.
    let bulk = (0..9_994).map(created).collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &bulk).await?;
    let at_cap = registry_payload(&database, &overview).await?;
    assert_eq!(at_cap["data"]["counts"]["events"], json!(10_000));
    let total = registry_payload(&database, &feed).await?;
    assert_eq!(total["page"]["total_count"], json!(10_000));

    // One more event: both are null, and the other counts are unchanged.
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[created(9_994)]).await?;
    let above = registry_payload(&database, &overview).await?;
    assert!(above["data"]["counts"].get("events").is_some());
    assert_eq!(above["data"]["counts"]["events"], Value::Null);
    assert_eq!(above["data"]["counts"]["roles"], uncapped["data"]["counts"]["roles"]);
    assert_eq!(above["data"]["counts"]["labels"], uncapped["data"]["counts"]["labels"]);
    let total = registry_payload(&database, &feed).await?;
    assert_eq!(total["page"]["total_count"], Value::Null);
    Ok(())
}
