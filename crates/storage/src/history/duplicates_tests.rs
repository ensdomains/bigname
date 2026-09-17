use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::super::{
    EventHistoryReadFilter, duplicates::push_product_history_duplicate_filter,
    paging::push_history_filters, selectors::HistorySelector,
    source::push_history_source_for_filter,
};

#[tokio::test]
async fn handoff_representatives_use_indexed_origin_bounds() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("handoff_plan").pool_max_connections(1))
            .await?;
    let result = {
        let mut connection = database.pool().acquire().await?;
        exercise_plan(&mut connection).await
    };
    database.cleanup().await?;
    result
}

async fn exercise_plan(connection: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(baseline).execute(&mut *connection).await?;
    }
    sqlx::raw_sql(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        SELECT 'ethereum-mainnet', 'block-' || n, n, to_timestamp(n), 'canonical'
        FROM generate_series(1, 100050) n;
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family, manifest_version,
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             log_index, derivation_kind, canonicality_state, after_state)
        SELECT 'ens_v1_unwrapped_authority:1:ethereum-mainnet:block-' || n ||
                   ':tx-' || n || ':0:' ||
                   CASE WHEN n <= 100000 THEN 'ResolverChanged:0'
                        ELSE 'ResolverChanged:registry-fallback-handoff:node-' ||
                             n || ':resource-' || copy || ':resolver:0' END,
               'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
               'ethereum-mainnet', 'block-' || n, n, 'tx-' || n, 0, 0,
               'ens_v1_unwrapped_authority', 'canonical',
               jsonb_build_object('node', 'node-' || n)
        FROM generate_series(1, 100050) n
        CROSS JOIN generate_series(1, 2) copy WHERE n > 100000 OR copy = 1;
        ANALYZE normalized_events;
        ANALYZE chain_lineage;
    "#,
    )
    .execute(&mut *connection)
    .await?;
    let filter = EventHistoryReadFilter::default();
    let mut query = QueryBuilder::<Postgres>::new("SELECT count(*)::bigint");
    push_history_source_for_filter(&mut query, &filter, true, false, false);
    push_history_filters(&mut query, &filter, true);
    push_product_history_duplicate_filter(&mut query, &filter, true);
    let sql = query.sql().to_owned();
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {sql}"))
        .fetch_one(&mut *connection)
        .await?;
    eprintln!("planned global count: {plan}");
    assert_no_unbounded_nested_loop_scan(&plan[0]["Plan"], false);
    let mut candidates = Vec::new();
    candidate_scans(&plan[0]["Plan"], false, &mut candidates);
    assert!(
        !candidates.is_empty(),
        "the production query must exercise a handoff lookup"
    );
    for scan in candidates {
        let bound = format!("{} {}", scan["Index Cond"], scan["Recheck Cond"]);
        assert!(
            bound.contains("chain_id")
                && (bound.contains("block_number") || bound.contains("block_hash")),
            "handoff candidate scan lacks indexed origin bounds: {scan}"
        );
    }
    // Execute only after the structural assertion: a bad plan fails deterministically,
    // without making a wall-clock timeout the regression contract.
    let actual: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) {sql}"
    ))
    .fetch_one(&mut *connection)
    .await?;
    let mut scans = Vec::new();
    candidate_scans(&actual[0]["Plan"], false, &mut scans);
    for scan in scans {
        let visited = scan["Actual Rows"].as_f64().unwrap_or(0.0)
            + scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0);
        assert!(
            visited <= 2.0,
            "unrelated history reached the handoff search: {scan}"
        );
        eprintln!("handoff scan: {scan}");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&sql)
            .fetch_one(&mut *connection)
            .await?,
        100050
    );
    eprintln!("global handoff count plan: {actual}");
    Ok(())
}

fn candidate_scans<'a>(node: &'a Value, in_subplan: bool, output: &mut Vec<&'a Value>) {
    let in_subplan = in_subplan || node["Parent Relationship"] == "SubPlan";
    if in_subplan && node["Relation Name"] == "normalized_events" {
        output.push(node);
    }
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            candidate_scans(child, in_subplan, output);
        }
    }
}

fn assert_no_unbounded_nested_loop_scan(node: &Value, repeated: bool) {
    assert!(
        !(repeated && node["Node Type"] == "Seq Scan"),
        "global history plan repeats an unbounded table scan: {node}"
    );
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            let repeated = repeated
                || (node["Node Type"] == "Nested Loop" && child["Parent Relationship"] == "Inner");
            assert_no_unbounded_nested_loop_scan(child, repeated);
        }
    }
}

#[tokio::test]
async fn expanded_history_selectors_fit_postgres_bind_limit() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("history_binds").pool_max_connections(1))
            .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
            .execute(&mut *connection)
            .await?;
        for baseline in [
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
            include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
            // Resource-scoped selectors read pointer-attributed record ids from the record
            // inventory projection.
            include_str!("../../../../schema-v2/baseline/06_projections.sql"),
        ] {
            sqlx::raw_sql(baseline).execute(&mut *connection).await?;
        }
        // Address expansion has no anchor count limit. Execute the production
        // outer filter and representative subquery with more than 32,768 anchors.
        // Empty history is sufficient: PostgreSQL rejects oversized bind lists
        // before row execution, independently of whether any anchor matches.
        let names: Vec<String> = (0..34000).map(|n| format!("ens:0x{n:064x}")).collect();
        let resources: Vec<uuid::Uuid> = (0..34000).map(uuid::Uuid::from_u128).collect();
        for selector in [
            HistorySelector::ProductRegistration {
                logical_name_ids: names.clone(),
                resource_ids: resources.clone(),
            },
            HistorySelector::logical_names(names.clone()),
            HistorySelector::resources(resources.clone()),
            HistorySelector::logical_names_or_resources(
                names[..17000].to_vec(),
                resources[..17000].to_vec(),
            ),
        ] {
            let filter = EventHistoryReadFilter {
                selectors: vec![selector],
                ..Default::default()
            };
            let mut query = QueryBuilder::<Postgres>::new("SELECT count(*)::bigint");
            push_history_source_for_filter(&mut query, &filter, true, false, false);
            push_history_filters(&mut query, &filter, true);
            push_product_history_duplicate_filter(&mut query, &filter, true);
            assert_eq!(
                query
                    .build_query_scalar::<i64>()
                    .fetch_one(&mut *connection)
                    .await?,
                0
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn registrar_snapshots_are_omitted_before_product_paging_but_remain_in_diagnostics()
-> Result<()> {
    use super::super::{
        EventHistoryFilter, HistoryCursor, HistorySummaryMode, InvalidHistoryCursor,
        load_event_history_page,
    };
    use serde_json::json;
    let database = TestDatabase::create(
        TestDatabaseConfig::new("registrar_snapshot_history").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let pool = database.pool();
        let mut connection = pool.acquire().await?;
        sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public").execute(&mut *connection).await?;
        for baseline in [
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
            include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        ] { sqlx::raw_sql(baseline).execute(&mut *connection).await?; }
        sqlx::raw_sql("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) SELECT 'ethereum-sepolia','block-'||n,n,to_timestamp(n),'canonical' FROM generate_series(1,8) n; INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ('70000000-0000-0000-0000-000000000001','ethereum-sepolia','block-1',1,'canonical')").execute(&mut *connection).await?;
        let states = [
            json!({}),
            json!({"registrar_surface_snapshot":true}),
            json!({"state_derived":true}),
            json!({"state_derived":null,"registrar_surface_snapshot":true}),
            json!({"state_derived":true,"registrar_surface_snapshot":null}),
            json!({"state_derived":false,"registrar_surface_snapshot":true}),
            json!({"state_derived":true,"registrar_surface_snapshot":true}),
            json!({"state_derived":true,"registrar_surface_snapshot":false}),
        ];
        for (index,state) in states.iter().enumerate() {
            let n=index as i64+1;
            sqlx::query("INSERT INTO normalized_events (event_identity,namespace,resource_id,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','70000000-0000-0000-0000-000000000001','RegistrationGranted','ens_v1_registrar_l1',1,'ethereum-sepolia',$2,$3,$4,0,0,'ens_v1_unwrapped_authority','canonical',$5)")
                .bind(format!("history-{n}")).bind(format!("block-{n}")).bind(n).bind(format!("tx-{n}")).bind(state)
                .execute(&mut *connection).await?;
        }
        drop(connection);
        let mut cursor=None;
        let mut seen=Vec::new();
        loop {
            let page=load_event_history_page(pool,EventHistoryFilter::default(),true,cursor.as_ref(),1,HistorySummaryMode::Count,false).await?;
            assert_eq!(page.summary.as_ref().unwrap().total_count,7);
            assert_eq!(page.rows.len(),1,"a marked row at a page boundary shortened the page");
            seen.push(page.rows[0].event_identity.clone());
            assert_ne!(page.rows[0].after_state,states[6]);
            match page.next_cursor { Some(next)=>cursor=Some(next), None=>break }
        }
        assert_eq!(seen,[8,6,5,4,3,2,1].map(|n|format!("history-{n}")).to_vec());
        let diagnostic=load_event_history_page(pool,EventHistoryFilter::default(),true,None,20,HistorySummaryMode::None,true).await?;
        assert_eq!(diagnostic.rows.len(),8);
        let snapshot=diagnostic.rows.iter().find(|row|row.event_identity=="history-7").unwrap();
        assert_eq!(snapshot.after_state,states[6]);
        let marked_cursor=HistoryCursor {normalized_event_id:snapshot.normalized_event_id,event_identity:snapshot.event_identity.clone()};
        let error=load_event_history_page(pool,EventHistoryFilter::default(),true,Some(&marked_cursor),1,HistorySummaryMode::None,false).await.unwrap_err();
        assert!(error.downcast_ref::<InvalidHistoryCursor>().is_some(),"marked snapshot became a product cursor: {error:#}");
        let original=diagnostic.rows.iter().find(|row|row.event_identity=="history-1").unwrap();
        assert!(original.logical_name_id.is_none());
        assert!(original.resource_id.is_some());
        assert_eq!(original.after_state,json!({}),"history selection changed the original resource-only grant");
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}
