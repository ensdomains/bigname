use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{Arguments, Execute, PgConnection, PgPool, Postgres, QueryBuilder};

use super::super::keyset::load_history_keyset;
use super::super::{
    ChainBlockRange, EventHistoryReadFilter, EventHistoryResolverFilter, HistoryBlockWindow,
    HistoryCursor, HistoryOrder, HistorySummaryMode,
};
use super::{load_history_page, order_index_drives_page, push_history_page_query};

const ORDER_INDEX: &str = "normalized_events_chain_block_number_desc_idx";

/// A phase schema holding only the tables a history page reads. The pool has one connection,
/// so the session `search_path` set here is the one every later read uses.
async fn phase_database(name: &str, fixture: &str) -> Result<TestDatabase> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    let mut connection = database.pool().acquire().await?;
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/04_manifests.sql"),
        include_str!("../../schema/baseline/05_normalized_events.sql"),
        fixture,
    ] {
        sqlx::raw_sql(baseline).execute(&mut *connection).await?;
    }
    Ok(database)
}

fn window(ranges: &[(&str, Option<i64>, Option<i64>)]) -> HistoryBlockWindow {
    HistoryBlockWindow {
        ranges: ranges
            .iter()
            .map(|(chain_id, from_block, to_block)| ChainBlockRange {
                chain_id: (*chain_id).to_owned(),
                from_block: *from_block,
                to_block: *to_block,
            })
            .collect(),
    }
}

fn events_filter(
    order: HistoryOrder,
    block_window: Option<HistoryBlockWindow>,
) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        namespace: Some("ens".to_owned()),
        event_kinds: vec![
            "RecordChanged".to_owned(),
            "ResolverChanged".to_owned(),
            "SourceManifestUpdated".to_owned(),
        ],
        order,
        block_window,
        ..EventHistoryReadFilter::default()
    }
}

async fn page_identities(
    pool: &PgPool,
    filter: &EventHistoryReadFilter,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
) -> Result<(Vec<String>, Option<HistoryCursor>)> {
    let page = load_history_page(
        pool,
        filter.clone(),
        true,
        cursor,
        page_size,
        HistorySummaryMode::None,
        false,
        None,
    )
    .await?;
    let identities = page
        .rows
        .into_iter()
        .map(|row| row.event_identity)
        .collect();
    Ok((identities, page.next_cursor))
}

/// Several events per block, blocks with the same number on two chains, events that share a
/// transaction and log index, and events without a block, so every sort key breaks a tie.
const EQUIVALENCE_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT chain, chain || '-block-' || n, n, to_timestamp(n), 'canonical'
    FROM unnest(ARRAY['chain-a', 'chain-b']) chain
    CROSS JOIN generate_series(1, 60) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state)
    SELECT 'fixture:' || chain || ':' || n || ':' || e,
           'ens',
           CASE WHEN e % 2 = 0 THEN 'ResolverChanged' ELSE 'RecordChanged' END,
           'ens_v1_registry_l1', 1, chain, chain || '-block-' || n, n,
           'tx-' || (e % 2), e % 2, e / 2,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-' || n)
    FROM unnest(ARRAY['chain-a', 'chain-b']) chain
    CROSS JOIN generate_series(1, 60) n
    CROSS JOIN LATERAL generate_series(0, n % 4) e;
    -- A second event derived from the same log differs only by identity.
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state)
    SELECT 'fixture:' || chain || ':' || n || ':0:derived',
           'ens', 'RecordChanged', 'ens_v1_registry_l1', 1, chain,
           chain || '-block-' || n, n, 'tx-0', 0, 0,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-' || n)
    FROM unnest(ARRAY['chain-a', 'chain-b']) chain
    CROSS JOIN generate_series(5, 60, 5) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, derivation_kind, canonicality_state)
    SELECT 'fixture:chain-a:manifest:' || n, 'ens', 'SourceManifestUpdated',
           'ens_v1_registry_l1', 1, 'chain-a', 'manifest_sync', 'canonical'
    FROM generate_series(1, 5) n;
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
"#;

#[tokio::test]
async fn keyset_pages_match_one_unpaged_read_in_both_orders() -> Result<()> {
    let database = phase_database("history_keyset_pages", EQUIVALENCE_FIXTURE).await?;
    let result = assert_pages_match_unpaged_read(database.pool()).await;
    database.cleanup().await?;
    result
}

async fn assert_pages_match_unpaged_read(pool: &PgPool) -> Result<()> {
    let windows = [
        // Without a window, events without a block are read too and sort last (newest first).
        None,
        Some(window(&[
            ("chain-a", None, Some(60)),
            ("chain-b", None, Some(60)),
        ])),
        Some(window(&[
            ("chain-a", None, Some(52)),
            ("chain-b", Some(9), Some(57)),
        ])),
        Some(window(&[("chain-a", None, None)])),
    ];
    for block_window in windows {
        for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
            let filter = events_filter(order, block_window.clone());
            let (expected, no_more) = page_identities(pool, &filter, None, 10_000).await?;
            ensure!(no_more.is_none(), "the unpaged read must fit on one page");
            ensure!(
                expected.len() > 100,
                "fixture too small for {order:?} {block_window:?}: {}",
                expected.len()
            );
            // Every page is planned afresh (about 20 ms here), so the sizes are few but uneven.
            for page_size in [2, 7, 25] {
                let mut paged = Vec::new();
                let mut cursor = None;
                loop {
                    let (rows, next) =
                        page_identities(pool, &filter, cursor.as_ref(), page_size).await?;
                    paged.extend(rows);
                    match next {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }
                ensure!(
                    paged == expected,
                    "pages of {page_size} differ from one read for {order:?} {block_window:?}"
                );
            }
        }
    }
    Ok(())
}

/// Two events in each of 10,000 blocks on the read chain and more on another chain, analyzed,
/// so the planner costs the read as it would on a populated table.
const PLAN_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT chain, chain || '-block-' || n, n, to_timestamp(n), 'canonical'
    FROM unnest(ARRAY['ethereum-sepolia', 'other-chain']) chain
    CROSS JOIN generate_series(1, 10000) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state)
    SELECT 'fixture:' || chain || ':' || n || ':' || e,
           'ens',
           CASE WHEN e = 0 THEN 'ResolverChanged' ELSE 'RecordChanged' END,
           'ens_v1_registry_l1', 1, chain, chain || '-block-' || n, n,
           'tx-' || n, 0, e,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-' || n)
    FROM unnest(ARRAY['ethereum-sepolia', 'other-chain']) chain
    CROSS JOIN generate_series(1, 10000) n
    CROSS JOIN generate_series(0, 1) e;
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
"#;

const PLAN_PAGE_LIMIT: i64 = 21;

/// How the page query reaches PostgreSQL in a plan check.
#[derive(Clone, Copy, Debug)]
enum PlanMode {
    /// As `load_history_page` sends an unanchored page: unprepared, planned with its values.
    Unprepared,
    /// A prepared statement on its generic plan, which does not see the bound values.
    Generic,
}

#[tokio::test]
async fn events_pages_read_the_order_index_from_the_cursor_block() -> Result<()> {
    let database = phase_database("history_keyset_plan", PLAN_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        let failures = order_index_plan_failures(&mut connection, PlanMode::Unprepared).await?;
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// Evidence for sending only unanchored pages unprepared: a generic plan, which guesses the
/// size of the block range, still reads the order index from the cursor block.
#[tokio::test]
async fn generic_plans_still_read_the_order_index_from_the_cursor_block() -> Result<()> {
    let database = phase_database("history_keyset_generic_plan", PLAN_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        let before: String = sqlx::query_scalar("SHOW plan_cache_mode")
            .fetch_one(&mut *connection)
            .await?;
        let failures = order_index_plan_failures(&mut connection, PlanMode::Generic).await?;
        let after: String = sqlx::query_scalar("SHOW plan_cache_mode")
            .fetch_one(&mut *connection)
            .await?;
        ensure!(
            before == after,
            "generic-plan helper changed the session mode"
        );
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The plan check fails without the index, so the index is what makes the pages cheap.
#[tokio::test]
async fn plan_check_fails_without_the_order_index() -> Result<()> {
    let database = phase_database("history_keyset_no_index", PLAN_FIXTURE).await?;
    let result = async {
        let mut transaction = database.pool().begin().await?;
        sqlx::query(&format!("DROP INDEX {ORDER_INDEX}"))
            .execute(&mut *transaction)
            .await?;
        let failures = order_index_plan_failures(&mut transaction, PlanMode::Unprepared).await?;
        transaction.rollback().await?;
        for page in PLAN_PAGES {
            ensure!(
                failures
                    .iter()
                    .any(|failure| failure.starts_with(&format!("{page}: the page must read"))),
                "{page} passed the plan check without {ORDER_INDEX}: {failures:?}"
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

const PLAN_PAGES: [&str; 4] = [
    "newest-first first page",
    "oldest-first first page",
    "newest-first deep page",
    "oldest-first deep page",
];

/// Plan each unanchored events page and report every page that does not read the order index
/// or visits more than a few blocks' events.
async fn order_index_plan_failures(
    connection: &mut PgConnection,
    mode: PlanMode,
) -> Result<Vec<String>> {
    let block_window = Some(window(&[("ethereum-sepolia", None, Some(10_000))]));
    // Each cursor sits 15,000 rows into its order, 5,000 rows from the other end.
    let newest_first_cursor = cursor_at(connection, "fixture:ethereum-sepolia:2500:0").await?;
    let oldest_first_cursor = cursor_at(connection, "fixture:ethereum-sepolia:7500:0").await?;
    let mut failures = Vec::new();
    for (order, cursor, page) in [
        (HistoryOrder::Desc, None, PLAN_PAGES[0]),
        (HistoryOrder::Asc, None, PLAN_PAGES[1]),
        (
            HistoryOrder::Desc,
            Some(&newest_first_cursor),
            PLAN_PAGES[2],
        ),
        (HistoryOrder::Asc, Some(&oldest_first_cursor), PLAN_PAGES[3]),
    ] {
        let filter = events_filter(order, block_window.clone());
        ensure!(
            order_index_drives_page(&filter),
            "{page}: the plan fixture must read an unanchored page"
        );
        let keyset = match cursor {
            Some(cursor) => {
                Some(load_history_keyset(connection, &filter, true, cursor, false).await?)
            }
            None => None,
        };
        let mut query = QueryBuilder::<Postgres>::new("");
        push_history_page_query(
            &mut query,
            &filter,
            true,
            keyset.as_ref(),
            false,
            PLAN_PAGE_LIMIT,
        );
        let plan = explain_page(connection, query, mode).await?;
        let mut scans = Vec::new();
        page_scans(&plan[0]["Plan"], &mut scans);
        ensure!(
            scans.len() == 1,
            "{page}: expected one scan of the page's events, found {}: {plan}",
            scans.len()
        );
        let scan = scans[0];
        let visited = (scan["Actual Rows"].as_f64().unwrap_or(0.0)
            + scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
            * scan["Actual Loops"].as_f64().unwrap_or(1.0);
        let summary = format!(
            "{mode:?} {} {} {} cond {} visited {visited}",
            scan["Node Type"], scan["Scan Direction"], scan["Index Name"], scan["Index Cond"]
        );
        eprintln!("{page}: {summary}");
        if scan["Index Name"] != ORDER_INDEX {
            failures.push(format!(
                "{page}: the page must read {ORDER_INDEX}; read {summary}"
            ));
        }
        // A page of 21 reads the few rows of the blocks it spans. Reading from the newest or
        // oldest row instead of the cursor block visits 15,000.
        if visited > (3 * PLAN_PAGE_LIMIT) as f64 {
            failures.push(format!(
                "{page}: the page visited {visited} events instead of starting at the cursor block; {summary}"
            ));
        }
    }
    Ok(failures)
}

/// `EXPLAIN ANALYZE` the page query. A bound `EXPLAIN` plans with the values as constants
/// whatever `plan_cache_mode` says, so the generic mode forces a generic prepared execution,
/// verifies PostgreSQL's plan counters, and restores the caller's session setting.
async fn explain_page(
    connection: &mut PgConnection,
    mut query: QueryBuilder<'_, Postgres>,
    mode: PlanMode,
) -> Result<Value> {
    match mode {
        PlanMode::Unprepared => {
            let mut explain =
                QueryBuilder::<Postgres>::new("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ");
            explain.push(query.sql());
            let sql = explain.into_sql();
            let mut built = query.build();
            let arguments = built.take_arguments().map_err(anyhow::Error::from_boxed)?;
            Ok(sqlx::query_scalar_with(&sql, arguments.unwrap_or_default())
                .persistent(false)
                .fetch_one(&mut *connection)
                .await?)
        }
        PlanMode::Generic => {
            let previous: String = sqlx::query_scalar("SHOW plan_cache_mode")
                .fetch_one(&mut *connection)
                .await?;
            sqlx::query("SET plan_cache_mode = force_generic_plan")
                .execute(&mut *connection)
                .await?;
            let mut prepared = false;
            let result: Result<Value> = async {
                sqlx::raw_sql(&format!("PREPARE history_page AS {}", query.sql()))
                    .execute(&mut *connection)
                    .await
                    .context("failed to prepare the page query")?;
                prepared = true;
                let mut built = query.build();
                let arguments = built
                    .take_arguments()
                    .map_err(anyhow::Error::from_boxed)?
                    .unwrap_or_default();
                // EXECUTE takes no protocol parameters, so PostgreSQL renders each bound value
                // as a quoted literal for it.
                let placeholders = (1..=arguments.len())
                    .map(|index| format!("quote_nullable(${index})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let literals: Vec<String> =
                    sqlx::query_scalar_with(&format!("SELECT ARRAY[{placeholders}]"), arguments)
                        .persistent(false)
                        .fetch_one(&mut *connection)
                        .await?;
                let plan = sqlx::query_scalar(&format!(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE history_page({})",
                    literals.join(", ")
                ))
                .persistent(false)
                .fetch_one(&mut *connection)
                .await?;
                let (generic, custom): (i64, i64) = sqlx::query_as(
                    "SELECT generic_plans, custom_plans FROM pg_prepared_statements
                     WHERE name = 'history_page'",
                )
                .fetch_one(&mut *connection)
                .await?;
                ensure!(generic == 1 && custom == 0,
                    "expected one generic execution, found generic_plans={generic}, custom_plans={custom}");
                eprintln!("verified history_page: generic_plans={generic}; custom_plans={custom}");
                Ok(plan)
            }.await;
            // Restore even when preparation, execution, or the evidence assertion fails.
            let cleanup = if prepared {
                sqlx::raw_sql("DEALLOCATE history_page")
                    .execute(&mut *connection)
                    .await
                    .map(|_| ())
            } else {
                Ok(())
            };
            let restore = sqlx::query("SELECT set_config('plan_cache_mode', $1, false)")
                .bind(previous)
                .execute(&mut *connection)
                .await;
            let plan = result?;
            cleanup?;
            restore?;
            Ok(plan)
        }
    }
}

/// A product cursor at `event_identity`: its id and its full history position.
async fn cursor_at(connection: &mut PgConnection, event_identity: &str) -> Result<HistoryCursor> {
    let (normalized_event_id, block_number, chain_id, block_hash, transaction_index, log_index) =
        sqlx::query_as(
            "SELECT normalized_event_id, block_number, chain_id, block_hash, transaction_index,
                    log_index
             FROM normalized_events WHERE event_identity = $1",
        )
        .bind(event_identity)
        .fetch_one(&mut *connection)
        .await?;
    Ok(HistoryCursor {
        normalized_event_id: Some(normalized_event_id),
        event_identity: event_identity.to_owned(),
        position: Some(crate::HistoryPosition {
            block_number,
            chain_id,
            block_hash,
            transaction_index,
            transaction_hash: None,
            log_index,
        }),
    })
}

/// Scans of `normalized_events ne` that produce page rows, not the per-row subqueries.
fn page_scans<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
    if matches!(
        node["Parent Relationship"].as_str(),
        Some("SubPlan" | "InitPlan")
    ) {
        return;
    }
    if node["Relation Name"] == "normalized_events" && node["Alias"] == "ne" {
        output.push(node);
    }
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            page_scans(child, output);
        }
    }
}

const RESOLVER_A: &str = "0x00000000000000000000000000000000000000a1";
const RESOLVER_B: &str = "0x00000000000000000000000000000000000000b1";
const RESOLVER_CHAIN: &str = "ethereum-sepolia";

fn resolver_filter(order: HistoryOrder, address: &str) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        namespace: Some("ens".to_owned()),
        event_kinds: vec!["RecordChanged".to_owned(), "ResolverChanged".to_owned()],
        order,
        resolver: Some(EventHistoryResolverFilter {
            chain_id: RESOLVER_CHAIN.to_owned(),
            address: address.to_owned(),
        }),
        ..EventHistoryReadFilter::default()
    }
}

/// Noise on two chains plus resolver matches on one chain. Resolver A has 35 matches
/// (14 emitted, 10 set to it, 10 cleared from it, 1 emitted pointer set to itself) and
/// resolver B has 89 (40 emitted, 25 set to it, 23 cleared from it, 1 overlap). A few
/// same-address rows on the other chain, non-canonical rows, and candidate rows must
/// never match.
const RESOLVER_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT chain, chain || '-block-' || n, n, to_timestamp(n), 'canonical'
    FROM unnest(ARRAY['ethereum-sepolia', 'other-chain']) chain
    CROSS JOIN generate_series(1, 150000) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref)
    SELECT 'noise:' || chain || ':' || n || ':' || e,
           'ens',
           CASE WHEN e = 0 THEN 'ResolverChanged' ELSE 'RecordChanged' END,
           'ens_v1_registry_l1', 1, chain, chain || '-block-' || n, n,
           'tx-' || n, 0, e,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-' || n), '{}'::jsonb
    FROM unnest(ARRAY['ethereum-sepolia', 'other-chain']) chain
    CROSS JOIN generate_series(1, 150000) n
    CROSS JOIN generate_series(0, 1) e;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT 'match:a:emit:' || n, 'ens', 'RecordChanged', 'ens_v1_resolver_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || (100 * n), 100 * n,
           'tx-a-emit-' || n, 0, 0, 'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-a'), '{}'::jsonb,
           jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000a1')
    FROM generate_series(1, 14) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT 'match:a:after:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || (100 * n + 30), 100 * n + 30,
           'tx-a-after-' || n, 0, 0, 'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-a', 'resolver', '0x00000000000000000000000000000000000000a1'),
           jsonb_build_object('resolver', '0x0000000000000000000000000000000000000099'),
           '{}'::jsonb
    FROM generate_series(1, 10) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT 'match:a:before:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || (100 * n + 60), 100 * n + 60,
           'tx-a-before-' || n, 0, 0, 'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-a', 'resolver', '0x0000000000000000000000000000000000000099'),
           jsonb_build_object('resolver', '0x00000000000000000000000000000000000000a1'),
           '{}'::jsonb
    FROM generate_series(1, 10) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT 'match:b:emit:' || n, 'ens', 'RecordChanged', 'ens_v1_resolver_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || (37 * n + 11), 37 * n + 11,
           'tx-b-emit-' || n, 0, 1, 'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-b'), '{}'::jsonb,
           jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000b1')
    FROM generate_series(1, 40) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT 'match:b:after:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || (53 * n + 17), 53 * n + 17,
           'tx-b-after-' || n, 0, 1, 'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-b', 'resolver', '0x00000000000000000000000000000000000000b1'),
           jsonb_build_object('resolver', '0x0000000000000000000000000000000000000099'),
           '{}'::jsonb
    FROM generate_series(1, 25) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT 'match:b:before:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || (71 * n + 23), 71 * n + 23,
           'tx-b-before-' || n, 0, 1, 'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-b', 'resolver', '0x0000000000000000000000000000000000000099'),
           jsonb_build_object('resolver', '0x00000000000000000000000000000000000000b1'),
           '{}'::jsonb
    FROM generate_series(1, 23) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    VALUES
        ('match:a:overlap', 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
         'ethereum-sepolia', 'ethereum-sepolia-block-9950', 9950, 'tx-a-overlap', 0, 0,
         'ens_v1_unwrapped_authority', 'canonical',
         jsonb_build_object('node', 'node-a', 'resolver', '0x00000000000000000000000000000000000000a1'),
         jsonb_build_object('resolver', '0x0000000000000000000000000000000000000099'),
         jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000a1')),
        ('match:b:overlap', 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
         'ethereum-sepolia', 'ethereum-sepolia-block-9960', 9960, 'tx-b-overlap', 0, 1,
         'ens_v1_unwrapped_authority', 'canonical',
         jsonb_build_object('node', 'node-b', 'resolver', '0x00000000000000000000000000000000000000b1'),
         jsonb_build_object('resolver', '0x0000000000000000000000000000000000000099'),
         jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000b1')),
        ('excluded:a:other-chain', 'ens', 'RecordChanged', 'ens_v1_resolver_l1', 1,
         'other-chain', 'other-chain-block-9000', 9000, 'tx-x1', 0, 0,
         'ens_v1_unwrapped_authority', 'canonical',
         jsonb_build_object('node', 'node-a'), '{}'::jsonb,
         jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000a1')),
        ('excluded:a:noncanonical', 'ens', 'RecordChanged', 'ens_v1_resolver_l1', 1,
         'ethereum-sepolia', 'ethereum-sepolia-block-9001', 9001, 'tx-x2', 0, 0,
         'ens_v1_unwrapped_authority', 'orphaned',
         jsonb_build_object('node', 'node-a'), '{}'::jsonb,
         jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000a1'));
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, consumer_visibility,
         migration_correlation_ids, after_state, before_state, raw_fact_ref)
    VALUES
        ('excluded:a:candidate', 'ens', 'RecordChanged', 'ens_v1_resolver_l1', 1,
         'ethereum-sepolia', 'ethereum-sepolia-block-9002', 9002, 'tx-x3', 0, 0,
         'ens_v1_unwrapped_authority', 'canonical', 'candidate', ARRAY['candidate-1'],
         jsonb_build_object('node', 'node-a'), '{}'::jsonb,
         jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000a1'));
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
"#;

/// The old resolver condition, written out in full, as the reference the new pages must
/// match row for row. The fixture rows trigger none of the product duplicate rules, so
/// those rules cannot hide a difference here.
async fn resolver_reference_identities(
    pool: &PgPool,
    address: &str,
    order: HistoryOrder,
) -> Result<Vec<String>> {
    let order_sql = match order {
        HistoryOrder::Desc => {
            "ne.block_number DESC NULLS LAST, ne.chain_id ASC NULLS LAST, \
             ne.block_hash DESC NULLS LAST, ne.transaction_index DESC NULLS LAST, \
             ne.log_index DESC NULLS LAST, ne.event_identity DESC"
        }
        HistoryOrder::Asc => {
            "ne.block_number ASC NULLS FIRST, ne.chain_id DESC NULLS FIRST, \
             ne.block_hash ASC NULLS FIRST, ne.transaction_index ASC NULLS FIRST, \
             ne.log_index ASC NULLS FIRST, ne.event_identity ASC"
        }
    };
    let identities: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT ne.event_identity FROM normalized_events ne \
         LEFT JOIN chain_lineage rb ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash \
         WHERE ne.consumer_visibility = 'activated' AND ne.namespace = 'ens' \
           AND ne.event_kind IN ('RecordChanged', 'ResolverChanged') \
           AND ne.chain_id = $1 \
           AND (lower(ne.raw_fact_ref ->> 'emitting_address') = $2 \
                OR (ne.event_kind = 'ResolverChanged' \
                    AND (lower(ne.after_state ->> 'resolver') = $2 \
                         OR lower(ne.before_state ->> 'resolver') = $2))) \
           AND ne.canonicality_state IN ('canonical', 'safe', 'finalized') \
           AND (ne.block_hash IS NULL OR rb.canonicality_state IN ('canonical', 'safe', 'finalized')) \
         ORDER BY {order_sql}"
    ))
    .bind(RESOLVER_CHAIN)
    .bind(address)
    .fetch_all(pool)
    .await?;
    Ok(identities)
}

#[tokio::test]
async fn resolver_pages_match_the_old_condition_row_for_row() -> Result<()> {
    let database = phase_database("history_resolver_equivalence", RESOLVER_FIXTURE).await?;
    let result = async {
        for (address, matches) in [(RESOLVER_A, 35usize), (RESOLVER_B, 89usize)] {
            for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
                let expected =
                    resolver_reference_identities(database.pool(), address, order).await?;
                ensure!(
                    expected.len() == matches,
                    "fixture must give {matches} matches for {address} {order:?}, gave {}",
                    expected.len()
                );
                let filter = resolver_filter(order, address);
                let (unpaged, no_more) =
                    page_identities(database.pool(), &filter, None, 10_000).await?;
                ensure!(
                    no_more.is_none(),
                    "unpaged resolver read must fit on one page"
                );
                ensure!(
                    unpaged == expected,
                    "unpaged resolver page differs from the old condition for {address} {order:?}"
                );
                for page_size in [2u64, 7, 20, 50, 100] {
                    let mut paged = Vec::new();
                    let mut cursor = None;
                    loop {
                        let (rows, next) =
                            page_identities(database.pool(), &filter, cursor.as_ref(), page_size)
                                .await?;
                        paged.extend(rows);
                        match next {
                            Some(next) => cursor = Some(next),
                            None => break,
                        }
                    }
                    ensure!(
                        paged == expected,
                        "resolver pages of {page_size} differ from the old condition \
                         for {address} {order:?}"
                    );
                }
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn resolver_pages_do_not_walk_the_chain_order_index() -> Result<()> {
    let database = phase_database("history_resolver_plan", RESOLVER_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        let mut failures = Vec::new();
        for (address, matches) in [(RESOLVER_A, 35.0f64), (RESOLVER_B, 89.0f64)] {
            for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
                for limit in [21i64, 41, 51, 101, 151, 201] {
                    let filter = resolver_filter(order, address);
                    let mut query = QueryBuilder::<Postgres>::new("");
                    push_history_page_query(&mut query, &filter, true, None, false, limit);
                    let plan = explain_page(&mut connection, query, PlanMode::Unprepared).await?;
                    let mut scans = Vec::new();
                    resolver_page_scans(&plan[0]["Plan"], &mut scans);
                    ensure!(
                        !scans.is_empty(),
                        "resolver page found no events scan: {plan}"
                    );
                    let visited: f64 = scans
                        .iter()
                        .map(|scan| {
                            (scan["Actual Rows"].as_f64().unwrap_or(0.0)
                                + scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
                                * scan["Actual Loops"].as_f64().unwrap_or(1.0)
                        })
                        .sum();
                    eprintln!(
                        "resolver {address} {order:?} limit {limit}: visited {visited} \
                         across {} scans",
                        scans.len()
                    );
                    for scan in &scans {
                        if scan["Index Name"] == ORDER_INDEX {
                            failures.push(format!(
                                "resolver {address} {order:?} limit {limit}: \
                                 the page must not read {ORDER_INDEX}"
                            ));
                        }
                        if scan["Node Type"] == "Seq Scan" {
                            failures.push(format!(
                                "resolver {address} {order:?} limit {limit}: \
                                 the page must not sequentially scan normalized_events"
                            ));
                        }
                    }
                    let ceiling = matches * 4.0 + 64.0;
                    if visited > ceiling {
                        failures.push(format!(
                            "resolver {address} {order:?} limit {limit}: \
                             the page visited {visited} events, over the ceiling {ceiling} \
                             for {matches} matches"
                        ));
                    }
                }
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// Every scan of `normalized_events` that produces page rows, whatever alias it uses,
/// not the per-row subqueries.
fn resolver_page_scans<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
    if matches!(
        node["Parent Relationship"].as_str(),
        Some("SubPlan" | "InitPlan")
    ) {
        return;
    }
    if node["Relation Name"] == "normalized_events" {
        output.push(node);
    }
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            resolver_page_scans(child, output);
        }
    }
}

#[path = "contract_count_tests.rs"]
mod contract_count_tests;
