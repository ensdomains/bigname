//! Plan-shape tests for the address history route (`GET /v1/addresses/{address}/history`, and
//! `GET /v1/names/{name}/history?scope=both`, which reads through the same selector). The
//! fixture is small, so `enable_seqscan` is off to stand in for a large `normalized_events`: the
//! assertions are about which access paths the planner can use at all, not about costs.

use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use super::super::{
    EventHistoryReadFilter,
    columns::push_history_select,
    duplicates::push_product_history_duplicate_filter,
    paging::{push_history_filters, push_history_order},
    selectors::HistorySelector,
    summary::push_history_count_query,
};
use super::push_historical_address_matches_query;

const TARGET: &str = "0x0000000000000000000000000000000000000a11";
const UNRELATED_NAMES: i64 = 300;
/// The target's rows: a registrant grant and a registry owner transfer on its name, a token
/// transfer on its resource with no name, and one node-keyed record write attributed to its
/// resource through the record inventory.
const TARGET_ROWS: i64 = 4;
const ANCHOR_INDEXES: [&str; 3] = [
    "normalized_events_address_registrant_match_idx",
    "normalized_events_address_token_holder_match_idx",
    "normalized_events_address_registry_owner_match_idx",
];
const SELECTOR_INDEXES: [&str; 3] = [
    "normalized_events_name_history_idx",
    "normalized_events_resource_history_idx",
    "normalized_events_pkey",
];

#[tokio::test]
async fn address_history_selector_plans_use_history_indexes() -> Result<()> {
    with_fixture("address_history_selector_plan", check_selector_plans).await
}

#[tokio::test]
async fn address_history_anchor_plan_uses_address_match_indexes() -> Result<()> {
    with_fixture("address_history_anchor_plan", check_anchor_plan).await
}

async fn with_fixture(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install_fixture(&mut connection).await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

/// The count and the page read the selector the anchors produce: the target's name and
/// resource, as load_address_history_selector builds them for `scope=both`.
async fn check_selector_plans(connection: &mut PgConnection) -> Result<()> {
    let filter = EventHistoryReadFilter {
        selectors: vec![HistorySelector::logical_names_or_resources(
            vec![target_name()],
            vec![target_resource()],
        )],
        ..EventHistoryReadFilter::default()
    };
    // Plan failures are collected so one run reports every statement that regressed.
    let mut plan_failures = Vec::new();

    let push_count = |builder: &mut _| {
        push_history_count_query(builder, &filter, true, Some(10_001));
    };
    for plan in explain_both(connection, push_count).await? {
        if let Err(error) = assert_selector_uses_history_indexes("count", &plan) {
            plan_failures.push(error.to_string());
        }
    }
    let mut count = QueryBuilder::<Postgres>::new("");
    push_count(&mut count);
    let total: i64 = count
        .build_query_scalar()
        .fetch_one(&mut *connection)
        .await?;
    ensure!(total == TARGET_ROWS, "count returned {total}");

    let push_page = |builder: &mut _| {
        push_history_select(builder, &filter, true, false, false);
        push_history_filters(builder, &filter, true);
        push_product_history_duplicate_filter(builder, &filter, true);
        push_history_order(builder, filter.order);
        builder.push(" LIMIT ");
        builder.push_bind(51_i64);
    };
    for plan in explain_both(connection, push_page).await? {
        if let Err(error) = assert_selector_uses_history_indexes("page", &plan) {
            plan_failures.push(error.to_string());
        }
    }
    let mut page = QueryBuilder::<Postgres>::new("");
    push_page(&mut page);
    let rows = page.build().fetch_all(&mut *connection).await?;
    let identities = rows
        .iter()
        .map(|row| row.try_get::<String, _>("event_identity"))
        .collect::<Result<Vec<_>, _>>()?;
    ensure!(
        identities
            == [
                "target:record",
                "target:owner",
                "target:transfer",
                "target:grant"
            ],
        "page returned {identities:?}"
    );
    ensure!(
        plan_failures.is_empty(),
        "address history plans:\n{}",
        plan_failures.join("\n\n")
    );
    Ok(())
}

/// The anchor lookup must reach the target's rows through the three address-match indexes,
/// not by reading every grant and transfer, and must find the target's name and resource.
async fn check_anchor_plan(connection: &mut PgConnection) -> Result<()> {
    let push_anchors = |builder: &mut _| {
        push_historical_address_matches_query(builder, TARGET, None, None, true, false, None);
    };
    let mut plan_failures = Vec::new();
    for plan in explain_both(connection, push_anchors).await? {
        if let Err(error) = assert_anchor_uses_address_match_indexes(&plan) {
            plan_failures.push(error.to_string());
        }
    }
    let mut anchors = QueryBuilder::<Postgres>::new("");
    push_anchors(&mut anchors);
    let rows = anchors.build().fetch_all(&mut *connection).await?;
    let anchors = rows
        .iter()
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .collect::<Result<BTreeSet<(Option<String>, Option<Uuid>)>>>()?;
    let expected = BTreeSet::from([
        (Some(target_name()), Some(target_resource())),
        (None, Some(target_resource())),
        (Some(target_name()), None),
    ]);
    ensure!(anchors == expected, "anchor lookup returned {anchors:?}");
    ensure!(
        plan_failures.is_empty(),
        "address history anchor plans:\n{}",
        plan_failures.join("\n\n")
    );
    Ok(())
}

fn assert_anchor_uses_address_match_indexes(plan: &Value) -> Result<()> {
    let nodes = main_plan_nodes(plan);
    let indexes = index_names(&nodes);
    // The keyed-scan check runs first so it alone catches a regressed shape.
    assert_no_unkeyed_event_scan("anchor", &nodes, plan, &ANCHOR_INDEXES)?;
    for expected in ANCHOR_INDEXES {
        ensure!(
            indexes.contains(&expected),
            "anchor lookup does not use {expected}: {plan}"
        );
    }
    Ok(())
}

/// The selector ORs names, resources and attributed record writes. Every branch must be an
/// index condition, so the planner combines the name history index, the resource history
/// index and the primary key instead of filtering a full scan.
fn assert_selector_uses_history_indexes(statement: &str, plan: &Value) -> Result<()> {
    let nodes = main_plan_nodes(plan);
    let indexes = index_names(&nodes);
    // The keyed-scan check runs first so it alone catches a regressed shape.
    assert_no_unkeyed_event_scan(statement, &nodes, plan, &SELECTOR_INDEXES)?;
    for expected in SELECTOR_INDEXES {
        ensure!(
            indexes.contains(&expected),
            "{statement} does not use {expected}: {plan}"
        );
    }
    ensure!(
        !indexes.contains(&"normalized_events_projection_idx"),
        "{statement} scans normalized_events_projection_idx: {plan}"
    );
    Ok(())
}

/// Every read of `ne` must be keyed by the statement's own indexes. A bitmap heap scan counts
/// only when every index under it (through BitmapAnd and BitmapOr) is one of `keyed_indexes`, or
/// its recheck condition compares a row key; a bitmap scan of an unrelated index with the
/// selector left as a filter is still a scan of every candidate row.
fn assert_no_unkeyed_event_scan(
    statement: &str,
    nodes: &[&Value],
    plan: &Value,
    keyed_indexes: &[&str],
) -> Result<()> {
    const ROW_KEYS: [&str; 3] = ["normalized_event_id", "logical_name_id", "resource_id"];
    let references_row_key = |condition: &Value| {
        condition
            .as_str()
            .is_some_and(|condition| ROW_KEYS.iter().any(|key| condition.contains(key)))
    };
    for node in nodes {
        if node["Relation Name"] != "normalized_events" || node["Alias"] != "ne" {
            continue;
        }
        let keyed = if node["Node Type"] == "Bitmap Heap Scan" {
            let mut bitmap_indexes = Vec::new();
            bitmap_index_names(node, &mut bitmap_indexes);
            (!bitmap_indexes.is_empty()
                && bitmap_indexes
                    .iter()
                    .all(|index| keyed_indexes.contains(index)))
                || references_row_key(&node["Recheck Cond"])
        } else {
            node.get("Index Cond").is_some()
                && (node["Index Name"]
                    .as_str()
                    .is_some_and(|index| keyed_indexes.contains(&index))
                    || references_row_key(&node["Index Cond"]))
        };
        ensure!(
            keyed,
            "{statement} reads normalized_events without an index key: {node}\n{plan}"
        );
    }
    Ok(())
}

fn bitmap_index_names<'a>(node: &'a Value, output: &mut Vec<&'a str>) {
    for child in node["Plans"].as_array().into_iter().flatten() {
        match child["Node Type"].as_str() {
            Some("Bitmap Index Scan") => output.extend(child["Index Name"].as_str()),
            Some("BitmapAnd" | "BitmapOr") => bitmap_index_names(child, output),
            _ => {}
        }
    }
}

/// The reverted `IN (SELECT ...)` selector can plan as a bitmap heap scan of an unrelated index
/// with the selector as a hashed sub-plan filter; that is not keyed. A bitmap OR of the
/// selector's own indexes is.
#[test]
fn bitmap_heap_scan_is_keyed_only_by_the_statement_indexes() {
    let scan = |bitmap: Value, recheck: &str| {
        serde_json::json!([{"Plan": {
            "Node Type": "Bitmap Heap Scan",
            "Relation Name": "normalized_events",
            "Alias": "ne",
            "Recheck Cond": recheck,
            "Filter": "((logical_name_id = ANY ($1)) OR (hashed SubPlan 1))",
            "Plans": [bitmap],
        }}])
    };
    let index =
        |name: &str| serde_json::json!({"Node Type": "Bitmap Index Scan", "Index Name": name});
    let unkeyed = scan(
        index("normalized_events_projection_idx"),
        "(canonicality_state = ANY ('{canonical,safe,finalized}'))",
    );
    let nodes = main_plan_nodes(&unkeyed);
    assert!(assert_no_unkeyed_event_scan("probe", &nodes, &unkeyed, &SELECTOR_INDEXES).is_err());

    let keyed = scan(
        serde_json::json!({"Node Type": "BitmapOr", "Plans": [
            index("normalized_events_name_history_idx"),
            index("normalized_events_resource_history_idx"),
            index("normalized_events_pkey"),
        ]}),
        "((logical_name_id = ANY ($1)) OR (resource_id = ANY ($2)))",
    );
    let nodes = main_plan_nodes(&keyed);
    assert!(assert_no_unkeyed_event_scan("probe", &nodes, &keyed, &SELECTOR_INDEXES).is_ok());
}

/// Plan the statement twice: as PostgreSQL's generic prepared-statement plan, which sqlx's
/// statement cache can reach, and with the values bound.
async fn explain_both<'a>(
    connection: &mut PgConnection,
    push: impl Fn(&mut QueryBuilder<'a, Postgres>),
) -> Result<[Value; 2]> {
    let mut plain = QueryBuilder::<Postgres>::new("");
    push(&mut plain);
    let generic_sql = format!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {}", plain.sql());
    let generic = sqlx::raw_sql(&generic_sql)
        .fetch_one(&mut *connection)
        .await
        .context("generic plan")?
        .try_get::<Value, _>(0)?;
    let mut bound = QueryBuilder::<Postgres>::new("EXPLAIN (FORMAT JSON) ");
    push(&mut bound);
    let bound = bound
        .build_query_scalar::<Value>()
        .fetch_one(&mut *connection)
        .await
        .context("bound plan")?;
    Ok([generic, bound])
}

/// Every node outside correlated sub-plans; those run per output row and are keyed there.
fn main_plan_nodes(plan: &Value) -> Vec<&Value> {
    fn walk<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
        if node["Parent Relationship"] == "SubPlan" {
            return;
        }
        output.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, output);
        }
    }
    let mut output = Vec::new();
    walk(&plan[0]["Plan"], &mut output);
    output
}

fn index_names<'a>(nodes: &[&'a Value]) -> Vec<&'a str> {
    nodes
        .iter()
        .filter_map(|node| node["Index Name"].as_str())
        .collect()
}

fn target_name() -> String {
    format!("ens:0x{:064x}", 0xa11)
}

fn target_resource() -> Uuid {
    Uuid::from_u128(0xa11)
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema-v2/baseline/06_projections.sql"),
    ] {
        sqlx::raw_sql(baseline).execute(&mut *connection).await?;
    }
    // Unrelated names each hold a resource and one grant, token transfer or registry owner
    // transfer to another address, plus a record write attributed to their resource. The
    // target (name 0xa11, resource 0xa11) holds TARGET_ROWS rows.
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        SELECT 'ethereum-mainnet', 'block-' || n, n, to_timestamp(n), 'canonical'::canonicality_state
        FROM generate_series(1, {blocks}) n;

        INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
             block_number, canonicality_state)
        SELECT 'ens:' || hash, 'ens', 'name-' || n || '.eth', ARRAY['name-' || n, 'eth'],
               '\x00'::bytea, hash, ARRAY[hash, 'eth'], 'test', 'active',
               'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
        FROM generate_series(1, {names}) n,
             LATERAL (SELECT '0x' || lpad(to_hex(n), 64, '0') AS hash) h
        UNION ALL
        SELECT '{target_name}', 'ens', 'target.eth', ARRAY['target', 'eth'], '\x00'::bytea,
               '{target_hash}', ARRAY['{target_hash}', 'eth'], 'test', 'active',
               'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state;

        INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
        SELECT lpad(to_hex(n), 32, '0')::uuid, 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
        FROM generate_series(1, {names}) n
        UNION ALL
        SELECT '{target_resource}', 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state;

        INSERT INTO normalized_events
            (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family,
             manifest_version, chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state, after_state)
        SELECT 'unrelated:' || n, 'ens',
               'ens:0x' || lpad(to_hex(n), 64, '0'), lpad(to_hex(n), 32, '0')::uuid,
               (ARRAY['RegistrationGranted', 'TokenControlTransferred',
                      'AuthorityTransferred'])[1 + n % 3],
               'ens_v2_registry_l1', 1, 'ethereum-mainnet', 'block-' || n, n, 'tx-' || n,
               0, 0, 'ens_v2_registry_resource_surface', 'canonical'::canonicality_state,
               jsonb_build_object('registrant', holder, 'to', holder, 'owner', holder)
        FROM generate_series(1, {names}) n,
             LATERAL (SELECT '0x' || lpad(to_hex(n), 40, '0') AS holder) h
        UNION ALL
        SELECT 'unrelated:record:' || n, 'ens', NULL, NULL, 'RecordChanged',
               'ens_v1_resolver_l1', 1, 'ethereum-mainnet', 'block-' || n, n,
               'tx-record-' || n, 0, 0, 'ens_v2_resolver', 'canonical'::canonicality_state, '{{}}'::jsonb
        FROM generate_series(1, {names}) n
        UNION ALL
        SELECT identity, 'ens', name, resource, kind, 'ens_v2_registry_l1', 1,
               'ethereum-mainnet', 'block-' || block, block, 'tx-' || identity, 0, 0,
               'ens_v2_registry_resource_surface', 'canonical'::canonicality_state, state
        FROM (VALUES
            ('target:grant', '{target_name}', '{target_resource}'::uuid,
             'RegistrationGranted', {first}, jsonb_build_object('registrant', upper('{TARGET}'))),
            ('target:transfer', NULL, '{target_resource}'::uuid,
             'TokenControlTransferred', {second}, jsonb_build_object('to', '{TARGET}')),
            ('target:owner', '{target_name}', NULL,
             'AuthorityTransferred', {third}, jsonb_build_object('owner', '{TARGET}'))
        ) target(identity, name, resource, kind, block, state)
        UNION ALL
        SELECT 'target:record', 'ens', NULL, NULL, 'RecordChanged', 'ens_v1_resolver_l1', 1,
               'ethereum-mainnet', 'block-{fourth}', {fourth}, 'tx-target-record', 0, 0,
               'ens_v2_resolver', 'canonical'::canonicality_state, '{{}}'::jsonb;

        INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, support_status, provenance,
             manifest_version)
        SELECT attributed.resource_id, 'boundary', 'supported',
               jsonb_build_object('attributed_event_ids',
                   jsonb_build_array(event.normalized_event_id::text)),
               1
        FROM (
            SELECT lpad(to_hex(n), 32, '0')::uuid AS resource_id,
                   'unrelated:record:' || n AS identity
            FROM generate_series(1, {names}) n
            UNION ALL
            SELECT '{target_resource}'::uuid, 'target:record'
        ) attributed
        JOIN normalized_events event ON event.event_identity = attributed.identity;

        ANALYZE;
        SET enable_seqscan = off;
        SET jit = off;
        "#,
        blocks = UNRELATED_NAMES + 10,
        names = UNRELATED_NAMES,
        target_name = target_name(),
        target_hash = format!("0x{:064x}", 0xa11),
        target_resource = target_resource(),
        first = UNRELATED_NAMES + 1,
        second = UNRELATED_NAMES + 2,
        third = UNRELATED_NAMES + 3,
        fourth = UNRELATED_NAMES + 4,
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
