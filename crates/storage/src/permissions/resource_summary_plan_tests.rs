//! Plan and row tests for `load_registry_permission_registration_map`, the lease lookup that
//! `GET /v1/permissions` runs for page rows whose resource has no current name.
//!
//! The statement must read every `normalized_events` row through an index seek on a key it
//! supplies: a resource id, one node key per equality, a logical name id, or a raw log's emitter,
//! block and log. It must read every `chain_lineage` row through a keyed lookup that ends in
//! `LIMIT 1`, which PostgreSQL can only run once per event row, whatever it estimates. The text
//! before this change walked every readable token transfer of the chain for each candidate name,
//! and its plain lineage join let the planner hash the whole lineage table.
//!
//! The fixture is small, so sequential scans are off to stand in for a large `normalized_events`.
//! The plan test checks every cell in custom and generic plans, on the lineage as built and again
//! with 200,000 more lineage rows. On the larger lineage the planner already prefers to hash or
//! merge the whole lineage table for a plain lineage join, and each cell also runs under
//! `EXPLAIN (ANALYZE, BUFFERS)` with no temp blocks allowed. It then plans each cell with nested
//! loops disabled. That leaves a plain lineage join only a hash or merge, while the keyed lookup
//! still has its nested loop.
//!
//! Two other texts sit beside this file: the statement before this change, and this change with
//! the plain lineage join and the previous Basenames family predicate. Each must still fail the
//! check it exists for, so the fixture keeps proving something.
//!
//! The row test runs every cell through this text and the previous one in both plan modes, and
//! compares their rows with each other and with the expected leases.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, raw_sql};
use uuid::Uuid;

use super::{load_registry_permission_registration_map, registry_permission_registration_map_sql};

const PREVIOUS: &str = include_str!("resource_summary_plan_tests/previous.sql");
const PLAIN_LINEAGE_JOIN: &str = include_str!("resource_summary_plan_tests/plain_lineage_join.sql");
const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];
const ENS: &str = "ethereum-mainnet";
const BASE: &str = "base-mainnet";
const PUBLISHED: i64 = 1_000;
const BOUNDS: &str = r#"{"ethereum-mainnet": 1000, "base-mainnet": 1000}"#;
const OWNER: &str = "0x0000000000000000000000000000000000000cc1";
const HOLDER: &str = "0x0000000000000000000000000000000000000cc2";
/// Index condition fragments that name a key the statement supplies. The node-key expression of
/// the two direct-node probe indexes is the only `COALESCE` an index condition can carry here.
const STATEMENT_KEYS: [&str; 7] = [
    "resource_id =",
    "logical_name_id =",
    "COALESCE(",
    "emitting_address",
    "block_hash =",
    "block_number =",
    "normalized_event_id =",
];
const BASENAMES_PROBE_INDEX: &str = "normalized_events_basenames_direct_node_probe_idx";

/// The anchors: nameless resources with registry-only authority on a node, and the leases on it.
const NO_LEASE: u128 = 0x101;
const ONE_TRANSFER: u128 = 0x201;
const ONE_TRANSFER_LEASE: u128 = 0x2201;
const TWO_TRANSFERS: u128 = 0x301;
const TWO_TRANSFERS_LEASE: u128 = 0x3301;
const RELEASED: u128 = 0x401;
const RELEASED_LEASE: u128 = 0x4401;
const EAC_RESOURCE: u128 = 0x4e01;
const SURFACE_KEY: u128 = 0x501;
const SURFACE_KEY_LEASE: u128 = 0x5501;
const TWO_NODES: u128 = 0x601;
const TWO_NODES_SECOND_NODE: u128 = 0x602;
const TWO_NODES_LEASE: u128 = 0x6601;
const MORE_ANCHORS: [u128; 4] = [0x701, 0x801, 0x901, 0xa01];
const BASENAMES: u128 = 0xb01;
const BASENAMES_LEASE: u128 = 0xbb01;
const FILLERS: u128 = 157;
const FILLER_BASE: u128 = 0xf000;

#[tokio::test]
async fn registration_map_plans_seek_every_key() -> Result<()> {
    with_fixture("registration_map_plan", async |connection| {
        raw_sql("SET enable_seqscan = off")
            .execute(&mut *connection)
            .await?;
        let mut failures = Vec::new();
        for lineage in [Lineage::AsBuilt, Lineage::Bulked] {
            if lineage == Lineage::Bulked {
                raw_sql(BULK_LINEAGE_SQL)
                    .execute(&mut *connection)
                    .await
                    .context("bulk the lineage")?;
            }
            prepare_texts(connection).await?;
            failures.extend(check_default_plans(connection, lineage).await?);
            if lineage == Lineage::Bulked {
                failures.extend(check_plans_without_nested_loops(connection).await?);
            }
        }
        ensure!(
            failures.is_empty(),
            "registration map plans:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn registration_map_rows_match_the_previous_statement() -> Result<()> {
    with_fixture("registration_map_rows", async |connection| {
        prepare_texts(connection).await?;
        let mut failures = Vec::new();
        for mode in PLAN_MODES {
            for cell in cells() {
                let label = format!("{}, {mode}", cell.label);
                let literals = cell.literals();
                let now = execute(connection, mode, "map_now", &literals).await?;
                let previous = execute(connection, mode, "map_previous", &literals).await?;
                if now != previous {
                    failures.push(format!(
                        "{label}: rows {now:?}, previous statement {previous:?}"
                    ));
                }
                if now != cell.expected() {
                    failures.push(format!(
                        "{label}: rows {now:?}, expected {:?}",
                        cell.expected()
                    ));
                }
            }
        }
        raw_sql("RESET plan_cache_mode")
            .execute(&mut *connection)
            .await?;
        let bounds = BTreeMap::from([(ENS.to_owned(), PUBLISHED), (BASE.to_owned(), PUBLISHED)]);
        for cell in cells() {
            let ids = cell.ids.iter().copied().map(rid).collect::<Vec<_>>();
            let map = load_registry_permission_registration_map(
                &mut *connection,
                &ids,
                cell.registration.map(rid),
                &bounds,
            )
            .await?;
            let expected = cell.expected().into_iter().collect::<BTreeMap<_, _>>();
            if map != expected {
                failures.push(format!(
                    "{}: map {map:?}, expected {expected:?}",
                    cell.label
                ));
            }
        }
        let distinct_nodes: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT after_state ->> 'node') FROM bigname_phase.normalized_events
             WHERE resource_id = $1 AND after_state ->> 'authority_kind' = 'registry_only'",
        )
        .bind(rid(TWO_NODES))
        .fetch_one(&mut *connection)
        .await?;
        if distinct_nodes != 2 {
            failures.push(format!(
                "the two-node anchor holds authority on {distinct_nodes} nodes"
            ));
        }
        ensure!(
            failures.is_empty(),
            "registration map rows:\n{}",
            failures.join("\n")
        );
        Ok(())
    })
    .await
}

/// A page passes a new array each time. A statement kept prepared on the connection switches to
/// a generic plan after five executions, so the read must leave none behind.
#[tokio::test]
async fn registration_map_keeps_no_prepared_statement() -> Result<()> {
    with_fixture("registration_map_unprepared", async |connection| {
        let bounds = BTreeMap::from([(ENS.to_owned(), PUBLISHED)]);
        for _ in 0..6 {
            load_registry_permission_registration_map(
                &mut *connection,
                &[rid(ONE_TRANSFER)],
                None,
                &bounds,
            )
            .await?;
        }
        let kept: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_prepared_statements
             WHERE NOT from_sql AND strpos(statement, 'WITH eligible AS NOT ' || 'MATERIALIZED') > 0",
        )
        .persistent(false)
        .fetch_one(&mut *connection)
        .await?;
        ensure!(
            kept == 0,
            "the connection keeps {kept} prepared map statements"
        );
        Ok(())
    })
    .await
}

/// A page with no nameless resource, or no published chain, runs no statement: the pool here is
/// closed, so any read would fail.
#[tokio::test]
async fn registration_map_without_resources_reads_nothing() -> Result<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://bigname@127.0.0.1:1/closed")?;
    pool.close().await;
    let bounds = BTreeMap::from([(ENS.to_owned(), PUBLISHED)]);
    let empty = load_registry_permission_registration_map(&pool, &[], None, &bounds).await?;
    ensure!(empty.is_empty(), "{empty:?}");
    let unpublished =
        load_registry_permission_registration_map(&pool, &[rid(NO_LEASE)], None, &BTreeMap::new())
            .await?;
    ensure!(unpublished.is_empty(), "{unpublished:?}");
    ensure!(
        load_registry_permission_registration_map(&pool, &[rid(NO_LEASE)], None, &bounds)
            .await
            .is_err(),
        "a read on the closed pool must fail"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lineage {
    AsBuilt,
    Bulked,
}

struct Cell {
    label: &'static str,
    ids: Vec<u128>,
    registration: Option<u128>,
    mapped: Vec<(u128, u128)>,
    basenames: bool,
}

impl Cell {
    fn new(label: &'static str, ids: Vec<u128>, mapped: &[(u128, u128)]) -> Self {
        Self {
            label,
            ids,
            registration: None,
            mapped: mapped.to_vec(),
            basenames: false,
        }
    }

    fn literals(&self) -> String {
        let ids = self
            .ids
            .iter()
            .map(|id| rid(*id).to_string())
            .collect::<Vec<_>>()
            .join(",");
        let registration = self.registration.map_or_else(
            || "NULL::uuid".to_owned(),
            |id| format!("'{}'::uuid", rid(id)),
        );
        format!("('{{{ids}}}'::uuid[], {registration}, '{BOUNDS}'::jsonb)")
    }

    fn expected(&self) -> BTreeSet<(Uuid, Uuid)> {
        self.mapped
            .iter()
            .map(|(resource, lease)| (rid(*resource), rid(*lease)))
            .collect()
    }
}

fn anchors() -> Vec<u128> {
    [
        NO_LEASE,
        ONE_TRANSFER,
        TWO_TRANSFERS,
        RELEASED,
        SURFACE_KEY,
        TWO_NODES,
    ]
    .into_iter()
    .chain(MORE_ANCHORS)
    .collect()
}

fn fillers(count: u128) -> impl Iterator<Item = u128> {
    (0..count).map(|n| FILLER_BASE + n)
}

const MAPPED_ANCHORS: [(u128, u128); 4] = [
    (ONE_TRANSFER, ONE_TRANSFER_LEASE),
    (TWO_TRANSFERS, TWO_TRANSFERS_LEASE),
    (SURFACE_KEY, SURFACE_KEY_LEASE),
    (TWO_NODES, TWO_NODES_LEASE),
];

/// The input classes. The labels name the route's page shapes: one anchor alone, a small page,
/// a page of 50 with one anchor, and pages of 200 with ten anchors.
fn cells() -> Vec<Cell> {
    let mut cells = vec![
        Cell::new("one anchor without a lease", vec![NO_LEASE], &[]),
        Cell::new(
            "one anchor with a live lease and one transfer",
            vec![ONE_TRANSFER],
            &[(ONE_TRANSFER, ONE_TRANSFER_LEASE)],
        ),
        Cell::new(
            "one anchor with a live lease and two transfers",
            vec![TWO_TRANSFERS],
            &[(TWO_TRANSFERS, TWO_TRANSFERS_LEASE)],
        ),
        Cell::new(
            "a released lease and an EAC resource",
            vec![RELEASED, EAC_RESOURCE],
            &[],
        ),
        Cell::new(
            "a small page of every single-anchor class",
            vec![
                NO_LEASE,
                ONE_TRANSFER,
                TWO_TRANSFERS,
                RELEASED,
                EAC_RESOURCE,
            ],
            &[
                (ONE_TRANSFER, ONE_TRANSFER_LEASE),
                (TWO_TRANSFERS, TWO_TRANSFERS_LEASE),
            ],
        ),
        Cell::new(
            "41 nameless resources, one anchor without a lease",
            std::iter::once(NO_LEASE).chain(fillers(40)).collect(),
            &[],
        ),
        Cell::new(
            "ten anchors, four with live leases",
            anchors(),
            &MAPPED_ANCHORS,
        ),
        Cell::new(
            "167 nameless resources holding the ten anchors",
            anchors().into_iter().chain(fillers(FILLERS)).collect(),
            &MAPPED_ANCHORS,
        ),
        Cell::new(
            "an anchor whose name's surface node is its own node",
            vec![SURFACE_KEY],
            &[(SURFACE_KEY, SURFACE_KEY_LEASE)],
        ),
        Cell::new(
            "an anchor with registry-only authority on two nodes",
            vec![TWO_NODES],
            &[(TWO_NODES, TWO_NODES_LEASE)],
        ),
        Cell::new(
            "a Basenames anchor with a live lease and a transfer",
            vec![BASENAMES],
            &[(BASENAMES, BASENAMES_LEASE)],
        ),
    ];
    cells.last_mut().expect("a Basenames cell").basenames = true;
    for (label, lease, resource, basenames) in [
        (
            "the controls of a requested lease",
            ONE_TRANSFER_LEASE,
            ONE_TRANSFER,
            false,
        ),
        (
            "the controls of a requested Basenames lease",
            BASENAMES_LEASE,
            BASENAMES,
            true,
        ),
    ] {
        cells.push(Cell {
            label,
            ids: Vec::new(),
            registration: Some(lease),
            mapped: vec![(resource, lease)],
            basenames,
        });
    }
    cells
}

async fn check_default_plans(
    connection: &mut PgConnection,
    lineage: Lineage,
) -> Result<Vec<String>> {
    let mut failures = Vec::new();
    for cell in cells() {
        let literals = cell.literals();
        for mode in PLAN_MODES {
            let label = format!("{lineage:?}, {}, {mode}", cell.label);
            let now = explain(connection, mode, "map_now", &literals, false).await?;
            let previous = explain(connection, mode, "map_previous", &literals, false).await?;
            let plain = explain(connection, mode, "map_plain", &literals, false).await?;
            println!(
                "{label}\n  this text:\n{}\n  previous text:\n{}\n  plain lineage join:\n{}",
                outline(&now),
                outline(&previous),
                outline(&plain)
            );
            let folded = cell.ids.is_empty() && mode == "force_custom_plan";
            let mut cell_failures = event_read_failures(&now, folded);
            cell_failures.extend(lineage_failures(&now));
            cell_failures.extend(probes_failures(&now));
            if basenames_probes(&now).is_empty() {
                cell_failures.push(format!(
                    "the Basenames arm does not seek {BASENAMES_PROBE_INDEX} by its node key"
                ));
            }
            if !basenames_probes(&previous).is_empty() {
                cell_failures.push(format!(
                    "the previous text seeks {BASENAMES_PROBE_INDEX}, so the Basenames arm \
                     proves nothing"
                ));
            }
            if lineage == Lineage::Bulked {
                let analyzed = explain(connection, mode, "map_now", &literals, true).await?;
                cell_failures.extend(event_read_failures(&analyzed, folded));
                cell_failures.extend(lineage_failures(&analyzed));
                cell_failures.extend(temp_failures(&analyzed));
                let executed = basenames_probes(&analyzed)
                    .into_iter()
                    .filter(|node| node["Actual Loops"].as_f64().unwrap_or_default() > 0.0)
                    .count();
                if cell.basenames && executed < 2 {
                    cell_failures.push(format!(
                        "{executed} executed {BASENAMES_PROBE_INDEX} seeks, expected the \
                         control and lease arms"
                    ));
                }
                if !lineage_read_whole(&plain) {
                    cell_failures.push(
                        "the plain lineage join probes the bulked lineage, so this cell proves \
                         nothing about the hash"
                            .to_owned(),
                    );
                }
            }
            failures.extend(prefixed(&label, cell_failures, &now));
        }
    }
    Ok(failures)
}

/// With nested loops disabled, a plain lineage join can only hash or merge the whole lineage
/// table, as it did when mainnet's estimates priced the nested loop out. The keyed lookup has
/// no other plan than one probe per row.
async fn check_plans_without_nested_loops(connection: &mut PgConnection) -> Result<Vec<String>> {
    raw_sql("SET enable_nestloop = off")
        .execute(&mut *connection)
        .await?;
    let mut failures = Vec::new();
    for cell in cells() {
        let literals = cell.literals();
        for mode in PLAN_MODES {
            let label = format!("no nested loops, {}, {mode}", cell.label);
            let now = explain(connection, mode, "map_now", &literals, false).await?;
            let plain = explain(connection, mode, "map_plain", &literals, false).await?;
            let previous = explain(connection, mode, "map_previous", &literals, false).await?;
            println!(
                "{label}\n  this text:\n{}\n  plain lineage join:\n{}",
                outline(&now),
                outline(&plain)
            );
            let mut cell_failures = lineage_failures(&now);
            for (text, plan) in [("plain lineage join", &plain), ("previous", &previous)] {
                if !lineage_read_whole(plan) {
                    cell_failures.push(format!(
                        "the {text} text still probes the lineage, so this cell proves \
                         nothing\n{}",
                        outline(plan)
                    ));
                }
            }
            failures.extend(prefixed(&label, cell_failures, &now));
        }
    }
    raw_sql("RESET enable_nestloop")
        .execute(&mut *connection)
        .await?;
    Ok(failures)
}

fn prefixed(label: &str, failures: Vec<String>, plan: &Value) -> Vec<String> {
    if failures.is_empty() {
        return failures;
    }
    vec![format!(
        "{label}: {}\n{}",
        failures.join("; "),
        outline(plan)
    )]
}

async fn prepare_texts(connection: &mut PgConnection) -> Result<()> {
    raw_sql("DEALLOCATE ALL").execute(&mut *connection).await?;
    // sqlx's statement cache still names the statements DEALLOCATE dropped.
    connection.clear_cached_statements().await?;
    for (name, sql) in [
        ("map_now", registry_permission_registration_map_sql()),
        ("map_previous", PREVIOUS.to_owned()),
        ("map_plain", PLAIN_LINEAGE_JOIN.to_owned()),
    ] {
        raw_sql(&format!("PREPARE {name} AS {sql}"))
            .execute(&mut *connection)
            .await
            .with_context(|| format!("prepare {name}"))?;
    }
    Ok(())
}

async fn plan_counts(connection: &mut PgConnection, statement: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = $1",
    )
    .bind(statement)
    .persistent(false)
    .fetch_one(&mut *connection)
    .await?)
}

/// `EXPLAIN (FORMAT JSON)` of one execution in `mode`, checked against the plan counters.
async fn explain(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    literals: &str,
    analyze: bool,
) -> Result<Value> {
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(&mut *connection)
        .await?;
    let (generic, custom) = plan_counts(connection, statement).await?;
    let options = if analyze {
        "ANALYZE, BUFFERS, FORMAT JSON"
    } else {
        "FORMAT JSON"
    };
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN ({options}) EXECUTE {statement}{literals}"
    ))
    .persistent(false)
    .fetch_one(&mut *connection)
    .await
    .with_context(|| format!("explain {statement} under {mode}"))?;
    let expected = if mode == "force_generic_plan" {
        (generic + 1, custom)
    } else {
        (generic, custom + 1)
    };
    let counts = plan_counts(connection, statement).await?;
    ensure!(
        counts == expected,
        "{statement} under {mode}: plan counts {counts:?}, expected {expected:?}"
    );
    Ok(plan)
}

async fn execute(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    literals: &str,
) -> Result<BTreeSet<(Uuid, Uuid)>> {
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(&mut *connection)
        .await?;
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(&format!("EXECUTE {statement}{literals}"))
        .persistent(false)
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("execute {statement} under {mode}"))?;
    Ok(rows.into_iter().collect())
}

/// Every plan node with its parent, init plans and sub-plans included.
fn nodes(plan: &Value) -> Vec<(&Value, Option<&Value>)> {
    fn walk<'a>(
        node: &'a Value,
        parent: Option<&'a Value>,
        output: &mut Vec<(&'a Value, Option<&'a Value>)>,
    ) {
        output.push((node, parent));
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, Some(node), output);
        }
    }
    let mut output = Vec::new();
    walk(&plan[0]["Plan"], None, &mut output);
    output
}

fn descendants(node: &Value) -> Vec<&Value> {
    let mut output = Vec::new();
    for child in node["Plans"].as_array().into_iter().flatten() {
        output.push(child);
        output.extend(descendants(child));
    }
    output
}

fn node_type(node: &Value) -> &str {
    node["Node Type"].as_str().unwrap_or("?")
}

fn index_condition(node: &Value) -> &str {
    node["Index Cond"].as_str().unwrap_or_default()
}

/// Every `normalized_events` read is an index seek whose condition names a statement key, and
/// none goes through an index keyed by the address of an event's new holder.
///
/// A request for one lease's controls passes no resource ids. A custom plan folds the anchors'
/// `resource_id = ANY('{}')` arm to false, and false implies every partial index predicate, so
/// the planner may give that arm a bitmap over any small partial index, with no condition. That
/// arm is outside this change, so `folded_empty_array` admits a bitmap index read with no
/// condition and nothing else.
fn event_read_failures(plan: &Value, folded_empty_array: bool) -> Vec<String> {
    let mut failures = Vec::new();
    let mut indexes = Vec::new();
    for (node, _) in nodes(plan) {
        if node["Relation Name"] != "normalized_events" {
            continue;
        }
        match node_type(node) {
            "Index Scan" | "Index Only Scan" => indexes.push(node),
            "Bitmap Heap Scan" => indexes.extend(
                descendants(node)
                    .into_iter()
                    .filter(|child| node_type(child) == "Bitmap Index Scan"),
            ),
            other => failures.push(format!(
                "a {other} on normalized_events {}",
                node["Alias"].as_str().unwrap_or_default()
            )),
        }
    }
    for node in indexes {
        let index = node["Index Name"].as_str().unwrap_or("?");
        let condition = index_condition(node);
        if folded_empty_array && condition.is_empty() && node_type(node) == "Bitmap Index Scan" {
            continue;
        }
        if index.starts_with("normalized_events_address_")
            || !STATEMENT_KEYS.iter().any(|key| condition.contains(key))
        {
            failures.push(format!(
                "normalized_events read through {index} with condition {condition:?}"
            ));
        }
    }
    failures
}

/// The lineage scan a join input reads, through a hash, materialize, memoize or sort.
fn lineage_input(node: &Value) -> Option<&Value> {
    if node["Relation Name"] == "chain_lineage" {
        return Some(node);
    }
    match node_type(node) {
        "Hash" | "Materialize" | "Memoize" | "Sort" | "Incremental Sort" | "Gather"
        | "Gather Merge" => {
            let children = node["Plans"].as_array()?;
            match children.as_slice() {
                [only] => lineage_input(only),
                _ => None,
            }
        }
        _ => None,
    }
}

fn lineage_hashed_or_merged(plan: &Value) -> bool {
    nodes(plan).into_iter().any(|(node, _)| {
        matches!(node_type(node), "Hash Join" | "Merge Join")
            && node["Plans"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|input| lineage_input(input).is_some())
    })
}

/// Every `chain_lineage` read is an index probe on the event's block directly under the lookup's
/// `LIMIT 1`, and no hash or merge join reads the lineage.
fn lineage_failures(plan: &Value) -> Vec<String> {
    let mut failures = Vec::new();
    for (node, parent) in nodes(plan) {
        if node["Relation Name"] != "chain_lineage" {
            continue;
        }
        let alias = node["Alias"].as_str().unwrap_or_default();
        let condition = index_condition(node);
        if !matches!(node_type(node), "Index Scan" | "Index Only Scan")
            || !(condition.contains("block_hash") || condition.contains("block_number"))
        {
            failures.push(format!(
                "lineage {alias} read by a {} with condition {condition:?}",
                node_type(node)
            ));
        }
        if parent.map(node_type) != Some("Limit") {
            failures.push(format!(
                "lineage {alias} is read under a {}, not the lookup's LIMIT 1",
                parent.map_or("statement root", node_type)
            ));
        }
    }
    if lineage_hashed_or_merged(plan) {
        failures.push("a hash or merge join reads the lineage".to_owned());
    }
    failures
}

/// The plain lineage join reads the lineage table whole: a sequential scan, an index scan with
/// no condition, or a hash or merge join input.
fn lineage_read_whole(plan: &Value) -> bool {
    lineage_hashed_or_merged(plan)
        || nodes(plan).into_iter().any(|(node, _)| {
            node["Relation Name"] == "chain_lineage"
                && (node_type(node) == "Seq Scan" || node.get("Index Cond").is_none())
        })
}

/// The candidate's probe set is one materialized CTE that both branches of the control lookup
/// read: the direct authority rows and the token transfers behind authority epochs.
fn probes_failures(plan: &Value) -> Vec<String> {
    let scans = nodes(plan)
        .into_iter()
        .filter(|(node, _)| node_type(node) == "CTE Scan" && node["CTE Name"] == "probes")
        .count();
    if scans < 2 {
        vec![format!(
            "{scans} reads of the probes CTE, expected both control branches"
        )]
    } else {
        Vec::new()
    }
}

fn basenames_probes(plan: &Value) -> Vec<&Value> {
    nodes(plan)
        .into_iter()
        .map(|(node, _)| node)
        .filter(|node| {
            node["Index Name"] == BASENAMES_PROBE_INDEX
                && index_condition(node).contains("COALESCE(")
        })
        .collect()
}

fn temp_failures(plan: &Value) -> Vec<String> {
    nodes(plan)
        .into_iter()
        .filter(|(node, _)| node["Temp Written Blocks"].as_i64().unwrap_or_default() > 0)
        .map(|(node, _)| format!("a {} writes temp blocks", node_type(node)))
        .collect()
}

/// The plan's node types, one per line, with each scan's alias, index and condition.
fn outline(plan: &Value) -> String {
    fn walk(node: &Value, depth: usize, lines: &mut Vec<String>) {
        let mut line = format!("{}{}", "  ".repeat(depth), node_type(node));
        if let Some(name) = node["Subplan Name"].as_str() {
            line.push_str(&format!(" {name}"));
        }
        if let Some(name) = node["CTE Name"].as_str() {
            line.push_str(&format!(" on {name}"));
        }
        if let Some(relation) = node["Relation Name"].as_str() {
            line.push_str(&format!(
                " on {relation} {}",
                node["Alias"].as_str().unwrap_or_default()
            ));
        }
        if let Some(index) = node["Index Name"].as_str() {
            line.push_str(&format!(" using {index} [{}]", index_condition(node)));
        }
        if let Some(loops) = node["Actual Loops"].as_f64() {
            line.push_str(&format!(" loops={loops}"));
        }
        lines.push(line);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, depth + 1, lines);
        }
    }
    let mut lines = Vec::new();
    walk(&plan[0]["Plan"], 2, &mut lines);
    lines.join("\n")
}

fn rid(id: u128) -> Uuid {
    Uuid::from_u128(id)
}

fn node(id: u128) -> String {
    format!("0x{id:064x}")
}

struct Event {
    identity: String,
    chain: &'static str,
    family: &'static str,
    kind: &'static str,
    resource: u128,
    logical_name_id: Option<String>,
    block: i64,
    log: i64,
    emitter: &'static str,
    state: Value,
}

fn families(chain: &str) -> (&'static str, &'static str, &'static str, &'static str) {
    if chain == BASE {
        (
            "basenames_base_registry",
            "basenames_base_registrar",
            "0x00000000000000000000000000000000000000b1",
            "0x00000000000000000000000000000000000000b2",
        )
    } else {
        (
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "0x00000000000000000000000000000000000000a1",
            "0x00000000000000000000000000000000000000a2",
        )
    }
}

/// Registry-only authority of `resource` on `node`: an ENSRegistry or Basenames registry
/// `Transfer` to the owner while the name has no registrar lease of its own on this resource.
fn control(chain: &'static str, resource: u128, node_id: u128, block: i64) -> Event {
    let (registry, _, emitter, _) = families(chain);
    let node = node(node_id);
    Event {
        identity: format!("control:{resource:x}:{node_id:x}"),
        chain,
        family: registry,
        kind: "AuthorityTransferred",
        resource,
        logical_name_id: None,
        block,
        log: 0,
        emitter,
        state: json!({"source_event": "Transfer", "node": node, "owner": OWNER,
            "authority_kind": "registry_only",
            "authority_key": format!("registry-only:{chain}:{node}")}),
    }
}

fn lease_event(
    chain: &'static str,
    kind: &'static str,
    lease: u128,
    node_id: u128,
    block: i64,
) -> Event {
    let (_, registrar, _, emitter) = families(chain);
    Event {
        identity: format!("{kind}:{lease:x}:{block}"),
        chain,
        family: registrar,
        kind,
        resource: lease,
        logical_name_id: None,
        block,
        log: 0,
        emitter,
        state: json!({"namehash": node(node_id), "registrant": HOLDER, "expiry": 1_900_000_000_i64}),
    }
}

/// A registrar token transfer: the `TokenControlTransferred` companion that names the node and
/// the node-less `AuthorityEpochChanged` of the same raw log.
fn transfer(chain: &'static str, lease: u128, node_id: u128, block: i64) -> [Event; 2] {
    let (_, registrar, _, emitter) = families(chain);
    let event = |kind: &'static str, state: Value| Event {
        identity: format!("{kind}:{lease:x}:{block}"),
        chain,
        family: registrar,
        kind,
        resource: lease,
        logical_name_id: None,
        block,
        log: 0,
        emitter,
        state,
    };
    [
        event(
            "TokenControlTransferred",
            json!({"source_event": "Transfer", "namehash": node(node_id), "from": HOLDER, "to": OWNER}),
        ),
        event(
            "AuthorityEpochChanged",
            json!({"source_event": "Transfer", "authority_kind": "registrar",
                "authority_key": format!("registrar:{chain}:{lease:x}"), "registry_owner": OWNER}),
        ),
    ]
}

/// Each anchor's events sit on blocks of their own, so no two anchors share a raw log. The
/// lease is granted and transferred before the registry-only authority, which stays the latest.
fn fixture_events() -> Vec<Event> {
    let mut events = Vec::new();
    let at = |anchor: i64, offset: i64| 500 + 10 * anchor + offset;
    events.push(control(ENS, NO_LEASE, NO_LEASE, at(1, 5)));
    events.push(lease_event(
        ENS,
        "RegistrationGranted",
        ONE_TRANSFER_LEASE,
        ONE_TRANSFER,
        at(2, 1),
    ));
    events.extend(transfer(ENS, ONE_TRANSFER_LEASE, ONE_TRANSFER, at(2, 2)));
    events.push(control(ENS, ONE_TRANSFER, ONE_TRANSFER, at(2, 5)));
    events.push(lease_event(
        ENS,
        "RegistrationGranted",
        TWO_TRANSFERS_LEASE,
        TWO_TRANSFERS,
        at(3, 1),
    ));
    events.extend(transfer(ENS, TWO_TRANSFERS_LEASE, TWO_TRANSFERS, at(3, 2)));
    events.extend(transfer(ENS, TWO_TRANSFERS_LEASE, TWO_TRANSFERS, at(3, 3)));
    events.push(control(ENS, TWO_TRANSFERS, TWO_TRANSFERS, at(3, 5)));
    events.push(lease_event(
        ENS,
        "RegistrationGranted",
        RELEASED_LEASE,
        RELEASED,
        at(4, 1),
    ));
    events.push(lease_event(
        ENS,
        "RegistrationReleased",
        RELEASED_LEASE,
        RELEASED,
        at(4, 4),
    ));
    events.push(control(ENS, RELEASED, RELEASED, at(4, 5)));
    events.push(Event {
        identity: "eac:role".to_owned(),
        chain: ENS,
        family: "ens_v2_registry_l1",
        kind: "PermissionChanged",
        resource: EAC_RESOURCE,
        logical_name_id: None,
        block: at(4, 6),
        log: 0,
        emitter: "0x00000000000000000000000000000000000000e1",
        state: json!({"subject": OWNER, "scope": {"kind": "resource"}}),
    });
    events.push(lease_event(
        ENS,
        "RegistrationGranted",
        SURFACE_KEY_LEASE,
        SURFACE_KEY,
        at(5, 1),
    ));
    events.extend(transfer(ENS, SURFACE_KEY_LEASE, SURFACE_KEY, at(5, 2)));
    let mut surface_control = control(ENS, SURFACE_KEY, SURFACE_KEY, at(5, 5));
    surface_control.logical_name_id = Some(format!("ens:{}", node(SURFACE_KEY)));
    events.push(surface_control);
    events.push(lease_event(
        ENS,
        "RegistrationGranted",
        TWO_NODES_LEASE,
        TWO_NODES,
        at(6, 1),
    ));
    events.extend(transfer(ENS, TWO_NODES_LEASE, TWO_NODES, at(6, 2)));
    events.push(control(ENS, TWO_NODES, TWO_NODES, at(6, 5)));
    events.push(control(ENS, TWO_NODES, TWO_NODES_SECOND_NODE, at(6, 6)));
    for (index, anchor) in (7..).zip(MORE_ANCHORS) {
        events.push(control(ENS, anchor, anchor, at(index, 5)));
    }
    events.push(lease_event(
        BASE,
        "RegistrationGranted",
        BASENAMES_LEASE,
        BASENAMES,
        at(11, 1),
    ));
    events.extend(transfer(BASE, BASENAMES_LEASE, BASENAMES, at(11, 2)));
    events.push(control(BASE, BASENAMES, BASENAMES, at(11, 5)));
    for (index, filler) in (0..).zip(fillers(FILLERS)) {
        events.push(Event {
            identity: format!("filler:{filler:x}"),
            chain: ENS,
            family: "ens_v1_registry_l1",
            kind: "PermissionChanged",
            resource: filler,
            logical_name_id: None,
            block: 700 + index,
            log: 0,
            emitter: "0x00000000000000000000000000000000000000a1",
            state: json!({"subject": OWNER, "scope": {"kind": "resource"},
                "effective_powers": ["resource_control"]}),
        });
    }
    events
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

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/04_manifests.sql"),
        include_str!("../../schema/baseline/05_normalized_events.sql"),
    ] {
        raw_sql(baseline).execute(&mut *connection).await?;
    }
    raw_sql(FIXTURE_SQL)
        .execute(&mut *connection)
        .await
        .context("install the lineage, resources and unrelated events")?;
    let events = fixture_events();
    let resources = events
        .iter()
        .map(|event| (event.chain, event.resource))
        .collect::<BTreeSet<_>>();
    for (chain, resource) in resources {
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
             VALUES ($1, $2, 'block:' || $2 || ':1', 1, 'canonical')",
        )
        .bind(rid(resource))
        .bind(chain)
        .execute(&mut *connection)
        .await?;
    }
    let surface = node(SURFACE_KEY);
    sqlx::query(
        "INSERT INTO name_surfaces
             (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
              labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
              block_number, canonicality_state)
         VALUES ('ens:' || $1, 'ens', 'surface.eth', ARRAY['surface', 'eth'], '\\x00'::bytea, $1,
                 ARRAY[$1, 'eth'], 'test', 'active', $2, 'block:' || $2 || ':1', 1, 'canonical')",
    )
    .bind(&surface)
    .bind(ENS)
    .execute(&mut *connection)
    .await?;
    for event in events {
        let block_hash = format!("block:{}:{}", event.chain, event.block);
        let transaction_hash = format!("tx:{}:{}", event.chain, event.block);
        let raw_fact_ref = json!({"kind": "raw_log", "chain_id": event.chain,
            "block_hash": block_hash, "block_number": event.block,
            "transaction_hash": transaction_hash, "transaction_index": 0,
            "log_index": event.log, "emitting_address": event.emitter});
        let derivation = if event.family.starts_with("ens_v2_") {
            "ens_v2_permissions"
        } else {
            "ens_v1_unwrapped_authority"
        };
        sqlx::query(
            "INSERT INTO normalized_events
                 (event_identity, namespace, logical_name_id, resource_id, event_kind,
                  source_family, manifest_version, chain_id, block_number, block_hash,
                  transaction_hash, transaction_index, log_index, raw_fact_ref, derivation_kind,
                  canonicality_state, after_state)
             VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, $9, $10, 0, $11, $12, $13, 'canonical', $14)",
        )
        .bind(&event.identity)
        .bind(if event.chain == BASE { "basenames" } else { "ens" })
        .bind(&event.logical_name_id)
        .bind(rid(event.resource))
        .bind(event.kind)
        .bind(event.family)
        .bind(event.chain)
        .bind(event.block)
        .bind(&block_hash)
        .bind(&transaction_hash)
        .bind(event.log)
        .bind(&raw_fact_ref)
        .bind(derivation)
        .bind(&event.state)
        .execute(&mut *connection)
        .await
        .with_context(|| format!("insert {}", event.identity))?;
    }
    raw_sql("ANALYZE; SET jit = off; SET max_parallel_workers_per_gather = 0")
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// Lineage for blocks 1 to 1,000 on both chains, and unrelated ENSv1 and Basenames events on
/// blocks below the anchors: registry transfers, registrar grants and token transfers, each on
/// a node of its own. They give every index the statement could read a size, so a walk of one
/// costs more than a seek.
const FIXTURE_SQL: &str = r#"
ALTER TABLE normalized_events SET (autovacuum_enabled = false);
ALTER TABLE chain_lineage SET (autovacuum_enabled = false);
INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT chain, 'block:' || chain || ':' || n, n, to_timestamp(1700000000 + n), 'canonical'
FROM unnest(ARRAY['ethereum-mainnet', 'base-mainnet']) chain, generate_series(1, 1000) n;

INSERT INTO normalized_events
    (event_identity, namespace, resource_id, event_kind, source_family, manifest_version,
     chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index,
     raw_fact_ref, derivation_kind, canonicality_state, after_state)
SELECT 'unrelated:' || chain || ':' || kind || ':' || n,
       CASE WHEN chain = 'base-mainnet' THEN 'basenames' ELSE 'ens' END, NULL, kind,
       CASE WHEN kind = 'AuthorityTransferred' THEN registry ELSE registrar END, 1, chain,
       block, 'block:' || chain || ':' || block, 'tx:' || chain || ':' || block, 0, log,
       jsonb_build_object('kind', 'raw_log', 'chain_id', chain,
           'block_hash', 'block:' || chain || ':' || block, 'block_number', block,
           'transaction_hash', 'tx:' || chain || ':' || block, 'transaction_index', 0,
           'log_index', log, 'emitting_address',
           CASE WHEN kind = 'AuthorityTransferred' THEN registry_address ELSE registrar_address END),
       'ens_v1_unwrapped_authority', 'canonical',
       CASE kind
           WHEN 'AuthorityTransferred' THEN jsonb_build_object('source_event', 'Transfer',
               'node', node, 'owner', holder, 'authority_kind', 'registry_only')
           WHEN 'RegistrationGranted' THEN jsonb_build_object('namehash', node,
               'registrant', holder, 'expiry', 1900000000)
           ELSE jsonb_build_object('source_event', 'Transfer', 'namehash', node, 'to', holder)
       END
FROM (VALUES
        ('ethereum-mainnet', 'ens_v1_registry_l1', 'ens_v1_registrar_l1',
         '0x00000000000000000000000000000000000000a1', '0x00000000000000000000000000000000000000a2', 3000),
        ('base-mainnet', 'basenames_base_registry', 'basenames_base_registrar',
         '0x00000000000000000000000000000000000000b1', '0x00000000000000000000000000000000000000b2', 500)
     ) families(chain, registry, registrar, registry_address, registrar_address, names),
     unnest(ARRAY['AuthorityTransferred', 'RegistrationGranted', 'TokenControlTransferred']) kind,
     LATERAL generate_series(1, CASE WHEN kind = 'AuthorityTransferred' THEN names ELSE names / 2 END) n,
     LATERAL (SELECT 1 + n % 400 AS block,
                     n + CASE kind WHEN 'AuthorityTransferred' THEN 0
                                   WHEN 'RegistrationGranted' THEN 100000 ELSE 200000 END AS log,
                     '0x' || lpad(to_hex(65536 + n), 64, '0') AS node,
                     '0x' || lpad(to_hex(n), 40, '0') AS holder) shape;
"#;

/// Lineage rows on the ENS chain far above every event, so hashing the chain's lineage costs
/// more than probing it once per event row.
const BULK_LINEAGE_SQL: &str = r#"
INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT 'ethereum-mainnet', 'pad:' || n, 10000 + n, to_timestamp(1700010000 + n), 'canonical'
FROM generate_series(1, 200000) n;
ANALYZE chain_lineage;
"#;
