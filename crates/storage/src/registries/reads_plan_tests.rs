//! Plan and row tests for how the registry reads reach `chain_lineage`. Each read joins its
//! announcement or pointer rows to their block's lineage row `rb` through a correlated lookup by
//! the lineage key that ends in `LIMIT 1`, so it can only run as one index probe per row. On this
//! fixture the previous plain joins probe by cost too, so the red row is the planner state with
//! nested loops disabled, where each plain join scans or merges the whole lineage table and the
//! lookup still probes. The row test compares each read with the previous statement.

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{Arguments, Execute, PgConnection, Row, postgres::PgArguments, raw_sql};

use super::{
    lineage_probe, references_page_query, registry_contract_sql, subregistry_pointers_sql,
};

const MAINNET: &str = "ethereum-mainnet";
/// The registry the reads ask about, `0x…01`. Twenty names point at it from a block, one name
/// from a row with no block.
const REGISTRY: &str = "0x0000000000000000000000000000000000000001";
const SERVED: i64 = 1_000_000;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Custom,
    Generic,
}

/// A read as it is written now and as it read before, with its arguments.
struct Statement {
    read: &'static str,
    sql: String,
    previous: String,
    arguments: PgArguments,
}

impl Statement {
    fn new(read: &'static str, sql: String, arguments: PgArguments) -> Self {
        // `LEFT JOIN` first: the inner probe's text is the tail of the outer one's.
        let mut previous = sql.clone();
        for join in ["LEFT JOIN", "JOIN"] {
            previous = previous.replace(
                &lineage_probe(join),
                &format!(
                    "{join} bigname_phase.chain_lineage rb \
                     ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash"
                ),
            );
        }
        Self {
            read,
            sql,
            previous,
            arguments,
        }
    }
}

fn statements() -> Result<Vec<Statement>> {
    let mut contract = PgArguments::default();
    contract.add(MAINNET).map_err(anyhow::Error::from_boxed)?;
    contract.add(REGISTRY).map_err(anyhow::Error::from_boxed)?;
    contract
        .add(Some(SERVED))
        .map_err(anyhow::Error::from_boxed)?;
    let mut pointers = PgArguments::default();
    pointers
        .add([4100, 4097, 5096].map(logical_name).to_vec())
        .map_err(anyhow::Error::from_boxed)?;
    // Unbounded by a served block, so the pointer with no block reaches the lineage lookup.
    pointers
        .add(None::<i64>)
        .map_err(anyhow::Error::from_boxed)?;
    let mut references = references_page_query(MAINNET, REGISTRY, None, None, 25, true);
    let references_sql = references.sql().to_owned();
    let references_arguments = references
        .build()
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .unwrap_or_default();
    Ok(vec![
        Statement::new("registry contract", registry_contract_sql(), contract),
        Statement::new("subregistry pointers", subregistry_pointers_sql(), pointers),
        Statement::new("registry references", references_sql, references_arguments),
    ])
}

#[tokio::test]
async fn registry_reads_probe_the_lineage_by_key() -> Result<()> {
    with_fixture("registry_lineage_plan", async |connection| {
        let mut failures = Vec::new();
        for statement in statements()? {
            if statement.previous == statement.sql
                || statement.previous.contains("LATERAL (SELECT rb.")
            {
                failures.push(format!(
                    "{}: the lineage lookups no longer read as this test expects",
                    statement.read
                ));
            }
            for mode in [Mode::Custom, Mode::Generic] {
                let cell = format!("{}, {mode:?}", statement.read);
                let plan = explain(connection, mode, &statement.sql, &statement.arguments)
                    .await
                    .with_context(|| cell.clone())?;
                let mut found = lineage_joins(&plan);
                found.extend(rb_failures(&plan));
                for node in lineage_scans(&plan) {
                    if node["Node Type"] == "Seq Scan" {
                        found.push(format!(
                            "a sequential scan of chain_lineage {}",
                            alias(node)
                        ));
                    }
                }
                failures.extend(report(&cell, &plan, found));

                // With nested loops disabled a plain join to `rb` scans or merges the whole
                // lineage table. The correlated lookup has no plan but the probe.
                let cell = format!("{cell}, nested loops off");
                raw_sql("SET enable_nestloop = off")
                    .execute(&mut *connection)
                    .await?;
                let plan = explain(connection, mode, &statement.sql, &statement.arguments).await;
                let before =
                    explain(connection, mode, &statement.previous, &statement.arguments).await;
                raw_sql("RESET enable_nestloop")
                    .execute(&mut *connection)
                    .await?;
                let plan = plan.with_context(|| cell.clone())?;
                let before = before.with_context(|| format!("{cell}, previous statement"))?;
                failures.extend(report(&cell, &plan, rb_failures(&plan)));
                if rb_failures(&before).is_empty() {
                    failures.push(format!(
                        "{cell}: the previous statement also probes rb by key, so this cell \
                         proves nothing about it\n{before:#}"
                    ));
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "registry lineage plans:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn registry_reads_keep_every_row() -> Result<()> {
    with_fixture("registry_lineage_rows", async |connection| {
        let mut failures = Vec::new();
        // The contract's announcement and its first pointer, both with a block. The names'
        // pointers and the registry's 21 references, read with no served block, keep the pointer
        // with no block, with no block time.
        for (statement, (expected, timeless)) in
            statements()?.into_iter().zip([(2, 0), (3, 1), (21, 1)])
        {
            let now = rows(connection, &statement.sql, &statement.arguments).await?;
            let before = rows(connection, &statement.previous, &statement.arguments).await?;
            let without_time = now
                .iter()
                .filter(|row| row["block_timestamp"].is_null())
                .count();
            if now != before || now.len() != expected || without_time != timeless {
                failures.push(format!(
                    "{}: rows {now:?}\nprevious {before:?}\nexpected {expected} rows, {timeless} \
                     without a block time",
                    statement.read
                ));
            }
        }
        ensure!(
            failures.is_empty(),
            "registry lineage rows:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

fn logical_name(k: u32) -> String {
    format!("ens:0x{k:064x}")
}

/// The read's rows as JSON objects, sorted, since a union of arms has no order of its own.
async fn rows(
    connection: &mut PgConnection,
    sql: &str,
    arguments: &PgArguments,
) -> Result<Vec<Value>> {
    let mut rows: Vec<Value> = sqlx::query_scalar_with(
        &format!("SELECT to_jsonb(read) FROM ({sql}) read"),
        arguments.clone(),
    )
    .persistent(false)
    .fetch_all(&mut *connection)
    .await?;
    rows.sort_by_key(Value::to_string);
    Ok(rows)
}

/// The plan PostgreSQL picks with the values bound, or its generic plan for a prepared
/// statement.
async fn explain(
    connection: &mut PgConnection,
    mode: Mode,
    sql: &str,
    arguments: &PgArguments,
) -> Result<Value> {
    Ok(match mode {
        Mode::Custom => {
            sqlx::query_scalar_with(&format!("EXPLAIN (FORMAT JSON) {sql}"), arguments.clone())
                .persistent(false)
                .fetch_one(&mut *connection)
                .await?
        }
        Mode::Generic => raw_sql(&format!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {sql}"))
            .fetch_one(&mut *connection)
            .await?
            .try_get(0)?,
    })
}

/// Every node of the plan.
fn plan_nodes(plan: &Value) -> Vec<&Value> {
    fn walk<'a>(node: &'a Value, nodes: &mut Vec<&'a Value>) {
        nodes.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, nodes);
        }
    }
    let mut nodes = Vec::new();
    walk(&plan[0]["Plan"], &mut nodes);
    nodes
}

fn lineage_scans(plan: &Value) -> Vec<&Value> {
    plan_nodes(plan)
        .into_iter()
        .filter(|node| node["Relation Name"] == "chain_lineage")
        .collect()
}

fn alias(node: &Value) -> &str {
    node["Alias"].as_str().unwrap_or_default()
}

fn reads_lineage(node: &Value) -> bool {
    node["Relation Name"] == "chain_lineage"
        || node["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .any(reads_lineage)
}

/// The hash or merge joins whose inner side reads `chain_lineage`.
fn lineage_joins(plan: &Value) -> Vec<String> {
    plan_nodes(plan)
        .into_iter()
        .filter(|node| node["Node Type"] == "Hash Join" || node["Node Type"] == "Merge Join")
        .filter(|node| {
            node["Plans"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|child| child["Parent Relationship"] == "Inner" && reads_lineage(child))
        })
        .map(|node| {
            format!(
                "{} over chain_lineage",
                node["Node Type"].as_str().unwrap_or_default()
            )
        })
        .collect()
}

/// Every scan of the row's lineage row `rb` (`rb_1` and so on where the read repeats it) must be
/// an index scan keyed by the lineage key, and there must be one.
fn rb_failures(plan: &Value) -> Vec<String> {
    let scans = lineage_scans(plan)
        .into_iter()
        .filter(|node| {
            alias(node) == "rb"
                || alias(node)
                    .strip_prefix("rb_")
                    .is_some_and(|suffix| suffix.chars().all(|c| c.is_ascii_digit()))
        })
        .collect::<Vec<_>>();
    let mut failures = Vec::new();
    if scans.is_empty() {
        failures.push("no scan of rb".to_owned());
    }
    for node in scans {
        let condition = node["Index Cond"].as_str().unwrap_or_default();
        if (node["Node Type"] != "Index Scan" && node["Node Type"] != "Index Only Scan")
            || !condition.contains("chain_id =")
            || !condition.contains("block_hash =")
        {
            failures.push(format!(
                "{} is read by {} with index condition {condition:?}",
                alias(node),
                node["Node Type"].as_str().unwrap_or_default()
            ));
        }
    }
    failures
}

fn report(cell: &str, plan: &Value, failures: Vec<String>) -> Vec<String> {
    if failures.is_empty() {
        return failures;
    }
    vec![format!("{cell}: {}\n{plan:#}", failures.join("; "))]
}

async fn with_fixture(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
            .execute(&mut *connection)
            .await?;
        for baseline in [
            include_str!("../../schema/baseline/01_chain.sql"),
            include_str!("../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../schema/baseline/03_identity.sql"),
            include_str!("../../schema/baseline/04_manifests.sql"),
            include_str!("../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../schema/baseline/06_projections.sql"),
            include_str!("../../schema/baseline/07_labels.sql"),
        ] {
            raw_sql(baseline).execute(&mut *connection).await?;
        }
        raw_sql("SET jit = off").execute(&mut *connection).await?;
        raw_sql(FIXTURE_SQL)
            .execute(&mut *connection)
            .await
            .context("install the registry lineage fixture")?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

/// Lineage for blocks 1..=200,000 of Ethereum Mainnet, block n hashed `m:n`. Name k of
/// 4096..=5095 points at registry `k % 50 + 1` from block `100 + k`, so twenty point at
/// `REGISTRY`. Name 5096 points at `REGISTRY` from a row with no block. `REGISTRY` announced
/// itself at block 50.
const FIXTURE_SQL: &str = r#"
ALTER TABLE normalized_events SET (autovacuum_enabled = false);
ALTER TABLE chain_lineage SET (autovacuum_enabled = false);
CREATE FUNCTION pg_temp.node(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 64, '0') $$;
CREATE FUNCTION pg_temp.address(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 40, '0') $$;

INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp,
    canonicality_state)
SELECT 'ethereum-mainnet', 'm:' || n, CASE WHEN n > 1 THEN 'm:' || (n - 1) END, n,
    to_timestamp(1500000000 + n * 12), 'canonical'::canonicality_state
FROM generate_series(1, 200000) n;

INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes, normalizer_version,
    visibility_state, chain_id, block_hash, block_number, canonicality_state)
SELECT 'ens:' || pg_temp.node(k), 'ens', pg_temp.node(k), ARRAY[pg_temp.node(k)], 'fixture',
    'active', 'ethereum-mainnet', 'm:1', 1, 'canonical'
FROM generate_series(4096, 5096) k;

INSERT INTO normalized_events (event_identity, namespace, logical_name_id, event_kind,
    source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash,
    transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
    after_state)
SELECT 'pointer:' || k, 'ens', 'ens:' || pg_temp.node(k), 'SubregistryChanged',
    'ens_v2_registry_l1', 1, 'ethereum-mainnet', 100 + k, 'm:' || (100 + k), 'tx:' || k, 0, 0,
    jsonb_build_object('emitting_address', pg_temp.address(192)),
    'ens_v2_registry_resource_surface', 'canonical'::canonicality_state,
    jsonb_build_object('subregistry', pg_temp.address(k % 50 + 1))
FROM generate_series(4096, 5095) k
UNION ALL
SELECT 'pointer:no-block', 'ens', 'ens:' || pg_temp.node(5096), 'SubregistryChanged',
    'ens_v2_registry_l1', 1, 'ethereum-mainnet', NULL, NULL, NULL, NULL, NULL,
    jsonb_build_object('emitting_address', pg_temp.address(192)),
    'ens_v2_registry_resource_surface', 'canonical',
    jsonb_build_object('subregistry', pg_temp.address(1))
UNION ALL
SELECT 'announcement', 'ens', NULL, 'RegistryCreated', 'ens_v2_registry_l1', 1,
    'ethereum-mainnet', 50, 'm:50', 'tx:announcement', 0, 0,
    jsonb_build_object('emitting_address', pg_temp.address(1)),
    'ens_v2_registry_resource_surface', 'canonical', '{}';
ANALYZE;"#;
