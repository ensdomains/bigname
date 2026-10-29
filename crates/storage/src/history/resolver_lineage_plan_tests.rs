//! Plan and row tests for how a resolver page's arms reach `chain_lineage`. The route is
//! `GET /v1/events?resolver=<chain_id>:<address>`: the events handler hands the filter to
//! `load_event_history_page_with_redo_policy`, whose page statement (`push_history_page_query`)
//! takes its source from `push_history_source_for_page`. When the request has the arm shape,
//! each arm part (`push_resolver_candidate_select`) reads its candidate rows' lineage
//! canonicality before its own page limit. The count statement takes no page bound and reads no
//! candidate lineage.
//!
//! An arm part's candidates are every row the resolver emitted or was set to or cleared from on
//! its chain, which for a busy resolver is millions of rows. As a plain join the planner may hash
//! or walk the whole lineage to join them. Each part now probes its candidate's lineage row by
//! key with a correlated `LIMIT 1` lookup, so no planner state can do that: the plan test plans
//! each page with nested loops disabled and still expects the probe, while the previous plain
//! join reads the lineage another way there. The row test pins the probe's rows to the previous
//! join's rows, including rows without a block and rows on orphaned blocks.
use sqlx::{Row, postgres::PgArguments, raw_sql};

use super::super::super::{keyset::HistoryKeyset, lineage::lineage_probe};
use super::*;

const LINEAGE_RESOLVER: &str = "0x00000000000000000000000000000000000000a9";
const LINEAGE_PAGE_LIMIT: i64 = 51;
/// Three arms, each split into the part with a block and the part without one.
const ARM_PARTS: usize = 6;

/// 200,000 canonical blocks and three orphaned ones. The resolver emitted 19,999 rows, was set
/// on 19,999 and cleared from 19,999, ten blocks apart per arm. Each arm also has two rows
/// without a block and a row on an orphaned block at each end of the chain, which only the
/// lineage canonicality drops.
const LINEAGE_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT 'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, to_timestamp(n),
           'canonical'::canonicality_state
    FROM generate_series(1, 200000) n
    UNION ALL
    SELECT 'ethereum-sepolia', 'ethereum-sepolia-orphan-' || n, n, to_timestamp(n), 'orphaned'
    FROM unnest(ARRAY[3, 4, 199990]) n;
    CREATE TEMP TABLE fixture_rows (identity text, arm text, block_number bigint, block_hash text);
    INSERT INTO fixture_rows
    SELECT 'lineage:' || arm || ':' || n, arm, 10 * n + step,
           'ethereum-sepolia-block-' || (10 * n + step)
    FROM (VALUES ('emit', 0), ('set', 1), ('clear', 2)) arms (arm, step)
    CROSS JOIN generate_series(1, 19999) n
    UNION ALL
    SELECT 'lineage:' || arm || ':' || member, arm, block_number, block_hash
    FROM (VALUES ('emit', 3), ('set', 4), ('clear', 3)) arms (arm, old_orphan)
    CROSS JOIN LATERAL (VALUES
        ('blockless-1', NULL::bigint, NULL::text),
        ('blockless-2', NULL, NULL),
        ('orphaned-old', old_orphan, 'ethereum-sepolia-orphan-' || old_orphan),
        ('orphaned-new', 199990, 'ethereum-sepolia-orphan-199990')
    ) member (member, block_number, block_hash);
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT identity, 'ens',
           CASE WHEN arm = 'emit' THEN 'RecordChanged' ELSE 'ResolverChanged' END,
           CASE WHEN arm = 'emit' THEN 'ens_v1_resolver_l1' ELSE 'ens_v1_registry_l1' END,
           1, 'ethereum-sepolia', block_hash, block_number,
           CASE WHEN block_hash IS NULL THEN NULL ELSE 'tx-' || identity END,
           CASE WHEN block_hash IS NULL THEN NULL ELSE 0 END,
           CASE WHEN block_hash IS NULL THEN NULL ELSE 0 END,
           'ens_v1_unwrapped_authority', 'canonical',
           CASE arm
               WHEN 'emit' THEN jsonb_build_object('node', 'node-lineage')
               WHEN 'set' THEN jsonb_build_object(
                   'node', 'node-lineage', 'resolver', '0x00000000000000000000000000000000000000a9')
               ELSE jsonb_build_object(
                   'node', 'node-lineage', 'resolver', '0x0000000000000000000000000000000000000099')
           END,
           CASE arm
               WHEN 'emit' THEN '{}'::jsonb
               WHEN 'set' THEN jsonb_build_object(
                   'resolver', '0x0000000000000000000000000000000000000099')
               ELSE jsonb_build_object('resolver', '0x00000000000000000000000000000000000000a9')
           END,
           CASE WHEN arm = 'emit'
                THEN jsonb_build_object(
                    'emitting_address', '0x00000000000000000000000000000000000000a9')
                ELSE '{}'::jsonb END
    FROM fixture_rows;
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
"#;

/// The pages each test reads, with the rows each returns and how many have no block: both orders
/// from the start, and from a cursor whose next page reaches the end of the chain. Newest first
/// the first page passes the newest orphaned rows, and the page from the cursor passes the
/// oldest orphaned rows and ends with the rows without a block. Oldest first the first page
/// starts with them and passes the oldest orphaned rows, and the page from the cursor passes the
/// newest orphaned rows.
const PAGES: [(HistoryOrder, Option<&str>, usize, usize); 4] = [
    (HistoryOrder::Desc, None, 51, 0),
    (HistoryOrder::Asc, None, 51, 6),
    (HistoryOrder::Desc, Some("lineage:set:1"), 7, 6),
    (HistoryOrder::Asc, Some("lineage:emit:19995"), 14, 0),
];

#[tokio::test]
async fn resolver_arms_probe_the_candidate_lineage_by_key() -> Result<()> {
    with_pages("resolver_lineage_plan", async |connection, label, page| {
        let mut failures = Vec::new();
        for mode in [Mode::Custom, Mode::Generic] {
            let cell = format!("{label}, {mode:?}");
            let plan = explain(connection, mode, &page.sql, &page.arguments).await?;
            failures.extend(probe_failures(&cell, &plan, false));
            raw_sql("SET enable_nestloop = off")
                .execute(&mut *connection)
                .await?;
            let plan = explain(connection, mode, &page.sql, &page.arguments).await;
            let before = explain(connection, mode, &page.previous, &page.arguments).await;
            raw_sql("RESET enable_nestloop")
                .execute(&mut *connection)
                .await?;
            let cell = format!("{cell}, nested loops off");
            failures.extend(probe_failures(&cell, &plan?, true));
            let before = before?;
            if candidate_scans(&before)
                .into_iter()
                .all(|(node, hashed)| keyed(node) && !hashed)
            {
                failures.push(format!(
                    "{cell}: the previous statement also probes candidate_lineage by key, so \
                     this planner state proves nothing about it\n{before:#}"
                ));
            }
        }
        Ok(failures)
    })
    .await
}

#[tokio::test]
async fn resolver_arm_lineage_probe_keeps_every_row() -> Result<()> {
    with_pages("resolver_lineage_rows", async |connection, label, page| {
        let mut failures = Vec::new();
        let now = identities(connection, &page.sql, &page.arguments).await?;
        let before = identities(connection, &page.previous, &page.arguments).await?;
        if now != before {
            failures.push(format!(
                "{label}: {now:?}\nthe previous statement: {before:?}"
            ));
        }
        let blockless = now.iter().filter(|row| row.contains("blockless")).count();
        if (now.len(), blockless) != (page.rows, page.blockless) {
            failures.push(format!(
                "{label}: {} rows and {blockless} without a block, expected {} and {}",
                now.len(),
                page.rows,
                page.blockless
            ));
        }
        if let Some(row) = now.iter().find(|row| row.contains("orphaned")) {
            failures.push(format!("{label}: {row} lies on an orphaned block"));
        }
        Ok(failures)
    })
    .await
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Custom,
    Generic,
}

/// A page statement as the builders write it now and with the plain lineage join each arm part
/// had before, with its arguments and the rows and rows without a block it should return.
struct Page {
    sql: String,
    previous: String,
    arguments: PgArguments,
    rows: usize,
    blockless: usize,
}

/// Runs `check` on every page of [`PAGES`] in a database holding [`LINEAGE_FIXTURE`].
async fn with_pages(
    name: &str,
    check: impl AsyncFn(&mut PgConnection, &str, &Page) -> Result<Vec<String>>,
) -> Result<()> {
    let database = phase_database(name, LINEAGE_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        let mut failures = Vec::new();
        for (order, cursor, rows, blockless) in PAGES {
            let label = match cursor {
                Some(identity) => format!("{order:?} from {identity}"),
                None => format!("{order:?} from the start"),
            };
            let filter = resolver_filter(order, LINEAGE_RESOLVER);
            let cursor = match cursor {
                Some(identity) => Some(cursor_at(&mut connection, identity).await?),
                None => None,
            };
            let keyset: Option<HistoryKeyset<'_>> = match cursor.as_ref() {
                Some(cursor) => {
                    Some(load_history_keyset(&mut connection, &filter, true, cursor, false).await?)
                }
                None => None,
            };
            let mut page = page_statement(&filter, keyset.as_ref(), &label, &mut failures)?;
            page.rows = rows;
            page.blockless = blockless;
            failures.extend(check(&mut *connection, label.as_str(), &page).await?);
        }
        ensure!(
            failures.is_empty(),
            "resolver arm lineage:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

fn page_statement<'a>(
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
    label: &str,
    failures: &mut Vec<String>,
) -> Result<Page> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_page_query(
        &mut builder,
        filter,
        true,
        keyset,
        false,
        LINEAGE_PAGE_LIMIT,
    );
    let sql = builder.sql().to_owned();
    let probe = lineage_probe("candidate_lineage", "candidate");
    let previous = sql.replace(
        &probe,
        "LEFT JOIN bigname_phase.chain_lineage candidate_lineage \
         ON candidate_lineage.chain_id = candidate.chain_id \
         AND candidate_lineage.block_hash = candidate.block_hash",
    );
    let probes = sql.matches(&probe).count();
    if probes != ARM_PARTS || previous.contains(&probe) {
        failures.push(format!(
            "{label}: {probes} candidate lineage probes, so the previous statement could not be \
             rebuilt"
        ));
    }
    let arguments = builder
        .build()
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .unwrap_or_default();
    Ok(Page {
        sql,
        previous,
        arguments,
        rows: 0,
        blockless: 0,
    })
}

async fn identities(
    connection: &mut PgConnection,
    sql: &str,
    arguments: &PgArguments,
) -> Result<Vec<String>> {
    let rows = sqlx::query_with(sql, arguments.clone())
        .persistent(false)
        .fetch_all(&mut *connection)
        .await?;
    rows.iter()
        .map(|row| row.try_get::<String, _>("event_identity"))
        .collect::<Result<_, _>>()
        .map_err(anyhow::Error::from)
}

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

/// Every arm part probes its candidate's lineage row by key, and every other lineage read of the
/// page does too. With `nested_loops_off`, a hash or merge join may still read another lookup's
/// keyed probe as its inner side, as the registration identity's lease-release check does there.
fn probe_failures(cell: &str, plan: &Value, nested_loops_off: bool) -> Vec<String> {
    let mut failures = Vec::new();
    let candidates = candidate_scans(plan);
    if candidates.len() != ARM_PARTS {
        failures.push(format!(
            "{} candidate_lineage scans, one per arm part expected",
            candidates.len()
        ));
    }
    for (node, hashed) in lineage_scans(plan) {
        // With nested loops off, the page's other lookups may run as hash or merge joins with
        // their keyed lineage probe on the inner side. Only the arm parts' probe must not.
        let hashed = hashed && (!nested_loops_off || candidate(node));
        if !keyed(node) || hashed {
            failures.push(format!(
                "{} is read by {} with index condition {:?}{}",
                node["Alias"].as_str().unwrap_or_default(),
                node["Node Type"].as_str().unwrap_or_default(),
                node["Index Cond"].as_str().unwrap_or_default(),
                if hashed {
                    " under a hash or merge join"
                } else {
                    ""
                }
            ));
        }
    }
    if failures.is_empty() {
        return failures;
    }
    vec![format!("{cell}:\n{}\n{plan:#}", failures.join("\n"))]
}

/// Each `chain_lineage` scan, with whether a hash or merge join reads it as its inner side.
fn lineage_scans(plan: &Value) -> Vec<(&Value, bool)> {
    fn walk<'a>(node: &'a Value, hashed: bool, scans: &mut Vec<(&'a Value, bool)>) {
        if node["Relation Name"] == "chain_lineage" {
            scans.push((node, hashed));
        }
        let joins = node["Node Type"] == "Hash Join" || node["Node Type"] == "Merge Join";
        for child in node["Plans"].as_array().into_iter().flatten() {
            let inner = joins && child["Parent Relationship"] == "Inner";
            walk(child, hashed || inner, scans);
        }
    }
    let mut scans = Vec::new();
    walk(&plan[0]["Plan"], false, &mut scans);
    scans
}

/// The arm parts' scans, named `candidate_lineage` with the suffix EXPLAIN adds where an alias
/// repeats.
fn candidate_scans(plan: &Value) -> Vec<(&Value, bool)> {
    lineage_scans(plan)
        .into_iter()
        .filter(|(node, _)| candidate(node))
        .collect()
}

fn candidate(node: &Value) -> bool {
    node["Alias"]
        .as_str()
        .is_some_and(|alias| alias.starts_with("candidate_lineage"))
}

/// An index scan keyed by the lineage key, `(chain_id, block_hash)`.
fn keyed(node: &Value) -> bool {
    let condition = node["Index Cond"].as_str().unwrap_or_default();
    (node["Node Type"] == "Index Scan" || node["Node Type"] == "Index Only Scan")
        && condition.contains("chain_id =")
        && condition.contains("block_hash =")
}
