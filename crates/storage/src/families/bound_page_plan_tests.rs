//! Cost test for the bound-name page (`name/bound.rs`) at the size of mainnet's busiest
//! resolvers. One resolver lists a million names whose surfaces store raw bytes and a hundred
//! thousand whose surfaces store none, each ordered by the name its summary stores. Beside them
//! it reaches names it does not list, and a thousand other resolvers list their own. One raw
//! name is also listed in a second namespace, so a page can start inside a run of equal order
//! names. The rows are inserted by SQL: the test is about the read.
//!
//! Most listed names are not exempt from the capability gate: every name without raw bytes,
//! which sort first, and nine raw names in ten. Under a resolver whose binding enumeration is
//! `supported` every listed name is served, and the page reads
//! `project_name_summary_bound_order_idx`. Under one whose enumeration is not, as on mainnet's
//! ENSv1 resolvers, only the exempt names are served, and the page reads
//! `project_name_summary_bound_exempt_order_idx`. A page of [`PAGE`] names, first or deep, must
//! read at most `PAGE + 1` rows of its index under either plan, so the names the gate drops cost
//! nothing.
//!
//! The count reads the same index in the same order under a limit. Capped, it reads at most
//! `CAP + 1` rows of the index under either plan and runs the surface fences once for each. A
//! thousand more listed names, after every other in page order, have a surface above the family
//! marker, so the fences drop them and the uncapped count leaves them out, as the fences written
//! as `EXISTS` do.

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgConnection, Row, raw_sql};

use super::{
    id_index_plan_tests::{PLAN_MODES, with_database},
    name::{bound_count, bound_count_plan, bound_count_sql, bound_page_sql},
};

const CHAIN: &str = "ethereum-mainnet";
const LISTED: i64 = 1_000_000;
/// Names the resolver serves that fail the listing rules, and names of other resolvers.
const UNLISTED: i64 = 50_000;
const OTHER: i64 = 50_000;
/// Listed names whose surface stores no raw bytes.
const TEXTLESS: i64 = 100_000;
/// One listed raw name in this many is exempt from the capability gate.
const EXEMPT_EVERY: i64 = 10;
const PAGE: i64 = 20;
const ORDER_INDEX: &str = "project_name_summary_bound_order_idx";
const EXEMPT_INDEX: &str = "project_name_summary_bound_exempt_order_idx";
/// The label hash of `eth`'s stand-in, whose preimage the fixture imports.
const ETH: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
/// The raw name listed in both namespaces, exempt in both.
const TWIN: i64 = 900_000;
/// The cap of the capped count legs, below both listed totals.
const CAP: i64 = 50_000;
/// Listed names whose surface is above the family marker, exempt, sorting after every other.
const FENCED: i64 = 1_000;

fn resolver(digit: char) -> String {
    format!("0x{}", digit.to_string().repeat(40))
}

/// A page cursor's keys: order name, namespace and namehash.
type Key = (String, &'static str, String);

/// The keys of raw-name name `n` in namespace `ens`.
fn raw_key(n: i64) -> Key {
    (format!("n{n:07}.eth"), "ens", format!("0x{n:064x}"))
}

/// The keys of textless name `n`: its two labels as label hashes, though the fixture imports a
/// preimage of the second.
fn textless_key(n: i64) -> Key {
    (
        format!("[{:064x}].[{ETH}]", 0x7e00 + n),
        "ens",
        format!("0xf{n:063x}"),
    )
}

/// The exempt raw names from `n` on, `count` of them.
fn exempt_keys(n: i64, count: i64) -> Vec<Key> {
    (0..count)
        .map(|step| raw_key(n + step * EXEMPT_EVERY))
        .collect()
}

/// The pages of one statement: label, cursor, and the order names the page must serve.
type Pages = Vec<(&'static str, Option<Key>, Vec<Key>)>;

fn pages(supported: bool) -> Pages {
    let mut twin = raw_key(TWIN);
    twin.1 = "basenames";
    if supported {
        vec![
            (
                "first page",
                None,
                (1..=PAGE + 1).map(textless_key).collect(),
            ),
            (
                "deep page among names without raw bytes",
                Some(textless_key(90_000)),
                (90_001..=90_001 + PAGE).map(textless_key).collect(),
            ),
            (
                "deep page among raw names",
                Some(raw_key(TWIN)),
                (TWIN + 1..=TWIN + 1 + PAGE).map(raw_key).collect(),
            ),
            (
                "deep page from inside a run of equal order names",
                Some(twin),
                (TWIN..=TWIN + PAGE).map(raw_key).collect(),
            ),
        ]
    } else {
        vec![
            (
                "first page, past every name without raw bytes",
                None,
                exempt_keys(EXEMPT_EVERY, PAGE + 1),
            ),
            (
                "deep page among raw names",
                Some(raw_key(TWIN)),
                exempt_keys(TWIN + EXEMPT_EVERY, PAGE + 1),
            ),
            (
                "deep page from inside a run of equal order names",
                Some(twin),
                exempt_keys(TWIN, PAGE + 1),
            ),
        ]
    }
}

#[tokio::test]
async fn a_bound_name_page_reads_one_page_of_summary_rows_at_a_million_names() -> Result<()> {
    with_database("bound_page_plan", async |connection| {
        install(connection).await?;
        let mut failures = Vec::new();
        for (supported, index, total) in [
            (true, ORDER_INDEX, LISTED + TEXTLESS + 1),
            (false, EXEMPT_INDEX, LISTED / EXEMPT_EVERY + 1),
        ] {
            let statement = if supported {
                "supported"
            } else {
                "unsupported"
            };
            raw_sql(&format!(
                "PREPARE bound_page_{statement} (text, text, text, text, text, text, bigint) AS {}",
                bound_page_sql(supported)
            ))
            .execute(&mut *connection)
            .await
            .context("prepare the bound-name page")?;
            for (page, after, expected) in pages(supported) {
                let expected: Vec<String> =
                    expected.iter().map(|(name, ..)| name.clone()).collect();
                for mode in PLAN_MODES {
                    let label = format!("{statement}: {page} ({mode})");
                    let values = values(after.as_ref());
                    let (plan, rows) = run(connection, statement, mode, &values).await?;
                    if rows != expected {
                        failures.push(format!("{label}: served {rows:?}, expected {expected:?}"));
                    }
                    let order = visited(&plan, index);
                    let other = visited(&plan, ORDER_INDEX) + visited(&plan, EXEMPT_INDEX) - order;
                    let scanned = sequential_scans(&plan);
                    if order == 0 || order > PAGE + 1 || other > 0 || !scanned.is_empty() {
                        failures.push(format!(
                            "{label}: read {order} rows of {index}, {other} of the other index, \
                             scanned {scanned:?}\n{plan:#}"
                        ));
                    }
                    let text: Vec<String> = raw_sql(&format!(
                        "EXPLAIN (ANALYZE, BUFFERS) EXECUTE bound_page_{statement} ({values})"
                    ))
                    .fetch_all(&mut *connection)
                    .await?
                    .iter()
                    .map(|row| row.try_get(0).map_err(Into::into))
                    .collect::<Result<_>>()?;
                    eprintln!(
                        "BOUND PAGE {label}: {:.1} ms, {order} rows of {index}\n{}",
                        plan[0]["Execution Time"].as_f64().unwrap_or_default(),
                        text.join("\n")
                    );
                }
            }
            count(connection, (supported, index, total), &mut failures).await?;
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
        reader_count(connection).await
    })
    .await
}

/// The prepared count under either plan, with sorts off as the reader's savepoint sets them, for
/// a resolver whose support is `supported`, whose listing reads `index` and holds `total` names.
/// Uncapped it counts `total`, as the fences written as `EXISTS` do. Capped it counts
/// `CAP + 1`: a `Limit` holds the scan of `index` to `CAP + 1` rows, the other index is not
/// read, and the fences run as a subplan once for each row, never as a join over the surfaces.
async fn count(
    connection: &mut PgConnection,
    (supported, index, total): (bool, &str, i64),
    failures: &mut Vec<String>,
) -> Result<()> {
    raw_sql(&format!(
        "PREPARE bound_count_{supported} (text, text, text, bigint) AS {}",
        bound_count_sql(supported)
    ))
    .execute(&mut *connection)
    .await
    .context("prepare the bound-name count")?;
    raw_sql("SET enable_sort = off")
        .execute(&mut *connection)
        .await?;
    for mode in PLAN_MODES {
        raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&mut *connection)
            .await?;
        for (limit, expected) in [(None, total), (Some(CAP + 1), CAP + 1)] {
            let label = format!("count (supported {supported}, {mode}, limit {limit:?})");
            let values = format!(
                "'{CHAIN}', '{}', NULL, {}",
                resolver('a'),
                limit.map_or("NULL".to_owned(), |limit| limit.to_string())
            );
            let started = std::time::Instant::now();
            let counted: i64 =
                sqlx::query_scalar(&format!("EXECUTE bound_count_{supported} ({values})"))
                    .fetch_one(&mut *connection)
                    .await?;
            eprintln!(
                "BOUND {label} of {counted} names: {} ms",
                started.elapsed().as_millis()
            );
            if counted != expected {
                failures.push(format!("{label}: counted {counted}, expected {expected}"));
            }
            if limit.is_none() {
                continue;
            }
            let plan: Value = sqlx::query_scalar::<_, sqlx::types::Json<Value>>(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE bound_count_{supported} ({values})"
            ))
            .fetch_one(&mut *connection)
            .await
            .with_context(|| format!("explain the {label}"))?
            .0;
            let read = visited(&plan, index);
            let other = visited(&plan, ORDER_INDEX) + visited(&plan, EXEMPT_INDEX) - read;
            let scanned = sequential_scans(&plan);
            let surfaces = scans_of(&plan, "name_surfaces");
            let probes: i64 = nodes(&plan)
                .iter()
                .filter(|node| node["Parent Relationship"] == "SubPlan")
                .map(|node| node["Actual Loops"].as_i64().unwrap_or_default())
                .sum();
            if read == 0
                || probes == 0
                || read > CAP + 1
                || other > 0
                || !scanned.is_empty()
                || !limits(&plan, index)
                || surfaces.is_empty()
                || surfaces.iter().any(|(_, held)| !held)
                || probes > CAP + 1
                || nodes(&plan).iter().any(|node| node["Join Type"] == "Semi")
            {
                failures.push(format!(
                    "{label}: read {read} rows of {index}, {other} of the other index, scanned \
                     {scanned:?}, probed the surfaces {probes} times\n{plan:#}"
                ));
            }
            eprintln!(
                "BOUND {label}: {:.1} ms, {read} rows of {index}",
                plan[0]["Execution Time"].as_f64().unwrap_or_default()
            );
        }
    }
    raw_sql("RESET plan_cache_mode; RESET enable_sort")
        .execute(&mut *connection)
        .await?;
    let existing: i64 = sqlx::query_scalar(&exists_count_sql(supported))
        .bind(CHAIN)
        .bind(resolver('a'))
        .bind(None::<&str>)
        .persistent(false)
        .fetch_one(&mut *connection)
        .await?;
    if existing != total {
        failures.push(format!(
            "the fences written as EXISTS count {existing} (supported {supported}), the listing \
             holds {total}"
        ));
    }
    Ok(())
}

/// The count statement as it was before it took a limit: the same listed rows, with the fences
/// written as `EXISTS`. The capped statement must count the same names below its cap.
fn exists_count_sql(supported: bool) -> String {
    let gate = if supported {
        ""
    } else {
        "AND summary.resolver_support_exempt"
    };
    format!(
        "SELECT count(*)
         FROM bigname_phase.project_name_summary summary
         WHERE summary.resolver_chain_id = $1
           AND summary.resolver_address = lower($2)
           AND summary.resolver_listing_eligible
           AND octet_length(summary.order_name) <= 2000
           {gate}
           AND ($3::text IS NULL OR summary.namespace = $3)
           AND EXISTS (
             SELECT 1 FROM bigname_phase.name_surfaces surface
             JOIN bigname_phase.chain_lineage lineage
               ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
             JOIN bigname_phase.project_family_marker marker
               ON marker.chain_id = surface.chain_id
             WHERE surface.logical_name_id = summary.logical_name_id
               AND surface.visibility_state = 'active'
               AND surface.block_number <= marker.current_block_number
               AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))"
    )
}

/// The reader sends the count unprepared, so it is planned with the resolver's own values and
/// its limit on every run, past the five runs after which PostgreSQL may switch a prepared
/// statement to its generic plan. It reads `CAP + 1` names under the cap. Under a namespace the
/// limit applies to that namespace's names: 0xaa.. lists one name in `basenames`, exempt, and
/// none in a namespace it does not serve.
async fn reader_count(connection: &mut PgConnection) -> Result<()> {
    let address = resolver('a');
    let cap = Some(u64::try_from(CAP)?);
    for run in 1..=6 {
        let started = std::time::Instant::now();
        let total = bound_count(&mut *connection, (CHAIN, &address, None), true, cap).await?;
        eprintln!(
            "BOUND COUNT (reader, run {run}) of {total} names: {} ms",
            started.elapsed().as_millis()
        );
        ensure!(
            i64::try_from(total)? == CAP + 1,
            "the reader counted {total} on run {run}"
        );
    }
    for (supported, namespace, expected) in [
        (true, "basenames", 1),
        (false, "basenames", 1),
        (true, "unserved", 0),
    ] {
        let total = bound_count(
            &mut *connection,
            (CHAIN, &address, Some(namespace)),
            supported,
            cap,
        )
        .await?;
        ensure!(
            total == expected,
            "the reader counted {total} in {namespace} (supported {supported})"
        );
    }
    let cached: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_prepared_statements WHERE statement = $1 OR statement = $2",
    )
    .bind(bound_count_sql(true))
    .bind(bound_count_sql(false))
    .fetch_one(&mut *connection)
    .await?;
    ensure!(
        cached == 0,
        "the reader's count was cached as a prepared statement"
    );
    let text: Vec<String> = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS) {}",
        bound_count_sql(true)
    ))
    .bind(CHAIN)
    .bind(&address)
    .bind(None::<&str>)
    .bind(CAP + 1)
    .persistent(false)
    .fetch_all(&mut *connection)
    .await?;
    let text = text.join("\n");
    eprintln!("BOUND COUNT (reader plan)\n{text}");
    ensure!(
        text.contains(&format!("'{CHAIN}'::text")),
        "the reader's count was planned without the resolver's values\n{text}"
    );
    // The reader's own savepoint reads either index in page order under the limit, with no
    // bitmap scan or sort, whatever the planner estimates for the resolver.
    for (supported, index) in [(true, ORDER_INDEX), (false, EXEMPT_INDEX)] {
        let plan =
            bound_count_plan(&mut *connection, (CHAIN, &address, None), supported, cap).await?;
        ensure!(
            plan.contains("Limit")
                && plan.contains(&format!("Index Scan using {index}"))
                && !plan.contains("Sort")
                && !plan.contains("Bitmap"),
            "the reader's count (supported {supported}) does not read {index} in order\n{plan}"
        );
    }
    let setting: String = sqlx::query_scalar("SHOW enable_sort")
        .fetch_one(&mut *connection)
        .await?;
    ensure!(setting == "on", "the count left enable_sort {setting}");
    forced_generic_count(connection).await
}

/// A session that forces generic plans, as a server or role setting can, still counts with the
/// custom plan, and keeps its own setting after the count.
async fn forced_generic_count(connection: &mut PgConnection) -> Result<()> {
    let address = resolver('a');
    let cap = Some(u64::try_from(CAP)?);
    raw_sql("SET plan_cache_mode = force_generic_plan")
        .execute(&mut *connection)
        .await?;
    let started = std::time::Instant::now();
    let total = bound_count(&mut *connection, (CHAIN, &address, None), true, cap).await?;
    eprintln!(
        "BOUND COUNT (reader, session force_generic_plan) of {total} names: {} ms",
        started.elapsed().as_millis()
    );
    let plan = bound_count_plan(&mut *connection, (CHAIN, &address, None), true, cap).await?;
    let setting: String = sqlx::query_scalar("SHOW plan_cache_mode")
        .fetch_one(&mut *connection)
        .await?;
    raw_sql("RESET plan_cache_mode")
        .execute(&mut *connection)
        .await?;
    eprintln!("BOUND COUNT (reader plan, session force_generic_plan)\n{plan}");
    ensure!(
        i64::try_from(total)? == CAP + 1,
        "the reader counted {total} under a forced generic plan"
    );
    ensure!(
        plan.contains(&format!("'{CHAIN}'::text")) && !plan.contains("$1"),
        "the count took the generic plan the session forces\n{plan}"
    );
    ensure!(
        setting == "force_generic_plan",
        "the count changed the session's plan_cache_mode to {setting}"
    );
    Ok(())
}

async fn install(connection: &mut PgConnection) -> Result<()> {
    raw_sql(include_str!("../../schema/baseline/07_labels.sql"))
        .execute(&mut *connection)
        .await?;
    let listed = resolver('a');
    // Names 1..=LISTED are listed by resolver 0xaa... The next UNLISTED serve it but fail the
    // listing rules. The next OTHER are listed by a thousand other resolvers, so a generic plan
    // estimates one resolver's names from many, as on a production database. TEXTLESS more are
    // listed by 0xaa..
    // and store no raw bytes, under a label with no preimage and one with an imported preimage,
    // which their order names do not read. Name TWIN is listed by 0xaa.. in `basenames` too.
    // Of 0xaa..'s listed names only the raw names whose number EXEMPT_EVERY divides are exempt
    // from the capability gate.
    let surfaces = "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)";
    let summaries = "INSERT INTO project_name_summary (chain_id, logical_name_id, namespace,
             serving, zero_owner, expiry_listable, search_supported, resolver_chain_id,
             resolver_address, resolver_listing_eligible, resolver_support_exempt, raw_name,
             order_name, namehash)";
    // Each large insert is followed by a checkpoint, so the write-ahead log the lane database
    // keeps in memory stays small beside the rows.
    let statements = [
        format!(
            "INSERT INTO chain_lineage
                 (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ('{CHAIN}', 'block-1', 1, to_timestamp(1), 'canonical');
             INSERT INTO project_family_marker (chain_id, current_block_number,
                 current_block_hash, state)
             VALUES ('{CHAIN}', 1, 'block-1', 'live');
             INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
                 normalized_under_version, source_kind, source_priority)
             VALUES ('0x{ETH}', convert_to('eth', 'UTF8'), 'eth', 'v1', true, 'fixture', 1);
             CREATE TEMP TABLE fixture_name AS
             SELECT n, '0x' || lpad(to_hex(n), 64, '0') AS namehash,
                    'n' || lpad(n::text, 7, '0') || '.eth' AS raw_name
             FROM generate_series(1, {LISTED} + {UNLISTED} + {OTHER}) n;
             CREATE TEMP TABLE fixture_textless AS
             SELECT n, '0xf' || lpad(to_hex(n), 63, '0') AS namehash,
                    lpad(to_hex(32256 + n), 64, '0') AS label
             FROM generate_series(1, {TEXTLESS}) n;
             INSERT INTO chain_lineage
                 (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ('{CHAIN}', 'block-2', 2, to_timestamp(2), 'canonical');
             CREATE TEMP TABLE fixture_fenced AS
             SELECT n, '0xe' || lpad(to_hex(n), 63, '0') AS namehash,
                    'zfenced' || lpad(n::text, 7, '0') || '.eth' AS raw_name
             FROM generate_series(1, {FENCED}) n"
        ),
        format!(
            "{surfaces}
             SELECT 'ens:' || namehash, 'ens', raw_name, ARRAY[raw_name], '\\x00', namehash,
                    ARRAY[namehash], 'v1', 'active', '{CHAIN}', 'block-1', 1, 'canonical'
             FROM fixture_name"
        ),
        format!(
            "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
                 normalizer_version, visibility_state, chain_id, block_hash, block_number,
                 canonicality_state)
             SELECT 'ens:' || namehash, 'ens', namehash, ARRAY['0x' || label, '0x{ETH}'],
                    'v1', 'active', '{CHAIN}', 'block-1', 1, 'canonical'
             FROM fixture_textless;
             {surfaces}
             SELECT 'basenames:' || namehash, 'basenames', raw_name, ARRAY[raw_name], '\\x00',
                    namehash, ARRAY[namehash], 'v1', 'active', '{CHAIN}', 'block-1', 1,
                    'canonical'
             FROM fixture_name WHERE n = {TWIN}"
        ),
        format!(
            "{summaries}
             SELECT '{CHAIN}', 'ens:' || namehash, 'ens', false, false, false, false, '{CHAIN}',
                    CASE WHEN n > {LISTED} + {UNLISTED}
                         THEN '0xb' || lpad(to_hex(n % 1000), 39, '0') ELSE '{listed}' END,
                    n <= {LISTED} OR n > {LISTED} + {UNLISTED},
                    n > {LISTED} OR n % {EXEMPT_EVERY} = 0, raw_name, raw_name, namehash
             FROM fixture_name"
        ),
        format!(
            "{summaries}
             SELECT '{CHAIN}', 'ens:' || namehash, 'ens', true, false, false, false, '{CHAIN}',
                    '{listed}', true, false, NULL, '[' || label || '].[{ETH}]', namehash
             FROM fixture_textless;
             {summaries}
             SELECT '{CHAIN}', 'basenames:' || namehash, 'basenames', false, false, false,
                    false, '{CHAIN}', '{listed}', true, true, raw_name, raw_name, namehash
             FROM fixture_name WHERE n = {TWIN};
             {surfaces}
             SELECT 'ens:' || namehash, 'ens', raw_name, ARRAY[raw_name], '\\x00', namehash,
                    ARRAY[namehash], 'v1', 'active', '{CHAIN}', 'block-2', 2, 'canonical'
             FROM fixture_fenced;
             {summaries}
             SELECT '{CHAIN}', 'ens:' || namehash, 'ens', false, false, false, false, '{CHAIN}',
                    '{listed}', true, true, raw_name, raw_name, namehash
             FROM fixture_fenced;
             DROP TABLE fixture_name, fixture_textless, fixture_fenced"
        ),
    ];
    for statement in statements {
        raw_sql(&statement)
            .execute(&mut *connection)
            .await
            .context("failed to install the bound-name fixture")?;
        raw_sql("CHECKPOINT").execute(&mut *connection).await?;
    }
    raw_sql("ANALYZE").execute(&mut *connection).await?;
    Ok(())
}

/// The page statement's values for resolver 0xaa.. after the name with keys `after`.
fn values(after: Option<&Key>) -> String {
    let quoted =
        |value: Option<&str>| value.map_or("NULL".to_owned(), |value| format!("'{value}'"));
    format!(
        "'{CHAIN}', '{}', NULL, {}, {}, {}, {}",
        resolver('a'),
        quoted(after.map(|(name, ..)| name.as_str())),
        quoted(after.map(|(_, namespace, _)| *namespace)),
        quoted(after.map(|(.., namehash)| namehash.as_str())),
        PAGE + 1
    )
}

/// The analyzed plan of a page of the prepared statement `bound_page_{statement}` and the order
/// names it returned.
async fn run(
    connection: &mut PgConnection,
    statement: &str,
    mode: &str,
    values: &str,
) -> Result<(Value, Vec<String>)> {
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(&mut *connection)
        .await?;
    let estimated: Vec<String> = raw_sql(&format!(
        "EXPLAIN EXECUTE bound_page_{statement} ({values})"
    ))
    .fetch_all(&mut *connection)
    .await?
    .iter()
    .map(|row| row.try_get(0).map_err(Into::into))
    .collect::<Result<_>>()?;
    eprintln!("BOUND PLAN ({mode}):\n{}", estimated.join("\n"));
    let plan: Value = sqlx::query_scalar::<_, sqlx::types::Json<Value>>(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE bound_page_{statement} ({values})"
    ))
    .fetch_one(&mut *connection)
    .await
    .with_context(|| format!("explain the bound-name page ({mode})"))?
    .0;
    let rows = sqlx::query(&format!("EXECUTE bound_page_{statement} ({values})"))
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(|row| row.try_get::<String, _>("name").map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;
    Ok((plan, rows))
}

/// Every plan node, depth first.
fn nodes(plan: &Value) -> Vec<&Value> {
    let mut out = Vec::new();
    let mut pending = vec![&plan[0]["Plan"]];
    while let Some(node) = pending.pop() {
        out.push(node);
        if let Some(children) = node["Plans"].as_array() {
            pending.extend(children);
        }
    }
    out
}

/// The index entries the scans of `index` read: the rows they returned and the rows their
/// filters removed, over every loop.
fn visited(plan: &Value, index: &str) -> i64 {
    nodes(plan)
        .into_iter()
        .filter(|node| node["Index Name"] == index)
        .map(|node| {
            let loops = node["Actual Loops"].as_i64().unwrap_or(1);
            let rows = node["Actual Rows"].as_i64().unwrap_or_default();
            let removed = node["Rows Removed by Filter"].as_i64().unwrap_or_default();
            (rows + removed) * loops
        })
        .sum()
}

/// The large relations a plan reads by a sequential scan.
fn sequential_scans(plan: &Value) -> Vec<String> {
    nodes(plan)
        .into_iter()
        .filter(|node| node["Node Type"] == "Seq Scan")
        .filter_map(|node| node["Relation Name"].as_str().map(str::to_owned))
        .filter(|relation| relation == "project_name_summary" || relation == "name_surfaces")
        .collect()
}

/// Whether a `Limit` holds a scan of `index`, outside any subplan below it.
fn limits(plan: &Value, index: &str) -> bool {
    nodes(plan)
        .into_iter()
        .filter(|node| node["Node Type"] == "Limit")
        .any(|limit| {
            let mut pending = vec![limit];
            while let Some(node) = pending.pop() {
                if node["Index Name"] == index {
                    return true;
                }
                if let Some(children) = node["Plans"].as_array() {
                    pending.extend(
                        children
                            .iter()
                            .filter(|child| child["Parent Relationship"] != "SubPlan"),
                    );
                }
            }
            false
        })
}

/// The scans of `relation`, each with whether a subplan holds it.
fn scans_of<'a>(plan: &'a Value, relation: &str) -> Vec<(&'a Value, bool)> {
    let mut out = Vec::new();
    let mut pending = vec![(&plan[0]["Plan"], false)];
    while let Some((node, held)) = pending.pop() {
        let held = held || node["Parent Relationship"] == "SubPlan";
        if node["Relation Name"] == relation {
            out.push((node, held));
        }
        if let Some(children) = node["Plans"].as_array() {
            pending.extend(children.iter().map(|child| (child, held)));
        }
    }
    out
}
