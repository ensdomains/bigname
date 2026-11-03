//! The resolver `/links` page against the statement it replaced, kept below in `today`, and
//! its plan. For every resolver of the fixture, every page size and every cursor, the loader must
//! return the rows, the order and the total that statement returns, so a cursor from either
//! statement resumes at the same row in the other. The fixture's resolvers have no links, only
//! record-id-0 links, one link, a few pages of mixed links and many links. The mixed resolver
//! holds ties on the padded record id, record ids of other lengths, the default node, links
//! without an event or above the marker block, and surfaces that are shadow, above the marker
//! block, on orphaned lineage, without raw bytes or in another namespace.
//!
//! The plan test requires the page to read `project_resolver_link_order_idx` from the cursor
//! under a `Limit`, reading only the page's rows, with no sort anywhere in the statement, under
//! both plan modes. The fixture is analyzed and costs are left alone, so the plans are the
//! planner's own choice for a resolver with many links.
//!
//! Expected keys and totals come from SQL over the fixture, never from a Rust ordering, because
//! the database collation decides the order.

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row, raw_sql};

use super::{FamilyCollectionPage, load_resolver_links_shadow};
use crate::families::name::rendered::rendered_name_sql;

const CHAIN: &str = "ethereum-mainnet";
const OTHER_CHAIN: &str = "base-mainnet";
const BLOCKS: i64 = 40_000;
const MARKER: i64 = 20_000;
const EMPTY: &str = "0x00000000000000000000000000000000000000e0";
const CLEARED: &str = "0x00000000000000000000000000000000000000c0";
const SINGLE: &str = "0x0000000000000000000000000000000000000001";
const MIXED: &str = "0x00000000000000000000000000000000000000a0";
const LARGE: &str = "0x00000000000000000000000000000000000000b0";
const LARGE_LINKS: i64 = 5_000;
/// The default link's node: the empty name.
const ZERO_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

type Key = (String, String);

#[tokio::test]
async fn links_match_the_statement_they_replace() -> Result<()> {
    with_database("resolver_links_today", async |connection| {
        let cases: [(&str, &[i64]); 5] = [
            (EMPTY, &[1, 51]),
            (CLEARED, &[1, 51]),
            (SINGLE, &[1, 2, 51]),
            (MIXED, &[1, 2, 3, 7, 51, 10_000]),
            (LARGE, &[51, 1_000]),
        ];
        for (resolver, limits) in cases {
            let (reference, count) = reference(connection, resolver).await?;
            let today_all = today(connection, resolver, None, 10_000).await?;
            ensure!(
                keys(&today_all) == reference && today_all.total_count == count,
                "{resolver}: the statement of record disagrees with the fixture"
            );
            for &limit in limits {
                let mut after: Option<Key> = None;
                let mut walked = Vec::new();
                loop {
                    let page = load(connection, resolver, after.as_ref(), limit).await?;
                    let expected = today(connection, resolver, after.as_ref(), limit).await?;
                    ensure!(
                        page == expected,
                        "{resolver} limit {limit} after {after:?}: {page:?}\nexpected {expected:?}"
                    );
                    ensure!(
                        page.total_count == count,
                        "{resolver}: total {}",
                        page.total_count
                    );
                    walked.extend(keys(&page));
                    if page.rows.len() < usize::try_from(limit)? {
                        break;
                    }
                    after = page
                        .rows
                        .last()
                        .map(|(key1, key2, _)| (key1.clone(), key2.clone()));
                }
                ensure!(
                    walked == reference,
                    "{resolver} limit {limit}: walked {walked:?}"
                );
            }
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn links_resume_from_any_cursor_as_today() -> Result<()> {
    with_database("resolver_links_cursor", async |connection| {
        let (reference, _) = reference(connection, MIXED).await?;
        let padded = |record: &str| format!("{record:0>78}");
        let mut cursors = reference.clone();
        cursors.extend([
            (String::new(), String::new()),
            ("0".repeat(78), String::new()),
            // Inside the tie group of record 1, between two of its nodes, and past its last.
            (padded("1"), String::new()),
            (
                padded("1"),
                format!("{}0", reference_node(&reference, &padded("1"), 1)?),
            ),
            (padded("1"), "0x~".to_owned()),
            // Between two record ids, and past every key.
            (padded("5"), ZERO_NODE.to_owned()),
            ("9".repeat(79), String::new()),
        ]);
        for after in &cursors {
            for limit in [1, 3, 51] {
                let page = load(connection, MIXED, Some(after), limit).await?;
                let expected = today(connection, MIXED, Some(after), limit).await?;
                ensure!(
                    page == expected,
                    "after {after:?} limit {limit}: {page:?}\nexpected {expected:?}"
                );
            }
        }
        Ok(())
    })
    .await
}

/// Each input class is present in the fixture and served as the fixture says it should be.
#[tokio::test]
async fn links_classes() -> Result<()> {
    with_database("resolver_links_classes", async |connection| {
        for resolver in [EMPTY, CLEARED, "0x00000000000000000000000000000000000000ff"] {
            let page = load(connection, resolver, None, 51).await?;
            ensure!(
                page == FamilyCollectionPage::default(),
                "{resolver}: {page:?}"
            );
        }
        let single = load(connection, SINGLE, None, 51).await?;
        ensure!(
            single.total_count == 1 && single.rows.len() == 1,
            "{single:?}"
        );
        ensure!(single.rows[0].0 == format!("{:0>78}", "5"));
        ensure!(single.rows[0].2["record_id"] == "5" && single.rows[0].2["default"] == false);

        let mixed = load(connection, MIXED, None, 10_000).await?;
        let (reference, count) = reference(connection, MIXED).await?;
        ensure!(keys(&mixed) == reference && mixed.total_count == count);
        // 150 links and the default one, less the 16 cleared to record 0.
        ensure!(count == 135, "mixed total {count}");
        ensure!(mixed.rows.iter().all(|(.., item)| item["record_id"] != "0"));
        let ones = mixed
            .rows
            .iter()
            .filter(|(.., item)| item["record_id"] == "1")
            .count();
        ensure!(ones > 2, "record 1 ties on {ones} nodes");
        // 7 and 07 pad to one key and are ordered by node.
        let sevens = mixed
            .rows
            .iter()
            .filter(|(key1, ..)| *key1 == format!("{:0>78}", "7"))
            .map(|(.., item)| item["record_id"].as_str().unwrap_or_default())
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(sevens.len() == 2, "record ids padding to 7: {sevens:?}");

        let item = |index: i64| -> Result<&Value> {
            let node = node(index);
            mixed
                .rows
                .iter()
                .find(|(_, key2, _)| *key2 == node)
                .map(|(.., item)| item)
                .with_context(|| format!("no link at {node}"))
        };
        ensure!(item(5)?["name"] == "n5.eth" && item(5)?["namespace"] == "ens");
        for (index, class) in [
            (7, "shadow"),
            (11, "above the marker"),
            (13, "orphaned"),
            (31, "basenames"),
        ] {
            ensure!(
                item(index)?.get("name").is_none(),
                "{class} surface names its link"
            );
        }
        ensure!(item(17)?["name"] == format!("[{:064x}]", 17));
        ensure!(item(17)?["name_is_rendered"] == true);
        ensure!(item(19)?["chain_position"].get("block_hash").is_none());
        ensure!(item(23)?["chain_position"]["block_number"] == json!(MARKER + 50));
        ensure!(item(29)?["chain_position"].get("log_index").is_none());
        let default = mixed
            .rows
            .iter()
            .find(|(_, key2, _)| key2 == ZERO_NODE)
            .map(|(.., item)| item)
            .context("no default link")?;
        ensure!(default["default"] == true && default.get("name").is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn links_page_is_a_limit_over_the_order_index() -> Result<()> {
    with_database("resolver_links_plan", async |connection| {
        let (reference, _) = reference(connection, LARGE).await?;
        let resume = reference[99].clone();
        load(connection, LARGE, None, 51).await?;
        load(connection, LARGE, Some(&resume), 51).await?;
        let prepared: Vec<(String, Vec<String>)> = sqlx::query_as(
            "SELECT name, parameter_types::text[] FROM pg_prepared_statements
             WHERE statement LIKE '%FROM bigname_phase.project_resolver_link link%'",
        )
        .fetch_all(&mut *connection)
        .await?;
        ensure!(
            !prepared.is_empty(),
            "the loader prepared no links statement"
        );
        let mut failures = Vec::new();
        for (statement, types) in &prepared {
            // The loader binds the chain, the resolver, the namespace, the cursor keys and the
            // limit first. Any later parameter is bound null.
            let bound = ["text", "text", "text", "text", "text", "bigint"];
            ensure!(
                types.starts_with(&bound.map(str::to_owned)),
                "{statement} binds {types:?}"
            );
            let rest = types[bound.len()..]
                .iter()
                .map(|ty| format!(", NULL::{ty}"))
                .collect::<String>();
            for (label, after) in [
                ("first page", "NULL, NULL".to_owned()),
                ("resume", format!("'{}', '{}'", resume.0, resume.1)),
            ] {
                let values = format!("'{CHAIN}', '{LARGE}', 'ens', {after}, 51{rest}");
                for mode in ["force_custom_plan", "force_generic_plan"] {
                    let plan = explain(connection, mode, statement, &values).await?;
                    failures.extend(
                        walk_problems(&plan, 51).into_iter().map(|problem| {
                            format!("{label}, {mode}: {problem}\n{}", plan.join("\n"))
                        }),
                    );
                }
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
        Ok(())
    })
    .await
}

/// What keeps `plan` from being a page that walks the order index under its limit and reads
/// `rows` rows: a sort anywhere, no `Limit` over an index scan of the order index, an index
/// condition without the resolver and the cursor, or a filter on the scan.
fn walk_problems(plan: &[String], rows: usize) -> Vec<String> {
    let mut problems = Vec::new();
    if plan.iter().any(|line| line.contains("Sort")) {
        problems.push("the plan sorts".to_owned());
    }
    let walk = (0..plan.len()).find(|&index| {
        plan_node(&plan[index]).starts_with("Limit")
            && plan.get(index + 1).is_some_and(|scan| {
                plan_node(scan).starts_with(
                    "Index Scan using project_resolver_link_order_idx on project_resolver_link",
                )
            })
    });
    let Some(index) = walk else {
        problems.push("no Limit over an index scan of project_resolver_link_order_idx".to_owned());
        return problems;
    };
    let scan = &plan[index + 1];
    if !scan.contains(&format!("actual rows={rows} ")) {
        problems.push(format!("the walk read other than {rows} rows: {scan}"));
    }
    let properties = plan[index + 2..]
        .iter()
        .take_while(|line| !line.contains("->"))
        .collect::<Vec<_>>();
    let condition = properties
        .iter()
        .find(|line| line.contains("Index Cond:"))
        .map_or("", |line| line.as_str());
    for term in [
        "(chain_id = ",
        "(resolver_address = ",
        "ROW(lpad(record_id, 78",
    ] {
        if !condition.contains(term) {
            problems.push(format!("the index condition lacks {term}: {condition}"));
        }
    }
    if properties.iter().any(|line| line.contains("Filter")) {
        problems.push("the walk filters rows".to_owned());
    }
    problems
}

/// A plan line without its indent and arrow.
fn plan_node(line: &str) -> &str {
    line.trim_start().trim_start_matches("->").trim_start()
}

async fn explain(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    values: &str,
) -> Result<Vec<String>> {
    raw_sql(&format!(
        "SET plan_cache_mode = {mode};
         EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) EXECUTE {statement} ({values})"
    ))
    .fetch_all(&mut *connection)
    .await
    .with_context(|| format!("{statement} {mode}"))?
    .iter()
    .map(|row| row.try_get(0).map_err(Into::into))
    .collect()
}

async fn load(
    connection: &mut PgConnection,
    resolver: &str,
    after: Option<&Key>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    load_resolver_links_shadow(&mut *connection, CHAIN, resolver, "ens", after, limit).await
}

/// The resolver's link keys in page order and their count, from the table alone.
async fn reference(connection: &mut PgConnection, resolver: &str) -> Result<(Vec<Key>, u64)> {
    let keys: Vec<Key> = sqlx::query_as(
        "SELECT lpad(record_id, 78, '0'), node FROM project_resolver_link
         WHERE chain_id = $1 AND resolver_address = $2 AND record_id <> '0'
         ORDER BY 1, 2",
    )
    .bind(CHAIN)
    .bind(resolver)
    .fetch_all(&mut *connection)
    .await?;
    let count = u64::try_from(keys.len())?;
    Ok((keys, count))
}

/// The `n`th node, counted from 1, of the tie group `key1` in `reference`.
fn reference_node(reference: &[Key], key1: &str, n: usize) -> Result<String> {
    reference
        .iter()
        .filter(|(group, _)| group == key1)
        .nth(n - 1)
        .map(|(_, node)| node.clone())
        .with_context(|| format!("no node {n} under {key1}"))
}

fn keys(page: &FamilyCollectionPage) -> Vec<Key> {
    page.rows
        .iter()
        .map(|(key1, key2, _)| (key1.clone(), key2.clone()))
        .collect()
}

fn node(index: i64) -> String {
    format!("0x{index:064x}")
}

/// The `/links` statement before the order index, as it stood with the `/roles` statement's
/// shared page: `items` holds every link of the resolver, rendered, and is read twice, once for
/// the total and once for the page.
async fn today(
    connection: &mut PgConnection,
    resolver: &str,
    after: Option<&Key>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    let statement = format!(
        "WITH clock AS (
            SELECT current_block_number AS block_number
            FROM bigname_phase.project_family_marker WHERE chain_id = $1
        ), items AS (
            SELECT lpad(link.record_id, 78, '0') AS key1, link.node AS key2,
                jsonb_strip_nulls(jsonb_build_object(
                    'record_id', link.record_id, 'namehash', link.node,
                    'default', link.node =
                        '0x0000000000000000000000000000000000000000000000000000000000000000',
                    'logical_name_id', named.logical_name_id, 'name', named.raw_name,
                    'name_is_rendered', NULLIF(named.rendered, false),
                    'namespace', named.namespace, 'normalized_event_id', link.normalized_event_id,
                    'chain_position', jsonb_strip_nulls(jsonb_build_object(
                        'chain_id', link.chain_id, 'block_number', link.block_number,
                        'block_hash', event.block_hash, 'transaction_hash', event.transaction_hash,
                        'log_index', link.log_index,
                        'timestamp', to_char(lineage.block_timestamp AT TIME ZONE 'UTC',
                                             'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'))))) AS item
            FROM bigname_phase.project_resolver_link link
            CROSS JOIN clock
            LEFT JOIN bigname_phase.normalized_events event
              ON event.event_identity = link.event_identity
            LEFT JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            LEFT JOIN LATERAL (
                SELECT surface.logical_name_id, {name} AS raw_name, surface.namespace,
                       surface.raw_name IS NULL AS rendered
                FROM bigname_phase.name_surfaces surface
                JOIN bigname_phase.chain_lineage surface_lineage
                  ON surface_lineage.chain_id = surface.chain_id
                 AND surface_lineage.block_hash = surface.block_hash
                 AND surface_lineage.block_number = surface.block_number
                 AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                WHERE link.node <>
                      '0x0000000000000000000000000000000000000000000000000000000000000000'
                  AND surface.logical_name_id = $3 || ':' || link.node
                  AND surface.chain_id = $1 AND surface.block_number <= clock.block_number
                  AND surface.visibility_state = 'active'
                  AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
            ) named ON TRUE
            WHERE link.chain_id = $1 AND link.resolver_address = $2
              AND link.record_id <> '0'
        ), selected_page AS (
            SELECT key1, key2, item FROM items
            WHERE $4::text IS NULL OR (key1, key2) > ($4, $5)
            ORDER BY key1, key2 LIMIT $6
        ) SELECT (SELECT count(*) FROM items) AS total,
            COALESCE((SELECT jsonb_agg(jsonb_build_object('key1', key1, 'key2', key2,
                'item', item) ORDER BY key1, key2) FROM selected_page), '[]'::jsonb) AS rows",
        name = rendered_name_sql("surface")
    );
    let row = sqlx::query(&statement)
        .bind(CHAIN)
        .bind(resolver)
        .bind("ens")
        .bind(after.map(|key| key.0.as_str()))
        .bind(after.map(|key| key.1.as_str()))
        .bind(limit)
        .fetch_one(&mut *connection)
        .await
        .context("the statement of record failed")?;
    let total: i64 = row.try_get("total")?;
    let rows: Value = row.try_get("rows")?;
    let rows = rows
        .as_array()
        .context("rows must be an array")?
        .iter()
        .map(|row| {
            Ok((
                row["key1"].as_str().context("key1")?.to_owned(),
                row["key2"].as_str().context("key2")?.to_owned(),
                row["item"].clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FamilyCollectionPage {
        rows,
        total_count: u64::try_from(total)?,
    })
}

async fn with_database(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        database.create_phase_schema().await?;
        let mut connection = database.pool().acquire().await?;
        for baseline in [
            include_str!("../../../schema/baseline/01_chain.sql"),
            include_str!("../../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../../schema/baseline/03_identity.sql"),
            include_str!("../../../schema/baseline/04_manifests.sql"),
            include_str!("../../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../../schema/baseline/06_projections.sql"),
            include_str!("../../../schema/baseline/07_labels.sql"),
            include_str!("../../../schema/baseline/08_heartbeats.sql"),
            include_str!("../../../schema/baseline/09_divergence.sql"),
            include_str!("../../../schema/baseline/10_phase_state.sql"),
        ] {
            raw_sql(baseline).execute(&mut *connection).await?;
        }
        install_fixture(&mut connection).await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

/// Mixed link `i` (1 to 150): record id by `i % 9` (`0` clears it), a block above the marker
/// when `i % 23 = 0`, no transaction or log index when `i % 29 = 0`, no event when
/// `i % 19 = 0`. Its surface, by the first that applies: named (`i % 5`), shadow (`i % 7`),
/// above the marker (`i % 11`), on orphaned lineage (`i % 13`), without raw bytes (`i % 17`) or
/// in the basenames namespace (`i % 31`). The default node links record 2.
async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    let hash = bigname_content_hash::PROJECTION_CONTENT_HASH;
    let max = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let long = format!("1{}", "2".repeat(79));
    raw_sql(&format!(
        r#"
        INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
            canonicality_state)
        SELECT '{CHAIN}', 'b' || n, n, to_timestamp(1700000000 + n), 'canonical'
        FROM generate_series(0, {BLOCKS}) n;
        INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
            canonicality_state)
        VALUES ('{CHAIN}', 'orphan-500', 500, to_timestamp(1700000500), 'orphaned');
        INSERT INTO project_family_marker (chain_id, current_block_number, current_block_hash,
            block_timestamp, input_content_hash, state)
        VALUES ('{CHAIN}', {MARKER}, 'b{MARKER}', to_timestamp(1700000000 + {MARKER}), '{hash}',
            'live');

        INSERT INTO project_resolver_link (chain_id, resolver_address, node, block_number,
            transaction_index, log_index, event_identity, record_id, storage_model)
        SELECT '{CHAIN}', '{MIXED}', '0x' || lpad(to_hex(i), 64, '0'),
               CASE WHEN i % 23 = 0 THEN {MARKER} + 50 ELSE 1000 + i END,
               CASE WHEN i % 29 = 0 THEN NULL ELSE 0 END,
               CASE WHEN i % 29 = 0 THEN NULL ELSE i END,
               'mixed:' || i,
               CASE i % 9 WHEN 0 THEN '0' WHEN 1 THEN '1' WHEN 2 THEN '2' WHEN 3 THEN '10'
                   WHEN 4 THEN '100' WHEN 5 THEN '7' WHEN 6 THEN '07' WHEN 7 THEN '{max}'
                   ELSE '{long}' END,
               'resolver_record_id'
        FROM generate_series(1, 150) i
        UNION ALL SELECT '{CHAIN}', '{MIXED}', '{ZERO_NODE}', 1200, 0, 1, 'mixed:default', '2',
               'resolver_record_id'
        UNION ALL SELECT '{CHAIN}', '{SINGLE}', '0x' || lpad(to_hex(9000), 64, '0'), 900, 0, 1,
               'single', '5', 'resolver_record_id'
        UNION ALL SELECT '{CHAIN}', '{CLEARED}', '0x' || lpad(to_hex(9100 + i), 64, '0'), 910,
               0, i, 'cleared:' || i, '0', 'resolver_record_id'
        FROM generate_series(1, 3) i
        UNION ALL SELECT '{CHAIN}', '{LARGE}', '0x' || lpad(to_hex(2000000 + n), 64, '0'),
               1 + n % {MARKER}, 0, n, 'large:' || n, (n % 997 + 1)::text, 'resolver_record_id'
        FROM generate_series(1, {LARGE_LINKS}) n
        UNION ALL SELECT '{CHAIN}', '0x' || lpad(to_hex(4096 + r), 40, '0'),
               '0x' || lpad(to_hex(1000000 + r * 100 + k), 64, '0'), 1 + (r * 50 + k) % {MARKER},
               0, k, 'filler-link:' || r || ':' || k, (k % 13 + 1)::text, 'resolver_record_id'
        FROM generate_series(1, 200) r, generate_series(1, 50) k
        UNION ALL SELECT '{OTHER_CHAIN}', '{MIXED}', '0x' || lpad(to_hex(i), 64, '0'), 10, 0, i,
               'other-chain:' || i, '1', 'resolver_record_id'
        FROM generate_series(1, 5) i;

        INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
            manifest_version, chain_id, block_number, block_hash, transaction_hash,
            transaction_index, log_index, derivation_kind, canonicality_state)
        SELECT link.event_identity, 'ens', 'ResolverRecordLinked', 'ens_v2_resolver_l1', 1,
               link.chain_id, link.block_number, 'b' || link.block_number,
               CASE WHEN link.log_index IS NULL THEN NULL ELSE 'tx' || link.block_number END,
               link.transaction_index, link.log_index, 'ens_v2_resolver',
               'canonical'::canonicality_state
        FROM project_resolver_link link
        WHERE link.chain_id = '{CHAIN}'
          AND link.event_identity NOT IN (SELECT 'mixed:' || i FROM generate_series(19, 150, 19) i)
        UNION ALL SELECT 'filler:' || n, 'ens', 'ResolverRecordLinked', 'ens_v2_resolver_l1', 1,
               '{CHAIN}', n, 'b' || n, NULL, NULL, NULL, 'ens_v2_resolver',
               'canonical'::canonicality_state
        FROM generate_series(1, {BLOCKS}) n;
        UPDATE project_resolver_link link SET normalized_event_id = event.normalized_event_id
        FROM normalized_events event WHERE event.event_identity = link.event_identity;

        INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
            dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
            deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
            canonicality_state)
        SELECT spec.namespace || ':' || spec.node, spec.namespace,
               CASE WHEN spec.class = 'rawless' THEN NULL ELSE 'n' || spec.i || '.eth' END,
               CASE WHEN spec.class = 'rawless' THEN NULL ELSE ARRAY['n' || spec.i, 'eth'] END,
               CASE WHEN spec.class = 'rawless' THEN NULL ELSE '\x00'::bytea END,
               spec.node,
               CASE WHEN spec.class = 'rawless' THEN ARRAY[spec.node] ELSE ARRAY['a', 'b'] END,
               'v',
               CASE WHEN spec.class = 'shadow' THEN 'shadow' ELSE 'active' END,
               CASE WHEN spec.class = 'shadow' THEN 'fixture' END,
               CASE WHEN spec.class = 'shadow' THEN now() END,
               '{CHAIN}',
               CASE spec.class WHEN 'later' THEN 'b' || ({MARKER} + 100)
                   WHEN 'orphaned' THEN 'orphan-500' ELSE 'b500' END,
               CASE WHEN spec.class = 'later' THEN {MARKER} + 100 ELSE 500 END,
               'canonical'::canonicality_state
        FROM (
            SELECT i, '0x' || lpad(to_hex(i), 64, '0') AS node,
                   CASE WHEN i % 31 = 0 AND i % 5 <> 0 THEN 'basenames' ELSE 'ens' END
                       AS namespace,
                   CASE WHEN i % 5 = 0 THEN 'named' WHEN i % 7 = 0 THEN 'shadow'
                        WHEN i % 11 = 0 THEN 'later' WHEN i % 13 = 0 THEN 'orphaned'
                        WHEN i % 17 = 0 THEN 'rawless' WHEN i % 31 = 0 THEN 'other' END AS class
            FROM generate_series(1, 150) i
        ) spec
        WHERE spec.class IS NOT NULL
        UNION ALL SELECT 'ens:{ZERO_NODE}', 'ens', '', ARRAY[]::text[], '\x00'::bytea,
               '{ZERO_NODE}', ARRAY[]::text[], 'v', 'active', NULL, NULL, '{CHAIN}', 'b500', 500,
               'canonical'::canonicality_state
        UNION ALL SELECT 'ens:0x' || lpad(to_hex(3000000 + n), 64, '0'), 'ens', 'f' || n || '.eth',
               ARRAY['f' || n, 'eth'], '\x00'::bytea, '0x' || lpad(to_hex(3000000 + n), 64, '0'),
               ARRAY['a', 'b'], 'v', 'active', NULL, NULL, '{CHAIN}', 'b' || (1 + n % {MARKER}),
               1 + n % {MARKER}, 'canonical'::canonicality_state
        FROM generate_series(1, 20000) n;

        ANALYZE chain_lineage, project_family_marker, project_resolver_link, normalized_events,
            name_surfaces;
        "#
    ))
    .execute(&mut *connection)
    .await
    .context("failed to install the resolver links fixture")?;
    Ok(())
}
