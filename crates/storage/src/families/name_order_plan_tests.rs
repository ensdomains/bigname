//! Plan-shape tests for the readers that walk readable name surfaces in name order: the search
//! candidates and a resolver's bound-name candidates.
//!
//! Search and bound names must be able to read `name_surfaces_name_order_idx` in page order, with
//! the keyset cursor as its index condition and no Sort, under either plan. The fixture is small,
//! so `enable_sort` is off to stand in for a large table: the assertions are about which access
//! paths the planner can use at all, not about costs. A `LIKE` prefix becomes an index range only
//! on a C-collated database, so it stays a filter on the ordered scan here.
//!
//! Every walk returns the same rows in batches as in one sorted read, including a name of exactly
//! 2000 bytes, and none returns the name longer than 2000 bytes, which inserts although the index
//! leaves it out.
//!
//! The readers run those statements as the first arm of a two-arm statement whose second arm
//! walks the surfaces that store no raw bytes. Inside it the first arm keeps the plan it has by
//! itself, and the merged walk is in served-name order. The second arm first asks whether any
//! such surface exists, through `name_surfaces_project_suffix_hash_idx`, and is not executed
//! when none does.

use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, raw_sql};

use super::{
    id_index_plan_tests::{PLAN_MODES, Probe, explain_execute, missing_probes, with_database},
    name::{
        BOUND_CANDIDATES_SQL, SEARCH_CANDIDATES_SQL,
        rendered::rendered_name_sql,
        seams::{bound_candidates_sql, search_candidates_sql},
    },
    topology::textless_surfaces_exist_sql,
};

const CHAIN: &str = "ethereum-sepolia";
const ROWS: i64 = 2_000;
/// Surfaces without raw bytes, after the `ROWS + 2` with them.
const TEXTLESS: i64 = 60;
const ORDER_INDEX: &str = "name_surfaces_name_order_idx";
const KEYSET: &str = "(ROW(raw_name, namespace, namehash) > ROW(";
const TEXTLESS_INDEX: &str = "name_surfaces_project_suffix_hash_idx";

type Candidate = (String, String, String, String);

/// A prepared reader and the values of one call, with `{after}` and `{limit}` left open.
struct Walk {
    label: &'static str,
    statement: &'static str,
    values: String,
    /// For a two-arm reader, the prepared first arm and the predicate of the names the walk
    /// returns, over `surface` and its served `name`.
    arms: Option<(&'static str, String)>,
}

#[tokio::test]
async fn name_ordered_walks_read_the_name_order_index() -> Result<()> {
    with_database("family_name_order_plan", async |connection| {
        install_fixture(connection).await?;
        prepare(connection).await?;
        check_ordered_plans(connection).await?;
        check_walks(connection).await?;
        check_skipped_arm(connection).await
    })
    .await
}

async fn prepare(connection: &mut PgConnection) -> Result<()> {
    let (search, bound) = (search_candidates_sql(), bound_candidates_sql());
    for (label, types, sql) in [
        (
            "search_candidates",
            "text[], text, text, text, text, text, bigint",
            SEARCH_CANDIDATES_SQL,
        ),
        (
            "bound_candidates",
            "text, text, text, text, text, text, bigint",
            BOUND_CANDIDATES_SQL,
        ),
        (
            "search_both",
            "text[], text, text, text, text, text, bigint",
            search.as_str(),
        ),
        (
            "bound_both",
            "text, text, text, text, text, text, bigint",
            bound.as_str(),
        ),
    ] {
        raw_sql(&format!("PREPARE {label} ({types}) AS {sql}"))
            .execute(&mut *connection)
            .await
            .with_context(|| format!("prepare {label}"))?;
    }
    Ok(())
}

fn walks() -> [Walk; 6] {
    let prefix = "'{ens}', NULL, 'a%', {after}, {limit}".to_owned();
    let contains = "'{ens,basenames}', NULL, '%a%', {after}, {limit}".to_owned();
    let bound = format!("'{CHAIN}', '{}', NULL, {{after}}, {{limit}}", address('a'));
    let reached = format!(
        "EXISTS (SELECT 1 FROM project_named_resource_pointer pointer
                 WHERE pointer.logical_name_id = surface.logical_name_id
                   AND pointer.resolver_address = '{}')",
        address('a')
    );
    [
        Walk {
            label: "search_candidates",
            statement: "search prefix",
            values: prefix.clone(),
            arms: None,
        },
        // The long name contains an `a` and is reached by resolver 0xaa..; only the length bound
        // keeps it out of these two walks. The 2000-byte name is in all three.
        Walk {
            label: "search_candidates",
            statement: "search contains",
            values: contains.clone(),
            arms: None,
        },
        Walk {
            label: "bound_candidates",
            statement: "bound names",
            values: bound.clone(),
            arms: None,
        },
        Walk {
            label: "search_both",
            statement: "search prefix, both arms",
            values: prefix,
            arms: Some((
                "search_candidates",
                "surface.namespace = 'ens' AND name LIKE 'a%'".to_owned(),
            )),
        },
        Walk {
            label: "search_both",
            statement: "search contains, both arms",
            values: contains,
            arms: Some(("search_candidates", "name LIKE '%a%'".to_owned())),
        },
        Walk {
            label: "bound_both",
            statement: "bound names, both arms",
            values: bound,
            arms: Some(("bound_candidates", reached)),
        },
    ]
}

async fn check_ordered_plans(connection: &mut PgConnection) -> Result<()> {
    raw_sql("SET enable_sort = off")
        .execute(&mut *connection)
        .await?;
    let probe = [Probe {
        index: ORDER_INDEX,
        conditions: &[KEYSET],
    }];
    // The question whether a surface without raw bytes exists probes its index. A table this
    // small is cheaper to scan, so the scans stand aside as the sort does.
    raw_sql(&format!(
        "SET enable_seqscan = off; SET enable_bitmapscan = off;
         PREPARE textless_exist AS SELECT {}",
        textless_surfaces_exist_sql()
    ))
    .execute(&mut *connection)
    .await?;
    let plan: Vec<String> = raw_sql("EXPLAIN (COSTS OFF) EXECUTE textless_exist")
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(|row| sqlx::Row::try_get(row, 0).map_err(Into::into))
        .collect::<Result<_>>()?;
    let mut failures = missing_probes(
        "a surface without raw bytes exists",
        &plan,
        &[Probe {
            index: TEXTLESS_INDEX,
            conditions: &[
                "namespace = ANY",
                "hash_array_extended(raw_labels",
                "IS NULL",
            ],
        }],
    );
    raw_sql("RESET enable_seqscan; RESET enable_bitmapscan")
        .execute(&mut *connection)
        .await?;
    for walk in &walks() {
        // The first batch and a continuation both bind the cursor as the index condition.
        for after in [None, Some(("a", "ens", "0x"))] {
            let values = values(walk, after, 201);
            for mode in PLAN_MODES {
                let plan = explain_execute(connection, mode, walk.label, &values).await?;
                let label = format!("{} after {after:?} ({mode})", walk.statement);
                failures.extend(missing_probes(&label, &plan, &probe));
                let Some((first_arm, _)) = &walk.arms else {
                    if plan.iter().any(|line| line.contains("Sort")) {
                        failures.push(format!("{label}: sorts\n{}", plan.join("\n")));
                    }
                    continue;
                };
                // The second arm sorts its served names; the first arm's plan is the one it has
                // as a statement of its own.
                let alone = explain_execute(connection, mode, first_arm, &values).await?;
                if !contains_plan(&plan, &alone) {
                    failures.push(format!(
                        "{label}: the first arm's plan changed\n{}\nalone:\n{}",
                        plan.join("\n"),
                        alone.join("\n")
                    ));
                }
            }
        }
    }
    raw_sql("RESET enable_sort")
        .execute(&mut *connection)
        .await?;
    ensure!(
        failures.is_empty(),
        "name-ordered plans:\n{}",
        failures.join("\n\n")
    );
    Ok(())
}

/// Whether `alone`, the plan of a statement, is a subtree of `plan` line for line, whatever its
/// depth there.
fn contains_plan(plan: &[String], alone: &[String]) -> bool {
    let nodes = |lines: &[String]| -> Vec<String> {
        lines
            .iter()
            .map(|line| line.trim_start().trim_start_matches("->").trim().to_owned())
            .collect()
    };
    let (plan, alone) = (nodes(plan), nodes(alone));
    plan.windows(alone.len()).any(|window| window == alone)
}

/// The rows a two-arm walk must return, in its order, read without either arm's statement.
async fn expected(connection: &mut PgConnection, predicate: &str) -> Result<Vec<Candidate>> {
    sqlx::query_as(&format!(
        "SELECT logical_name_id, name, namespace, namehash
         FROM (SELECT surface.*, {name} AS name FROM name_surfaces surface) surface
         JOIN chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         WHERE surface.visibility_state = 'active'
           AND surface.canonicality_state = 'canonical'
           AND lineage.canonicality_state = 'canonical'
           AND (surface.raw_name IS NULL OR octet_length(surface.raw_name) <= 2000)
           AND {predicate}
         ORDER BY name, namespace, namehash",
        name = rendered_name_sql("surface")
    ))
    .fetch_all(&mut *connection)
    .await
    .context("expected walk")
}

async fn check_walks(connection: &mut PgConnection) -> Result<()> {
    let (long, boundary) = sqlx::query_as::<_, (String, String)>(
        "SELECT
             (SELECT logical_name_id FROM name_surfaces WHERE octet_length(raw_name) > 2000),
             (SELECT logical_name_id FROM name_surfaces WHERE octet_length(raw_name) = 2000)",
    )
    .fetch_one(&mut *connection)
    .await?;
    let textless: Vec<String> =
        sqlx::query_scalar("SELECT logical_name_id FROM name_surfaces WHERE raw_name IS NULL")
            .fetch_all(&mut *connection)
            .await?;
    for walk in walks() {
        let sorted = read(connection, &walk, None, ROWS * 2).await?;
        ensure!(
            sorted.len() > 20,
            "{}: {} rows",
            walk.statement,
            sorted.len()
        );
        ensure!(
            sorted.iter().all(|(id, ..)| *id != long),
            "{}: returned the long name",
            walk.statement
        );
        ensure!(
            sorted.iter().any(|(id, ..)| *id == boundary),
            "{}: missed the 2000-byte name",
            walk.statement
        );
        if let Some((_, predicate)) = &walk.arms {
            ensure!(
                sorted == expected(connection, predicate).await?,
                "{}: the two arms differ from the surfaces in served-name order",
                walk.statement
            );
            ensure!(
                sorted
                    .iter()
                    .filter(|(id, ..)| textless.contains(id))
                    .count()
                    > 3,
                "{}: too few surfaces without raw bytes",
                walk.statement
            );
        } else {
            ensure!(
                sorted.iter().all(|(id, ..)| !textless.contains(id)),
                "{}: the first arm returned a surface without raw bytes",
                walk.statement
            );
        }
        raw_sql("SET enable_sort = off")
            .execute(&mut *connection)
            .await?;
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *connection)
                .await?;
            let mut walked = Vec::new();
            loop {
                let after = walked
                    .last()
                    .map(|(_, name, namespace, namehash): &Candidate| {
                        (name.as_str(), namespace.as_str(), namehash.as_str())
                    });
                let batch = read(connection, &walk, after, 7).await?;
                let done = batch.len() < 7;
                walked.extend(batch);
                if done {
                    break;
                }
            }
            ensure!(
                walked == sorted,
                "{} ({mode}): the batched walk differs from the sorted read",
                walk.statement
            );
        }
        raw_sql("RESET enable_sort; RESET plan_cache_mode")
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

/// With no surface without raw bytes, a two-arm statement returns its first arm's rows and never
/// executes the second arm.
async fn check_skipped_arm(connection: &mut PgConnection) -> Result<()> {
    raw_sql("DELETE FROM name_surfaces WHERE raw_name IS NULL; ANALYZE name_surfaces")
        .execute(&mut *connection)
        .await?;
    for walk in walks() {
        let Some((first_arm, _)) = &walk.arms else {
            continue;
        };
        let alone = Walk {
            label: first_arm,
            statement: walk.statement,
            values: walk.values.clone(),
            arms: None,
        };
        ensure!(
            read(connection, &walk, None, ROWS * 2).await?
                == read(connection, &alone, None, ROWS * 2).await?,
            "{}: differs from its first arm",
            walk.statement
        );
        for mode in PLAN_MODES {
            let plan: Vec<String> = raw_sql(&format!(
                "SET plan_cache_mode = {mode};
                 EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) EXECUTE {} ({})",
                walk.label,
                values(&walk, None, 201)
            ))
            .fetch_all(&mut *connection)
            .await?
            .iter()
            .map(|row| sqlx::Row::try_get(row, 0).map_err(Into::into))
            .collect::<Result<_>>()?;
            let skipped = plan.windows(2).any(|lines| {
                lines[0].contains("One-Time Filter") && lines[1].contains("(never executed)")
            });
            ensure!(
                skipped,
                "{} ({mode}): the second arm ran\n{}",
                walk.statement,
                plan.join("\n")
            );
        }
    }
    raw_sql("RESET plan_cache_mode")
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn read(
    connection: &mut PgConnection,
    walk: &Walk,
    after: Option<(&str, &str, &str)>,
    limit: i64,
) -> Result<Vec<Candidate>> {
    sqlx::query_as(&format!(
        "EXECUTE {} ({})",
        walk.label,
        values(walk, after, limit)
    ))
    .persistent(false)
    .fetch_all(&mut *connection)
    .await
    .with_context(|| walk.statement)
}

fn values(walk: &Walk, after: Option<(&str, &str, &str)>, limit: i64) -> String {
    let after = after.map_or_else(
        || "NULL, NULL, NULL".to_owned(),
        |(name, namespace, namehash)| format!("'{name}', '{namespace}', '{namehash}'"),
    );
    walk.values
        .replace("{after}", &after)
        .replace("{limit}", &limit.to_string())
}

fn address(fill: char) -> String {
    format!("0x{}", fill.to_string().repeat(40))
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    raw_sql(include_str!("../../schema/baseline/07_labels.sql"))
        .execute(&mut *connection)
        .await?;
    // Name n is active unless n is a multiple of 50 and readable unless its block, every 70th, is
    // orphaned; even names are ens and odd names basenames. Name ROWS + 1 is about 6.6 KB of
    // incompressible labels, past the btree entry limit, and name ROWS + 2 is exactly 2000 bytes.
    // Resolver 0xbb.. names the first three names and 0xaa.. all others.
    raw_sql(&format!(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT '{CHAIN}', 'block-' || n, n, to_timestamp(n),
                (CASE WHEN n % 70 = 0 THEN 'orphaned' ELSE 'canonical' END)::canonicality_state
         FROM generate_series(1, {ROWS} + 2 + {TEXTLESS}) n;

         INSERT INTO project_family_marker (chain_id, current_block_number, current_block_hash,
             state)
         VALUES ('{CHAIN}', {ROWS} + 2 + {TEXTLESS},
                 'block-' || ({ROWS} + 2 + {TEXTLESS}), 'live');

         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT namespace || ':' || namehash, namespace, raw_name, ARRAY[raw_name], '\\x00',
                namehash, ARRAY[namehash], 'v1',
                CASE WHEN n % 50 = 0 THEN 'shadow' ELSE 'active' END,
                CASE WHEN n % 50 = 0 THEN 'invalid' END, CASE WHEN n % 50 = 0 THEN now() END,
                '{CHAIN}', 'block-' || n, n,
                (CASE WHEN n % 70 = 0 THEN 'orphaned' ELSE 'canonical' END)::canonicality_state
         FROM generate_series(1, {ROWS} + 2) n,
         LATERAL (
             SELECT CASE WHEN n % 2 = 0 THEN 'ens' ELSE 'basenames' END AS namespace,
                    '0x' || lpad(to_hex(n), 64, '0') AS namehash,
                    CASE WHEN n = {ROWS} + 1
                         THEN (SELECT string_agg(md5(i::text), '.')
                               FROM generate_series(1, 200) i) || '.eth'
                         WHEN n = {ROWS} + 2
                         THEN 'a' || left((SELECT string_agg(md5(i::text), '.')
                                           FROM generate_series(1, 200) i), 1995) || '.eth'
                         ELSE substr(md5(n::text), 1, 8)
                              || CASE WHEN n % 2 = 0 THEN '.eth' ELSE '.base.eth' END
                    END AS raw_name
         ) surface;

         -- Surfaces without raw bytes, of two labels. Every third has a usable preimage of its
         -- first label, an `a` and hex digits, so it is served as text among the names above;
         -- the others are served by their bracketed label hash.
         INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT namespace || ':' || namehash, namespace, namehash,
                ARRAY['0x' || md5(n::text) || md5('label' || n), '0x' || repeat('e', 64)],
                'v1', 'active', '{CHAIN}', 'block-' || n, n,
                (CASE WHEN n % 70 = 0 THEN 'orphaned' ELSE 'canonical' END)::canonicality_state
         FROM generate_series({ROWS} + 3, {ROWS} + 2 + {TEXTLESS}) n,
         LATERAL (
             SELECT CASE WHEN n % 2 = 0 THEN 'ens' ELSE 'basenames' END AS namespace,
                    '0x' || lpad(to_hex(n), 64, '0') AS namehash
         ) surface;

         INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority)
         SELECT '0x' || md5(n::text) || md5('label' || n), convert_to(label, 'UTF8'), label,
                'v1', true, 'fixture', 0
         FROM generate_series({ROWS} + 3, {ROWS} + 2 + {TEXTLESS}, 3) n,
         LATERAL (SELECT 'a' || substr(md5(n::text), 1, 7) AS label) preimage;

         INSERT INTO project_named_resource_pointer (chain_id, resource_id, logical_name_id,
             block_number, event_identity, resolver_address, source_family)
         SELECT chain_id, lpad(to_hex(block_number), 32, '0')::uuid, logical_name_id,
                block_number, 'pointer:' || block_number,
                '0x' || repeat(CASE WHEN block_number <= 3 THEN 'b' ELSE 'a' END, 40),
                'ens_v1_registry_l1'
         FROM name_surfaces;

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
