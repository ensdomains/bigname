//! Plan and row tests for the ENSv1 registry pointer lookup of the mirror substitution
//! (`registry_pointer_rows` in `push_mirror_writes`).
//!
//! The pointer index's key is an expression and the index is partial. The planner never reads a
//! partial index's expression statistics, so it estimates the node equality at the default
//! selectivity whatever the node. The previous statement put that equality beside the chain and
//! the bound's block under `ORDER BY … LIMIT 1`, and against the estimate the planner could read
//! the chain's rows up to the bound on the chain-and-block index. The fixture has unrelated ENSv1
//! pointer and registrar rows interleaved as on a live chain, and pointers of the walked names
//! that are old. There the previous statement's generic plan reads the walked node's pointers
//! through a bitmap of the chain-and-block index over every block up to the bound. The plan test
//! checks that it still does, so the fixture keeps proving something, and that the fenced
//! statement reads only the pointer index, never the chain-and-block index, in both plan modes
//! with statistics off and on. A second fixture adds enough unrelated pointers that they are most
//! of the chain's rows. There the previous statement's custom plan walks the chain's rows newest
//! first on the chain-and-block index until the walked node's pointer, and the fenced statement
//! still reads only the pointer index. The row test pins each input class to its selected pointer
//! and to the previous statement's rows.

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{
    Connection, Execute, PgConnection, Postgres, QueryBuilder, Row, postgres::PgArguments, raw_sql,
};
use uuid::Uuid;

use super::{MirrorPointer, MirrorWalk, push_mirror_writes, registry_pointer_rows};

const POINTER_INDEX: &str = "normalized_events_project_v1_pointer_addressed_node_idx";
const MAINNET: &str = "ethereum-mainnet";
const BOUND: i64 = 30_150;

/// The fixture's names, by the number their namehash encodes.
const CHILD_NODE: u32 = 0xc1;
const NAMEHASH: u32 = 0xc2;
const NODE: u32 = 0xc3;
const NO_POINTER: u32 = 0xc4;
const CLEARED: u32 = 0xc5;
const UNDER_ROOT: u32 = 0xc6;
const HOT_NODE: u32 = 0xc7;
const ABOVE_BOUND: u32 = 0xc8;
const ORPHANED: u32 = 0xc9;
const UNPOSITIONED: u32 = 0xca;
const NAME_ONLY: u32 = 0xcb;
const ETH: u32 = 0xe7;

/// One row class: its label, the walked names (each with its ancestors, one depth further each)
/// and the selected pointer as (depth, resolver number), or none.
type RowCase = (
    &'static str,
    &'static [(u32, &'static [u32])],
    Option<(i32, u32)>,
);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Statistics {
    /// No statistics on the probe expression. The planner ignores them on a partial index either
    /// way, so both variants should plan alike. The pair guards against that ever changing.
    Off,
    On,
}

impl Statistics {
    fn label(self) -> &'static str {
        match self {
            Self::Off => "no expression statistics",
            Self::On => "expression statistics",
        }
    }

    async fn apply(self, connection: &mut PgConnection) -> Result<()> {
        let target = match self {
            Self::Off => 0,
            Self::On => -1,
        };
        // A sample of 300 x 300 rows covers every fixture table whole, so each run plans on the
        // same statistics.
        raw_sql(&format!(
            "ALTER INDEX {POINTER_INDEX} ALTER COLUMN 3 SET STATISTICS {target};
             SET default_statistics_target = 300;
             ANALYZE normalized_events, name_surfaces, chain_lineage;
             RESET default_statistics_target;"
        ))
        .execute(&mut *connection)
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn mirror_registry_probe_reads_only_the_pointer_index() -> Result<()> {
    with_fixture("mirror_registry_plan", async |connection| {
        let published = BTreeMap::from([(MAINNET.to_owned(), BOUND)]);
        let mut failures = Vec::new();
        for statistics in [Statistics::Off, Statistics::On] {
            statistics.apply(connection).await?;
            for (label, k) in [
                ("old pointer", CHILD_NODE),
                ("hot node", HOT_NODE),
                ("no pointer", NO_POINTER),
            ] {
                let walk = walk(&[(k, &[])]);
                let (sql, arguments) = render(&walk, &published)?;
                let previous = previous_sql(&sql)?;
                for generic in [false, true] {
                    let mode = if generic {
                        "generic plan"
                    } else {
                        "custom plan"
                    };
                    let cell = format!("{}, {mode}, {label}", statistics.label());
                    let plan = explain(connection, &sql, arguments.clone(), generic).await?;
                    let reads = registry_reads(&plan[0]["Plan"]);
                    println!("{cell}: {}", json!(reads));
                    if reads.is_empty() {
                        failures.push(format!("{cell}: no registry pointer read in the plan"));
                    }
                    for read in &reads {
                        if read["indexes"] != json!([POINTER_INDEX]) {
                            failures.push(format!(
                                "{cell}: the registry pointer read is not on {POINTER_INDEX} \
                                 alone: {read}"
                            ));
                        }
                    }
                    let before = explain(connection, &previous, arguments.clone(), generic).await?;
                    let before_reads = registry_reads(&before[0]["Plan"]);
                    println!("{cell}, previous statement: {}", json!(before_reads));
                    // Here the previous statement's generic plan reads the walked node's pointers
                    // through a bitmap of the chain-and-block index over every block up to the
                    // bound, for every class and either statistics setting.
                    if generic
                        && !before_reads.iter().any(|read| {
                            read["type"] == "Bitmap Heap Scan"
                                && read["indexes"].as_array().is_some_and(|indexes| {
                                    indexes.iter().any(|index| {
                                        index == "normalized_events_chain_block_number_desc_idx"
                                    })
                                })
                        })
                    {
                        failures.push(format!(
                            "{cell}: the previous statement no longer reads the chain-and-block \
                             index, so this fixture proves nothing about it"
                        ));
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "mirror registry plans:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn mirror_registry_probe_rows_and_oracle_parity() -> Result<()> {
    with_fixture("mirror_registry_rows", async |connection| {
        let published = BTreeMap::from([(MAINNET.to_owned(), BOUND)]);
        // The child_node and namehash names also carry a newer pointer the readable terms or the
        // family list exclude, inside the fence and outside it.
        let cases: [RowCase; 11] = [
            (
                "pointer by child_node",
                &[(CHILD_NODE, &[])],
                Some((0, CHILD_NODE)),
            ),
            (
                "pointer by namehash",
                &[(NAMEHASH, &[])],
                Some((0, NAMEHASH)),
            ),
            ("pointer by node", &[(NODE, &[])], Some((0, NODE))),
            ("no pointer", &[(NO_POINTER, &[])], None),
            ("latest pointer cleared", &[(CLEARED, &[])], None),
            (
                "eth as the ancestor",
                &[(UNDER_ROOT, &[ETH])],
                Some((1, ETH)),
            ),
            ("hot node", &[(HOT_NODE, &[])], Some((0, 0x7063))),
            (
                "pointer above the bound",
                &[(ABOVE_BOUND, &[])],
                Some((0, ABOVE_BOUND)),
            ),
            (
                "pointer on an orphaned block",
                &[(ORPHANED, &[])],
                Some((0, ORPHANED)),
            ),
            (
                "pointer with no chain position",
                &[(UNPOSITIONED, &[])],
                Some((0, UNPOSITIONED)),
            ),
            ("name match with no node fields", &[(NAME_ONLY, &[])], None),
        ];
        let mut failures = Vec::new();
        for statistics in [Statistics::Off, Statistics::On] {
            statistics.apply(connection).await?;
            for mode in ["force_custom_plan", "force_generic_plan"] {
                raw_sql(&format!("SET plan_cache_mode = {mode}"))
                    .execute(&mut *connection)
                    .await?;
                for (label, names, expected) in &cases {
                    let cell = format!("{}, {mode}, {label}", statistics.label());
                    let walk = walk(names);
                    let (sql, arguments) = render(&walk, &published)?;
                    let previous = previous_sql(&sql)?;
                    let candidates =
                        rows(connection, &candidates_sql(&sql)?, arguments.clone()).await?;
                    let before =
                        rows(connection, &candidates_sql(&previous)?, arguments.clone()).await?;
                    let expected = expected.map_or(json!([]), |(depth, resolver)| {
                        json!([[depth, resolver_address(resolver)]])
                    });
                    let selected = json!(
                        candidates
                            .as_array()
                            .into_iter()
                            .flatten()
                            .map(|row| json!([
                                row["ancestor_depth"],
                                row["mirrored_resolver_address"]
                            ]))
                            .collect::<Vec<_>>()
                    );
                    if candidates != before || selected != expected {
                        failures.push(format!(
                            "{cell}: pointers {candidates}\nexpected {expected}\nprevious {before}"
                        ));
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "mirror registry rows:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

/// The custom-plan walk itself, on a chain where ENSv1 pointers are most of the rows: 360,000 more
/// unrelated pointers over 2,000 other nodes. There, with the default selectivity for the node
/// equality, the previous statement's custom plan walks the chain's rows newest first on the
/// chain-and-block index and filters them until the walked node's pointer. The fenced statement
/// still reads only the pointer index, in both plan modes with statistics off and on.
#[tokio::test]
async fn mirror_registry_probe_custom_walk_on_a_pointer_heavy_chain() -> Result<()> {
    let started = std::time::Instant::now();
    let result = mirror_registry_probe_custom_walk_inner().await;
    let _ = std::io::Write::write_fmt(
        &mut std::io::stderr(),
        format_args!("timing | custom walk test | {:?}\n", started.elapsed()),
    );
    result
}

async fn mirror_registry_probe_custom_walk_inner() -> Result<()> {
    with_fixture("mirror_registry_custom_walk", async |connection| {
        raw_sql(POINTER_HEAVY_SQL).execute(&mut *connection).await?;
        let published = BTreeMap::from([(MAINNET.to_owned(), BOUND)]);
        let mut failures = Vec::new();
        for statistics in [Statistics::Off, Statistics::On] {
            statistics.apply(connection).await?;
            for (label, k) in [("old pointer", CHILD_NODE), ("hot node", HOT_NODE)] {
                let walk = walk(&[(k, &[])]);
                let (sql, arguments) = render(&walk, &published)?;
                let previous = previous_sql(&sql)?;
                for generic in [false, true] {
                    let mode = if generic {
                        "generic plan"
                    } else {
                        "custom plan"
                    };
                    let cell = format!(
                        "pointer-heavy chain, {}, {mode}, {label}",
                        statistics.label()
                    );
                    let plan = explain(connection, &sql, arguments.clone(), generic).await?;
                    let reads = registry_reads(&plan[0]["Plan"]);
                    println!("{cell}: {}", json!(reads));
                    if reads.is_empty()
                        || reads
                            .iter()
                            .any(|read| read["indexes"] != json!([POINTER_INDEX]))
                    {
                        failures.push(format!(
                            "{cell}: the registry pointer read is not on {POINTER_INDEX} alone: \
                             {}",
                            json!(reads)
                        ));
                    }
                    if statistics == Statistics::Off && !generic {
                        let before =
                            explain(connection, &previous, arguments.clone(), false).await?;
                        let before_reads = registry_reads(&before[0]["Plan"]);
                        println!("{cell}, previous statement: {}", json!(before_reads));
                        if !before_reads.iter().any(|read| {
                            read["type"] == "Index Scan"
                                && read["indexes"]
                                    == json!(["normalized_events_chain_block_number_desc_idx"])
                                && read["removed"].as_f64().unwrap_or(0.0) >= 100_000.0
                        }) {
                            failures.push(format!(
                                "{cell}: the previous statement no longer walks the \
                                 chain-and-block index here, so this fixture proves nothing \
                                 about it"
                            ));
                        }
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "mirror registry plans on a pointer-heavy chain:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

/// The fenced and the previous mirror substitution for one name on a retained database, for the
/// before and after record: each as a custom plan executed under `EXPLAIN (ANALYZE, BUFFERS)`
/// and as a generic plan. Read-only. The name comes from the environment. The published bound is
/// the database's family marker.
#[tokio::test]
#[ignore = "manual read-only plans of the fenced and previous mirror registry probe for one name"]
async fn mirror_registry_probe_retained_plans() -> Result<()> {
    let url = std::env::var("BIGNAME_HISTORY_RETAINED_DATABASE_URL")?;
    let directory = std::path::PathBuf::from(std::env::var("BIGNAME_ADDRESS_HISTORY_PLAN_DIR")?);
    std::fs::create_dir_all(&directory)?;
    let name = std::env::var("BIGNAME_HISTORY_MIRROR_NAME")?;
    let mut connection = PgConnection::connect(&url).await?;
    raw_sql(
        "SET search_path=bigname_phase,public; SET jit=off; SET statement_timeout='280s'; BEGIN READ ONLY",
    )
    .execute(&mut connection)
    .await?;
    let surface = sqlx::query(
        "SELECT surface.chain_id, surface.namespace, surface.raw_labels, surface.labelhashes,
                surface.namehash, marker.current_block_number
         FROM name_surfaces surface
         JOIN project_family_marker marker ON marker.chain_id = surface.chain_id
         WHERE surface.raw_name = $1
           AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY surface.block_number DESC NULLS LAST
         LIMIT 1",
    )
    .bind(&name)
    .fetch_one(&mut connection)
    .await
    .with_context(|| format!("no readable surface named {name}"))?;
    let chain_id: String = surface.try_get(0)?;
    let marker: Option<i64> = surface.try_get(5)?;
    let published = BTreeMap::from([(
        chain_id.clone(),
        marker.context("the family marker has no block")?,
    )]);
    let walk = MirrorWalk::new(&[MirrorPointer {
        resource_id: Uuid::nil(),
        chain_id,
        namespace: surface.try_get(1)?,
        raw_labels: surface.try_get(2)?,
        labelhashes: surface.try_get(3)?,
        namehash: surface.try_get(4)?,
        followable: true,
    }])?;
    let (sql, arguments) = render(&walk, &published)?;
    let previous = previous_sql(&sql)?;
    for (version, statement) in [("fenced", &sql), ("previous", &previous)] {
        std::fs::write(directory.join(format!("mirror-{version}.sql")), statement)?;
        for generic in [false, true] {
            let mode = if generic { "generic" } else { "custom" };
            let plan = explain(&mut connection, statement, arguments.clone(), generic).await?;
            std::fs::write(
                directory.join(format!("mirror-{version}-{mode}.json")),
                serde_json::to_vec_pretty(&plan)?,
            )?;
            println!(
                "mirror-{version}-{mode}: {}",
                json!({"planning_ms":plan[0]["Planning Time"], "execution_ms":plan[0]["Execution Time"],
                       "registry_reads":registry_reads(&plan[0]["Plan"])})
            );
        }
    }
    raw_sql("ROLLBACK").execute(&mut connection).await?;
    Ok(())
}

fn namehash(k: u32) -> String {
    format!("0x{k:064x}")
}

fn resolver_address(k: u32) -> String {
    format!("0x{k:040x}")
}

/// A walk over the first name and then each listed ancestor, one depth further each, with the
/// fixture's raw labels.
fn walk(names: &[(u32, &[u32])]) -> MirrorWalk {
    let mut walk = MirrorWalk::default();
    for (k, ancestors) in names {
        let queried = namehash(*k);
        for (depth, node) in std::iter::once(k).chain(ancestors.iter()).enumerate() {
            walk.resource_ids.push(Uuid::from_u128(u128::from(*k)));
            walk.chain_ids.push(MAINNET.to_owned());
            walk.namespaces.push("ens".to_owned());
            walk.depths
                .push(i32::try_from(depth).expect("a short walk"));
            walk.nodes.push(namehash(*node));
            walk.labels.push(Some(labels(*node)));
            walk.hashes.push(Value::Array(Vec::new()));
            walk.queried_nodes.push(queried.clone());
        }
    }
    walk
}

fn labels(k: u32) -> Value {
    if k == ETH {
        json!(["eth"])
    } else {
        json!([format!("n{k:x}"), "eth"])
    }
}

fn render(walk: &MirrorWalk, published: &BTreeMap<String, i64>) -> Result<(String, PgArguments)> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_mirror_writes(&mut builder, walk, Some(published), None, None);
    let mut query = builder.build();
    let sql = query.sql().to_owned();
    let arguments = query
        .take_arguments()
        .map_err(|error| anyhow!("statement arguments: {error}"))?
        .context("bound statement")?;
    Ok((sql, arguments))
}

/// The statement as it was before the fence: the pointer rows read from the table itself.
fn previous_sql(sql: &str) -> Result<String> {
    let fence = registry_pointer_rows();
    ensure!(
        sql.matches(&fence).count() == 1,
        "the statement reads the pointer rows behind the fence exactly once"
    );
    Ok(sql.replacen(&fence, "bigname_phase.normalized_events registry", 1))
}

/// The statement's `candidates` CTE alone: the selected pointer of every walked node. The
/// later statement's parameters stay bound and unused.
fn candidates_sql(sql: &str) -> Result<String> {
    let (head, _) = sql
        .split_once("\n        nearest AS (")
        .context("the statement has a nearest CTE")?;
    let head = head
        .trim_end()
        .strip_suffix(',')
        .context("the candidates CTE ends before nearest")?;
    Ok(format!(
        "{head} SELECT * FROM candidates ORDER BY ancestor_depth, mirrored_resolver_address"
    ))
}

async fn explain(
    connection: &mut PgConnection,
    sql: &str,
    arguments: PgArguments,
    generic: bool,
) -> Result<Value> {
    if generic {
        raw_sql("SET plan_cache_mode = force_generic_plan")
            .execute(&mut *connection)
            .await?;
        // Unbound parameters: GENERIC_PLAN infers their types from the statement.
        let plan: Value = raw_sql(&format!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {sql}"))
            .fetch_one(&mut *connection)
            .await
            .context("generic plan")?
            .try_get(0)?;
        Ok(plan)
    } else {
        raw_sql("SET plan_cache_mode = force_custom_plan")
            .execute(&mut *connection)
            .await?;
        sqlx::query_scalar_with(
            &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"),
            arguments,
        )
        .persistent(false)
        .fetch_one(&mut *connection)
        .await
        .context("custom plan")
    }
}

async fn rows(connection: &mut PgConnection, sql: &str, arguments: PgArguments) -> Result<Value> {
    sqlx::query_scalar_with(
        &format!("SELECT coalesce(jsonb_agg(to_jsonb(result)), '[]'::jsonb) FROM ({sql}) result"),
        arguments,
    )
    .fetch_one(&mut *connection)
    .await
    .context("statement rows")
}

/// The registry pointer reads: `normalized_events` scans the planner aliases `registry…`.
fn registry_reads(node: &Value) -> Vec<Value> {
    let mut found = Vec::new();
    if node["Relation Name"] == "normalized_events"
        && node["Alias"]
            .as_str()
            .is_some_and(|alias| alias.starts_with("registry"))
    {
        let mut indexes = Vec::new();
        index_names(node, &mut indexes);
        found.push(
            json!({"type":node["Node Type"],"alias":node["Alias"],"indexes":indexes,
            "rows":node["Actual Rows"],"removed":node["Rows Removed by Filter"],
            "loops":node["Actual Loops"],"shared_hit":node["Shared Hit Blocks"]}),
        );
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        found.extend(registry_reads(child));
    }
    found
}

fn index_names(node: &Value, indexes: &mut Vec<String>) {
    if let Some(index) = node["Index Name"].as_str() {
        indexes.push(index.to_owned());
    }
    if matches!(
        node["Node Type"].as_str(),
        Some("Bitmap Heap Scan" | "BitmapOr" | "BitmapAnd")
    ) {
        for child in node["Plans"].as_array().into_iter().flatten() {
            index_names(child, indexes);
        }
    }
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
            include_str!("../../../schema/baseline/01_chain.sql"),
            include_str!("../../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../../schema/baseline/03_identity.sql"),
            include_str!("../../../schema/baseline/04_manifests.sql"),
            include_str!("../../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../../schema/baseline/06_projections.sql"),
        ] {
            raw_sql(baseline).execute(&mut *connection).await?;
        }
        raw_sql(FIXTURE_SQL)
            .execute(&mut *connection)
            .await
            .context("install the mirror registry fixture")?;
        raw_sql("SET jit = off").execute(&mut *connection).await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

/// 360,000 more unrelated ENSv1 pointers over 2,000 other nodes, in blocks the fixture's lineage
/// already has.
const POINTER_HEAVY_SQL: &str = r#"
INSERT INTO normalized_events(event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, block_hash, block_number, transaction_hash, transaction_index, log_index,
    derivation_kind, canonicality_state, after_state)
SELECT 'heavy:pointer:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1, 'ethereum-mainnet',
       'block-' || (100 + n % 30000), 100 + n % 30000, 'tx-heavy-' || n, 1, n % 1000,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('node', '0x' || lpad(to_hex(200000 + n % 2000), 64, '0'),
                          'resolver', '0x' || lpad(to_hex(9), 40, '0'))
FROM generate_series(1, 360000) n;
"#;

/// Lineage for blocks 1..=30200 with one orphaned block hash at 90. Name k has namehash k as 32
/// bytes of hex, raw labels `n<k>.eth`, and the resolver k as 20 bytes of hex. Names 0xc1 and
/// 0xc2 also have a newer pointer that is unreadable or from a family outside the ENSv1 list. Between block 100
/// and 30100 the chain carries 30,000 unrelated ENSv1 pointers over 2,000 nodes, interleaved with
/// 30,000 registrar rows. Autovacuum is off so the statistics are only the ones a test collects.
const FIXTURE_SQL: &str = r#"
ALTER TABLE normalized_events SET (autovacuum_enabled = false);
ALTER TABLE chain_lineage SET (autovacuum_enabled = false);
ALTER TABLE name_surfaces SET (autovacuum_enabled = false);
CREATE FUNCTION pg_temp.node(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 64, '0') $$;
CREATE FUNCTION pg_temp.resolver(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 40, '0') $$;
INSERT INTO chain_lineage(chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT 'ethereum-mainnet', 'block-' || b, b, to_timestamp(b), 'canonical'::canonicality_state
FROM generate_series(1, 30200) b
UNION ALL
SELECT 'ethereum-mainnet', 'orphan-90', 90, to_timestamp(90), 'orphaned'::canonicality_state;
INSERT INTO name_surfaces(logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
    namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
    block_number, canonicality_state)
SELECT 'ens:' || pg_temp.node(k), 'ens',
       CASE WHEN k = 231 THEN 'eth' ELSE 'n' || to_hex(k) || '.eth' END,
       CASE WHEN k = 231 THEN ARRAY['eth'] ELSE ARRAY['n' || to_hex(k), 'eth'] END,
       '\x00'::bytea, pg_temp.node(k),
       CASE WHEN k = 231 THEN ARRAY[pg_temp.node(k)] ELSE ARRAY[pg_temp.node(k), pg_temp.node(231)] END,
       'test', 'active', 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
FROM unnest(ARRAY[193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 231]) k;
INSERT INTO normalized_events(event_identity, namespace, logical_name_id, event_kind, source_family,
    manifest_version, chain_id, block_hash, block_number, transaction_hash, transaction_index,
    log_index, derivation_kind, canonicality_state, after_state)
SELECT 'unrelated:pointer:' || n, 'ens', NULL, 'ResolverChanged', 'ens_v1_registry_l1', 1,
       'ethereum-mainnet', 'block-' || (100 + n), 100 + n, 'tx-' || n, 0, 0,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('node', pg_temp.node(100000 + n % 2000), 'resolver', pg_temp.resolver(9))
FROM generate_series(1, 30000) n
UNION ALL
SELECT 'unrelated:grant:' || n, 'ens', NULL, 'RegistrationGranted', 'ens_v1_registrar_l1', 1,
       'ethereum-mainnet', 'block-' || (100 + n), 100 + n, 'tx-' || n, 0, 1,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('registrant', pg_temp.resolver(10))
FROM generate_series(1, 30000) n
UNION ALL
SELECT 'class:' || label, 'ens', NULL, 'ResolverChanged', 'ens_v1_registry_l1', 1,
       'ethereum-mainnet', 'block-' || block, block, 'tx-class-' || label, 0, 2,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object(field, pg_temp.node(k), 'resolver', pg_temp.resolver(resolver))
FROM (VALUES
    ('child-node', 'child_node', 193, 50, 193),
    ('namehash', 'namehash', 194, 51, 194),
    ('node', 'node', 195, 52, 195),
    ('cleared-before', 'node', 197, 40, 197),
    ('cleared', 'node', 197, 60, 0),
    ('eth', 'node', 231, 30, 231),
    ('above-before', 'node', 200, 70, 200),
    ('above', 'node', 200, 30180, 51200),
    ('orphaned-before', 'node', 201, 80, 201)
) class(label, field, k, block, resolver)
UNION ALL
SELECT 'class:child-node:observed', 'ens', NULL, 'ResolverChanged', 'ens_v1_registry_l1', 1,
       'ethereum-mainnet', 'block-56', 56, 'tx-class-observed', 0, 2,
       'ens_v1_unwrapped_authority', 'observed'::canonicality_state,
       jsonb_build_object('child_node', pg_temp.node(193), 'resolver', pg_temp.resolver(61193))
UNION ALL
SELECT 'class:namehash:other-family', 'ens', NULL, 'ResolverChanged', 'ens_v1_resolver_l1', 1,
       'ethereum-mainnet', 'block-57', 57, 'tx-class-other-family', 0, 2,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('namehash', pg_temp.node(194), 'resolver', pg_temp.resolver(61194))
UNION ALL
SELECT 'class:hot:' || i, 'ens', NULL, 'ResolverChanged', 'ens_v1_registry_l1', 1,
       'ethereum-mainnet', 'block-' || (200 + i), 200 + i, 'tx-hot-' || i, 0, 3,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('node', pg_temp.node(199), 'resolver', pg_temp.resolver(28672 + i))
FROM generate_series(0, 99) i
UNION ALL
SELECT 'class:orphaned', 'ens', NULL, 'ResolverChanged', 'ens_v1_registry_l1', 1,
       'ethereum-mainnet', 'orphan-90', 90, 'tx-orphaned', 0, 2,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('node', pg_temp.node(201), 'resolver', pg_temp.resolver(51201))
UNION ALL
SELECT 'class:name-only', 'ens', 'ens:' || pg_temp.node(203), 'ResolverChanged', 'ens_v1_registry_l1', 1,
       'ethereum-mainnet', 'block-55', 55, 'tx-name-only', 0, 2,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('resolver', pg_temp.resolver(203));
INSERT INTO normalized_events(event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, derivation_kind, canonicality_state, after_state)
VALUES ('class:unpositioned', 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1, 'ethereum-mainnet',
        'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
        jsonb_build_object('node', pg_temp.node(202), 'resolver', pg_temp.resolver(202)));
"#;

/// PROBE ONLY (ci/ branch, never merged): the smallest pointer-heavy step that flips the
/// previous custom plan, with each step's cost.
#[tokio::test]
async fn mirror_registry_probe_scale_bisect() -> Result<()> {
    use std::io::Write;
    with_fixture("mirror_registry_scale_bisect", async |connection| {
        let published = BTreeMap::from([(MAINNET.to_owned(), BOUND)]);
        let mut done = 0;
        for target in [150_000, 200_000, 250_000, 300_000, 360_000] {
            let started = std::time::Instant::now();
            raw_sql(&format!(
                "INSERT INTO normalized_events(event_identity, namespace, event_kind, source_family,
                    manifest_version, chain_id, block_hash, block_number, transaction_hash,
                    transaction_index, log_index, derivation_kind, canonicality_state, after_state)
                 SELECT 'bisect:pointer:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
                        'ethereum-mainnet', 'block-' || (100 + n % 30000), 100 + n % 30000,
                        'tx-bisect-' || n, 1, n % 1000, 'ens_v1_unwrapped_authority',
                        'canonical'::canonicality_state,
                        jsonb_build_object('node', '0x' || lpad(to_hex(200000 + n % 2000), 64, '0'),
                                           'resolver', '0x' || lpad(to_hex(9), 40, '0'))
                 FROM generate_series({}, {}) n",
                done + 1,
                target
            ))
            .execute(&mut *connection)
            .await?;
            let inserted = started.elapsed();
            Statistics::Off.apply(connection).await?;
            let analyzed = started.elapsed();
            for (label, k) in [("old pointer", CHILD_NODE), ("hot node", HOT_NODE)] {
                let walk = walk(&[(k, &[])]);
                let (sql, arguments) = render(&walk, &published)?;
                let previous = previous_sql(&sql)?;
                for _ in 0..3 {
                    let plan = explain(connection, &previous, arguments.clone(), false).await?;
                    let _ = writeln!(
                        std::io::stderr(),
                        "bisect | extra {target} | insert {inserted:?} | analyze {:?} | {label} | exec {} | {}",
                        analyzed - inserted,
                        plan[0]["Execution Time"],
                        json!(registry_reads(&plan[0]["Plan"]))
                    );
                    Statistics::Off.apply(connection).await?;
                }
            }
            done = target;
        }
        Ok(())
    })
    .await
}
