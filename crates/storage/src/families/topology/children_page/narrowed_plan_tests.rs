//! The children relation narrowed to some children over the parents that can list them: the
//! statement the Project writer runs for a chunk of a block's touched children
//! (`topology::listing_statement` with children), across the parent-cardinality axes.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, raw_sql};

use super::performance_tests::{CHAIN, CHILDREN, PARENT, install_with};
use crate::families::topology::{ListingBlock, listing_rows};

/// The children of one statement: the writer's chunk (`CHUNK`, derived/children.rs).
const CHUNK: i64 = 1_000;
/// The other parents of the skewed case, one child each.
const SKEWED_OTHERS: i64 = 99;
/// The parents of the many-parents case: one per child, and a former parent for 200 of them.
const MANY: i64 = 1_200;
/// On the fixture without statistics the children `1..=SURFACED` have a surface of their own,
/// read by `wanted_child`. The fixture with statistics keeps none, the shape on which the
/// namespace and node probe once took `name_surfaces_project_parent_key_idx`.
const SURFACED: i64 = 500;
/// Every plan cache mode: the custom plan, the generic plan, and PostgreSQL's own choice.
const PLAN_MODES: [&str; 3] = ["force_custom_plan", "force_generic_plan", "auto"];

/// The child node `n` (`0x`, 64 hex digits), as `install` and `seed` write it.
fn node(n: i64) -> String {
    format!("0x{n:064x}")
}

/// The executed scans of `plan` on `relation`: those the executor reached.
fn executed<'a>(plan: &'a Value, relation: &str, scans: &mut Vec<&'a Value>) {
    if plan["Relation Name"] == relation && plan["Actual Loops"].as_i64().unwrap_or(0) > 0 {
        scans.push(plan);
    }
    for child in plan["Plans"].as_array().into_iter().flatten() {
        executed(child, relation, scans);
    }
}

/// The index condition a scan reads by, if any.
fn condition(scan: &Value) -> Option<&str> {
    match scan["Node Type"].as_str() {
        Some("Index Scan" | "Index Only Scan") => scan["Index Cond"].as_str(),
        Some("Bitmap Heap Scan") => scan["Recheck Cond"].as_str(),
        _ => None,
    }
}

/// The skewed and many-parents cases' parents, surfaces and edges, written after `install`'s
/// statistics, so the planner counts their parents as about one row. Skewed: 99 parents `s<k>`
/// with child `10_000 + k` each, beside `install`'s parent and its 2,000 children. Many: parents
/// `m<k>` with child `20_000 + k` for k up to 1,000, and parents `m<1_000 + k>` that listed
/// child `20_000 + k` for k up to 200 before it moved (an earlier position). The children
/// `1..=surfaced` also get a surface, with the id every surface has (`<namespace>:<namehash>`).
async fn seed(conn: &mut PgConnection, surfaced: i64) -> Result<()> {
    raw_sql(&format!(
        "INSERT INTO name_surfaces(logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         SELECT 'ens:' || id, 'ens', id || '.eth', ARRAY[id, 'eth'], '\\x00', id,
             ARRAY[id, 'eth'], 'fixture', 'active', '{CHAIN}', 'block', 100, 'canonical'
         FROM (SELECT 's' || n AS id FROM generate_series(1, {SKEWED_OTHERS}) n
               UNION ALL SELECT 'm' || n FROM generate_series(1, {MANY}) n) parents;
         INSERT INTO project_child_edge_candidate(chain_id, namespace, parent_node,
             child_node, authority_arm, block_number, transaction_index, log_index,
             event_identity, owner, labelhash, source_family)
         SELECT '{CHAIN}', 'ens', parent, '0x' || lpad(to_hex(child), 64, '0'), 'ens_v1', 100,
             position, child, parent || ':' || child, '0x1111111111111111111111111111111111111111',
             '0x' || lpad(to_hex(child), 64, '0'), 'ens_v1_registry_l1'
         FROM (SELECT 's' || n AS parent, 10000 + n AS child, 2 AS position
               FROM generate_series(1, {SKEWED_OTHERS}) n
               UNION ALL SELECT 'm' || n, 20000 + n, 2 FROM generate_series(1, {CHUNK}) n
               UNION ALL SELECT 'm' || ({CHUNK} + n), 20000 + n, 1
               FROM generate_series(1, {MANY} - {CHUNK}) n) edges;
         INSERT INTO name_surfaces(logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         SELECT 'ens:' || node, 'ens', 'c' || n || '.eth', ARRAY['c' || n, 'eth'], '\\x00', node,
             ARRAY[node, 'eth'], 'fixture', 'active', '{CHAIN}', 'block', 100, 'canonical'
         FROM (SELECT n, '0x' || lpad(to_hex(n), 64, '0') AS node
               FROM generate_series(1, {surfaced}) n) children;"
    ))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

// The dense touched-children rows (TYR-455), one fixture per parent-cardinality axis, each under
// the custom and the generic plan: one parent (`install`'s, a chunk of its 2,000 children); a
// skewed parent holding 901 of the chunk beside 99 parents with one child each; and 1,200
// parents above the chunk, one per child plus 200 former parents. The parents are written after
// the planner statistics, so the planner counts them as about one row, as it does a block's own
// parents. Each statement returns the chunk's 1,000 rows and reads every parent's surface and
// name state by its key and every wanted child's edges and surface by its node, never by the
// chain, a block range or the namespace alone, and each parent's name state once at most.
#[tokio::test]
async fn touched_children_read_each_child_by_its_node_across_parent_counts() -> Result<()> {
    narrowed_plans("children_narrowed_plan", true).await
}

/// The same cells on tables with no planner statistics, as a redo's family step finds them: the
/// child surfaces must still be read by their id, never by a scan of the namespace.
#[tokio::test]
async fn touched_children_read_each_child_by_its_key_without_statistics() -> Result<()> {
    narrowed_plans("children_narrowed_plan_nostats", false).await
}

async fn narrowed_plans(prefix: &str, statistics: bool) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install_with(&mut conn, statistics).await?;
        let surfaced = if statistics { 0 } else { SURFACED };
        seed(&mut conn, surfaced).await?;
        let skewed_kept = CHUNK - SKEWED_OTHERS;
        ensure!(
            CHILDREN > skewed_kept,
            "the skewed parent holds more than it lists here"
        );
        let cases: [(&str, Vec<String>, Vec<i64>); 3] = [
            ("one", vec![PARENT.to_owned()], (1..=CHUNK).collect()),
            (
                "skewed",
                std::iter::once(PARENT.to_owned())
                    .chain((1..=SKEWED_OTHERS).map(|n| format!("ens:s{n}")))
                    .collect(),
                (1..=skewed_kept)
                    .chain((1..=SKEWED_OTHERS).map(|n| 10_000 + n))
                    .collect(),
            ),
            (
                "many",
                (1..=MANY).map(|n| format!("ens:m{n}")).collect(),
                (1..=CHUNK).map(|n| 20_000 + n).collect(),
            ),
        ];
        let block = ListingBlock {
            chain_id: CHAIN,
            number: 100,
            timestamp_seconds: 100,
        };
        for (statement, (case, parents, children)) in cases.iter().enumerate() {
            let nodes: Vec<String> = children.iter().map(|&n| node(n)).collect();
            let query = listing_rows("", block, parents, Some(nodes.as_slice()));
            raw_sql(&format!(
                "PREPARE n{statement} (text[], text, bigint, bigint, bigint, text[]) AS {}",
                query.sql()
            ))
            .execute(&mut *conn)
            .await?;
            let found = children.iter().filter(|&&n| n <= surfaced).count();
            for mode in PLAN_MODES {
                raw_sql(&format!("SET plan_cache_mode = {mode}"))
                    .execute(&mut *conn)
                    .await?;
                let plan: Value = sqlx::query_scalar(&format!(
                    "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE n{statement} \
                     ('{{{}}}', '{CHAIN}', 100, 100, 100, '{{{}}}')",
                    parents.join(","),
                    nodes.join(",")
                ))
                .fetch_one(&mut *conn)
                .await?;
                let cell = format!("{case}, {mode}, statistics {statistics}");
                check(&plan, parents.len(), &cell)?;
                check_child_probe(&plan, nodes.len(), found, &cell)?;
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The checks of one cell: the chunk's rows, and every executed read of an edge, a surface and
/// a name state by its key, the name states at most once per parent.
fn check(plan: &Value, parents: usize, cell: &str) -> Result<()> {
    let root = &plan[0]["Plan"];
    ensure!(
        root["Actual Rows"] == CHUNK,
        "{cell}: {} rows: {plan}",
        root["Actual Rows"]
    );
    let mut edges = Vec::new();
    executed(root, "project_child_edge_candidate", &mut edges);
    ensure!(
        !edges.is_empty()
            && edges
                .iter()
                .all(|scan| condition(scan).is_some_and(|text| text.contains("child_node"))),
        "{cell}: the wanted children's edges are not read by their node: {plan}"
    );
    let mut surfaces = Vec::new();
    executed(root, "name_surfaces", &mut surfaces);
    ensure!(
        !surfaces.is_empty()
            && surfaces.iter().all(|scan| {
                condition(scan).is_some_and(|text| {
                    text.contains("logical_name_id") || text.contains("namehash")
                })
            }),
        "{cell}: a surface is not read by its id or node: {plan}"
    );
    let mut states = Vec::new();
    executed(root, "project_name_state", &mut states);
    ensure!(
        states.iter().all(|scan| {
            condition(scan).is_some_and(|text| text.contains("logical_name_id"))
                && scan["Actual Loops"].as_u64().unwrap_or(0) <= parents as u64
        }),
        "{cell}: the parents' name states are not probed by key once each: {plan}"
    );
    Ok(())
}

/// The rows a scan read: those it returned and those its filter removed, over every loop.
fn rows_read(scan: &Value) -> f64 {
    let loops = scan["Actual Loops"].as_f64().unwrap_or(0.0);
    (scan["Actual Rows"].as_f64().unwrap_or(0.0)
        + scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
        * loops
}

/// The checks of `wanted_child`'s probe: every executed surface read keyed on a wanted child's
/// node (not the parents' probe, which is keyed on the wanted parent ids) goes
/// through `name_surfaces_pkey`, returns at most one row per loop, and reads no more rows than
/// there are wanted children, and together they find each surfaced child once.
fn check_child_probe(plan: &Value, wanted: usize, surfaced: usize, cell: &str) -> Result<()> {
    let root = &plan[0]["Plan"];
    let mut surfaces = Vec::new();
    executed(root, "name_surfaces", &mut surfaces);
    let probes: Vec<&Value> = surfaces
        .into_iter()
        .filter(|scan| {
            [condition(scan), scan["Filter"].as_str()]
                .into_iter()
                .flatten()
                .any(|text| text.contains("wanted") && text.contains(".node"))
        })
        .collect();
    ensure!(
        !probes.is_empty(),
        "{cell}: no surface read is keyed on a wanted child: {plan}"
    );
    let mut found = 0.0;
    for scan in &probes {
        ensure!(
            scan["Index Name"] == "name_surfaces_pkey",
            "{cell}: a wanted child's surface is read by {} {}: {plan}",
            scan["Node Type"],
            scan["Index Name"]
        );
        ensure!(
            scan["Actual Rows"].as_f64().unwrap_or(0.0) <= 1.0,
            "{cell}: a wanted child's probe returns {} rows a loop: {plan}",
            scan["Actual Rows"]
        );
        let read = rows_read(scan);
        ensure!(
            read <= wanted as f64,
            "{cell}: the wanted children's probe read {read} rows for {wanted} children: {plan}"
        );
        found += scan["Actual Rows"].as_f64().unwrap_or(0.0)
            * scan["Actual Loops"].as_f64().unwrap_or(0.0);
    }
    ensure!(
        found == surfaced as f64,
        "{cell}: the probe found {found} surfaces for {surfaced} surfaced children: {plan}"
    );
    Ok(())
}
