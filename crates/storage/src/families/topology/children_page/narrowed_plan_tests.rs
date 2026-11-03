//! The children relation narrowed to some children over the parents that can list them: the
//! statement the Project writer runs for a chunk of a block's touched children
//! (`topology::listing_statement` with children), across the parent-cardinality axes.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, raw_sql};

use super::performance_tests::{CHAIN, CHILDREN, PARENT, install};
use crate::families::topology::{ListingBlock, listing_rows};

/// The children of one statement: the writer's chunk (`CHUNK`, derived/children.rs).
const CHUNK: i64 = 1_000;
/// The other parents of the skewed case, one child each.
const SKEWED_OTHERS: i64 = 99;
/// The parents of the many-parents case: one per child, and a former parent for 200 of them.
const MANY: i64 = 1_200;

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
/// child `20_000 + k` for k up to 200 before it moved (an earlier position).
async fn seed(conn: &mut PgConnection) -> Result<()> {
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
               FROM generate_series(1, {MANY} - {CHUNK}) n) edges;"
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
    let database = TestDatabase::create(
        TestDatabaseConfig::new("children_narrowed_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        seed(&mut conn).await?;
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
            for mode in ["force_custom_plan", "force_generic_plan"] {
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
                check(&plan, parents.len(), &format!("{case}, {mode}"))?;
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
