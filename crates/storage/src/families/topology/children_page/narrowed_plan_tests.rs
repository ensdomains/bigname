//! The children relation narrowed to some children over many parents: the statement the Project
//! writer runs for a block's touched children under every parent that can list them
//! (`topology::listing_statement` with children).
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::raw_sql;

use super::performance_tests::{CHAIN, CHILDREN, install};

const PARENTS: i64 = 500;

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

// The dense touched-children row (TYR-455): a block that touches two children under each of
// 500 parents, written after the planner statistics were taken, so the planner counts the
// parents as about one row. The statement reads every parent's surface and name state by its
// key and every wanted child's edges and surface by its node, never by the chain, a block range
// or the namespace alone, and returns the 1,000 rows.
#[tokio::test]
async fn touched_children_under_many_parents_read_each_child_by_its_node() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("children_narrowed_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        // After the statistics: the parents' surfaces and edges, two children each.
        raw_sql(&format!(
            "INSERT INTO name_surfaces(logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state)
             SELECT 'ens:p' || n, 'ens', 'p' || n || '.eth', ARRAY['p' || n, 'eth'], '\\x00',
                 'p' || n, ARRAY['p' || n, 'eth'], 'fixture', 'active', '{CHAIN}', 'block', 100,
                 'canonical'
             FROM generate_series(1, {PARENTS}) n;
             INSERT INTO project_child_edge_candidate(chain_id, namespace, parent_node,
                 child_node, authority_arm, block_number, transaction_index, log_index,
                 event_identity, owner, labelhash, source_family)
             SELECT '{CHAIN}', 'ens', 'p' || (n % {PARENTS} + 1),
                 '0x' || lpad(to_hex({CHILDREN} + n), 64, '0'), 'ens_v1', 100, 2, n,
                 'dense:' || n || ':0', '0x1111111111111111111111111111111111111111',
                 '0x' || lpad(to_hex({CHILDREN} + n), 64, '0'), 'ens_v1_registry_l1'
             FROM generate_series(1, 2 * {PARENTS}) n;"
        ))
        .execute(&mut *conn)
        .await?;
        let parents: Vec<String> = (1..=PARENTS).map(|n| format!("ens:p{n}")).collect();
        let children: Vec<String> = (1..=2 * PARENTS)
            .map(|n| format!("0x{:064x}", CHILDREN + n))
            .collect();
        let block = crate::families::topology::ListingBlock {
            chain_id: CHAIN,
            number: 100,
            timestamp_seconds: 100,
        };
        let (sql, arguments) = crate::families::topology::listing_statement(
            "",
            block,
            &parents,
            Some(children.as_slice()),
        )?;
        let plan: Value =
            sqlx::query_scalar_with(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}"), arguments)
                .fetch_one(&mut *conn)
                .await?;
        let root = &plan[0]["Plan"];
        ensure!(
            root["Actual Rows"] == 2 * PARENTS,
            "{} rows: {plan}",
            root["Actual Rows"]
        );
        let mut edges = Vec::new();
        executed(root, "project_child_edge_candidate", &mut edges);
        ensure!(
            !edges.is_empty()
                && edges
                    .iter()
                    .all(|scan| condition(scan).is_some_and(|text| text.contains("child_node"))),
            "the wanted children's edges are not read by their node: {plan}"
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
            "a surface is not read by its id or node: {plan}"
        );
        // The migration gate probes each parent's name state by its key, once per parent.
        let mut states = Vec::new();
        executed(root, "project_name_state", &mut states);
        ensure!(
            states.iter().all(|scan| {
                condition(scan).is_some_and(|text| text.contains("logical_name_id"))
                    && scan["Actual Loops"].as_i64().unwrap_or(0) <= PARENTS
            }),
            "the parents' name states are not probed by key once each: {plan}"
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
