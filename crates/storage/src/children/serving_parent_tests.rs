//! The serving-parent lookup over the family subregistry pointers, at a pointer population
//! larger than the Sepolia ENSv2 deployment's.
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;

use super::SERVING_PARENT;

const REGISTRY: &str = "0x00000000000000000000000000000000000000aa";

/// 50,000 parent pointers on one chain; names 30, 20 and 10 point at the registry, 30 first.
const FIXTURE: &str = r#"
    INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    VALUES ('ethereum-sepolia', 'block-1', 1, to_timestamp(1), 'canonical');
    INSERT INTO project_parent_subregistry
        (chain_id, logical_name_id, block_number, transaction_index, log_index, event_identity,
         subregistry_address)
    SELECT 'ethereum-sepolia', 'ens:node-' || n,
           CASE n WHEN 30 THEN 5 WHEN 20 THEN 6 WHEN 10 THEN 7 ELSE n END, 0, 0, 'pointer-' || n,
           CASE WHEN n IN (10, 20, 30) THEN '0x00000000000000000000000000000000000000aa'
                ELSE '0x' || lpad(to_hex(n + 4096), 40, '0') END
    FROM generate_series(1, 50000) n;
    INSERT INTO name_surfaces
        (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes,
         normalizer_version, visibility_state, chain_id, block_hash, block_number,
         canonicality_state)
    SELECT 'ens:node-' || n, 'ens', 'name' || n || '.eth', ARRAY['name' || n, 'eth'], '\x00',
           'node-' || n, ARRAY['label-' || n, 'label-eth'], 'test', 'active',
           'ethereum-sepolia', 'block-1', 1, 'canonical'
    FROM generate_series(1, 50000) n;
    ANALYZE project_parent_subregistry;
    ANALYZE name_surfaces;
"#;

fn pointer_rows_visited(node: &Value) -> f64 {
    let own = if node["Relation Name"] == "project_parent_subregistry" {
        (node["Actual Rows"].as_f64().unwrap_or(0.0)
            + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
            * node["Actual Loops"].as_f64().unwrap_or(1.0)
    } else {
        0.0
    };
    own + node["Plans"]
        .as_array()
        .map(|children| children.iter().map(pointer_rows_visited).sum())
        .unwrap_or(0.0)
}

#[tokio::test]
async fn serving_parent_is_the_earliest_current_pointer_and_reads_the_chain_pointers() -> Result<()>
{
    let database = TestDatabase::create(
        TestDatabaseConfig::new("serving_parent_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
            .execute(&mut *connection)
            .await?;
        for baseline in [
            include_str!("../../schema/baseline/01_chain.sql"),
            include_str!("../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../schema/baseline/03_identity.sql"),
            include_str!("../../schema/baseline/04_manifests.sql"),
            include_str!("../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../schema/baseline/06_projections.sql"),
            FIXTURE,
        ] {
            sqlx::raw_sql(baseline).execute(&mut *connection).await?;
        }
        let parent: Option<String> = sqlx::query_scalar(SERVING_PARENT)
            .bind("ethereum-sepolia")
            .bind(REGISTRY)
            .fetch_optional(&mut *connection)
            .await?;
        assert_eq!(parent.as_deref(), Some("ens:node-30"));
        let plan: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {SERVING_PARENT}"
        ))
        .bind("ethereum-sepolia")
        .bind(REGISTRY)
        .fetch_one(&mut *connection)
        .await?;
        let visited = pointer_rows_visited(&plan[0]["Plan"]);
        // No reverse index yet: the lookup reads the chain's pointer rows, not more.
        assert!(visited <= 50000.0, "visited {visited} pointer rows: {plan}");
        eprintln!(
            "serving parent lookup: pointer_rows_visited={visited}; execution_ms={}; plan={plan}",
            plan[0]["Execution Time"]
        );
        anyhow::Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
