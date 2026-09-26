//! The ENSv2 release-names operator on the rebuild-performance corpus, under `EXPLAIN ANALYZE`:
//! it runs in every incremental batch, so it must read `surface_bindings` by resource, not scan
//! it, both when the batch holds no release without a name and when it holds many.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::raw_sql;

const CHAIN: &str = "ethereum-sepolia";
const NAMES: i64 = 20_000;
const SEED: &str = include_str!("../../tests/rebuild_performance/seed.sql");
const BASELINE: &[&str] = &[
    include_str!("../../../../schema-v2/baseline/01_chain.sql"),
    include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
    include_str!("../../../../schema-v2/baseline/03_identity.sql"),
    include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
    include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
    include_str!("../../../../schema-v2/baseline/06_projections.sql"),
    include_str!("../../../../schema-v2/baseline/07_labels.sql"),
    include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
    include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
    include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
];

fn scans(node: &Value, relation: &str, found: &mut Vec<String>) {
    if node["Relation Name"].as_str() == Some(relation) {
        found.push(node["Node Type"].as_str().unwrap_or_default().to_owned());
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        scans(child, relation, found);
    }
}

#[tokio::test]
async fn release_names_read_bindings_by_resource_in_a_one_block_batch() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("scope_v2_release_names")).await?;
    let mut setup = database.pool().begin().await?;
    raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *setup)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut *setup).await?;
    }
    raw_sql(
        &SEED
            .replace("__NAMES__", &NAMES.to_string())
            .replace("__CHAIN__", CHAIN),
    )
    .execute(&mut *setup)
    .await?;
    // One release without a name at block 300 on every tenth ENSv2 binding's resource.
    raw_sql(&format!(
        "INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
             event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state)
         SELECT 'plan:nameless-release:' || binding.resource_id, 'ens', NULL, binding.resource_id,
                'RegistrationReleased', 'ens_v2_registry_l1', 1, '{CHAIN}', 300,
                '0x' || lpad(to_hex(300), 64, '0'), 'ens_v2_registry_resource_surface',
                'canonical',
                '{{\"source_event\":\"RegistryPathExpired\",\"status\":\"released\"}}'::jsonb
         FROM (
             SELECT resource_id, row_number() OVER (ORDER BY resource_id) AS n
             FROM surface_bindings WHERE authority_arm = 'ens_v2'
         ) binding
         WHERE binding.n % 10 = 0;
         ANALYZE normalized_events; ANALYZE surface_bindings; ANALYZE chain_lineage"
    ))
    .execute(&mut *setup)
    .await?;
    setup.commit().await?;

    let statement = super::V2_RELEASE_NAMES.replace("$1", "300");
    let mut report = Vec::new();
    for (label, from_block) in [("quiet", 299), ("releases", 300)] {
        let mut tx = database.pool().begin().await?;
        raw_sql("SET LOCAL search_path TO bigname_phase, public")
            .execute(&mut *tx)
            .await?;
        super::super::create_scope_tables(&mut tx).await?;
        super::super::stage_changed_events(&mut tx, CHAIN, from_block, from_block).await?;
        let releases: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_changed_events
             WHERE logical_name_id IS NULL AND event_kind = 'RegistrationReleased'",
        )
        .fetch_one(&mut *tx)
        .await?;
        let plan: Value =
            sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {statement}"))
                .fetch_one(&mut *tx)
                .await?;
        let names: i64 = sqlx::query_scalar("SELECT count(*) FROM project_scope_names")
            .fetch_one(&mut *tx)
            .await?;
        let mut found = Vec::new();
        scans(&plan[0]["Plan"], "surface_bindings", &mut found);
        ensure!(
            !found.iter().any(|node| node == "Seq Scan"),
            "{label}: surface_bindings scanned: {plan}"
        );
        let time = plan[0]["Execution Time"].as_f64().unwrap_or_default();
        report.push(format!(
            "{label}: {releases} releases without a name, {names} names scoped, {time:.3} ms"
        ));
        if label == "releases" {
            ensure!(
                releases > 0 && names >= releases,
                "{label}: {releases} and {names}"
            );
        }
        tx.rollback().await?;
    }
    eprintln!("V2_RELEASE_NAMES_PLAN {NAMES} names: {}", report.join("; "));
    database.cleanup().await?;
    Ok(())
}
