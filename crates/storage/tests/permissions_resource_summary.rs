use anyhow::{Context, Result, ensure};
use bigname_storage::{
    PermissionCoverageExhaustiveness, PermissionCoverageStatus,
    PermissionCoverageUnsupportedReason, ResourcePermissionCoverage,
    load_permissions_current_resource_summaries, load_permissions_current_resource_summary,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, raw_sql};
use uuid::Uuid;

const CHAIN: &str = "permission-summary-test";
const BLOCK_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const RESOURCE_ID: &str = "11111111-2222-3333-4444-555555555555";

const BASELINE: &[&str] = &[
    include_str!("../../../schema-v2/baseline/01_chain.sql"),
    include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
    include_str!("../../../schema-v2/baseline/03_identity.sql"),
    include_str!("../../../schema-v2/baseline/04_manifests.sql"),
    include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
    include_str!("../../../schema-v2/baseline/06_projections.sql"),
    include_str!("../../../schema-v2/baseline/07_labels.sql"),
    include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
    include_str!("../../../schema-v2/baseline/09_divergence.sql"),
    include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
    include_str!("../../../schema-v2/baseline/12_project_generation_failures.sql"),
    include_str!("../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
    include_str!("../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
];

#[tokio::test]
async fn supported_permission_summary_with_reason_fails_closed_in_single_and_batch_reads()
-> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("permissions_resource_summary").pool_max_connections(1),
    )
    .await?;
    let pool = database.pool().clone();

    let observed = exercise_malformed_summary(&pool).await;
    drop(pool);
    database.cleanup().await?;
    let (single, batch) = observed?;

    assert_fails_closed(&single);
    assert_fails_closed(&batch);
    Ok(())
}

async fn exercise_malformed_summary(
    pool: &PgPool,
) -> Result<(ResourcePermissionCoverage, ResourcePermissionCoverage)> {
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;

    let constraints: Vec<String> = sqlx::query_scalar(
        "SELECT conname
         FROM pg_constraint
         WHERE conrelid = 'bigname_phase.permissions_current_resource_summary'::regclass
           AND contype = 'c'
           AND pg_get_constraintdef(oid) LIKE '%support_status%'
           AND pg_get_constraintdef(oid) LIKE '%unsupported_reason%'",
    )
    .fetch_all(pool)
    .await?;
    ensure!(
        constraints.len() == 1,
        "expected one permission-summary status/reason CHECK constraint, found {constraints:?}"
    );
    raw_sql(&format!(
        "ALTER TABLE bigname_phase.permissions_current_resource_summary DROP CONSTRAINT {}",
        quote_identifier(&constraints[0])
    ))
    .execute(pool)
    .await?;

    let resource_id = Uuid::parse_str(RESOURCE_ID)?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, 1, '2026-09-01T00:00:00Z', 'canonical')",
    )
    .bind(CHAIN)
    .bind(BLOCK_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(resource_id)
    .bind(CHAIN)
    .bind(BLOCK_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.permissions_current_resource_summary (
             resource_id, support_status, unsupported_reason, provenance,
             chain_positions, canonicality_summary, manifest_version
         ) VALUES (
             $1, 'supported', 'operator_approval_surfaces_not_ingested',
             jsonb_build_object('chain_id', $2::text),
             jsonb_build_object('target_block_hash', $3::text),
             '{\"state\":\"canonical_lineage\"}'::jsonb, 1
         )",
    )
    .bind(resource_id)
    .bind(CHAIN)
    .bind(BLOCK_HASH)
    .execute(pool)
    .await?;

    let single = load_permissions_current_resource_summary(pool, resource_id)
        .await?
        .context("single summary read omitted the canonical fixture")?;
    let batch = load_permissions_current_resource_summaries(pool, &[resource_id])
        .await?
        .remove(&resource_id)
        .context("batch summary read omitted the canonical fixture")?;
    Ok((single.coverage, batch.coverage))
}

fn assert_fails_closed(coverage: &ResourcePermissionCoverage) {
    assert_eq!(coverage.status(), PermissionCoverageStatus::Partial);
    assert_eq!(
        coverage.exhaustiveness(),
        PermissionCoverageExhaustiveness::BestEffort
    );
    assert_eq!(
        coverage.unsupported_reason(),
        Some(PermissionCoverageUnsupportedReason::OperatorApprovalSurfacesNotIngested)
    );
}

fn quote_identifier(identifier: &str) -> String {
    format!(r#""{}""#, identifier.replace('"', r#""""#))
}
