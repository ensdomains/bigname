use std::path::Path;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use phase_runner::{database::RunnerDatabase, schema::initialize_schema_v2};

#[tokio::test]
async fn bootstrap_coexists_with_legacy_public_schema_and_reaches_manifest_sync() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_supported_bootstrap")
            .pool_max_connections(4)
            .parse_context("failed to parse phase-runner bootstrap test database URL")
            .admin_connect_context("failed to connect phase-runner bootstrap admin pool")
            .pool_connect_context("failed to connect phase-runner bootstrap pool"),
    )
    .await?;
    bigname_storage::MIGRATOR.run(database.pool()).await?;
    let runner =
        RunnerDatabase::connect_with_options(database.pool().connect_options().as_ref().clone(), 4)
            .await?;
    initialize_schema_v2(runner.pool()).await?;
    let manifests_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = bigname_manifests::load_repository(&manifests_root)?;
    let summary = bigname_manifests::sync_schema_v2_repository(runner.pool(), &repository).await?;
    assert!(summary.manifest_count > 0);

    let (legacy_table_exists, phase_table_exists): (bool, bool) = sqlx::query_as(
        "SELECT to_regclass('public.manifest_versions') IS NOT NULL, \
                to_regclass('bigname_phase.chain_phase_state') IS NOT NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(legacy_table_exists);
    assert!(phase_table_exists);
    let active_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(runner.pool())
        .await?;
    assert_eq!(active_schema, "bigname_phase");

    runner.pool().close().await;
    database.cleanup().await
}

#[tokio::test]
async fn bootstrap_rejects_structural_drift_in_an_existing_phase_schema() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("phase_runner_bootstrap_structural_drift")
            .pool_max_connections(4)
            .parse_context("failed to parse phase-runner bootstrap test database URL")
            .admin_connect_context("failed to connect phase-runner bootstrap admin pool")
            .pool_connect_context("failed to connect phase-runner bootstrap pool"),
    )
    .await?;
    initialize_schema_v2(database.pool()).await?;
    sqlx::query(
        "ALTER TABLE bigname_phase.chain_phase_state \
         DROP CONSTRAINT chain_phase_state_verification_phase_check",
    )
    .execute(database.pool())
    .await?;

    let error = initialize_schema_v2(database.pool())
        .await
        .expect_err("a nonempty phase schema must require a reviewed upgrade or rebuild");
    assert!(
        error
            .to_string()
            .contains("requires an empty bigname_phase schema")
    );
    let constraint_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_constraint constraint_row
        JOIN pg_class relation ON relation.oid = constraint_row.conrelid
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'bigname_phase'
          AND relation.relname = 'chain_phase_state'
          AND constraint_row.conname = 'chain_phase_state_verification_phase_check'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        constraint_count, 0,
        "rejection must not repair or rewrite drift"
    );

    database.cleanup().await
}
